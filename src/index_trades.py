from __future__ import annotations

import json
import math
import re
import time
from typing import Any, Iterable

from web3 import Web3
from web3._utils.events import get_event_data

from db import db_conn, init_db


ORDER_FILLED_EVENT_ABI: dict[str, Any] = {
    "anonymous": False,
    "inputs": [
        {"indexed": True, "internalType": "bytes32", "name": "orderHash", "type": "bytes32"},
        {"indexed": True, "internalType": "address", "name": "maker", "type": "address"},
        {"indexed": True, "internalType": "address", "name": "taker", "type": "address"},
        {"indexed": False, "internalType": "uint256", "name": "makerAssetId", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "takerAssetId", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "makerAmountFilled", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "takerAmountFilled", "type": "uint256"},
        {"indexed": False, "internalType": "uint256", "name": "fee", "type": "uint256"},
    ],
    "name": "OrderFilled",
    "type": "event",
}


def _event_topic(w3: Web3) -> str:
    sig = "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
    # Web3 JSON-RPC expects 0x-prefixed topic hex strings.
    return Web3.to_hex(w3.keccak(text=sig))


def _hex(value: Any) -> str | None:
    if value is None:
        return None
    try:
        s = value.hex()
        return s if s.startswith("0x") else ("0x" + s)
    except Exception:
        return str(value)


def _json_safe_log(log: Any) -> dict[str, Any]:
    # Web3 log values include HexBytes, which are not JSON serializable.
    data = log.get("data")
    if data is not None and not isinstance(data, str):
        data = _hex(data)
    return {
        "address": (log.get("address") or "").lower(),
        "blockNumber": int(log.get("blockNumber")),
        "transactionHash": _hex(log.get("transactionHash")),
        "transactionIndex": int(log.get("transactionIndex")) if log.get("transactionIndex") is not None else None,
        "blockHash": _hex(log.get("blockHash")),
        "logIndex": int(log.get("logIndex")),
        "data": data,
        "topics": [_hex(t) for t in (log.get("topics") or [])],
    }


def _infer_trade_fields(
    maker_asset_id: int,
    taker_asset_id: int,
    maker_amount: int,
    taker_amount: int,
    fee: int,
) -> tuple[str, str, int, int, float | None]:
    """
    Returns: (side, token_id, collateral_amount, token_amount, price)
    - side is from the maker order's perspective (BUY/SELL/UNKNOWN)
    - token_amount is the outcome token amount transferred buyer<-seller (net of fee if BUY)
    """
    if maker_asset_id == 0 and taker_asset_id != 0:
        side = "BUY"
        token_id = str(taker_asset_id)
        collateral_amount = maker_amount
        token_amount = max(taker_amount - fee, 0)
    elif taker_asset_id == 0 and maker_asset_id != 0:
        side = "SELL"
        token_id = str(maker_asset_id)
        collateral_amount = taker_amount
        token_amount = maker_amount
    else:
        side = "UNKNOWN"
        token_id = str(taker_asset_id if taker_asset_id != 0 else maker_asset_id)
        collateral_amount = taker_amount if taker_asset_id == 0 else maker_amount
        token_amount = maker_amount if maker_asset_id != 0 else taker_amount

    price = None
    if token_amount > 0:
        price = float(collateral_amount) / float(token_amount)
        if math.isfinite(price) is False:
            price = None
    return side, token_id, int(collateral_amount), int(token_amount), price


def _decode_order_filled(w3: Web3, log: dict[str, Any]) -> dict[str, Any]:
    decoded = get_event_data(w3.codec, ORDER_FILLED_EVENT_ABI, log)
    args = decoded["args"]
    return {
        "orderHash": _hex(args.get("orderHash")),
        "maker": str(args.get("maker")).lower(),
        "taker": str(args.get("taker")).lower(),
        "makerAssetId": int(args.get("makerAssetId")),
        "takerAssetId": int(args.get("takerAssetId")),
        "makerAmountFilled": int(args.get("makerAmountFilled")),
        "takerAmountFilled": int(args.get("takerAmountFilled")),
        "fee": int(args.get("fee")),
    }


def _iter_logs(
    w3: Web3,
    address: str,
    start_block: int,
    end_block: int,
    chunk_size: int,
    topic0: str,
) -> Iterable[tuple[int, int, list[dict[str, Any]]]]:
    addr = Web3.to_checksum_address(address)
    # Some public Polygon RPC providers enforce a strict max block range per `eth_getLogs`.
    # Make the fetch adaptive: when the provider complains, reduce the range and retry.
    from_block = int(start_block)
    max_chunk = max(1, int(chunk_size))
    cur_chunk = max_chunk
    # If a single response is extremely large, reduce future chunk sizes to avoid
    # huge payloads that can appear to "freeze" a laptop.
    max_logs_hint = 10_000

    while from_block <= end_block:
        to_block = min(from_block + cur_chunk - 1, end_block)
        params = {
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": addr,
            "topics": [topic0],
        }

        attempt = 0
        while True:
            try:
                logs = w3.eth.get_logs(params)
                break
            except Exception as e:
                msg = str(e).lower()

                # Provider-specific limits (typical messages include:
                # - "Block range is too large"
                # - "query returned more than X results"
                # - "response size exceeded"
                too_wide = any(
                    s in msg
                    for s in (
                        "block range is too large",
                        "range too large",
                        "too many results",
                        "response size exceeded",
                        "limit exceeded",
                        "query returned more than",
                    )
                )
                if too_wide:
                    if cur_chunk <= 1:
                        raise
                    cur_chunk = max(1, cur_chunk // 2)
                    to_block = min(from_block + cur_chunk - 1, end_block)
                    params["toBlock"] = to_block
                    attempt = 0
                    continue

                if "rate limit" in msg or "too many requests" in msg:
                    m = re.search(r"retry in (\\d+)s", msg)
                    wait_s = int(m.group(1)) if m else min(30, 2**attempt)
                    time.sleep(wait_s)
                    attempt += 1
                    if attempt <= 8:
                        continue
                raise

        # Normalize to plain dicts for JSON safety downstream
        out_logs = [dict(log) for log in logs]
        yield from_block, to_block, out_logs

        # Advance window; try ramping back up (bounded) for efficiency.
        from_block = to_block + 1
        if len(logs) > max_logs_hint and max_chunk > 1:
            max_chunk = max(1, max_chunk // 2)
        if cur_chunk < max_chunk:
            cur_chunk = min(max_chunk, max(1, cur_chunk * 2))


def backfill_trades(
    db_path: str,
    polygon_rpc_url: str,
    exchange_addresses: list[str],
    start_block: int,
    end_block: int,
    chunk_size: int = 50_000,
    *,
    stop_after: int | None = None,
    sleep_ms: int = 0,
    verbose: bool = True,
) -> None:
    # Ensure schema/migrations (including trades.timestamp) exist for older DBs.
    init_db(db_path)
    w3 = Web3(Web3.HTTPProvider(polygon_rpc_url, request_kwargs={"timeout": 60}))
    topic0 = _event_topic(w3)

    insert_sql = """
        INSERT INTO trades (
          tx_hash, log_index, block_number, timestamp, contract_address,
          order_hash, maker, taker,
          maker_asset_id, taker_asset_id, token_id,
          maker_amount, taker_amount, fee,
          collateral_amount, token_amount, side, price,
          decoded_json, raw_log_json
        ) VALUES (
          ?, ?, ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?, ?,
          ?, ?
        )
        ON CONFLICT(tx_hash, log_index) DO UPDATE SET
          timestamp = COALESCE(trades.timestamp, excluded.timestamp)
        """

    total_inserted = 0
    block_ts_cache: dict[int, int | None] = {}
    with db_conn(db_path) as conn:
        for addr in exchange_addresses:
            for from_block, to_block, logs in _iter_logs(
                w3=w3,
                address=addr,
                start_block=start_block,
                end_block=end_block,
                chunk_size=chunk_size,
                topic0=topic0,
            ):
                if verbose:
                    print(
                        f"[backfill] {addr.lower()} blocks {from_block}-{to_block} logs={len(logs)}",
                        flush=True,
                    )

                # Fetch block timestamps for this chunk (cached).
                # Polygon RPC call: eth_getBlockByNumber
                unique_blocks = sorted({int(l.get("blockNumber")) for l in logs if l and l.get("blockNumber") is not None})
                for bn in unique_blocks:
                    if bn in block_ts_cache:
                        continue
                    try:
                        blk = w3.eth.get_block(bn)
                        ts_raw = getattr(blk, "timestamp", None) or blk.get("timestamp")
                        ts = int(ts_raw) if ts_raw is not None else None
                        if ts is not None and ts <= 0:
                            ts = None
                    except Exception:
                        ts = None
                    block_ts_cache[bn] = ts

                rows: list[tuple[Any, ...]] = []
                for log in logs:
                    raw = _json_safe_log(log)
                    decoded = _decode_order_filled(w3, log)

                    maker_asset_id = int(decoded["makerAssetId"])
                    taker_asset_id = int(decoded["takerAssetId"])
                    maker_amount = int(decoded["makerAmountFilled"])
                    taker_amount = int(decoded["takerAmountFilled"])
                    fee = int(decoded["fee"])

                    side, token_id, collateral_amount, token_amount, price = _infer_trade_fields(
                        maker_asset_id, taker_asset_id, maker_amount, taker_amount, fee
                    )

                    decoded_json = {
                        "event": "OrderFilled",
                        **decoded,
                        "inferred": {
                            "side": side,
                            "tokenId": token_id,
                            "collateralAmount": collateral_amount,
                            "tokenAmount": token_amount,
                            "price": price,
                            "assumptions": {
                                "usdc_decimals": 6,
                                "note": "Token amounts are in CTF ERC1155 units; for Polymarket these typically align with 6-decimal USDC collateral.",
                            },
                        },
                        "proof": {
                            "tx_hash": raw["transactionHash"],
                            "log_index": raw["logIndex"],
                            "block_number": raw["blockNumber"],
                            "contract": raw["address"],
                            "topics": raw["topics"],
                            "data": raw["data"],
                        },
                    }

                    tx_hash = raw["transactionHash"]
                    log_index = int(raw["logIndex"])
                    if not tx_hash:
                        continue
                    block_number = int(raw["blockNumber"])
                    ts_val = block_ts_cache.get(block_number)
                    timestamp = int(ts_val) if ts_val is not None else None

                    rows.append(
                        (
                            tx_hash,
                            log_index,
                            block_number,
                            timestamp,
                            raw["address"],
                            decoded.get("orderHash"),
                            decoded["maker"],
                            decoded["taker"],
                            str(maker_asset_id),
                            str(taker_asset_id),
                            token_id,
                            maker_amount,
                            taker_amount,
                            fee,
                            collateral_amount,
                            token_amount,
                            side,
                            price,
                            json.dumps(decoded_json, ensure_ascii=False),
                            json.dumps(raw, ensure_ascii=False),
                        )
                    )

                before = conn.total_changes
                if rows:
                    conn.executemany(insert_sql, rows)
                inserted = conn.total_changes - before
                total_inserted += int(inserted)
                conn.commit()

                if verbose:
                    print(f"[backfill] inserted={inserted} total_inserted={total_inserted}", flush=True)

                if stop_after is not None and stop_after > 0 and total_inserted >= stop_after:
                    if verbose:
                        print(f"[backfill] stop-after reached: {stop_after}", flush=True)
                    return

                if sleep_ms > 0:
                    time.sleep(float(sleep_ms) / 1000.0)


def inspect_tx(
    *,
    polygon_rpc_url: str,
    tx_hash: str,
    exchange_addresses: list[str],
) -> dict[str, Any]:
    """
    Fetch a transaction receipt and decode OrderFilled logs (if any).
    Useful to locate the correct block range before running a large backfill.
    """
    w3 = Web3(Web3.HTTPProvider(polygon_rpc_url, request_kwargs={"timeout": 60}))
    topic0 = _event_topic(w3)
    receipt = w3.eth.get_transaction_receipt(tx_hash)

    receipt_logs = receipt.get("logs") or []
    addr_set = {a.lower() for a in (exchange_addresses or [])}

    decoded_events: list[dict[str, Any]] = []
    for log in receipt_logs:
        log_d = dict(log)
        log_addr = (log_d.get("address") or "").lower()
        topics = log_d.get("topics") or []
        first_topic = _hex(topics[0]) if topics else None

        if addr_set and log_addr not in addr_set:
            continue
        if first_topic != topic0:
            continue

        raw = _json_safe_log(log_d)
        decoded = _decode_order_filled(w3, log_d)

        maker_asset_id = int(decoded["makerAssetId"])
        taker_asset_id = int(decoded["takerAssetId"])
        maker_amount = int(decoded["makerAmountFilled"])
        taker_amount = int(decoded["takerAmountFilled"])
        fee = int(decoded["fee"])

        side, token_id, collateral_amount, token_amount, price = _infer_trade_fields(
            maker_asset_id, taker_asset_id, maker_amount, taker_amount, fee
        )

        decoded_events.append(
            {
                "event": "OrderFilled",
                **decoded,
                "inferred": {
                    "side": side,
                    "tokenId": token_id,
                    "collateralAmount": collateral_amount,
                    "tokenAmount": token_amount,
                    "price": price,
                    "assumptions": {"usdc_decimals": 6},
                },
                "proof": {
                    "tx_hash": raw["transactionHash"],
                    "log_index": raw["logIndex"],
                    "block_number": raw["blockNumber"],
                    "contract": raw["address"],
                    "topics": raw["topics"],
                    "data": raw["data"],
                },
            }
        )

    return {
        "tx_hash": tx_hash,
        "block_number": int(receipt.get("blockNumber") or 0),
        "status": int(receipt.get("status") or 0) if receipt.get("status") is not None else None,
        "logs_total": len(receipt_logs),
        "order_filled_matches": len(decoded_events),
        "matches": decoded_events,
    }
