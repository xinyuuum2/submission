from __future__ import annotations

import argparse
import sys

from config import load_settings
from db import connect, init_db


def _chunks(items: list[str], n: int) -> list[list[str]]:
    if n <= 0:
        n = 1
    return [items[i : i + n] for i in range(0, len(items), n)]


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="polyreputation")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db", help="Create SQLite schema")

    sm = sub.add_parser("sync-markets", help="Sync Gamma markets into DB")
    sm.add_argument("--limit", type=int, default=5000)
    sm.add_argument("--offset", type=int, default=0)
    sm.add_argument("--pages", type=int, default=1, help="Fetch N pages (offset += limit)")
    sm.add_argument("--closed-only", action="store_true", help="Only fetch closed markets")

    stm = sub.add_parser("sync-traded-markets", help="Sync Gamma markets for tokenIds seen in trades")
    stm.add_argument("--closed-only", action="store_true")
    stm.add_argument("--batch", type=int, default=10, help="How many tokenIds per Gamma request")
    stm.add_argument("--max-token-ids", type=int, default=2000, help="Limit distinct tokenIds to sync")

    bt = sub.add_parser("backfill-trades", help="Backfill Polygon OrderFilled logs")
    bt.add_argument("--start-block", type=int, required=False)
    bt.add_argument("--end-block", type=int, required=False)
    bt.add_argument(
        "--chunk",
        type=int,
        default=500,
        help="Max block chunk size for eth_getLogs (auto-shrinks on RPC limits)",
    )
    bt.add_argument(
        "--stop-after",
        type=int,
        default=0,
        help="Stop after inserting N trades (useful for >=100 proof quickly)",
    )
    bt.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Sleep between RPC calls (helps with rate limits)",
    )
    bt.add_argument("--quiet", action="store_true", help="Reduce progress output")

    itx = sub.add_parser("inspect-tx", help="Inspect a tx receipt and decode OrderFilled (find block range)")
    itx.add_argument("--tx-hash", required=True, help="Transaction hash (0x...)")

    sub.add_parser("compute", help="Compute realized PnL + tags + leaderboards")

    api = sub.add_parser("serve-api", help="Run FastAPI server")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    if args.cmd == "init-db":
        settings = load_settings(require_rpc=False)
        init_db(settings.db_path)
        return 0

    if args.cmd == "sync-markets":
        settings = load_settings(require_rpc=False)
        init_db(settings.db_path)
        from gamma import sync_markets  # local import (requests)

        for i in range(max(args.pages, 1)):
            sync_markets(
                db_path=settings.db_path,
                gamma_api_base=settings.gamma_api_base,
                limit=args.limit,
                offset=args.offset + (i * args.limit),
                closed_only=args.closed_only,
            )
        return 0

    if args.cmd == "sync-traded-markets":
        settings = load_settings(require_rpc=False)
        init_db(settings.db_path)
        from gamma import sync_markets_by_token_ids  # local import (requests)

        conn = connect(settings.db_path)
        try:
            rows = conn.execute(
                """
                -- Prioritize most recently traded tokenIds so UI proof/slug fills fast
                SELECT t.token_id
                FROM trades t
                LEFT JOIN token_map tm ON tm.token_id = t.token_id
                WHERE tm.token_id IS NULL
                GROUP BY t.token_id
                ORDER BY COALESCE(MAX(t.timestamp), 0) DESC, MAX(t.block_number) DESC
                LIMIT ?
                """,
                (args.max_token_ids,),
            ).fetchall()
        finally:
            conn.close()

        token_ids = [r["token_id"] for r in rows]
        for chunk in _chunks(token_ids, args.batch):
            sync_markets_by_token_ids(
                db_path=settings.db_path,
                gamma_api_base=settings.gamma_api_base,
                token_ids=chunk,
                closed_only=args.closed_only,
            )
        return 0

    if args.cmd == "backfill-trades":
        settings = load_settings(require_rpc=True)
        init_db(settings.db_path)
        from index_trades import backfill_trades  # local import (web3)

        start_block = args.start_block if args.start_block is not None else settings.start_block
        end_block = args.end_block if args.end_block is not None else settings.end_block
        if start_block is None or end_block is None:
            raise RuntimeError("Need --start-block and --end-block (or set START_BLOCK/END_BLOCK in .env)")
        backfill_trades(
            db_path=settings.db_path,
            polygon_rpc_url=settings.polygon_rpc_url,
            exchange_addresses=settings.ctf_exchange_addresses,
            start_block=start_block,
            end_block=end_block,
            chunk_size=args.chunk,
            stop_after=(args.stop_after if args.stop_after and args.stop_after > 0 else None),
            sleep_ms=int(args.sleep_ms or 0),
            verbose=(not bool(args.quiet)),
        )
        return 0

    if args.cmd == "inspect-tx":
        settings = load_settings(require_rpc=True)
        from index_trades import inspect_tx  # local import (web3)

        out = inspect_tx(
            polygon_rpc_url=settings.polygon_rpc_url,
            tx_hash=args.tx_hash,
            exchange_addresses=settings.ctf_exchange_addresses,
        )
        print(__import__("json").dumps(out, ensure_ascii=False, indent=2))
        return 0

    if args.cmd == "compute":
        settings = load_settings(require_rpc=False)
        init_db(settings.db_path)
        from compute import compute_all  # local import

        compute_all(settings.db_path)
        return 0

    if args.cmd == "serve-api":
        settings = load_settings(require_rpc=False)
        init_db(settings.db_path)
        from api_app import run_api  # local import (fastapi/uvicorn)

        run_api(db_path=settings.db_path, host=args.host, port=args.port)
        return 0

    raise RuntimeError(f"Unknown cmd: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
