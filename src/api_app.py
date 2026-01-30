from __future__ import annotations

import json
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query

from db import connect


USDC_DECIMALS = 6
EXCLUDED_ADDRESSES = {
    # Polymarket Exchange contracts (system addresses)
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0x0000000000000000000000000000000000000000",
}


def _to_usdc(x: int | None) -> float | None:
    if x is None:
        return None
    return float(x) / (10**USDC_DECIMALS)


def create_app(db_path: str) -> FastAPI:
    app = FastAPI(title="PolyReputation API", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"ok": True}

    @app.get("/leaderboard")
    def leaderboard(
        sort: str = Query("roi", pattern="^(roi|profit)$"),
        limit: int = Query(50, ge=1, le=200),
    ) -> dict[str, Any]:
        order_by = "roi DESC" if sort == "roi" else "total_profit DESC"
        where_parts = ["address NOT IN ('0xc5d563a36ae78145c45a50134d48a1215220f80a','0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e','0x0000000000000000000000000000000000000000')"]
        if sort == "roi":
            where_parts.append("roi IS NOT NULL")
        where = "WHERE " + " AND ".join(where_parts)

        conn = connect(db_path)
        try:
            rows = conn.execute(
                f"""
                SELECT
                  address, total_cost, total_profit, roi,
                  markets_traded, win_rate, trades_count, max_trade_usdc
                FROM user_stats
                {where}
                ORDER BY {order_by}
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        finally:
            conn.close()

        items = []
        for r in rows:
            items.append(
                {
                    "address": r["address"],
                    "total_cost_raw": r["total_cost"],
                    "total_profit_raw": r["total_profit"],
                    "total_cost_usdc": _to_usdc(r["total_cost"]),
                    "total_profit_usdc": _to_usdc(r["total_profit"]),
                    "roi": r["roi"],
                    "markets_traded": r["markets_traded"],
                    "win_rate": r["win_rate"],
                    "trades_count": r["trades_count"],
                    "max_trade_usdc": _to_usdc(r["max_trade_usdc"]),
                }
            )

        return {"sort": sort, "limit": limit, "items": items}

    @app.get("/profile/{address}")
    def profile(address: str) -> dict[str, Any]:
        addr = address.strip().lower()
        if not addr.startswith("0x") or len(addr) < 6:
            raise HTTPException(status_code=400, detail="Invalid address")

        conn = connect(db_path)
        try:
            stats = conn.execute("SELECT * FROM user_stats WHERE address = ?", (addr,)).fetchone()
            tags = conn.execute(
                "SELECT tag FROM user_tags WHERE address = ? ORDER BY tag ASC", (addr,)
            ).fetchall()

            recent_trades = conn.execute(
                """
                SELECT
                  t.tx_hash, t.log_index, t.block_number, t.contract_address,
                  t.maker, t.taker, t.side,
                  t.token_id, tm.market_id, tm.outcome_label,
                  m.slug, m.question,
                  t.collateral_amount, t.token_amount, t.price,
                  t.decoded_json
                FROM trades t
                LEFT JOIN token_map tm ON tm.token_id = t.token_id
                LEFT JOIN markets m ON m.id = tm.market_id
                WHERE lower(t.maker) = ? OR lower(t.taker) = ?
                ORDER BY t.block_number DESC, t.log_index DESC
                LIMIT 10
                """,
                (addr, addr),
            ).fetchall()

            market_pnl = conn.execute(
                """
                SELECT
                  ump.market_id, m.slug, m.question, m.resolution_outcome,
                  ump.cost, ump.trading_revenue, ump.settlement_payout,
                  ump.profit, ump.roi, ump.win
                FROM user_market_pnl ump
                JOIN markets m ON m.id = ump.market_id
                WHERE ump.address = ?
                ORDER BY ump.profit DESC
                LIMIT 20
                """,
                (addr,),
            ).fetchall()
        finally:
            conn.close()

        out_stats = None
        if stats is not None:
            out_stats = {
                "address": stats["address"],
                "total_cost_raw": stats["total_cost"],
                "total_profit_raw": stats["total_profit"],
                "total_cost_usdc": _to_usdc(stats["total_cost"]),
                "total_profit_usdc": _to_usdc(stats["total_profit"]),
                "roi": stats["roi"],
                "markets_traded": stats["markets_traded"],
                "win_rate": stats["win_rate"],
                "trades_count": stats["trades_count"],
                "max_trade_usdc": _to_usdc(stats["max_trade_usdc"]),
                "updated_at": stats["updated_at"],
            }

        out_trades = []
        for t in recent_trades:
            try:
                decoded = json.loads(t["decoded_json"])
            except Exception:
                decoded = {"raw": t["decoded_json"]}
            out_trades.append(
                {
                    "tx_hash": t["tx_hash"],
                    "log_index": t["log_index"],
                    "block_number": t["block_number"],
                    "contract_address": t["contract_address"],
                    "maker": t["maker"],
                    "taker": t["taker"],
                    "side": t["side"],
                    "token_id": t["token_id"],
                    "market_id": t["market_id"],
                    "slug": t["slug"],
                    "question": t["question"],
                    "outcome_label": t["outcome_label"],
                    "collateral_usdc": _to_usdc(t["collateral_amount"]),
                    "token_amount_raw": t["token_amount"],
                    "price": t["price"],
                    "decoded": decoded,
                }
            )

        out_pnl = []
        for p in market_pnl:
            out_pnl.append(
                {
                    "market_id": p["market_id"],
                    "slug": p["slug"],
                    "question": p["question"],
                    "resolution_outcome": p["resolution_outcome"],
                    "cost_usdc": _to_usdc(p["cost"]),
                    "trading_revenue_usdc": _to_usdc(p["trading_revenue"]),
                    "settlement_payout_usdc": _to_usdc(p["settlement_payout"]),
                    "profit_usdc": _to_usdc(p["profit"]),
                    "roi": p["roi"],
                    "win": bool(p["win"]),
                }
            )

        return {
            "address": addr,
            "stats": out_stats,
            "tags": [r["tag"] for r in tags],
            "market_pnl_top": out_pnl,
            "recent_trades": out_trades,
        }

    return app


def run_api(db_path: str, host: str = "127.0.0.1", port: int = 8000) -> None:
    app = create_app(db_path)
    uvicorn.run(app, host=host, port=port, log_level="info")
