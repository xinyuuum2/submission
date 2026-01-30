from __future__ import annotations

import json
import os

import pandas as pd
import streamlit as st

from db import connect


USDC_DECIMALS = 6
EXCLUDED_ADDRESSES = {
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0x0000000000000000000000000000000000000000",
}


def _to_usdc(x: int | None) -> float | None:
    if x is None:
        return None
    return float(x) / (10**USDC_DECIMALS)


def _load_db_path() -> str:
    return os.environ.get("DB_PATH", "./polyreputation.sqlite").strip()


def _fetch_leaderboard(conn, sort: str, limit: int = 50) -> pd.DataFrame:
    order_by = "roi DESC" if sort == "roi" else "total_profit DESC"
    where_parts = ["address NOT IN ('0xc5d563a36ae78145c45a50134d48a1215220f80a','0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e','0x0000000000000000000000000000000000000000')"]
    if sort == "roi":
        where_parts.append("roi IS NOT NULL")
    where = "WHERE " + " AND ".join(where_parts)
    rows = conn.execute(
        f"""
        SELECT
          address,
          total_cost,
          total_profit,
          roi,
          win_rate,
          markets_traded,
          trades_count,
          max_trade_usdc
        FROM user_stats
        {where}
        ORDER BY {order_by}
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    data = []
    for r in rows:
        data.append(
            {
                "address": r["address"],
                "total_profit_usdc": _to_usdc(r["total_profit"]),
                "roi": r["roi"],
                "win_rate": r["win_rate"],
                "markets_traded": r["markets_traded"],
                "trades_count": r["trades_count"],
                "max_trade_usdc": _to_usdc(r["max_trade_usdc"]),
            }
        )
    return pd.DataFrame(data)


def _fetch_profile(conn, address: str):
    addr = address.strip().lower()
    stats = conn.execute("SELECT * FROM user_stats WHERE address = ?", (addr,)).fetchone()
    tags = conn.execute("SELECT tag FROM user_tags WHERE address = ? ORDER BY tag", (addr,)).fetchall()
    pnl = conn.execute(
        """
        SELECT
          ump.market_id, m.slug, m.question, m.resolution_outcome,
          ump.cost, ump.trading_revenue, ump.settlement_payout, ump.profit, ump.roi, ump.win
        FROM user_market_pnl ump
        JOIN markets m ON m.id = ump.market_id
        WHERE ump.address = ?
        ORDER BY ump.profit DESC
        LIMIT 20
        """,
        (addr,),
    ).fetchall()
    trades = conn.execute(
        """
        SELECT
          t.tx_hash, t.log_index, t.block_number,
          t.maker, t.taker, t.side,
          t.token_id, tm.market_id, tm.outcome_label, m.slug, m.question,
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
    return stats, [t["tag"] for t in tags], pnl, trades


def main() -> None:
    st.set_page_config(page_title="PolyReputation", layout="wide")
    st.title("PolyReputation — Polymarket 信誉与社交看板")

    db_path = _load_db_path()
    conn = connect(db_path)

    st.caption(f"DB: `{db_path}`")

    col1, col2 = st.columns([1, 1])
    with col1:
        sort = st.selectbox("Leaderboard sort", options=["roi", "profit"], index=0)
    with col2:
        limit = st.slider("Leaderboard size", min_value=10, max_value=200, value=50, step=10)

    st.subheader("Leaderboard")
    df = _fetch_leaderboard(conn, sort=sort, limit=limit)
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Address Profile")
    address = st.text_input("Enter an address (0x...)")

    if address.strip():
        stats, tags, pnl_rows, trade_rows = _fetch_profile(conn, address)

        if stats is None:
            st.warning("No stats found for this address yet. Make sure you ran `sync-markets`, `backfill-trades`, then `compute`.")
        else:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Profit (USDC)", f"{_to_usdc(stats['total_profit']):,.2f}")
            c2.metric("ROI", f"{(stats['roi'] * 100):.2f}%" if stats["roi"] is not None else "n/a")
            c3.metric("Win rate", f"{(stats['win_rate'] * 100):.1f}%" if stats["win_rate"] is not None else "n/a")
            c4.metric("Markets traded", int(stats["markets_traded"]))

            st.write("**Tags**:", ", ".join(tags) if tags else "(none)")

            if pnl_rows:
                st.subheader("Top resolved markets (PnL)")
                pnl_df = pd.DataFrame(
                    [
                        {
                            "slug": r["slug"],
                            "resolution": r["resolution_outcome"],
                            "profit_usdc": _to_usdc(r["profit"]),
                            "roi": r["roi"],
                            "cost_usdc": _to_usdc(r["cost"]),
                        }
                        for r in pnl_rows
                    ]
                )
                st.dataframe(pnl_df, use_container_width=True, hide_index=True)

            st.subheader("Proof: tx_hash + decoded OrderFilled JSON (latest 10)")
            st.caption("这些数据直接来自 Polygon `eth_getLogs`，并按 `OrderFilled` ABI 解码。")

            for t in trade_rows:
                with st.expander(f"{t['tx_hash']}  (block {t['block_number']}, log {t['log_index']})"):
                    st.write(
                        {
                            "side": t["side"],
                            "token_id": t["token_id"],
                            "market_id": t["market_id"],
                            "outcome_label": t["outcome_label"],
                            "slug": t["slug"],
                            "collateral_usdc": _to_usdc(t["collateral_amount"]),
                            "token_amount_raw": t["token_amount"],
                            "price": t["price"],
                        }
                    )
                    try:
                        st.json(json.loads(t["decoded_json"]))
                    except Exception:
                        st.code(t["decoded_json"])

    conn.close()


if __name__ == "__main__":
    main()
