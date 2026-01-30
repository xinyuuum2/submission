from __future__ import annotations

from datetime import datetime, timezone

from db import db_conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def compute_all(db_path: str) -> None:
    now = _now_iso()
    with db_conn(db_path) as conn:
        # Rebuild aggregates (simple MVP approach)
        conn.executescript(
            """
            DELETE FROM user_market_pnl;
            DELETE FROM user_stats;
            DELETE FROM user_tags;
            """
        )

        # Per-address, per-resolved-market realized PnL
        conn.executescript(
            """
            WITH resolved_trades AS (
              SELECT
                t.*,
                tm.market_id,
                m.winning_token_id
              FROM trades t
              JOIN token_map tm ON tm.token_id = t.token_id
              JOIN markets m ON m.id = tm.market_id
              WHERE m.resolved = 1 AND m.winning_token_id IS NOT NULL
            ),
            flows AS (
              -- maker leg
              SELECT
                lower(maker) AS address,
                market_id,
                token_id,
                CASE
                  WHEN side = 'BUY' THEN token_amount
                  WHEN side = 'SELL' THEN -token_amount
                  ELSE 0
                END AS token_delta,
                CASE
                  WHEN side = 'BUY' THEN -collateral_amount
                  WHEN side = 'SELL' THEN (collateral_amount - fee)
                  ELSE 0
                END AS collateral_delta
              FROM resolved_trades
              UNION ALL
              -- taker leg
              SELECT
                lower(taker) AS address,
                market_id,
                token_id,
                CASE
                  WHEN side = 'BUY' THEN -token_amount
                  WHEN side = 'SELL' THEN token_amount
                  ELSE 0
                END AS token_delta,
                CASE
                  WHEN side = 'BUY' THEN collateral_amount
                  WHEN side = 'SELL' THEN -collateral_amount
                  ELSE 0
                END AS collateral_delta
              FROM resolved_trades
            ),
            per_market AS (
              SELECT
                f.address,
                f.market_id,
                SUM(CASE WHEN f.collateral_delta < 0 THEN -f.collateral_delta ELSE 0 END) AS cost,
                SUM(CASE WHEN f.collateral_delta > 0 THEN f.collateral_delta ELSE 0 END) AS trading_revenue,
                SUM(CASE WHEN f.token_id = m.winning_token_id THEN f.token_delta ELSE 0 END) AS winning_token_net
              FROM flows f
              JOIN markets m ON m.id = f.market_id
              GROUP BY f.address, f.market_id
            ),
            pnl_raw AS (
              SELECT
                address,
                market_id,
                cost,
                trading_revenue,
                CASE WHEN winning_token_net > 0 THEN winning_token_net ELSE 0 END AS settlement_payout,
                (trading_revenue + (CASE WHEN winning_token_net > 0 THEN winning_token_net ELSE 0 END) - cost) AS profit,
                CASE
                  WHEN cost > 0 THEN 1.0 * (trading_revenue + (CASE WHEN winning_token_net > 0 THEN winning_token_net ELSE 0 END) - cost) / cost
                  ELSE NULL
                END AS roi,
                CASE
                  WHEN (trading_revenue + (CASE WHEN winning_token_net > 0 THEN winning_token_net ELSE 0 END) - cost) > 0 THEN 1
                  ELSE 0
                END AS win
              FROM per_market
            ),
            pnl AS (
              SELECT
                address, market_id, cost, trading_revenue, settlement_payout, profit, roi, win
              FROM pnl_raw
              WHERE cost > 0 OR trading_revenue > 0 OR settlement_payout > 0
            )
            INSERT INTO user_market_pnl(address, market_id, cost, trading_revenue, settlement_payout, profit, roi, win)
            SELECT address, market_id, cost, trading_revenue, settlement_payout, profit, roi, win
            FROM pnl;
            """
        )

        # User-level stats
        conn.execute(
            """
            WITH stats AS (
              SELECT
                address,
                SUM(cost) AS total_cost,
                SUM(profit) AS total_profit,
                CASE WHEN SUM(cost) > 0 THEN 1.0 * SUM(profit) / SUM(cost) ELSE NULL END AS roi,
                COUNT(*) AS markets_traded,
                CASE WHEN COUNT(*) > 0 THEN 1.0 * SUM(win) / COUNT(*) ELSE NULL END AS win_rate
              FROM user_market_pnl
              GROUP BY address
            ),
            trade_counts AS (
              SELECT address, COUNT(*) AS trades_count FROM (
                SELECT lower(maker) AS address FROM trades
                UNION ALL
                SELECT lower(taker) AS address FROM trades
              )
              GROUP BY address
            ),
            max_trade AS (
              SELECT address, MAX(notional) AS max_trade_usdc FROM (
                SELECT lower(maker) AS address, collateral_amount AS notional FROM trades
                UNION ALL
                SELECT lower(taker) AS address, collateral_amount AS notional FROM trades
              )
              GROUP BY address
            )
            INSERT INTO user_stats(address, total_cost, total_profit, roi, markets_traded, win_rate, trades_count, max_trade_usdc, updated_at)
            SELECT
              s.address,
              COALESCE(s.total_cost, 0),
              COALESCE(s.total_profit, 0),
              s.roi,
              COALESCE(s.markets_traded, 0),
              s.win_rate,
              COALESCE(tc.trades_count, 0),
              COALESCE(mt.max_trade_usdc, 0),
              ?
            FROM stats s
            LEFT JOIN trade_counts tc ON tc.address = s.address
            LEFT JOIN max_trade mt ON mt.address = s.address
            """,
            (now,),
        )

        # Tags
        # Diamond Hands: bought/held into settlement (approx: no trading revenue on that market)
        conn.executescript(
            """
            INSERT OR IGNORE INTO user_tags(address, tag)
            SELECT address, 'Diamond Hands'
            FROM user_market_pnl
            WHERE cost > 0 AND trading_revenue = 0 AND settlement_payout > 0
            GROUP BY address;

            INSERT OR IGNORE INTO user_tags(address, tag)
            SELECT address, 'Smart Money'
            FROM user_stats
            WHERE win_rate IS NOT NULL AND win_rate > 0.60 AND markets_traded > 10;

            INSERT OR IGNORE INTO user_tags(address, tag)
            SELECT address, 'Whale'
            FROM user_stats
            WHERE max_trade_usdc >= 1000000000; -- 1000 USDC with 6 decimals

            INSERT OR IGNORE INTO user_tags(address, tag)
            SELECT address, 'Contra'
            FROM user_stats
            WHERE roi IS NOT NULL AND roi < -0.50;
            """
        )
