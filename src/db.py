from __future__ import annotations

import sqlite3
from contextlib import contextmanager


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def db_conn(db_path: str):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _column_type(conn: sqlite3.Connection, table: str, column: str) -> str | None:
    if not _table_exists(conn, table):
        return None
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    for r in rows:
        if r["name"] == column:
            t = (r["type"] or "").strip()
            return t.upper() if t else None
    return None


def _needs_tokenid_text_migration(conn: sqlite3.Connection) -> bool:
    # Polymarket tokenIds are uint256 and may exceed SQLite INTEGER range.
    token_map_type = _column_type(conn, "token_map", "token_id")
    trades_type = _column_type(conn, "trades", "token_id")
    markets_type = _column_type(conn, "markets", "winning_token_id")

    return any(
        t is not None and t != "TEXT"
        for t in (token_map_type, trades_type, markets_type)
    )


def _migrate_tokenids_to_text(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("BEGIN;")
    try:
        # Derived tables can be safely rebuilt after migration
        if _table_exists(conn, "user_market_pnl"):
            conn.execute("DROP TABLE user_market_pnl;")
        if _table_exists(conn, "user_stats"):
            conn.execute("DROP TABLE user_stats;")
        if _table_exists(conn, "user_tags"):
            conn.execute("DROP TABLE user_tags;")

        if _table_exists(conn, "token_map"):
            conn.execute("ALTER TABLE token_map RENAME TO token_map_old;")
        if _table_exists(conn, "trades"):
            conn.execute("ALTER TABLE trades RENAME TO trades_old;")
        if _table_exists(conn, "markets"):
            conn.execute("ALTER TABLE markets RENAME TO markets_old;")

        # Recreate with TEXT token ids (see schema below)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS markets (
              id TEXT PRIMARY KEY,
              question TEXT,
              condition_id TEXT,
              slug TEXT,
              closed INTEGER,
              resolved INTEGER,
              resolution_outcome TEXT,
              winning_token_id TEXT,
              outcomes_json TEXT,
              outcome_prices_json TEXT,
              clob_token_ids_json TEXT,
              updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS token_map (
              token_id TEXT PRIMARY KEY,
              market_id TEXT NOT NULL,
              outcome_index INTEGER,
              outcome_label TEXT,
              FOREIGN KEY(market_id) REFERENCES markets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS trades (
              tx_hash TEXT NOT NULL,
              log_index INTEGER NOT NULL,
              block_number INTEGER NOT NULL,
              timestamp INTEGER,
              contract_address TEXT NOT NULL,

              order_hash TEXT,
              maker TEXT NOT NULL,
              taker TEXT NOT NULL,

              maker_asset_id TEXT NOT NULL,
              taker_asset_id TEXT NOT NULL,
              token_id TEXT NOT NULL,

              maker_amount INTEGER NOT NULL,
              taker_amount INTEGER NOT NULL,
              fee INTEGER NOT NULL,

              collateral_amount INTEGER NOT NULL,
              token_amount INTEGER NOT NULL,
              side TEXT NOT NULL,
              price REAL,

              decoded_json TEXT NOT NULL,
              raw_log_json TEXT NOT NULL,

              PRIMARY KEY (tx_hash, log_index)
            );
            """
        )

        # Copy data (best effort; tables may be empty)
        if _table_exists(conn, "markets_old"):
            conn.execute(
                """
                INSERT INTO markets (
                  id, question, condition_id, slug, closed, resolved,
                  resolution_outcome, winning_token_id,
                  outcomes_json, outcome_prices_json, clob_token_ids_json, updated_at
                )
                SELECT
                  id, question, condition_id, slug, closed, resolved,
                  resolution_outcome, CAST(winning_token_id AS TEXT),
                  outcomes_json, outcome_prices_json, clob_token_ids_json, updated_at
                FROM markets_old
                """
            )
            conn.execute("DROP TABLE markets_old;")

        if _table_exists(conn, "token_map_old"):
            conn.execute(
                """
                INSERT INTO token_map(token_id, market_id, outcome_index, outcome_label)
                SELECT CAST(token_id AS TEXT), market_id, outcome_index, outcome_label
                FROM token_map_old
                """
            )
            conn.execute("DROP TABLE token_map_old;")

        if _table_exists(conn, "trades_old"):
            cols = {r["name"] for r in conn.execute("PRAGMA table_info(trades_old)").fetchall()}
            has_ts = "timestamp" in cols
            if has_ts:
                conn.execute(
                    """
                    INSERT INTO trades (
                      tx_hash, log_index, block_number, timestamp, contract_address,
                      order_hash, maker, taker,
                      maker_asset_id, taker_asset_id, token_id,
                      maker_amount, taker_amount, fee,
                      collateral_amount, token_amount, side, price,
                      decoded_json, raw_log_json
                    )
                    SELECT
                      tx_hash, log_index, block_number, timestamp, contract_address,
                      order_hash, maker, taker,
                      CAST(maker_asset_id AS TEXT), CAST(taker_asset_id AS TEXT), CAST(token_id AS TEXT),
                      maker_amount, taker_amount, fee,
                      collateral_amount, token_amount, side, price,
                      decoded_json, raw_log_json
                    FROM trades_old
                    """
                )
            else:
                conn.execute(
                    """
                    INSERT INTO trades (
                      tx_hash, log_index, block_number, timestamp, contract_address,
                      order_hash, maker, taker,
                      maker_asset_id, taker_asset_id, token_id,
                      maker_amount, taker_amount, fee,
                      collateral_amount, token_amount, side, price,
                      decoded_json, raw_log_json
                    )
                    SELECT
                      tx_hash, log_index, block_number, NULL AS timestamp, contract_address,
                      order_hash, maker, taker,
                      CAST(maker_asset_id AS TEXT), CAST(taker_asset_id AS TEXT), CAST(token_id AS TEXT),
                      maker_amount, taker_amount, fee,
                      collateral_amount, token_amount, side, price,
                      decoded_json, raw_log_json
                    FROM trades_old
                    """
                )
            conn.execute("DROP TABLE trades_old;")

        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON;")


def _ensure_trades_timestamp(conn: sqlite3.Connection) -> None:
    """
    Backwards-compatible migration: older DBs may not have trades.timestamp.
    We store Unix epoch seconds (UTC) for time-window filtering & charts.
    """
    if _column_type(conn, "trades", "timestamp") is None:
        conn.execute("ALTER TABLE trades ADD COLUMN timestamp INTEGER;")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);")


def init_db(db_path: str) -> None:
    with db_conn(db_path) as conn:
        if _needs_tokenid_text_migration(conn):
            _migrate_tokenids_to_text(conn)

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS markets (
              id TEXT PRIMARY KEY,
              question TEXT,
              condition_id TEXT,
              slug TEXT,
              closed INTEGER,
              resolved INTEGER,
              resolution_outcome TEXT,
              winning_token_id TEXT,
              outcomes_json TEXT,
              outcome_prices_json TEXT,
              clob_token_ids_json TEXT,
              updated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_markets_resolved ON markets(resolved);

            CREATE TABLE IF NOT EXISTS token_map (
              token_id TEXT PRIMARY KEY,
              market_id TEXT NOT NULL,
              outcome_index INTEGER,
              outcome_label TEXT,
              FOREIGN KEY(market_id) REFERENCES markets(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_token_map_market ON token_map(market_id);

            CREATE TABLE IF NOT EXISTS trades (
              tx_hash TEXT NOT NULL,
              log_index INTEGER NOT NULL,
              block_number INTEGER NOT NULL,
              timestamp INTEGER,
              contract_address TEXT NOT NULL,

              order_hash TEXT,
              maker TEXT NOT NULL,
              taker TEXT NOT NULL,

              maker_asset_id TEXT NOT NULL,
              taker_asset_id TEXT NOT NULL,
              token_id TEXT NOT NULL,

              maker_amount INTEGER NOT NULL,
              taker_amount INTEGER NOT NULL,
              fee INTEGER NOT NULL,

              collateral_amount INTEGER NOT NULL,
              token_amount INTEGER NOT NULL,
              side TEXT NOT NULL,
              price REAL,

              decoded_json TEXT NOT NULL,
              raw_log_json TEXT NOT NULL,

              PRIMARY KEY (tx_hash, log_index)
            );

            CREATE INDEX IF NOT EXISTS idx_trades_block ON trades(block_number);
            CREATE INDEX IF NOT EXISTS idx_trades_maker ON trades(maker);
            CREATE INDEX IF NOT EXISTS idx_trades_taker ON trades(taker);
            CREATE INDEX IF NOT EXISTS idx_trades_token ON trades(token_id);

            CREATE TABLE IF NOT EXISTS user_market_pnl (
              address TEXT NOT NULL,
              market_id TEXT NOT NULL,

              cost INTEGER NOT NULL,
              trading_revenue INTEGER NOT NULL,
              settlement_payout INTEGER NOT NULL,
              profit INTEGER NOT NULL,
              roi REAL,
              win INTEGER NOT NULL,

              PRIMARY KEY(address, market_id),
              FOREIGN KEY(market_id) REFERENCES markets(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_user_market_pnl_profit ON user_market_pnl(profit);
            CREATE INDEX IF NOT EXISTS idx_user_market_pnl_roi ON user_market_pnl(roi);

            CREATE TABLE IF NOT EXISTS user_stats (
              address TEXT PRIMARY KEY,
              total_cost INTEGER NOT NULL,
              total_profit INTEGER NOT NULL,
              roi REAL,
              markets_traded INTEGER NOT NULL,
              win_rate REAL,
              trades_count INTEGER NOT NULL,
              max_trade_usdc INTEGER NOT NULL,
              updated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_user_stats_profit ON user_stats(total_profit);
            CREATE INDEX IF NOT EXISTS idx_user_stats_roi ON user_stats(roi);

            CREATE TABLE IF NOT EXISTS user_tags (
              address TEXT NOT NULL,
              tag TEXT NOT NULL,
              PRIMARY KEY(address, tag)
            );

            CREATE TABLE IF NOT EXISTS index_state (
              key TEXT PRIMARY KEY,
              value TEXT
            );

            -- Dating / social layer (local-only)
            CREATE TABLE IF NOT EXISTS dating_swipes (
              from_address TEXT NOT NULL,
              to_address TEXT NOT NULL,
              action TEXT NOT NULL, -- like / pass
              created_at TEXT NOT NULL,
              PRIMARY KEY(from_address, to_address)
            );

            CREATE INDEX IF NOT EXISTS idx_dating_swipes_from ON dating_swipes(from_address);
            CREATE INDEX IF NOT EXISTS idx_dating_swipes_to ON dating_swipes(to_address);
            CREATE INDEX IF NOT EXISTS idx_dating_swipes_action ON dating_swipes(action);

            CREATE TABLE IF NOT EXISTS dating_matches (
              user_a TEXT NOT NULL,
              user_b TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY(user_a, user_b)
            );

            CREATE INDEX IF NOT EXISTS idx_dating_matches_a ON dating_matches(user_a);
            CREATE INDEX IF NOT EXISTS idx_dating_matches_b ON dating_matches(user_b);

            CREATE TABLE IF NOT EXISTS dating_daily_picks (
              pick_date TEXT NOT NULL,          -- YYYY-MM-DD
              for_address TEXT NOT NULL,        -- viewer address
              rank INTEGER NOT NULL,            -- 1..N
              candidate_address TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY(pick_date, for_address, candidate_address)
            );

            CREATE INDEX IF NOT EXISTS idx_dating_daily_for ON dating_daily_picks(for_address, pick_date);

            -- Follow / copytrade (local-only)
            CREATE TABLE IF NOT EXISTS user_follows (
              follower_address TEXT NOT NULL,
              followee_address TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY(follower_address, followee_address)
            );

            CREATE INDEX IF NOT EXISTS idx_user_follows_follower ON user_follows(follower_address);
            CREATE INDEX IF NOT EXISTS idx_user_follows_followee ON user_follows(followee_address);
            """
        )

        # Ensure new columns exist on older DB files.
        _ensure_trades_timestamp(conn)
