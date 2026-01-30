from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

import requests

from db import db_conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_maybe_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        # Most Gamma fields are JSON-encoded arrays (string type in docs)
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except Exception:
            # fall back to CSV-ish parsing
            if s.startswith("[") and s.endswith("]"):
                s = s[1:-1]
            parts = [p.strip().strip('"').strip("'") for p in s.split(",") if p.strip()]
            return parts
    return [value]


def _safe_float(x: Any) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


def _infer_winner(
    outcomes: list[str], prices: list[float], token_ids: list[str]
) -> tuple[bool, str | None, str | None]:
    if not outcomes or not prices:
        return False, None, None
    if len(prices) != len(outcomes):
        # still try a best-effort alignment (min length)
        n = min(len(prices), len(outcomes))
        outcomes = outcomes[:n]
        prices = prices[:n]
        token_ids = token_ids[:n]

    best_idx = max(range(len(prices)), key=lambda i: prices[i])
    best_price = prices[best_idx]
    if best_price is None:
        return False, None, None

    # For resolved markets, winner outcome price is typically ~1.0
    if best_price >= 0.99:
        winning_token_id = token_ids[best_idx] if best_idx < len(token_ids) else None
        return True, outcomes[best_idx], winning_token_id

    return False, None, None


def _iter_markets(
    gamma_api_base: str,
    *,
    limit: int,
    offset: int,
    closed_only: bool,
    clob_token_ids: Sequence[str] | None = None,
) -> Iterable[dict[str, Any]]:
    url = gamma_api_base.rstrip("/") + "/markets"
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if closed_only:
        params["closed"] = "true"
    if clob_token_ids:
        # Gamma expects repeated query params: clob_token_ids[]=... (requests will encode lists)
        params["clob_token_ids"] = list(clob_token_ids)
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected Gamma response shape: {type(data)}")
    for item in data:
        if isinstance(item, dict):
            yield item


def _upsert_market(conn, m: dict[str, Any], now: str) -> None:
    market_id = str(m.get("id") or "").strip()
    if not market_id:
        return

    question = m.get("question")
    condition_id = m.get("conditionId")
    slug = m.get("slug")
    closed = bool(m.get("closed")) if m.get("closed") is not None else None

    clob_token_ids_raw = m.get("clobTokenIds")
    outcomes_raw = m.get("outcomes")
    outcome_prices_raw = m.get("outcomePrices")

    # Token IDs are uint256 and can exceed SQLite INTEGER range; store as TEXT.
    token_ids = [str(x) for x in _parse_maybe_list(clob_token_ids_raw) if x is not None]
    outcomes = [str(x) for x in _parse_maybe_list(outcomes_raw)]
    prices = [_safe_float(x) for x in _parse_maybe_list(outcome_prices_raw)]
    prices = [x for x in prices if x is not None]

    resolved, resolution_outcome, winning_token_id = _infer_winner(outcomes, prices, token_ids)
    resolved_int = 1 if (closed and resolved) else 0
    closed_int = 1 if closed else 0

    conn.execute(
        """
        INSERT INTO markets (
          id, question, condition_id, slug, closed, resolved,
          resolution_outcome, winning_token_id,
          outcomes_json, outcome_prices_json, clob_token_ids_json, updated_at
        ) VALUES (
          ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(id) DO UPDATE SET
          question=excluded.question,
          condition_id=excluded.condition_id,
          slug=excluded.slug,
          closed=excluded.closed,
          resolved=excluded.resolved,
          resolution_outcome=excluded.resolution_outcome,
          winning_token_id=excluded.winning_token_id,
          outcomes_json=excluded.outcomes_json,
          outcome_prices_json=excluded.outcome_prices_json,
          clob_token_ids_json=excluded.clob_token_ids_json,
          updated_at=excluded.updated_at
        """,
        (
            market_id,
            question,
            condition_id,
            slug,
            closed_int,
            resolved_int,
            resolution_outcome,
            winning_token_id,
            json.dumps(outcomes, ensure_ascii=False),
            json.dumps(prices, ensure_ascii=False),
            json.dumps(token_ids, ensure_ascii=False),
            now,
        ),
    )

    # Update token mapping (tokenId -> market/outcome)
    for idx, token_id in enumerate(token_ids):
        outcome_label = outcomes[idx] if idx < len(outcomes) else None
        conn.execute(
            """
            INSERT INTO token_map(token_id, market_id, outcome_index, outcome_label)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(token_id) DO UPDATE SET
              market_id=excluded.market_id,
              outcome_index=excluded.outcome_index,
              outcome_label=excluded.outcome_label
            """,
            (token_id, market_id, idx, outcome_label),
        )


def sync_markets(
    db_path: str,
    gamma_api_base: str,
    limit: int = 5000,
    offset: int = 0,
    closed_only: bool = False,
) -> None:
    now = _now_iso()
    markets = list(
        _iter_markets(gamma_api_base, limit=limit, offset=offset, closed_only=closed_only)
    )

    with db_conn(db_path) as conn:
        for m in markets:
            _upsert_market(conn, m, now=now)


def sync_markets_by_token_ids(
    db_path: str,
    gamma_api_base: str,
    token_ids: Sequence[str],
    *,
    closed_only: bool = False,
) -> None:
    """
    Fetch markets filtered by clob token ids (best for quickly mapping traded tokenIds).
    """
    now = _now_iso()
    markets = list(
        _iter_markets(
            gamma_api_base,
            limit=5000,
            offset=0,
            closed_only=closed_only,
            clob_token_ids=token_ids,
        )
    )

    with db_conn(db_path) as conn:
        for m in markets:
            _upsert_market(conn, m, now=now)
