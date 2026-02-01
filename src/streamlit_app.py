from __future__ import annotations

import hashlib
import html
import json
import os
import random
import time
from datetime import date, datetime, timezone

import pandas as pd
import streamlit as st

from db import connect, init_db


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


def _short_addr(addr: str, *, n: int = 6) -> str:
    a = (addr or "").strip()
    if len(a) <= (2 * n + 2):
        return a
    return a[: 2 + n] + "…" + a[-n:]


def _user_id(addr: str) -> str:
    """
    Local-only friendly identifier derived from address (NOT an official Polymarket user id).
    """
    a = (addr or "").strip().lower()
    if not a:
        return "user_??????"
    h = hashlib.sha256(a.encode("utf-8")).hexdigest()
    num = int(h[:8], 16) % 1_000_000
    return f"user_{num:06d}"


def _cred_alias(addr: str) -> str:
    """
    Deterministic "reputation alias" for demos (cyber/social vibe).
    """
    a = (addr or "").strip().lower()
    if not a:
        return "匿名信号源"
    titles = [
        "全能先知",
        "链上巨鳄",
        "胜率怪物",
        "稳健猎手",
        "消息面雷达",
        "赔率收割机",
        "回撤克星",
        "反脆弱玩家",
        "逆向狙击手",
        "流动性幽灵",
        "趋势捕手",
        "事件型交易员",
        "高胜率工匠",
        "赛博操盘手",
    ]
    h = hashlib.sha256(a.encode("utf-8")).hexdigest()
    return titles[int(h[:8], 16) % len(titles)]


def _blogger_intro(stats) -> str:
    """
    Social-note style short bio based on win_rate + profit.
    """
    if stats is None:
        return "该博主暂无足够链上统计，先收藏观察。"
    win = _safe_float(stats["win_rate"], 0.0) or 0.0
    profit_usdc = _to_usdc(_safe_int(stats["total_profit"], 0)) or 0.0
    trades = _safe_int(stats["trades_count"], 0)

    if win >= 0.90 and profit_usdc > 0:
        return f"该博主擅长在预测市场伏击，胜率高达 {win*100:.0f}%，属于「稳健型博主」。"
    if win >= 0.80 and profit_usdc > 0:
        return f"该博主胜率 {win*100:.0f}% 且长期正收益，属于「稳健输出型」。"
    if profit_usdc >= 5000:
        return "该博主带单收益突出，属于「高收益笔记号」。"
    if profit_usdc < 0 and trades >= 30:
        return "该博主更新很勤快，但收益波动大，属于「高频试错型」。"
    return "该博主风格偏克制，适合做你的 Alpha 备忘录。"


def _polygonscan_tx_url(tx_hash: str) -> str:
    return f"https://polygonscan.com/tx/{tx_hash}"


def _polygonscan_addr_url(addr: str) -> str:
    return f"https://polygonscan.com/address/{addr}"

def _polymarket_market_url(slug: str) -> str:
    s = (slug or "").strip()
    if not s or s == "n/a":
        return ""
    return f"https://polymarket.com/market/{s}"

def _safe_int(x, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _safe_float(x, default: float | None = None) -> float | None:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _esc(s: object) -> str:
    return html.escape("" if s is None else str(s))


def _row_get(row: object, key: str, default=None):
    """
    Safe getter for rows that might be dict-like or sqlite3.Row.
    - dict: uses .get
    - sqlite3.Row: uses row[key]
    """
    if row is None:
        return default
    try:
        getter = getattr(row, "get", None)
        if callable(getter):
            return getter(key, default)
    except Exception:
        pass
    try:
        keys = getattr(row, "keys", None)
        if callable(keys):
            if key in keys():
                return row[key]  # type: ignore[index]
            return default
    except Exception:
        pass
    try:
        return row[key]  # type: ignore[index]
    except Exception:
        return default


def _fmt_usdc(x: int | None) -> str:
    v = _to_usdc(x)
    if v is None:
        return "n/a"
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    return f"{v:,.2f}"


def _fmt_pct(x: float | None) -> str:
    if x is None:
        return "n/a"
    return f"{x * 100:.1f}%"


def _pfp_colors(seed: str) -> tuple[str, str]:
    h = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return f"#{h[:6]}", f"#{h[6:12]}"


def _pfp_html(address: str) -> str:
    a = (address or "").strip().lower()
    c1, c2 = _pfp_colors(a)
    initials = _short_addr(a, n=2).replace("0x", "").replace("…", "")
    return (
        f'<div class="pfp" style="background: linear-gradient(135deg, {c1}, {c2});">'
        f'<span class="pfp-text">{_esc(initials.upper())}</span>'
        f"</div>"
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _date_today() -> str:
    return date.today().isoformat()


def _norm_addr(addr: str) -> str:
    return (addr or "").strip().lower()


def _fmt_hms_from_ts(ts: int | None) -> str:
    try:
        if ts is None:
            return "--:--:--"
        ts_i = int(ts)
        if ts_i <= 0:
            return "--:--:--"
        return datetime.fromtimestamp(ts_i, tz=timezone.utc).strftime("%H:%M:%S")
    except Exception:
        return "--:--:--"


def _load_known_addresses(conn, limit: int = 50) -> list[str]:
    rows = conn.execute(
        """
        SELECT address
        FROM user_stats
        WHERE address NOT IN ('0xc5d563a36ae78145c45a50134d48a1215220f80a',
                              '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e',
                              '0x0000000000000000000000000000000000000000')
        ORDER BY total_profit DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [r["address"] for r in rows if r and r["address"]]


# --- Market / event tags (derived, heuristic) ---
_SECTOR_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("Crypto", ("bitcoin", "btc", "ethereum", "eth", "solana", "sol", "crypto", "binance", "coinbase")),
    ("Politics", ("election", "president", "trump", "biden", "democrat", "republican", "senate", "congress")),
    ("Macro", ("fed", "fomc", "cpi", "inflation", "rates", "interest rate", "gdp", "recession", "unemployment")),
    ("Sports", ("nba", "nfl", "mlb", "nhl", "ufc", "champions league", "premier league", "world cup")),
    ("Tech", ("openai", "nvidia", "apple", "tesla", "microsoft", "google", "meta", "ai", "chatgpt")),
    ("Geopolitics", ("ukraine", "russia", "israel", "gaza", "china", "taiwan", "iran")),
]

SECTOR_COLORS: dict[str, str] = {
    "Politics": "#ff4b4b",
    "Geopolitics": "#ff7a59",
    "Crypto": "#0083b8",
    "Tech": "#7d44ad",
    "Macro": "#f5a623",
    "Sports": "#2ecc71",
    "Other": "#9ca3af",
}

SECTOR_EMOJI: dict[str, str] = {
    "Politics": "🗳️",
    "Geopolitics": "🌍",
    "Crypto": "₿",
    "Tech": "🧠",
    "Macro": "📈",
    "Sports": "🏟️",
    "Other": "🧩",
}


def _sector_badge(sector: str) -> str:
    s_raw = (sector or "").strip()
    s = s_raw if s_raw else "Other"
    # Be forgiving about casing / stray formatting so we don't fall back to a generic emoji.
    candidates = [s, s.title(), s.upper(), s.lower()]
    emoji = None
    for k in candidates:
        if k in SECTOR_EMOJI:
            emoji = SECTOR_EMOJI[k]
            break
    if emoji is None:
        emoji = "🧩"
    label = s.title() if s.lower() in {"crypto", "politics", "macro", "tech", "sports", "geopolitics", "other"} else s
    return f"{emoji} {label}"


def _sector_for_market_text(text: str) -> str:
    t = (text or "").lower()
    for sector, keys in _SECTOR_RULES:
        if any(k in t for k in keys):
            return sector
    return "Other"


def _top_sectors_for_address(
    conn,
    address: str,
    *,
    recent_trades: int = 500,
    top_k: int = 3,
) -> list[str]:
    """
    Derive "market sectors" from the markets this address traded most recently.
    This is heuristic and depends on Gamma metadata being present in `token_map/markets`.
    """
    addr = _norm_addr(address)
    if not addr:
        return []

    rows = conn.execute(
        """
        WITH recent AS (
          SELECT token_id
          FROM trades
          WHERE maker = ? OR taker = ?
          ORDER BY block_number DESC, log_index DESC
          LIMIT ?
        )
        SELECT m.slug AS slug, m.question AS question, COUNT(*) AS n
        FROM recent r
        JOIN token_map tm ON tm.token_id = r.token_id
        JOIN markets m ON m.id = tm.market_id
        GROUP BY m.id
        ORDER BY n DESC
        LIMIT 60
        """,
        (addr, addr, int(recent_trades)),
    ).fetchall()

    counts: dict[str, int] = {}
    for r in rows:
        text = f"{r['slug'] or ''} {r['question'] or ''}"
        sector = _sector_for_market_text(text)
        counts[sector] = counts.get(sector, 0) + int(r["n"] or 0)

    ranked = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
    return [k for k, _ in ranked[: max(0, int(top_k))] if k]


def _vspace(px: int = 16) -> None:
    st.markdown(f"<div style='height:{int(px)}px'></div>", unsafe_allow_html=True)

def _truncate_label(s: str, n: int = 15) -> str:
    t = (s or "").strip()
    if len(t) <= n:
        return t
    return t[:n] + "..."


def _pie_chart(data: pd.DataFrame, *, label_col: str, value_col: str, title: str) -> None:
    """
    Render a pie chart using Vega-Lite (no extra deps).
    """
    if data is None or data.empty:
        st.caption("(no data)")
        return
    spec = {
        "title": title,
        "data": {"values": data.to_dict(orient="records")},
        "mark": {"type": "arc", "innerRadius": 30},
        "encoding": {
            "theta": {"field": value_col, "type": "quantitative"},
            "color": {
                "field": label_col,
                "type": "nominal",
                "legend": {"title": ""},
                "scale": {"domain": list(SECTOR_COLORS.keys()), "range": list(SECTOR_COLORS.values())},
            },
            "tooltip": [
                {"field": label_col, "type": "nominal"},
                {"field": value_col, "type": "quantitative"},
            ],
        },
        "view": {"stroke": None},
    }
    st.vega_lite_chart(spec, use_container_width=True)


def _barh_chart(data: pd.DataFrame, *, label_col: str, value_col: str, title: str) -> None:
    """
    Horizontal bar chart (Vega-Lite) to avoid label overlap.
    """
    if data is None or data.empty:
        st.caption("(no data)")
        return
    spec = {
        "title": title,
        "data": {"values": data.to_dict(orient="records")},
        "mark": {"type": "bar", "cornerRadiusEnd": 4},
        "encoding": {
            "y": {"field": label_col, "type": "nominal", "sort": "-x"},
            "x": {"field": value_col, "type": "quantitative"},
            "tooltip": [
                {"field": label_col, "type": "nominal"},
                {"field": value_col, "type": "quantitative"},
            ],
        },
        "view": {"stroke": None},
        "height": {"step": 22},
    }
    st.vega_lite_chart(spec, use_container_width=True)


def _latest_trade_ts(conn) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(timestamp), 0) AS mx FROM trades WHERE timestamp IS NOT NULL AND timestamp > 0"
    ).fetchone()
    return int(row["mx"] or 0) if row else 0


def _window_start_ts(window: str, *, anchor_ts: int) -> int | None:
    """
    Convert UI "Time" window into an epoch start timestamp (seconds).
    We anchor on the latest trade timestamp in DB (not wall-clock) so filters still
    work when the DB is not perfectly up-to-date.
    """
    w = (window or "").strip().lower()
    if w in {"all", "all time", "all-time"}:
        return None
    seconds: int | None = None
    if w in {"day", "past day", "24h"}:
        seconds = 24 * 60 * 60
    elif w in {"week", "past week", "7d"}:
        seconds = 7 * 24 * 60 * 60
    elif w in {"month", "past month", "30d"}:
        seconds = 30 * 24 * 60 * 60
    if seconds is None:
        return None
    return max(0, int(anchor_ts) - int(seconds))


def _recent_trade_counts(conn, *, start_ts: int) -> dict[str, int]:
    """
    Recent activity: counts trades per address in a time window (timestamp-based).
    """
    rows = conn.execute(
        """
        SELECT lower(maker) AS addr, COUNT(*) AS n
        FROM trades
        WHERE timestamp IS NOT NULL AND timestamp >= ?
        GROUP BY lower(maker)
        UNION ALL
        SELECT lower(taker) AS addr, COUNT(*) AS n
        FROM trades
        WHERE timestamp IS NOT NULL AND timestamp >= ?
        GROUP BY lower(taker)
        """,
        (int(start_ts), int(start_ts)),
    ).fetchall()
    out: dict[str, int] = {}
    for r in rows:
        if not r:
            continue
        a = (r["addr"] or "").strip().lower()
        if not a:
            continue
        out[a] = out.get(a, 0) + int(r["n"] or 0)
    return out


def _top_markets_in_window(conn, *, start_ts: int, limit: int = 12) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT tm.market_id AS market_id, m.slug AS slug, COUNT(*) AS n
        FROM trades t
        LEFT JOIN token_map tm ON tm.token_id = t.token_id
        LEFT JOIN markets m ON m.id = tm.market_id
        WHERE t.timestamp IS NOT NULL AND t.timestamp >= ?
        GROUP BY tm.market_id
        ORDER BY n DESC
        LIMIT ?
        """,
        (int(start_ts), int(limit)),
    ).fetchall()
    data = []
    for r in rows:
        if not r:
            continue
        data.append({"market_id": r["market_id"], "slug": r["slug"] or "n/a", "trades": int(r["n"] or 0)})
    return pd.DataFrame(data)


def _trading_volume_by_period(conn, *, start_ts: int, bucket_seconds: int) -> pd.DataFrame:
    """
    Non-cumulative trading volume: count(trades) per time bucket.
    Returns a DataFrame indexed by UTC datetime.
    """
    rows = conn.execute(
        """
        SELECT
          (CAST(timestamp / ? AS INTEGER) * ?) AS bucket_ts,
          COUNT(*) AS n
        FROM trades
        WHERE timestamp IS NOT NULL AND timestamp >= ?
        GROUP BY bucket_ts
        ORDER BY bucket_ts ASC
        """,
        (int(bucket_seconds), int(bucket_seconds), int(start_ts)),
    ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["trades"])
    df = pd.DataFrame([{"bucket_ts": int(r["bucket_ts"] or 0), "trades": int(r["n"] or 0)} for r in rows])
    df["dt"] = pd.to_datetime(df["bucket_ts"], unit="s", utc=True)
    return df.set_index("dt")[["trades"]]


def _global_signal_score(stats_row) -> int:
    """
    A demo-friendly "Signal Score" independent of a viewer address.
    Uses only on-chain-derived aggregates (from user_stats).
    """
    if stats_row is None:
        return 0
    roi = _safe_float(stats_row["roi"], 0.0) or 0.0
    win = _safe_float(stats_row["win_rate"], 0.0) or 0.0
    trades = float(_safe_int(stats_row["trades_count"], 0))
    markets = float(_safe_int(stats_row["markets_traded"], 0))
    profit = float(_to_usdc(_safe_int(stats_row["total_profit"], 0)) or 0.0)

    # Normalize to [0,1] with soft caps (fast & stable for demos)
    roi_n = max(0.0, min(1.0, roi / 3.0))  # 0..300%+
    win_n = max(0.0, min(1.0, win))        # already 0..1
    trades_n = max(0.0, min(1.0, trades / 120.0))
    markets_n = max(0.0, min(1.0, markets / 40.0))
    profit_n = max(0.0, min(1.0, (profit / 5000.0)))  # 5k USDC+

    score = int(round(100.0 * (0.28 * roi_n + 0.22 * win_n + 0.18 * profit_n + 0.18 * trades_n + 0.14 * markets_n)))
    return max(0, min(100, score))


def _radar5_chart(values: dict[str, float], *, title: str = "") -> None:
    """
    Lightweight 5D radar (Vega-Lite) without extra deps.
    values: 0..1 for each key.
    """
    keys = list(values.keys())
    if not keys:
        st.caption("(no data)")
        return
    n = len(keys)
    import math as _m

    pts = []
    for i, k in enumerate(keys):
        v = float(values.get(k, 0.0) or 0.0)
        v = max(0.0, min(1.0, v))
        ang = (2 * _m.pi * (i / max(1, n))) - (_m.pi / 2)  # start at top
        pts.append({"k": k, "v": v, "ang": ang})
    pts.append(dict(pts[0]))  # close polygon

    spec = {
        "title": {"text": title, "color": "#E5E7EB", "fontSize": 12} if title else None,
        "data": {"values": pts},
        "transform": [
            {"calculate": "datum.v * cos(datum.ang)", "as": "x"},
            {"calculate": "datum.v * sin(datum.ang)", "as": "y"},
        ],
        "layer": [
            {
                "mark": {"type": "area", "opacity": 0.18, "color": "#7C3AED"},
                "encoding": {"x": {"field": "x", "type": "quantitative"}, "y": {"field": "y", "type": "quantitative"}},
            },
            {
                "mark": {"type": "line", "strokeWidth": 2, "color": "#A78BFA"},
                "encoding": {"x": {"field": "x", "type": "quantitative"}, "y": {"field": "y", "type": "quantitative"}},
            },
            {
                "mark": {"type": "point", "filled": True, "size": 40, "color": "#E5E7EB"},
                "encoding": {
                    "x": {"field": "x", "type": "quantitative"},
                    "y": {"field": "y", "type": "quantitative"},
                    "tooltip": [{"field": "k", "type": "nominal"}, {"field": "v", "type": "quantitative"}],
                },
            },
        ],
        "config": {
            "background": "transparent",
            "axis": {"grid": True, "gridColor": "rgba(61,68,82,0.35)", "labels": False, "ticks": False, "domain": False},
            "view": {"stroke": None},
        },
        "encoding": {
            "x": {"field": "x", "type": "quantitative", "scale": {"domain": [-1.05, 1.05]}},
            "y": {"field": "y", "type": "quantitative", "scale": {"domain": [-1.05, 1.05]}},
        },
    }
    if spec.get("title") is None:
        del spec["title"]
    st.vega_lite_chart(spec, use_container_width=True)


def _upsert_swipe(conn, *, from_addr: str, to_addr: str, action: str) -> None:
    from_addr = _norm_addr(from_addr)
    to_addr = _norm_addr(to_addr)
    if not from_addr or not to_addr or from_addr == to_addr:
        return
    if action not in {"like", "pass", "follow"}:
        return
    conn.execute(
        """
        INSERT INTO dating_swipes(from_address, to_address, action, created_at)
        VALUES(?, ?, ?, ?)
        ON CONFLICT(from_address, to_address) DO UPDATE SET
          action=excluded.action,
          created_at=excluded.created_at
        """,
        (from_addr, to_addr, action, _now_iso()),
    )
    conn.commit()


def _maybe_create_match(conn, *, a: str, b: str) -> bool:
    a = _norm_addr(a)
    b = _norm_addr(b)
    if not a or not b or a == b:
        return False
    # Mutual like?
    row = conn.execute(
        """
        SELECT
          (SELECT action FROM dating_swipes WHERE from_address=? AND to_address=?) AS a_to_b,
          (SELECT action FROM dating_swipes WHERE from_address=? AND to_address=?) AS b_to_a
        """,
        (a, b, b, a),
    ).fetchone()
    if not row or row["a_to_b"] != "like" or row["b_to_a"] != "like":
        return False
    user_a, user_b = (a, b) if a < b else (b, a)
    conn.execute(
        """
        INSERT OR IGNORE INTO dating_matches(user_a, user_b, created_at)
        VALUES(?, ?, ?)
        """,
        (user_a, user_b, _now_iso()),
    )
    conn.commit()
    return True


def _fetch_matches(conn, me: str, limit: int = 50) -> list[str]:
    me = _norm_addr(me)
    if not me:
        return []
    rows = conn.execute(
        """
        SELECT
          CASE WHEN user_a = ? THEN user_b ELSE user_a END AS other
        FROM dating_matches
        WHERE user_a = ? OR user_b = ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (me, me, me, limit),
    ).fetchall()
    return [r["other"] for r in rows if r and r["other"]]


def _fetch_likes(conn, me: str, limit: int = 100) -> list[dict[str, str]]:
    me = _norm_addr(me)
    if not me:
        return []
    rows = conn.execute(
        """
        SELECT to_address, created_at
        FROM dating_swipes
        WHERE from_address = ? AND action = 'like'
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (me, limit),
    ).fetchall()
    out: list[dict[str, str]] = []
    for r in rows:
        if not r:
            continue
        addr = r["to_address"]
        if addr:
            out.append({"address": addr, "created_at": (r["created_at"] or "")})
    return out


def _distinct_user_tags(conn, *, limit: int = 30) -> list[str]:
    rows = conn.execute(
        """
        SELECT tag, COUNT(*) AS n
        FROM user_tags
        GROUP BY tag
        ORDER BY n DESC, tag ASC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()
    return [str(r["tag"]) for r in rows if r and r["tag"]]


def _follow(conn, *, follower: str, followee: str) -> None:
    follower = _norm_addr(follower)
    followee = _norm_addr(followee)
    if not follower or not followee or follower == followee:
        return
    conn.execute(
        """
        INSERT INTO user_follows(follower_address, followee_address, created_at)
        VALUES(?, ?, ?)
        ON CONFLICT(follower_address, followee_address) DO UPDATE SET
          created_at=excluded.created_at
        """,
        (follower, followee, _now_iso()),
    )
    conn.commit()


def _unfollow(conn, *, follower: str, followee: str) -> None:
    follower = _norm_addr(follower)
    followee = _norm_addr(followee)
    if not follower or not followee or follower == followee:
        return
    conn.execute(
        "DELETE FROM user_follows WHERE follower_address = ? AND followee_address = ?",
        (follower, followee),
    )
    conn.commit()


def _fetch_followees(conn, follower: str, limit: int = 200) -> list[str]:
    follower = _norm_addr(follower)
    if not follower:
        return []
    rows = conn.execute(
        """
        SELECT followee_address
        FROM user_follows
        WHERE follower_address = ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (follower, limit),
    ).fetchall()
    return [r["followee_address"] for r in rows if r and r["followee_address"]]


def _fetch_follow_feed(
    conn,
    follower: str,
    *,
    limit: int = 50,
    followee: str | None = None,
    side: str | None = None,
) -> list[dict[str, object]]:
    """
    Latest trades where maker/taker is in my follow list.
    """
    follower = _norm_addr(follower)
    if not follower:
        return []
    followee = _norm_addr(followee or "")
    side = (side or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        side = ""

    where: list[str] = [
        "uf.follower_address = ?",
        "(lower(t.maker) = uf.followee_address OR lower(t.taker) = uf.followee_address)",
    ]
    params: list[object] = [follower]
    if followee:
        where.append("uf.followee_address = ?")
        params.append(followee)
    if side:
        where.append("t.side = ?")
        params.append(side)

    sql = f"""
        SELECT
          uf.followee_address AS followee,
          t.tx_hash,
          t.log_index,
          t.block_number,
          t.timestamp,
          t.maker,
          t.taker,
          t.side,
          t.price,
          t.collateral_amount,
          t.token_amount,
          t.token_id,
          tm.market_id AS market_id,
          m.slug,
          m.question,
          tm.outcome_label
        FROM trades t
        JOIN user_follows uf
          ON {where[0]}
        LEFT JOIN token_map tm ON tm.token_id = t.token_id
        LEFT JOIN markets m ON m.id = tm.market_id
        WHERE {" AND ".join(where[1:])}
        ORDER BY t.block_number DESC, t.log_index DESC
        LIMIT ?
    """
    params.append(int(limit))
    rows = conn.execute(sql, tuple(params)).fetchall()
    out: list[dict[str, object]] = []
    for r in rows:
        if not r:
            continue
        out.append(dict(r))
    return out


def _fetch_recent_trades_for_address(conn, address: str, limit: int = 3) -> list[dict[str, object]]:
    addr = _norm_addr(address)
    if not addr:
        return []
    rows = conn.execute(
        """
        SELECT
          t.tx_hash,
          t.log_index,
          t.block_number,
          t.side,
          t.price,
          t.collateral_amount,
          t.token_amount,
          tm.market_id AS market_id,
          m.slug,
          m.question,
          tm.outcome_label,
          t.decoded_json
        FROM trades t
        LEFT JOIN token_map tm ON tm.token_id = t.token_id
        LEFT JOIN markets m ON m.id = tm.market_id
        WHERE lower(t.maker) = ? OR lower(t.taker) = ?
        ORDER BY t.block_number DESC, t.log_index DESC
        LIMIT ?
        """,
        (addr, addr, int(limit)),
    ).fetchall()
    return [dict(r) for r in rows if r]


def _top_market_ids(conn, address: str, limit: int = 25) -> list[str]:
    addr = _norm_addr(address)
    if not addr:
        return []
    rows = conn.execute(
        """
        SELECT tm.market_id, COUNT(*) AS n
        FROM trades t
        JOIN token_map tm ON tm.token_id = t.token_id
        WHERE lower(t.maker) = ? OR lower(t.taker) = ?
        GROUP BY tm.market_id
        ORDER BY n DESC
        LIMIT ?
        """,
        (addr, addr, int(limit)),
    ).fetchall()
    return [r["market_id"] for r in rows if r and r["market_id"]]


def _buy_ratio(conn, address: str, limit: int = 300) -> float | None:
    addr = _norm_addr(address)
    if not addr:
        return None
    row = conn.execute(
        """
        WITH recent AS (
          SELECT side
          FROM trades
          WHERE lower(maker) = ? OR lower(taker) = ?
          ORDER BY block_number DESC, log_index DESC
          LIMIT ?
        )
        SELECT
          SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) AS buys,
          COUNT(*) AS n
        FROM recent
        """,
        (addr, addr, int(limit)),
    ).fetchone()
    if not row:
        return None
    n = _safe_int(row["n"])
    if n <= 0:
        return None
    buys = _safe_int(row["buys"])
    return float(buys) / float(n)


def _avg_trade_size_usdc(conn, address: str, *, limit: int = 200) -> float | None:
    addr = _norm_addr(address)
    if not addr:
        return None
    row = conn.execute(
        """
        WITH recent AS (
          SELECT collateral_amount
          FROM trades
          WHERE lower(maker) = ? OR lower(taker) = ?
          ORDER BY block_number DESC, log_index DESC
          LIMIT ?
        )
        SELECT AVG(CAST(collateral_amount AS REAL)) AS avg_amt
        FROM recent
        """,
        (addr, addr, int(limit)),
    ).fetchone()
    if not row:
        return None
    try:
        v = row["avg_amt"]
        if v is None:
            return None
        return _to_usdc(int(float(v)))
    except Exception:
        return None


def _sector_concentration(conn, address: str, *, recent_trades: int = 500) -> tuple[str, float, dict[str, int]]:
    """
    Returns (top_sector, top_ratio, counts) based on recent traded markets.
    Uses Gamma metadata when available; falls back to "Other".
    """
    addr = _norm_addr(address)
    if not addr:
        return "Other", 0.0, {}
    rows = conn.execute(
        """
        WITH recent AS (
          SELECT token_id
          FROM trades
          WHERE lower(maker) = ? OR lower(taker) = ?
          ORDER BY block_number DESC, log_index DESC
          LIMIT ?
        )
        SELECT m.slug AS slug, m.question AS question, COUNT(*) AS n
        FROM recent r
        LEFT JOIN token_map tm ON tm.token_id = r.token_id
        LEFT JOIN markets m ON m.id = tm.market_id
        GROUP BY tm.market_id
        ORDER BY n DESC
        LIMIT 80
        """,
        (addr, addr, int(recent_trades)),
    ).fetchall()
    counts: dict[str, int] = {}
    total = 0
    for r in rows:
        if not r:
            continue
        text = f"{r['slug'] or ''} {r['question'] or ''}"
        sector = _sector_for_market_text(text)
        n = int(r["n"] or 0)
        total += n
        counts[sector] = counts.get(sector, 0) + n
    if total <= 0:
        return "Other", 0.0, {}
    top_sector, top_n = max(counts.items(), key=lambda kv: kv[1]) if counts else ("Other", 0)
    return top_sector, float(top_n) / float(total), counts


def _odds_hunter_win_rate(conn, address: str, *, max_price: float = 0.25) -> tuple[int, int, float]:
    """
    For BUY trades on resolved markets where price <= max_price, compute (n, wins, win_rate).
    """
    addr = _norm_addr(address)
    if not addr:
        return 0, 0, 0.0
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS n,
          SUM(CASE WHEN m.winning_token_id IS NOT NULL AND t.token_id = m.winning_token_id THEN 1 ELSE 0 END) AS wins
        FROM trades t
        LEFT JOIN token_map tm ON tm.token_id = t.token_id
        LEFT JOIN markets m ON m.id = tm.market_id
        WHERE (lower(t.maker) = ? OR lower(t.taker) = ?)
          AND t.side = 'BUY'
          AND t.price IS NOT NULL
          AND t.price <= ?
          AND m.resolved = 1
        """,
        (addr, addr, float(max_price)),
    ).fetchone()
    if not row:
        return 0, 0, 0.0
    n = _safe_int(row["n"])
    wins = _safe_int(row["wins"])
    wr = (float(wins) / float(n)) if n > 0 else 0.0
    return n, wins, wr


def _discover_personas(conn, address: str, stats) -> list[str]:
    """
    Persona tags (profiling) for Discover card.
    """
    if stats is None:
        return ["🧩 Unknown"]
    tags: list[str] = []
    win = _safe_float(stats["win_rate"], 0.0) or 0.0
    trades = _safe_int(stats["trades_count"], 0)
    markets = _safe_int(stats["markets_traded"], 0)
    total_cost = _safe_int(stats["total_cost"], 0)

    top_sector, top_ratio, _ = _sector_concentration(conn, address, recent_trades=500)
    sectors = _top_sectors_for_address(conn, address, recent_trades=500, top_k=5)

    # 💎 Elite Oracle
    if win >= 0.80 and markets >= 5:
        tags.append("💎 顶级先知 (Elite Oracle)")

    # 🐋 Directional Whale: big volume + concentrated sectors
    if total_cost >= 8_000 * (10**USDC_DECIMALS) and trades >= 30 and top_ratio >= 0.70:
        tags.append(f"🐋 定向巨鲸 (Directional Whale) · {top_sector}")

    # 🎰 Odds Hunter: buys cheap and wins
    n, wins, wr = _odds_hunter_win_rate(conn, address, max_price=0.25)
    if n >= 3 and wr >= 0.66:
        tags.append("🎰 赔率猎人 (Odds Hunter)")

    # 🌐 Generalist: multi-sector
    if len({s for s in (sectors or []) if s and s != "Other"}) >= 3 and markets >= 10 and top_ratio <= 0.55:
        tags.append("🌐 全能选手 (Generalist)")

    if not tags:
        tags.append("🧊 冷静观察者 (Calm Operator)")
    return tags[:3]


def _fetch_trade_proof_lines(conn, address: str, *, limit: int = 5) -> list[dict[str, str]]:
    addr = _norm_addr(address)
    if not addr:
        return []
    rows = conn.execute(
        """
        SELECT
          t.tx_hash,
          t.block_number,
          t.side,
          t.price,
          t.collateral_amount,
          tm.market_id AS market_id,
          m.slug,
          m.question,
          m.resolved,
          m.winning_token_id,
          t.token_id
        FROM trades t
        LEFT JOIN token_map tm ON tm.token_id = t.token_id
        LEFT JOIN markets m ON m.id = tm.market_id
        WHERE lower(t.maker) = ? OR lower(t.taker) = ?
        ORDER BY t.block_number DESC, t.log_index DESC
        LIMIT ?
        """,
        (addr, addr, int(limit)),
    ).fetchall()
    out: list[dict[str, str]] = []
    for r in rows:
        if not r:
            continue
        tx = str(r["tx_hash"] or "")
        block = str(r["block_number"] or "")
        collateral = _fmt_usdc(r["collateral_amount"])
        resolved = int(r["resolved"] or 0) if r["resolved"] is not None else 0
        win_tok = str(r["winning_token_id"] or "")
        tok = str(r["token_id"] or "")
        pred = "Unknown"
        if resolved and win_tok:
            pred = "Win" if tok == win_tok else "Lose"
        out.append(
            {
                "block": block,
                "collateral": collateral,
                "prediction": pred,
                "tx": tx,
            }
        )
    return out


def _quick_pick_address(conn, *, label: str, input_key: str, pick_key: str, use_key: str) -> str:
    """
    Address input with a 'known address' quick picker.
    Important: the quick-pick writes to session_state[input_key] BEFORE instantiating the text_input.
    """
    known = _load_known_addresses(conn, limit=30)
    if known:
        qp1, qp2 = st.columns([3, 1])
        picked = qp1.selectbox(
            label,
            options=["(none)"] + known,
            index=0,
            key=pick_key,
        )
        if qp2.button("Use", use_container_width=True, key=use_key):
            if picked and picked != "(none)":
                st.session_state[input_key] = picked

    val = st.text_input("Your address (0x...)", key=input_key, placeholder="0x...")
    return _norm_addr(val)

def _top_token_ids(conn, address: str, limit: int = 30) -> list[str]:
    addr = _norm_addr(address)
    if not addr:
        return []
    rows = conn.execute(
        """
        SELECT token_id, COUNT(*) AS n
        FROM trades
        WHERE lower(maker) = ? OR lower(taker) = ?
        GROUP BY token_id
        ORDER BY n DESC
        LIMIT ?
        """,
        (addr, addr, limit),
    ).fetchall()
    return [r["token_id"] for r in rows if r and r["token_id"]]


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return float(inter) / float(union) if union else 0.0


def _compatibility_score(conn, *, me: str, other: str) -> tuple[int, list[str]]:
    """
    Simple explainable score 0..100 based on:
    - token overlap (interest)
    - style similarity (tempo/risk)
    - trust (sample size)
    """
    me = _norm_addr(me)
    other = _norm_addr(other)
    reasons: list[str] = []
    if not me or not other or me == other:
        return 0, reasons

    me_stats, me_tags = _fetch_profile_stats(conn, me)
    ot_stats, ot_tags = _fetch_profile_stats(conn, other)
    if me_stats is None or ot_stats is None:
        return 0, reasons

    me_style = _classify_style(me_stats, me_tags)
    ot_style = _classify_style(ot_stats, ot_tags)

    # Interest overlap (token + market)
    me_tokens = set(_top_token_ids(conn, me, limit=25))
    ot_tokens = set(_top_token_ids(conn, other, limit=25))
    token_overlap = _jaccard(me_tokens, ot_tokens)
    token_score = int(round(25 * token_overlap))
    if token_overlap > 0:
        reasons.append(f"共同交易 token overlap≈{token_overlap:.2f}")

    me_markets = set(_top_market_ids(conn, me, limit=25))
    ot_markets = set(_top_market_ids(conn, other, limit=25))
    market_overlap = _jaccard(me_markets, ot_markets)
    market_score = int(round(25 * market_overlap))
    if market_overlap > 0:
        reasons.append(f"共同市场 overlap≈{market_overlap:.2f}")

    # Direction bias similarity (BUY ratio)
    me_buy = _buy_ratio(conn, me, limit=200)
    ot_buy = _buy_ratio(conn, other, limit=200)
    side_score = 5
    if me_buy is not None and ot_buy is not None:
        diff = abs(me_buy - ot_buy)
        side_score = int(round(10 * (1.0 - min(1.0, diff))))
        reasons.append(f"方向偏好相近（BUY≈{me_buy:.2f} vs {ot_buy:.2f}）")

    # Tempo match
    tempo_score = 5
    if me_style.get("tempo") == ot_style.get("tempo"):
        tempo_score = 10
        reasons.append(f"交易节奏相近（{me_style.get('tempo')}）")

    # Risk match
    risk_score = 5
    if me_style.get("risk") == ot_style.get("risk"):
        risk_score = 10
        reasons.append(f"风险偏好相近（{me_style.get('risk')}）")

    # Trust (sample size)
    trust = 0
    ot_trades = _safe_int(ot_stats["trades_count"])
    if ot_trades >= 200:
        trust = 20
    elif ot_trades >= 80:
        trust = 16
    elif ot_trades >= 20:
        trust = 12
    else:
        trust = 6
    reasons.append(f"样本量：trades={ot_trades}")

    score = max(0, min(100, token_score + market_score + side_score + tempo_score + risk_score + trust))
    return score, reasons


def _load_or_build_daily_picks(
    conn,
    *,
    me: str,
    pool_size: int,
    picks: int = 10,
    # filters
    min_trades: int = 20,
    min_markets: int = 0,
    min_roi: float | None = None,
    min_win_rate: float | None = None,
    min_profit_usdc: float | None = None,
    required_tags: list[str] | None = None,
    sort: str = "profit",
    sector_filter: list[str] | None = None,
) -> list[str]:
    """
    Deterministic daily picks persisted in SQLite so it's demo-friendly.
    """
    me = _norm_addr(me)
    if not me:
        return []
    today = _date_today()
    rows = conn.execute(
        """
        SELECT candidate_address
        FROM dating_daily_picks
        WHERE pick_date = ? AND for_address = ?
        ORDER BY rank ASC
        """,
        (today, me),
    ).fetchall()
    if rows:
        return [r["candidate_address"] for r in rows if r and r["candidate_address"]]

    # Build: sample a subset, score, take top N excluding already swiped
    swiped = conn.execute(
        "SELECT to_address FROM dating_swipes WHERE from_address = ?",
        (me,),
    ).fetchall()
    swiped_set = {r["to_address"] for r in swiped if r and r["to_address"]}

    candidates = _fetch_dating_candidates(
        conn,
        limit=int(pool_size),
        min_trades=int(min_trades),
        min_markets=int(min_markets),
        min_roi=min_roi,
        min_win_rate=min_win_rate,
        min_profit_usdc=min_profit_usdc,
        required_tags=required_tags,
        sort=sort,
    )
    sector_filter_set = {str(s) for s in (sector_filter or []) if s}
    rng = _rng_for_address(me, nonce=int(time.time() // 86400), salt="daily-picks")
    rng.shuffle(candidates)
    sampled = [a for a in candidates if a != me and a not in swiped_set][:200]
    if sector_filter_set:
        sampled = [
            a
            for a in sampled
            if set(_top_sectors_for_address(conn, a, recent_trades=500, top_k=2)) & sector_filter_set
        ]

    scored: list[tuple[int, str]] = []
    for addr in sampled:
        s, _ = _compatibility_score(conn, me=me, other=addr)
        scored.append((s, addr))
    scored.sort(key=lambda x: x[0], reverse=True)
    top = [a for _, a in scored[:picks]]

    now = _now_iso()
    for i, addr in enumerate(top, start=1):
        conn.execute(
            """
            INSERT OR IGNORE INTO dating_daily_picks(pick_date, for_address, rank, candidate_address, created_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (today, me, i, addr, now),
        )
    conn.commit()
    return top

def _dating_tags(stats, tags: list[str]) -> list[str]:
    """
    Dating-style tags (separate from trading tags).
    Derived from on-chain stats but expressed as "personality" labels.
    """
    tags_set = set(tags or [])
    style = _classify_style(stats, tags)

    roi = _safe_float(stats["roi"]) if stats is not None else None
    win_rate = _safe_float(stats["win_rate"]) if stats is not None else None
    trades = _safe_int(stats["trades_count"]) if stats is not None else 0
    markets = _safe_int(stats["markets_traded"]) if stats is not None else 0

    out: list[str] = []

    # Core vibe
    if "Diamond Hands" in tags_set:
        out.append("长情型")
    if "Whale" in tags_set:
        out.append("大方型")
    if "Smart Money" in tags_set:
        out.append("脑子在线")
    if "Contra" in tags_set:
        out.append("反差感")

    # Style-derived
    risk = style.get("risk")
    if risk == "激进":
        out.append("敢爱敢恨")
    elif risk == "中性偏猛":
        out.append("有点上头")
    elif risk == "偏保守":
        out.append("慢热但稳")

    tempo = style.get("tempo")
    if tempo == "高频":
        out.append("话很多")
    elif tempo == "中频":
        out.append("在线频率稳定")
    else:
        out.append("低调选手")

    # Data-aware hints (kept light)
    if win_rate is not None and win_rate >= 0.62:
        out.append("很会选")
    if roi is not None and roi >= 0.2:
        out.append("带来好运")
    if markets >= 30:
        out.append("爱逛街（市场版）")
    if trades >= 200:
        out.append("行动派")

    # Keep tags short and non-repetitive
    dedup: list[str] = []
    for t in out:
        if t and t not in dedup:
            dedup.append(t)
    return dedup[:8]


def _dating_bio(stats, tags: list[str]) -> str:
    """
    1-2 lines, playful but not cringe.
    """
    style = _classify_style(stats, tags)
    roi = _safe_float(stats["roi"]) if stats is not None else None
    win_rate = _safe_float(stats["win_rate"]) if stats is not None else None
    trades = _safe_int(stats["trades_count"]) if stats is not None else 0

    bits2: list[str] = []
    bits2.append(f"On-chain vibe: {style.get('volume','')} · {style.get('tempo','')} · {style.get('edge','')}")
    if roi is not None:
        bits2.append(f"ROI≈{roi:.2f}")
    if win_rate is not None:
        bits2.append(f"Win≈{win_rate:.2f}")
    return " · ".join(bits2)


def _fetch_dating_candidates(
    conn,
    *,
    limit: int = 500,
    min_trades: int = 20,
    min_markets: int = 0,
    min_roi: float | None = None,
    min_win_rate: float | None = None,
    min_profit_usdc: float | None = None,
    required_tags: list[str] | None = None,
    sort: str = "profit",
) -> list[str]:
    """
    Candidate pool for Discover.
    Note: this is intentionally "fast + coarse"; sector filtering is applied later in Python.
    """
    where: list[str] = [
        "address NOT IN ('0xc5d563a36ae78145c45a50134d48a1215220f80a',"
        "              '0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e',"
        "              '0x0000000000000000000000000000000000000000')",
        "trades_count >= ?",
        "markets_traded >= ?",
    ]
    params: list[object] = [int(min_trades), int(min_markets)]

    if min_roi is not None:
        where.append("roi IS NOT NULL AND roi >= ?")
        params.append(float(min_roi))
    if min_win_rate is not None:
        where.append("win_rate IS NOT NULL AND win_rate >= ?")
        params.append(float(min_win_rate))
    if min_profit_usdc is not None and float(min_profit_usdc) != 0.0:
        # total_profit is stored as USDC base units (6 decimals)
        where.append("total_profit >= ?")
        params.append(int(round(float(min_profit_usdc) * (10**USDC_DECIMALS))))

    # Optional tag filter (any-of)
    required_tags = [t for t in (required_tags or []) if t]
    if required_tags:
        q_marks = ",".join(["?"] * len(required_tags))
        where.append(f"address IN (SELECT address FROM user_tags WHERE tag IN ({q_marks}))")
        params.extend(required_tags)

    order_by = "total_profit DESC"
    if sort == "roi":
        order_by = "roi DESC"
    elif sort == "win_rate":
        order_by = "win_rate DESC"
    elif sort == "trades":
        order_by = "trades_count DESC"

    sql = f"""
        SELECT address
        FROM user_stats
        WHERE {' AND '.join(where)}
        ORDER BY {order_by}
        LIMIT ?
    """
    params.append(int(limit))

    rows = conn.execute(sql, tuple(params)).fetchall()
    return [r["address"] for r in rows if r and r["address"]]


def _fetch_profile_stats(conn, address: str):
    addr = address.strip().lower()
    stats = conn.execute("SELECT * FROM user_stats WHERE address = ?", (addr,)).fetchone()
    tags = conn.execute("SELECT tag FROM user_tags WHERE address = ? ORDER BY tag", (addr,)).fetchall()
    return stats, [t["tag"] for t in tags]


def _rng_for_address(address: str, *, nonce: int = 0, salt: str = "polyrep-v1") -> random.Random:
    """
    Deterministic "random" for a given address (stable copy for screenshots).
    Increment nonce to get a new roll.
    """
    a = (address or "").strip().lower()
    raw = f"{salt}:{a}:{int(nonce)}".encode("utf-8")
    digest = hashlib.sha256(raw).digest()
    seed = int.from_bytes(digest[:8], "big", signed=False)
    return random.Random(seed)


def _pick_trading_archetype(stats, tags: list[str]) -> str:
    tags_set = set(tags or [])
    roi = stats["roi"] if stats is not None else None
    win_rate = stats["win_rate"] if stats is not None else None
    trades = int(stats["trades_count"]) if (stats is not None and stats["trades_count"] is not None) else 0
    markets = (
        int(stats["markets_traded"]) if (stats is not None and stats["markets_traded"] is not None) else 0
    )

    if "Whale" in tags_set:
        return "鲸鱼流动性搬运工"
    if "Smart Money" in tags_set:
        return "聪明钱（但不告诉你怎么聪明）"
    if "Diamond Hands" in tags_set:
        return "钻石手守夜人"
    if "Contra" in tags_set:
        return "反向指标发动机"
    if win_rate is not None and win_rate >= 0.62 and trades >= 30:
        return "胜率派机会主义者"
    if roi is not None and roi >= 0.5 and trades >= 20:
        return "高 ROI 突击手"
    if trades >= 200:
        return "高频搓单选手"
    if trades >= 60:
        return "勤奋刷单型观察员"
    if markets >= 20 and trades < 40:
        return "广撒网的轻仓选手"
    return "佛系下注人"


def _classify_style(stats, tags: list[str]) -> dict[str, str]:
    """
    Produce deterministic, data-driven labels so the persona sentence is explainable.
    """
    tags_set = set(tags or [])
    out: dict[str, str] = {}

    roi = stats["roi"] if stats is not None else None
    win_rate = stats["win_rate"] if stats is not None else None
    trades = int(stats["trades_count"]) if (stats is not None and stats["trades_count"] is not None) else 0
    markets = (
        int(stats["markets_traded"]) if (stats is not None and stats["markets_traded"] is not None) else 0
    )
    total_cost = int(stats["total_cost"]) if (stats is not None and stats["total_cost"] is not None) else 0
    total_profit = int(stats["total_profit"]) if (stats is not None and stats["total_profit"] is not None) else 0
    max_trade = int(stats["max_trade_usdc"]) if (stats is not None and stats["max_trade_usdc"] is not None) else 0

    # Risk profile (position sizing)
    if "Whale" in tags_set or max_trade >= 5_000 * (10**USDC_DECIMALS):
        out["risk"] = "激进"
    elif max_trade >= 1_000 * (10**USDC_DECIMALS):
        out["risk"] = "中性偏猛"
    elif max_trade > 0:
        out["risk"] = "偏保守"
    else:
        out["risk"] = "未知"

    # Tempo (how frequently they trade)
    trades_per_market = (trades / markets) if markets > 0 else float(trades)
    if trades >= 250 or trades_per_market >= 8:
        out["tempo"] = "高频"
    elif trades >= 80 or trades_per_market >= 3:
        out["tempo"] = "中频"
    else:
        out["tempo"] = "低频"

    # Edge (win rate / roi)
    if "Smart Money" in tags_set:
        out["edge"] = "偏强"
    elif win_rate is not None and win_rate >= 0.62:
        out["edge"] = "偏强"
    elif roi is not None and roi >= 0.20:
        out["edge"] = "偏正"
    elif "Contra" in tags_set or (roi is not None and roi <= -0.20):
        out["edge"] = "偏反向"
    else:
        out["edge"] = "中性"

    # Volume (how much they have put to work)
    if total_cost >= 20_000 * (10**USDC_DECIMALS):
        out["volume"] = "重仓"
    elif total_cost >= 2_000 * (10**USDC_DECIMALS):
        out["volume"] = "中仓"
    elif total_cost > 0:
        out["volume"] = "轻仓"
    else:
        out["volume"] = "未知"

    # Profit direction
    if total_profit > 0:
        out["pnl"] = "赚钱"
    elif total_profit < 0:
        out["pnl"] = "交学费"
    else:
        out["pnl"] = "打平"

    return out


def _generate_style_sentence(
    address: str,
    stats,
    tags: list[str],
    *,
    nonce: int,
    persona_tone: str = "normal",
) -> str:
    rng = _rng_for_address(address, nonce=nonce)
    archetype = _pick_trading_archetype(stats, tags)
    style = _classify_style(stats, tags)

    # Keep randomness only for phrasing; the underlying labels come from data.
    adjectives_by_risk = {
        "激进": ["激进", "敢打敢拼", "火力全开"],
        "中性偏猛": ["有点猛", "不太保守", "敢于出手"],
        "偏保守": ["谨慎", "稳一点", "保守派"],
        "未知": ["神秘", "低调", "资料不足但气场很足"],
    }
    adjectives = adjectives_by_risk.get(style.get("risk", "未知"), ["神秘"])

    habits_by_tempo = {
        "高频": [
            "像在做市，手速和心态都很稳",
            "高频刷存在感：不一定每次都赢，但参与度拉满",
            "更像在跟盘口跳舞",
        ],
        "中频": [
            "喜欢挑时机出手，做事不急但很连续",
            "节奏稳定：看准了才动",
            "偶尔连击，偶尔观望",
        ],
        "低频": [
            "更像狙击手：不常出手，但每次都很认真",
            "不爱乱动，出手前先把市场看明白",
            "佛系但不随便：有把握才下单",
        ],
    }
    habits = habits_by_tempo.get(style.get("tempo", "低频"), ["出手不多，但很认真"])

    edge_lines = {
        "偏强": ["胜率/标签都挺能打", "更像“有边际的人”", "看起来不是随便蒙的"],
        "偏正": ["整体偏正收益", "小赚稳赚型倾向", "有点东西但还在进化"],
        "偏反向": ["偶尔会当市场的反向教材", "逆风也很倔强", "主打一个“我来对冲你们的自信”"],
        "中性": ["更像在探索市场的节奏", "有赢有输，属于正常玩家", "整体中性，不靠玄学靠样本"],
    }

    adj = rng.choice(adjectives)
    habit = rng.choice(habits)
    edge = rng.choice(edge_lines.get(style.get("edge", "中性"), edge_lines["中性"]))

    # Add a light, data-aware hint (optional)
    hint_parts: list[str] = []
    if stats is not None:
        # Use human-readable USDC values
        max_trade_usdc = _to_usdc(int(stats["max_trade_usdc"])) if stats["max_trade_usdc"] is not None else None
        total_cost_usdc = _to_usdc(int(stats["total_cost"])) if stats["total_cost"] is not None else None
        total_profit_usdc = _to_usdc(int(stats["total_profit"])) if stats["total_profit"] is not None else None

        if stats["roi"] is not None:
            hint_parts.append(f"ROI≈{stats['roi']:.2f}")
        if stats["win_rate"] is not None:
            hint_parts.append(f"胜率≈{stats['win_rate']:.2f}")
        if stats["trades_count"] is not None:
            hint_parts.append(f"成交≈{int(stats['trades_count'])}")
        if max_trade_usdc is not None:
            hint_parts.append(f"最大单笔≈{max_trade_usdc:,.0f}U")
        if total_cost_usdc is not None and total_profit_usdc is not None:
            hint_parts.append(f"投入≈{total_cost_usdc:,.0f}U/{style.get('pnl','')}")
    hint = ("（" + "，".join(hint_parts) + "）") if hint_parts else ""

    tone_norm = (persona_tone or "normal").strip().lower()
    is_roast = tone_norm in {"roast", "toxic", "毒舌"}

    # Make the *entire* sentence structure differ by tone (not just the tail).
    if is_roast:
        pnl = style.get("pnl")
        roast_lines = {
            "交学费": [
                "这波有点伤，没事，能复盘就没白交。",
                "感觉你在认真体验波动。"
            ],
            "赚钱": [
                "害行吧。",
                "下次作业借我抄抄呗。"
            ],
            "打平": [
                "至少没上供。",
                "稳住了，但还差一点杀伤力。",
            ],
            None: ["锐评：信息不足，但气场很足。"],
        }
        roast = rng.choice(roast_lines.get(pnl, roast_lines[None]))

        roast_templates = [
            "{archetype}：{adj}，{habit}。你是{volume}的{tempo}选手，优势判断：{edge}；结果是“{pnl}”。{hint}{roast}",
            "人设观察：{archetype}。风格{adj}，节奏{tempo}，仓位{volume}，边际{edge}。结论：{pnl}。{hint}{roast}",
            "链上旁白：{archetype}。\n\n{tempo}节奏 + {volume}仓位，{edge}边际。\n当前结局：{pnl}。\n\n{hint}{roast}",
        ]
        tpl = rng.choice(roast_templates)
        return tpl.format(
            archetype=archetype,
            adj=adj,
            habit=habit,
            tempo=style.get("tempo", ""),
            volume=style.get("volume", ""),
            edge=edge,
            pnl=style.get("pnl", ""),
            hint=hint,
            roast=roast,
        ).strip()

    serious_templates = [
        "风格画像：{archetype}。风险偏好：{risk}；交易节奏：{tempo}；仓位：{volume}；优势判断：{edge}。{hint}",
        "{archetype}（链上画像）：整体{volume}，{tempo}节奏，风险偏好{risk}，优势判断{edge}。{hint}",
        "结论：{archetype}。{risk}风险偏好，{tempo}交易节奏，{volume}仓位配置；优势判断：{edge}。{hint}",
    ]
    tpl = rng.choice(serious_templates)
    return tpl.format(
        archetype=archetype,
        risk=style.get("risk", ""),
        tempo=style.get("tempo", ""),
        volume=style.get("volume", ""),
        edge=edge,
        hint=hint,
    ).strip()


def _load_db_path() -> str:
    return os.environ.get("DB_PATH", "./polyreputation.sqlite").strip()

def _fetch_leaderboard(
    conn,
    sort: str,
    limit: int = 50,
    *,
    required_tags: list[str] | None = None,
) -> pd.DataFrame:
    order_by = "roi DESC" if sort == "roi" else "total_profit DESC"
    where_parts = ["address NOT IN ('0xc5d563a36ae78145c45a50134d48a1215220f80a','0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e','0x0000000000000000000000000000000000000000')"]
    if sort == "roi":
        where_parts.append("roi IS NOT NULL")
    required_tags = [t for t in (required_tags or []) if t]
    if required_tags:
        q_marks = ",".join(["?"] * len(required_tags))
        where_parts.append(f"address IN (SELECT address FROM user_tags WHERE tag IN ({q_marks}))")
    where = "WHERE " + " AND ".join(where_parts)
    params: list[object] = []
    if required_tags:
        params.extend(required_tags)
    params.append(limit)
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
        tuple(params),
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


def _apply_ui_css() -> None:
    st.markdown(
        """
<style>
  /* Force dark color-scheme everywhere */
  html, body, [data-testid="stAppViewContainer"] { color-scheme: dark; }

  /* Layout tweaks */
  .block-container { padding-top: 1.25rem; padding-bottom: 2rem; max-width: 1200px; }
  .stMetric { border: 1px solid #3d4452; border-radius: 8px; padding: 10px 12px; background: rgba(17,24,39,0.35); }
  div[data-testid="stMetricValue"] { font-size: 1.5rem; }
  .muted { color: rgba(229,231,235,0.75); }

  /* Cyberpunk / Web3 card frame */
  .cyber-frame {
    border: 1px solid #3d4452;
    border-radius: 8px;
    background: linear-gradient(135deg, rgba(124, 58, 237, 0.16), rgba(59, 130, 246, 0.08));
    box-shadow: 0 0 0 1px rgba(61, 68, 82, 0.45), 0 10px 28px rgba(0,0,0,0.35);
  }
  .pill {
    display: inline-block;
    padding: 0.22rem 0.55rem;
    margin: 0 0.35rem 0.35rem 0;
    border-radius: 999px;
    border: 1px solid rgba(255,255,255,0.12);
    background: rgba(124,58,237,0.10);
    font-size: 0.85rem;
  }
  .pill-buy {
    border-color: rgba(16, 185, 129, 0.35);
    background: rgba(16, 185, 129, 0.14);
    color: rgba(167, 243, 208, 0.98);
  }
  .pill-sell {
    border-color: rgba(248, 113, 113, 0.35);
    background: rgba(248, 113, 113, 0.14);
    color: rgba(254, 202, 202, 0.98);
  }
  .feed-card {
    border: 1px solid #3d4452;
    border-radius: 8px;
    padding: 14px 14px;
    background: linear-gradient(135deg, rgba(124, 58, 237, 0.16), rgba(59, 130, 246, 0.08));
    box-shadow: 0 0 0 1px rgba(61, 68, 82, 0.45), 0 10px 28px rgba(0,0,0,0.35);
  }
  .feed-header { display: flex; align-items: center; gap: 10px; margin-bottom: 0.4rem; }
  .feed-title { font-weight: 900; }
  .feed-sub { color: rgba(229,231,235,0.75); font-size: 0.92rem; }
  .card {
    border: 1px solid #3d4452;
    border-radius: 8px;
    padding: 14px 14px;
    background: linear-gradient(135deg, rgba(124, 58, 237, 0.16), rgba(59, 130, 246, 0.08));
    box-shadow: 0 0 0 1px rgba(61, 68, 82, 0.45), 0 10px 28px rgba(0,0,0,0.35);
    white-space: pre-line; /* allow \n line breaks inside the card */
  }
  .dating-card {
    border: 1px solid #3d4452;
    border-radius: 8px;
    padding: 18px 18px;
    background: linear-gradient(135deg, rgba(124, 58, 237, 0.18), rgba(59, 130, 246, 0.08));
    color: rgba(229, 231, 235, 0.95);
    box-shadow: 0 0 0 1px rgba(61, 68, 82, 0.45), 0 10px 28px rgba(0,0,0,0.35);
  }
  .dating-header { display: flex; align-items: center; gap: 12px; }
  .pfp {
    width: 56px;
    height: 56px;
    border-radius: 999px;
    display: flex;
    align-items: center;
    justify-content: center;
    border: 1px solid rgba(255,255,255,0.10);
    box-shadow: 0 10px 22px rgba(0,0,0,0.35);
  }
  .pfp-text { color: rgba(255,255,255,0.95); font-weight: 900; letter-spacing: 0.5px; }
  .dating-title { font-size: 1.10rem; font-weight: 900; margin-bottom: 0.20rem; color: rgba(229,231,235,0.95); }
  .alias { color: rgba(167, 243, 208, 0.95); font-weight: 800; margin-left: 0.35rem; }
  .dating-sub { color: rgba(229,231,235,0.70); margin-bottom: 0.6rem; }
  .dating-card .muted { color: rgba(229,231,235,0.70) !important; }
  .dating-card a { color: rgba(167, 139, 250, 0.95); }
  .kv-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 10px;
    margin-top: 12px;
  }
  .kv {
    border: 1px solid #3d4452;
    border-radius: 8px;
    padding: 10px 10px;
    background: rgba(17,24,39,0.45);
    backdrop-filter: blur(4px);
  }
  .kv-label { color: rgba(229,231,235,0.65); font-size: 0.78rem; margin-bottom: 0.15rem; }
  .kv-value { font-weight: 900; font-size: 1.05rem; color: rgba(229,231,235,0.95); font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
  .kv-help { color: rgba(229,231,235,0.55); font-size: 0.72rem; margin-top: 0.10rem; line-height: 1.15; }
  .pos { color: rgba(52, 211, 153, 0.95); }
  .neg { color: rgba(248, 113, 113, 0.95); }
  .neu { color: rgba(229,231,235,0.95); }
  .tag {
    display: inline-block;
    padding: 0.2rem 0.55rem;
    margin: 0 0.35rem 0.35rem 0;
    border-radius: 999px;
    border: 1px solid #3d4452;
    background: rgba(17,24,39,0.35);
    color: rgba(229,231,235,0.85);
    font-size: 0.85rem;
  }
  /* Buttons: neon purple */
  button[kind="primary"] {
    background: rgba(124, 58, 237, 0.95) !important;
    border-color: rgba(124, 58, 237, 0.95) !important;
    box-shadow: 0 0 0 1px rgba(124, 58, 237, 0.35), 0 8px 20px rgba(124, 58, 237, 0.18);
  }
  button[kind="primary"]:hover {
    background: rgba(124, 58, 237, 1.0) !important;
    border-color: rgba(124, 58, 237, 1.0) !important;
  }
  div[data-testid="stButton"] > button {
    border-radius: 999px !important;
    height: 52px;
    font-weight: 800;
  }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }

  /* Live syncing indicator (top-right blinking dot) */
  .live-indicator {
    position: fixed;
    top: 12px;
    right: 18px;
    display: flex;
    align-items: center;
    gap: 10px;
    z-index: 9999;
    padding: 6px 10px;
    border: 1px solid #3d4452;
    border-radius: 999px;
    background: rgba(17,24,39,0.55);
    color: rgba(229,231,235,0.85);
    font-size: 0.85rem;
    backdrop-filter: blur(6px);
  }
  .live-dot {
    width: 10px;
    height: 10px;
    border-radius: 999px;
    background: rgba(34, 197, 94, 0.95);
    box-shadow: 0 0 0 2px rgba(34,197,94,0.18), 0 0 14px rgba(34,197,94,0.55);
    animation: pulse 1.25s infinite;
  }
  @keyframes pulse {
    0% { transform: scale(0.85); opacity: 0.7; }
    50% { transform: scale(1.10); opacity: 1.0; }
    100% { transform: scale(0.85); opacity: 0.7; }
  }

  /* Log-style feed */
  .log-box {
    border: 1px solid #3d4452;
    border-radius: 8px;
    padding: 10px 12px;
    background: rgba(17,24,39,0.45);
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    color: rgba(229,231,235,0.92);
    white-space: pre-wrap;
    line-height: 1.35;
  }

  /* Dataframe container: keep it cyber/dark */
  div[data-testid="stDataFrame"] {
    border: 1px solid #3d4452;
    border-radius: 8px;
    overflow: hidden;
  }
  div[data-testid="stDataFrame"] * {
    color: rgba(229,231,235,0.92) !important;
  }
</style>
        """,
        unsafe_allow_html=True,
    )


def _copy() -> dict[str, str]:
    """
    Centralized UI copy (single serious tone).
    """
    return {
        "title": "PolyBook",
        "subtitle": "记录链上 Alpha 的种草笔记 · Polymarket (Polygon) on-chain proof.",
        # Sidebar navigation labels
        "nav_lb": "Leaderboard",
        "nav_following": "Following",
        "nav_dating": "Discover",
        "nav_profile": "Profile",
        "nav_about": "About",
        # Page headings/subtitles
        "page_lb_title": "🏆 PolyBook 影响力榜单",
        "page_lb_subtitle": "Rank addresses by ROI or profit (derived from resolved markets, MVP accounting).",
        "page_following_title": "Following · 订阅动态",
        "page_following_subtitle": "把关注列表变成信息流：订阅博主的链上“笔记”。（关注行为本地 SQLite）",
        "page_dating_title": "Discover · 发现好博主",
        "page_dating_subtitle": "从链上数据里找值得订阅的交易者，并保留可验证的 on-chain proof。",
        "page_profile_title": "Profile",
        "page_profile_subtitle": "Address tags, PnL, and verifiable on-chain proof.",
        "page_about_title": "About",
        "page_about_subtitle": "Project overview, data sources, and limitations.",
        "proof_title": "Proof: tx_hash + decoded OrderFilled JSON",
        "hint_zero": "No stats found yet for this address. Backfill trades, sync markets, then run compute.",
    }


def main() -> None:
    st.set_page_config(page_title="PolyBook", layout="wide", initial_sidebar_state="expanded")
    _apply_ui_css()
    st.markdown(
        """
    <style>
    .stTable {font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;}
    .stChatMessage {border-radius: 10px; margin-bottom: 10px;}
    </style>
        """,
        unsafe_allow_html=True,
    )

    db_path = _load_db_path()
    # Ensure schema exists (including Dating tables). Safe and idempotent.
    init_db(db_path)
    conn = connect(db_path)

    # Sidebar controls
    st.sidebar.title("📖 PolyBook")
    st.sidebar.caption("记录链上 Alpha 的种草笔记")
    st.sidebar.caption("🟢 Real-time Indexing...")
    show_full_address = st.sidebar.checkbox("地址显示完整", value=True)
    c = _copy()

    # Routing
    route_key = "route"
    nav_key = "nav"  # visible sidebar selection only
    profile_address_key = "profile_address"
    follow_me_key = "follow_me"

    # Default follow identity so Follow buttons always work (demo-friendly).
    # Users can switch to a wallet identity on the Following page.
    if not st.session_state.get(follow_me_key):
        st.session_state[follow_me_key] = "local"

    # Jump handler: Discover -> Profile
    jump_addr = st.session_state.pop("dating_profile_jump", None)
    if jump_addr:
        st.session_state["route_prev"] = st.session_state.get(route_key, c["nav_lb"])
        st.session_state[route_key] = c["nav_profile"]
        st.session_state[profile_address_key] = str(jump_addr)

    st.title(c["title"])
    st.caption(c["subtitle"])
    st.caption(f"DB: `{db_path}`")

    # Sidebar shows only "main" pages; Profile is reachable via in-page buttons.
    nav_options = [c["nav_lb"], c["nav_following"], c["nav_dating"], c["nav_about"]]
    if st.session_state.get(route_key) not in (nav_options + [c["nav_profile"]]):
        st.session_state[route_key] = nav_options[0]
    if st.session_state.get(nav_key) not in nav_options:
        st.session_state[nav_key] = nav_options[0]
    nav_visible = st.sidebar.radio("导航", options=nav_options, index=0, key=nav_key)
    # Clicking the sidebar sets the route, except when we're on Profile
    # (Profile is only reachable via in-page buttons).
    if st.session_state.get(route_key) != c["nav_profile"] and nav_visible != st.session_state.get(route_key):
        st.session_state[route_key] = nav_visible

    nav = st.session_state.get(route_key)

    if nav == c["nav_lb"]:
        PER_PAGE = 10
        MAX_PAGES = 10

        st.subheader(c["page_lb_title"])
        st.caption(c["page_lb_subtitle"])

        # Filters (horizontal row under title; no expander)
        f0, f1, f2 = st.columns([1, 1, 1])
        with f0:
            sort = st.selectbox("Sort", options=["roi", "profit"], index=0, key="lb_sort")
        with f1:
            time_window = st.selectbox(
                "Time",
                options=["All time", "Past day", "Past week", "Past month"],
                index=0,
                key="lb_time_window",
            )
        with f2:
            scan_depth = st.selectbox("Scan depth", options=[100, 200, 500, 1000, 2000], index=2, key="lb_scan_depth")

        st.caption(f"Last Synced: {_now_iso()} (UTC) | Database: SQLite")
        st.caption("Time filter uses `trades.timestamp` (Polygon block timestamp, epoch seconds).")

        # Quick summary
        try:
            totals = conn.execute(
                "SELECT (SELECT COUNT(*) FROM trades) AS trades, (SELECT COUNT(*) FROM user_stats) AS users"
            ).fetchone()
            trades_total = int(totals["trades"]) if totals else 0
            users_total = int(totals["users"]) if totals else 0
        except Exception:
            trades_total, users_total = 0, 0

        m1, m2, m3 = st.columns(3)
        m1.metric(label="全网笔记数", value=f"{trades_total:,}", delta="链上成交笔记", delta_color="normal")
        m2.metric(label="活跃博主数", value=f"{users_total:,}", delta="本地画像已计算", delta_color="normal")
        m3.metric(label="当前风向指标", value=("ROI" if sort == "roi" else "Profit"), delta=time_window, delta_color="normal")

        # Fetch a larger pool, then segment (so "top by sector/tag" makes sense)
        pool = int(scan_depth or 500)
        df_pool = _fetch_leaderboard(conn, sort=sort, limit=pool, required_tags=None)

        # Time window filter (activity-based): keep only addresses that traded recently (timestamp-based)
        anchor_ts = _latest_trade_ts(conn)
        if anchor_ts <= 0:
            anchor_ts = int(time.time())
        start_ts = _window_start_ts(time_window, anchor_ts=anchor_ts)
        recent_counts: dict[str, int] = {}
        if start_ts is not None:
            recent_counts = _recent_trade_counts(conn, start_ts=start_ts)
        if not df_pool.empty:
            df_pool = df_pool.copy()
            if start_ts is not None:
                df_pool["recent_trades"] = df_pool["address"].map(lambda a: int(recent_counts.get(str(a).lower(), 0)))
                df_pool = df_pool[df_pool["recent_trades"] > 0].copy()
            else:
                df_pool["recent_trades"] = 0
        if not df_pool.empty:
            df_pool = df_pool.copy()
            df_pool["top_sector"] = df_pool["address"].apply(
                lambda a: (_top_sectors_for_address(conn, a, recent_trades=600, top_k=1) or ["Other"])[0]
            )

        # Quick search (filters pool + table in realtime)
        q = st.text_input("🔍 Quick Search Address", value="", placeholder="0x1234...abcd", key="lb_search")
        if q and not df_pool.empty:
            q_l = q.strip().lower()
            df_pool = df_pool[df_pool["address"].astype(str).str.lower().str.contains(q_l)].copy()

        # Pagination state (table-only; no page selector in header)
        filters_sig = (
            str(sort),
            int(pool),
            str(time_window),
            str(q or ""),
        )
        if st.session_state.get("lb_filters_sig") != filters_sig:
            st.session_state["lb_filters_sig"] = filters_sig
            st.session_state["lb_page"] = 1

        page = int(st.session_state.get("lb_page") or 1)
        total_rows = int(len(df_pool)) if not df_pool.empty else 0
        total_pages = max(1, (total_rows + PER_PAGE - 1) // PER_PAGE) if total_rows else 1
        total_pages = min(MAX_PAGES, total_pages)
        page = max(1, min(total_pages, page))
        st.session_state["lb_page"] = page

        start = (page - 1) * PER_PAGE
        end = start + PER_PAGE
        df = df_pool.iloc[start:end].copy() if not df_pool.empty else df_pool
        if not df.empty:
            toast_sig = (str(sort), str(time_window), int(pool), int(total_rows))
            if st.session_state.get("lb_toast_sig") != toast_sig:
                st.session_state["lb_toast_sig"] = toast_sig
                try:
                    st.toast("Leaderboard loaded", icon="✅")
                except Exception:
                    pass

            # Dashboard charts (based on the segmented pool)
            dash = df_pool.copy()
            if not dash.empty:
                dash["profit (USDC)"] = dash["total_profit_usdc"]
                dash["roi"] = dash["roi"]

                s1, s2 = st.columns(2)
                with s1:
                    sec_counts = dash["top_sector"].value_counts().rename_axis("sector").reset_index(name="addresses")
                    st.caption("Addresses by sector (segmented pool)")
                    _pie_chart(sec_counts, label_col="sector", value_col="addresses", title="Addresses by sector")
                with s2:
                    sec_profit = (
                        dash.groupby("top_sector", as_index=False)["profit (USDC)"]
                        .sum()
                        .sort_values("profit (USDC)", ascending=False)
                        .rename(columns={"top_sector": "sector", "profit (USDC)": "profit_sum"})
                    )
                    st.caption("Profit sum by sector (segmented pool)")
                    sec_profit = sec_profit.copy()
                    # Pie charts can't show negative values; use absolute profit magnitude for "share".
                    sec_profit["profit_abs"] = sec_profit["profit_sum"].abs()
                    _pie_chart(sec_profit, label_col="sector", value_col="profit_abs", title="Profit share by sector (abs)")

                # Extra visuals under the pies
                # Charts follow the selected time window; for "All time" show the most recent 30d for readability.
                chart_start_ts = start_ts if start_ts is not None else max(0, int(anchor_ts) - 30 * 24 * 60 * 60)
                bucket_seconds = 3600 if (time_window or "").strip().lower() == "past day" else 86400
                chart_start_label = datetime.fromtimestamp(int(chart_start_ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                _vspace(8)
                c1, c2 = st.columns(2)
                with c1:
                    st.caption(f"Top markets (since {chart_start_label} UTC)")
                    topm = _top_markets_in_window(conn, start_ts=int(chart_start_ts), limit=12)
                    if not topm.empty:
                        topm = topm.copy()
                        topm["label"] = topm["slug"].map(lambda s: _truncate_label(str(s), 15))
                        _barh_chart(topm[["label", "trades"]], label_col="label", value_col="trades", title="Top markets")
                with c2:
                    unit = "hour" if int(bucket_seconds) == 3600 else "day"
                    st.caption(f"Trading Volume (count per {unit})")
                    df_vol = _trading_volume_by_period(conn, start_ts=int(chart_start_ts), bucket_seconds=int(bucket_seconds))
                    if df_vol is not None and not df_vol.empty:
                        st.bar_chart(df_vol, height=250)
                    else:
                        st.caption("(no data)")

            _vspace(12)
            # Profile controls (move above table)
            prof1, prof2, prof3 = st.columns([2, 1, 1])
            options_addr = [str(a) for a in df_pool["address"].head(200).tolist()] if not df_pool.empty else []
            pick_addr = prof1.selectbox(
                "Open Profile",
                options=options_addr,
                format_func=(lambda a: a if show_full_address else _short_addr(a, n=4)),
                key="lb_open_profile_pick",
            )
            if prof2.button("Open", type="primary", use_container_width=True, key="lb_open_profile_btn"):
                st.session_state["route_prev"] = st.session_state.get(route_key, c["nav_lb"])
                st.session_state[route_key] = c["nav_profile"]
                st.session_state[profile_address_key] = pick_addr
                st.rerun()
            if prof3.button("Surprise Me", use_container_width=True, key="lb_surprise"):
                import random as _r
                pool_addrs = [str(a) for a in df_pool["address"].head(50).tolist()] if not df_pool.empty else []
                if pool_addrs:
                    chosen = _r.choice(pool_addrs)
                    st.session_state["route_prev"] = st.session_state.get(route_key, c["nav_lb"])
                    st.session_state[route_key] = c["nav_profile"]
                    st.session_state[profile_address_key] = chosen
                    st.rerun()

            # Leaderboard table (pandas styler)
            df_table = df.copy()
            df_table.insert(0, "rank", list(range(start + 1, start + 1 + len(df_table))))
            df_table["address_display"] = df_table["address"].apply(
                lambda a: ("👤 " + (str(a) if show_full_address else _short_addr(str(a), n=4)))
            )
            df_table["sector"] = df_table["top_sector"].apply(lambda s: f"#{(str(s or 'Other')).replace(' ', '')}")
            df_table = df_table.rename(
                columns={
                    "address_display": "Address",
                    "total_profit_usdc": "累计带单收益",
                    "roi": "ROI",
                    "win_rate": "Win rate",
                    "markets_traded": "Markets",
                    "trades_count": "Trades",
                    "recent_trades": "Recent trades",
                    "max_trade_usdc": "Max trade (USDC)",
                    "rank": "Rank",
                }
            )
            df_table = df_table[
                ["Rank", "Address", "sector", "累计带单收益", "ROI", "Win rate", "Markets", "Trades", "Recent trades", "Max trade (USDC)"]
            ].copy()

            def _profit_style(v):
                try:
                    v = float(v)
                except Exception:
                    return ""
                if v > 0:
                    return "color: #10b981; font-weight: 700;"
                if v < 0:
                    return "color: #f87171; font-weight: 700;"
                return "color: rgba(255,255,255,0.85);"

            def _win_rate_gradient_bg(v):
                """
                Dark-mode gradient: 0% deep red -> 100% deep purple.
                """
                try:
                    x = float(v)
                except Exception:
                    return ""
                if x != x:  # NaN
                    return ""
                x = max(0.0, min(1.0, x))
                lo = (127, 29, 29)  # #7f1d1d
                hi = (91, 33, 182)  # #5b21b6
                r = int(lo[0] + (hi[0] - lo[0]) * x)
                g = int(lo[1] + (hi[1] - lo[1]) * x)
                b = int(lo[2] + (hi[2] - lo[2]) * x)
                bg = f"#{r:02x}{g:02x}{b:02x}"
                return f"background-color: {bg}; color: #ffffff; font-weight: 900;"

            styler = (
                df_table.style.format(
                    {
                        "累计带单收益": "{:,.2f}",
                        "ROI": "{:.2f}",
                        "Win rate": "{:.1%}",
                        "Max trade (USDC)": "{:,.2f}",
                    }
                )
                .applymap(_profit_style, subset=["累计带单收益"])
                .applymap(_win_rate_gradient_bg, subset=["Win rate"])
            )
            st.dataframe(styler, use_container_width=True, hide_index=True)

            # Pagination controls (below table)
            p1, p2, p3 = st.columns([1, 1, 4])
            prev_disabled = page <= 1
            next_disabled = page >= total_pages
            if p1.button("← Prev", disabled=prev_disabled, use_container_width=True, key="lb_prev"):
                st.session_state["lb_page"] = max(1, page - 1)
                st.rerun()
            if p2.button("Next →", disabled=next_disabled, use_container_width=True, key="lb_next"):
                st.session_state["lb_page"] = min(total_pages, page + 1)
                st.rerun()
            shown_to = min(end, total_rows) if total_rows else 0
            shown_from = (start + 1) if total_rows else 0
            p3.caption(f"Page {page}/{total_pages} · {PER_PAGE}/page · showing {shown_from}-{shown_to} of {total_rows}")
        else:
            st.info("暂无榜单数据：请先运行 `python -m src.main compute`。")

    elif nav == c["nav_following"]:
        st.subheader(c["page_following_title"])
        st.caption(c["page_following_subtitle"])

        # Default to a local demo identity (less confusing for demos)
        if not st.session_state.get(follow_me_key):
            st.session_state[follow_me_key] = "local"
        # Identity
        with st.expander("Identity (optional)", expanded=False):
            mode = st.selectbox(
                "Who is this watchlist for?",
                options=["Local demo (recommended)", "Wallet address (0x...)"],
                index=0,
                key="following_identity_mode",
            )
            if mode == "Local demo (recommended)":
                st.session_state[follow_me_key] = "local"
                st.caption("Stored in SQLite under `follower_address=local`.")
            else:
                _quick_pick_address(
                    conn,
                    label="Your address",
                    input_key=follow_me_key,
                    pick_key="following_me_quickpick",
                    use_key="following_me_use_quickpick",
                )

        me_follow = _norm_addr(st.session_state.get(follow_me_key, "") or "")
        followees = _fetch_followees(conn, me_follow, limit=500) if me_follow else []

        m1, m2, m3 = st.columns(3)
        m1.metric("Following", len(followees))
        m2.metric("Identity", me_follow if show_full_address else _short_addr(me_follow))
        m3.metric("Feed source", "maker/taker ∈ watchlist")

        _vspace(12)
        st.markdown("#### Add to watchlist")
        add1, add2, add3 = st.columns([2, 2, 1])
        known_add = _load_known_addresses(conn, limit=50)
        pick_add = add1.selectbox("Pick from leaderboard", options=["(none)"] + known_add, index=0)
        manual_add = add2.text_input("Or paste address", value="", placeholder="0x...", key="following_manual_add")
        if add3.button("Follow", type="primary", use_container_width=True, disabled=not bool(me_follow)):
            cand = manual_add.strip() if manual_add.strip() else pick_add
            if cand and cand != "(none)":
                _follow(conn, follower=me_follow, followee=cand)
                st.rerun()

        _vspace(14)
        st.markdown("#### 巨鲸列表 (Watchlist)")
        if not me_follow:
            st.info("Set an identity first.")
        elif not followees:
            st.write("(empty) Follow someone from Leaderboard / Discover, or add above.")
        else:
            if len(followees) <= 1:
                show_n = len(followees)
            else:
                show_n = st.slider(
                    "Show cards",
                    min_value=1,
                    max_value=min(50, len(followees)),
                    value=min(12, len(followees)),
                    step=1,
                    key="following_show_cards",
                )
            cols = st.columns(2)
            for i, a in enumerate(followees[: int(show_n)]):
                with cols[i % 2]:
                    s_stats, s_tags = _fetch_profile_stats(conn, a)
                    sectors = _top_sectors_for_address(conn, a, recent_trades=800, top_k=3) if s_stats is not None else []
                    sector_tags = [f"Sector: {s}" for s in (sectors or [])[:2]]
                    style_tags = _dating_tags(s_stats, s_tags) if s_stats is not None else []
                    card_tags = (style_tags[:6] + sector_tags)[:8]
                    tags_html = "".join([f'<span class="tag">{_esc(t)}</span>' for t in card_tags])

                    profit_raw = s_stats["total_profit"] if s_stats is not None else None
                    profit_class = "neu"
                    if profit_raw is not None:
                        try:
                            profit_int = int(profit_raw)
                            if profit_int > 0:
                                profit_class = "pos"
                            elif profit_int < 0:
                                profit_class = "neg"
                        except Exception:
                            profit_class = "neu"

                    kv_html = (
                        f'<div class="kv-grid">'
                        f'<div class="kv"><div class="kv-label">Profit (USDC)</div><div class="kv-value {profit_class}">{_esc(_fmt_usdc(s_stats["total_profit"] if s_stats is not None else None))}</div></div>'
                        f'<div class="kv"><div class="kv-label">ROI</div><div class="kv-value">{_esc(_fmt_pct(_safe_float(s_stats["roi"]) if s_stats is not None else None))}</div></div>'
                        f'<div class="kv"><div class="kv-label">Win rate</div><div class="kv-value">{_esc(_fmt_pct(_safe_float(s_stats["win_rate"]) if s_stats is not None else None))}</div></div>'
                        f'<div class="kv"><div class="kv-label">Markets</div><div class="kv-value">{_esc(_safe_int(s_stats["markets_traded"]) if s_stats is not None else 0)}</div></div>'
                        f'<div class="kv"><div class="kv-label">Trades</div><div class="kv-value">{_esc(_safe_int(s_stats["trades_count"]) if s_stats is not None else 0)}</div></div>'
                        f'<div class="kv"><div class="kv-label">BUY %</div><div class="kv-value">{_esc(_fmt_pct(_buy_ratio(conn, a, limit=200) if s_stats is not None else None))}</div></div>'
                        f'<div class="kv"><div class="kv-label">Max trade</div><div class="kv-value">{_esc(_fmt_usdc(s_stats["max_trade_usdc"] if s_stats is not None else None))}</div></div>'
                        f'<div class="kv"><div class="kv-label">Top sector</div><div class="kv-value">{_esc(sectors[0] if sectors else "n/a")}</div></div>'
                        f'<div class="kv"><div class="kv-label">PFP</div><div class="kv-value">on</div></div>'
                        f"</div>"
                    )
                    persona = ""
                    if s_stats is not None:
                        try:
                            persona = _generate_style_sentence(a, s_stats, s_tags, nonce=0, persona_tone="normal")
                        except Exception:
                            persona = ""

                    alias = _cred_alias(a)
                    handle = _short_addr(a, n=6) if not show_full_address else a
                    card = (
                        f'<div class="dating-card">'
                        f'<div class="dating-header">{_pfp_html(a)}'
                        f'<div style="min-width:0">'
                        f'<div class="dating-title"><span class="mono">{_esc(handle)}</span><span class="alias">· {_esc(alias)}</span></div>'
                        f'<div class="dating-sub">{_esc(persona)}</div>'
                        f"</div></div>"
                        f"{tags_html}"
                        f"{kv_html}"
                        f"</div>"
                    )
                    st.markdown(card, unsafe_allow_html=True)

                    b1, b2 = st.columns(2)
                    if b1.button("查看 Proof (JSON)", use_container_width=True, key=f"following_card_proof_{me_follow}_{a}"):
                        st.session_state["following_proof_addr"] = a
                        st.session_state["following_proof_from"] = c["nav_following"]
                        st.rerun()
                    if b2.button("进入 Profile", use_container_width=True, key=f"following_card_profile_{me_follow}_{a}"):
                        st.session_state["route_prev"] = st.session_state.get(route_key, c["nav_following"])
                        st.session_state[route_key] = c["nav_profile"]
                        st.session_state[profile_address_key] = a
                        st.rerun()
                    if st.button("Unfollow", type="secondary", use_container_width=True, key=f"following_card_unfollow_{me_follow}_{a}"):
                        _unfollow(conn, follower=me_follow, followee=a)
                        st.rerun()

                    # Inline proof panel for the last clicked address
                    if st.session_state.get("following_proof_addr") == a:
                        recent = _fetch_recent_trades_for_address(conn, a, limit=1)
                        if recent:
                            tx = str(recent[0].get("tx_hash") or "")
                            st.caption(f"Latest proof · tx `{_short_addr(tx, n=10)}`")
                            decoded = recent[0].get("decoded_json")
                            if decoded:
                                try:
                                    st.json(json.loads(str(decoded)))
                                except Exception:
                                    st.code(str(decoded))
                        else:
                            st.caption("(no trades found for proof)")
                    _vspace(14)

        _vspace(10)
        st.markdown("#### 最近成交动态 (Whale feed)")
        if not me_follow:
            st.info("Set an identity first.")
        elif not followees:
            st.info("Your watchlist is empty. Add some followees above.")
        else:
            f1, f2, f3 = st.columns([2, 1, 1])
            with f1:
                followee_filter = st.selectbox("Followee", options=["(all)"] + followees, index=0)
            with f2:
                side_filter = st.selectbox("Side", options=["(all)", "BUY", "SELL"], index=0)
            with f3:
                feed_limit = st.slider("Feed size", min_value=10, max_value=200, value=50, step=10)

            view_mode = st.radio(
                "View",
                options=["Notes", "Log", "Cards", "Table"],
                horizontal=True,
                index=0,
                key="following_feed_view",
            )

            feed = _fetch_follow_feed(
                conn,
                me_follow,
                limit=int(feed_limit),
                followee=(None if followee_filter == "(all)" else str(followee_filter)),
                side=(None if side_filter == "(all)" else str(side_filter)),
            )
            if not feed:
                st.write("(no trades found yet — make sure you have backfilled trades)")
            else:
                if view_mode == "Notes":
                    for t in feed[: int(feed_limit)]:
                        followee = str(_row_get(t, "followee", "") or "")
                        slug = str(_row_get(t, "slug", "n/a") or "n/a")
                        question = str(_row_get(t, "question", "") or "")
                        market = question.strip() if question.strip() else slug
                        amount = _fmt_usdc(_row_get(t, "collateral_amount"))
                        tx = str(_row_get(t, "tx_hash", "") or "")

                        with st.chat_message("user"):
                            st.markdown(
                                f"博主 **{_short_addr(followee, n=4)}** 刚刚发布了一笔新『笔记』：在 **{_esc(market)}** 投入了 **{amount} USDC**。",
                                unsafe_allow_html=True,
                            )
                            if st.button(
                                "查看原始凭证",
                                key=f"following_proof_btn_{tx}",
                                use_container_width=False,
                            ):
                                st.markdown(f"[Verify on Explorer]({_polygonscan_tx_url(tx)})")
                    _vspace(8)
                elif view_mode == "Log":
                    lines = []
                    for t in feed[: int(feed_limit)]:
                        ts = _safe_int(_row_get(t, "timestamp"), 0)
                        hhmmss = _fmt_hms_from_ts(ts)
                        followee = str(_row_get(t, "followee", "") or "")
                        side = str(_row_get(t, "side", "") or "")
                        question = str(_row_get(t, "question", "") or "")
                        slug = str(_row_get(t, "slug", "n/a") or "n/a")
                        title = question.strip() if question.strip() else slug
                        collateral = _fmt_usdc(_row_get(t, "collateral_amount"))
                        emoji = "🟢" if side == "BUY" else ("🔴" if side == "SELL" else "🟣")
                        whale = "🐋"
                        addr_disp = _short_addr(followee, n=4)
                        lines.append(f"[{hhmmss}] {whale} {addr_disp} 刚刚 {emoji} {side or 'TRADE'} 了 “{title}” | {collateral} USDC")
                    st.markdown(f"<div class='log-box'>{_esc(chr(10).join(lines))}</div>", unsafe_allow_html=True)
                elif view_mode == "Cards":
                    for t in feed:
                        tx = str(_row_get(t, "tx_hash", "") or "")
                        followee = str(_row_get(t, "followee", "") or "")
                        side = str(_row_get(t, "side", "") or "")
                        market_id = str(_row_get(t, "market_id", "") or "")
                        slug = str(_row_get(t, "slug", "n/a") or "n/a")
                        question = str(_row_get(t, "question", "") or "")
                        outcome = str(_row_get(t, "outcome_label", "n/a") or "n/a")
                        sector = _sector_for_market_text(f"{slug} {question}")
                        pm_url = _polymarket_market_url(slug)
                        collateral = _fmt_usdc(_row_get(t, "collateral_amount"))
                        price = _row_get(t, "price")
                        side_cls = "pill-buy" if side == "BUY" else ("pill-sell" if side == "SELL" else "")
                        side_pill = f'<span class="pill {side_cls}">{_esc(side or "n/a")}</span>'
                        sector_pill = f'<span class="pill">{_esc(sector)}</span>'
                        if pm_url:
                            market_html = f'<a href="{pm_url}" target="_blank">{_esc(slug)}</a>'
                        else:
                            market_html = _esc(slug)
                        card = (
                            f'<div class="feed-card">'
                            f'<div class="feed-header">{_pfp_html(followee)}'
                            f'<div style="min-width:0">'
                            f'<div class="feed-title">{_esc(followee if show_full_address else _short_addr(followee))} {side_pill} {sector_pill}</div>'
                            f'<div class="feed-sub">block {_esc(_row_get(t, "block_number"))} · collateral { _esc(collateral) } USDC · price {_esc(price)}</div>'
                            f"</div></div>"
                            f'<div style="margin-top:0.45rem">'
                            f"<div><b>market</b>: {market_html} · <b>market_id</b>: {_esc(market_id or 'n/a')} · <b>outcome</b>: {_esc(outcome)}</div>"
                            f"</div>"
                            f'<div class="muted" style="margin-top:0.55rem">'
                            f'<a href="{_polygonscan_tx_url(tx)}" target="_blank">tx {_esc(_short_addr(tx, n=10))}</a>'
                            f"</div>"
                            f"</div>"
                        )
                        st.markdown(card, unsafe_allow_html=True)
                        _vspace(10)
                else:
                    rows = []
                    for t in feed:
                        tx = str(_row_get(t, "tx_hash", "") or "")
                        followee = str(_row_get(t, "followee", "") or "")
                        market_id = str(_row_get(t, "market_id", "") or "")
                        slug = str(_row_get(t, "slug", "n/a") or "n/a")
                        question = str(_row_get(t, "question", "") or "")
                        outcome = str(_row_get(t, "outcome_label", "n/a") or "n/a")
                        sector = _sector_for_market_text(f"{slug} {question}")
                        pm_url = _polymarket_market_url(slug)
                        rows.append(
                            {
                                "block": _row_get(t, "block_number"),
                                "followee": followee if show_full_address else _short_addr(followee),
                                "side": _row_get(t, "side"),
                                "sector": sector,
                                "market_id": market_id or "n/a",
                                "slug": slug,
                                "outcome": outcome,
                                "collateral_usdc": _to_usdc(_row_get(t, "collateral_amount")),
                                "price": _row_get(t, "price"),
                                "pm": pm_url if pm_url else "",
                                "tx": _polygonscan_tx_url(tx) if tx else "",
                            }
                        )
                    df_feed = pd.DataFrame(rows)
                    st.dataframe(
                        df_feed,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "pm": st.column_config.LinkColumn("pm", display_text="market"),
                            "tx": st.column_config.LinkColumn("tx", display_text="view"),
                        },
                    )

    elif nav == c["nav_profile"]:
        back_col1, back_col2 = st.columns([1, 5])
        if back_col1.button("← Back", use_container_width=True, key="profile_back"):
            st.session_state[route_key] = st.session_state.get("route_prev", c["nav_lb"])
            st.rerun()
        _vspace(8)

        st.subheader(c["page_profile_title"])
        st.caption(c["page_profile_subtitle"])
        address = st.text_input(
            "输入地址（0x...）",
            placeholder="0x1234...abcd",
            key=profile_address_key,
        )

        if address.strip():
            stats, tags, pnl_rows, trade_rows = _fetch_profile(conn, address)

            if stats is None:
                st.warning(c["hint_zero"])
            else:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total Profit (USDC)", f"{_to_usdc(stats['total_profit']):,.2f}")
                c2.metric("ROI", f"{(stats['roi'] * 100):.2f}%" if stats["roi"] is not None else "n/a")
                c3.metric("Win rate", f"{(stats['win_rate'] * 100):.1f}%" if stats["win_rate"] is not None else "n/a")
                c4.metric("Markets traded", int(stats["markets_traded"]))
                _vspace(16)

                addr = stats["address"]
                st.caption(f"Polygonscan: `{addr}` · [{_short_addr(addr, n=10)}]({_polygonscan_addr_url(addr)})")
                _vspace(10)

                if tags:
                    st.markdown("#### Tags")
                    st.markdown(
                        " ".join([f'<span class="pill">{t}</span>' for t in tags]),
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("Tags: (none)")

                sectors = _top_sectors_for_address(conn, addr, recent_trades=800, top_k=5)
                if sectors:
                    _vspace(8)
                    st.markdown("#### Market sectors (heuristic)")
                    st.markdown(
                        " ".join([f'<span class="pill">{_esc(s)}</span>' for s in sectors]),
                        unsafe_allow_html=True,
                    )

                _vspace(16)

                st.markdown("#### 交易风格生成器")
                st.caption("基于链上统计生成的风格摘要。")
                nonce = 0
                persona = _generate_style_sentence(
                    stats["address"], stats, tags, nonce=nonce, persona_tone="normal"
                )
                st.markdown(f'<div class="card">{persona}</div>', unsafe_allow_html=True)
                _vspace(10)

                with st.expander("判定依据"):
                    style = _classify_style(stats, tags)
                    st.write(
                        {
                            "risk": style.get("risk"),
                            "tempo": style.get("tempo"),
                            "edge": style.get("edge"),
                            "volume": style.get("volume"),
                            "pnl": style.get("pnl"),
                            "roi": stats["roi"],
                            "win_rate": stats["win_rate"],
                            "trades_count": int(stats["trades_count"]),
                            "markets_traded": int(stats["markets_traded"]),
                            "max_trade_usdc": _to_usdc(int(stats["max_trade_usdc"])),
                            "total_cost_usdc": _to_usdc(int(stats["total_cost"])),
                            "total_profit_usdc": _to_usdc(int(stats["total_profit"])),
                            "tags": tags,
                        }
                    )

                if pnl_rows:
                    st.markdown("#### Top resolved markets (PnL)")
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

                st.markdown(f"#### {c['proof_title']} (latest 10)")
                st.caption("数据来源：Polygon `eth_getLogs` + `OrderFilled` ABI 解码（可验证）。")

                for t in trade_rows:
                    tx = t["tx_hash"]
                    title = f"{_short_addr(tx, n=10)}  (block {t['block_number']}, log {t['log_index']})"
                    with st.expander(title):
                        slug = str(_row_get(t, "slug", "n/a") or "n/a")
                        market_id = str(_row_get(t, "market_id", "") or "")
                        pm_url = _polymarket_market_url(slug)
                        st.markdown(
                            f"- **tx**: [`{tx}`]({_polygonscan_tx_url(tx)})\n"
                            f"- **side**: `{t['side']}`\n"
                            f"- **Polymarket market**: {f'[`{slug}`]({pm_url})' if pm_url else f'`{slug}`'}\n"
                            f"- **market_id**: `{market_id or 'n/a'}`\n"
                            f"- **outcome**: `{t['outcome_label']}`\n"
                            f"- **collateral (USDC)**: `{_to_usdc(t['collateral_amount'])}`\n"
                            f"- **price**: `{t['price']}`\n"
                        )
                        try:
                            st.json(json.loads(t["decoded_json"]))
                        except Exception:
                            st.code(t["decoded_json"])

    elif nav == c["nav_dating"]:
        st.subheader(c["page_dating_title"])
        st.caption(c["page_dating_subtitle"])

        # Discover-only frosted-glass styling (scoped)
        st.markdown(
            """
<style>
  /* Frosted glass card (Discover) */
  .discover-glass {
    background: rgba(255, 255, 255, 0.05);
    border: 1px solid rgba(0, 255, 255, 0.20);
    border-radius: 15px;
    padding: 18px 18px;
    box-shadow: 0 4px 30px rgba(0, 0, 0, 0.35);
    backdrop-filter: blur(6px);
  }
  .persona-pill {
    display: inline-block;
    padding: 0.22rem 0.60rem;
    margin: 0 0.40rem 0.40rem 0;
    border-radius: 999px;
    border: 1px solid rgba(0, 255, 255, 0.20);
    background: rgba(0, 255, 255, 0.06);
    color: rgba(229,231,235,0.92);
    font-size: 0.90rem;
    font-weight: 800;
  }
  .bento {
    display: grid;
    grid-template-columns: 1.4fr 1fr;
    gap: 12px;
    margin-top: 12px;
  }
  .tile {
    border: 1px solid #3d4452;
    border-radius: 12px;
    padding: 12px 12px;
    background: rgba(17,24,39,0.45);
    box-shadow: inset 0 0 0 1px rgba(61,68,82,0.30);
  }
  .tile-title { color: rgba(229,231,235,0.70); font-size: 0.78rem; margin-bottom: 0.25rem; }
  .tile-big { font-size: 1.55rem; font-weight: 900; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
  .tile-sub { color: rgba(229,231,235,0.70); margin-top: 0.25rem; font-size: 0.85rem; }
  .tile-pos { background: rgba(34, 197, 94, 0.10); border-color: rgba(34, 197, 94, 0.25); }
  .tile-neg { background: rgba(239, 68, 68, 0.10); border-color: rgba(239, 68, 68, 0.25); }
  .hash-tag { color: rgba(167, 139, 250, 0.95); font-weight: 900; }
</style>
            """,
            unsafe_allow_html=True,
        )

        # One-tap demo: jump to a high-signal profile
        top_row = st.columns([1, 3, 3])
        if top_row[0].button("✨ Surprise Me", use_container_width=True, key="discover_surprise_me"):
            rows = conn.execute(
                """
                SELECT address, total_profit, roi, win_rate, trades_count, markets_traded
                FROM user_stats
                ORDER BY total_profit DESC
                LIMIT 1200
                """
            ).fetchall()
            pool = []
            for r in rows:
                if not r:
                    continue
                s = _global_signal_score(r)
                if s >= 80:
                    pool.append(str(r["address"]))
            if not pool and rows:
                pool = [str(r["address"]) for r in rows[:200] if r and r["address"]]
            if pool:
                chosen = random.choice(pool)
                st.session_state["dating_profile_jump"] = chosen
                st.rerun()

        follower_id = _norm_addr(st.session_state.get(follow_me_key, "") or "local")

        # Shuffle: sample 10 targets (score > 60, not already followed)
        if "discover_targets" not in st.session_state:
            st.session_state["discover_targets"] = []
        if "discover_target_idx" not in st.session_state:
            st.session_state["discover_target_idx"] = 0

        if top_row[1].button("✨ 换一批潜在目标", use_container_width=True, key="discover_shuffle"):
            followees = set(_fetch_followees(conn, follower_id, limit=2000)) if follower_id else set()
            rows = conn.execute(
                """
                SELECT address, total_profit, total_cost, roi, win_rate, trades_count, markets_traded, max_trade_usdc
                FROM user_stats
                ORDER BY total_profit DESC
                LIMIT 2000
                """
            ).fetchall()
            pool = []
            for r in rows:
                if not r:
                    continue
                a = str(r["address"] or "").strip().lower()
                if not a or a in EXCLUDED_ADDRESSES:
                    continue
                if follower_id and a == follower_id:
                    continue
                if a in followees:
                    continue
                if _global_signal_score(r) >= 60:
                    pool.append(a)
            random.shuffle(pool)
            st.session_state["discover_targets"] = pool[:10]
            st.session_state["discover_target_idx"] = 0
            try:
                st.toast("已刷新 10 个潜在目标", icon="✨")
            except Exception:
                pass
            st.rerun()

        # Optional toast (e.g. follow confirmation)
        toast_msg = st.session_state.pop("discover_toast", None)
        if toast_msg:
            try:
                st.toast(str(toast_msg), icon="✅")
            except Exception:
                pass

        # Auto-initialize targets (first load)
        if not st.session_state["discover_targets"]:
            try:
                st.session_state["discover_toast"] = "点击 ✨ 换一批潜在目标 开始发现"
            except Exception:
                pass

        idx = _safe_int(st.session_state.get("discover_target_idx", 0))
        targets = st.session_state.get("discover_targets") or []
        if not targets:
            st.info("还没有目标卡片。点上方 “✨ 换一批潜在目标”。")
        elif idx >= len(targets):
            st.success("这一批刷完了。点 “✨ 换一批潜在目标”。")
        else:
            addr = str(targets[idx])
            stats, _tags = _fetch_profile_stats(conn, addr)
            score = _global_signal_score(stats) if stats is not None else 0
            handle = _short_addr(addr, n=6) if not show_full_address else addr
            alias = _cred_alias(addr)
            personas = _discover_personas(conn, addr, stats)
            intro = _blogger_intro(stats)

            buy_pct = _buy_ratio(conn, addr, limit=250)
            avg_size = _avg_trade_size_usdc(conn, addr, limit=250)
            sectors = _top_sectors_for_address(conn, addr, recent_trades=600, top_k=3) if stats is not None else []
            profit_usdc = _to_usdc(_safe_int(stats["total_profit"], 0)) if stats is not None else None
            profit_tile_cls = "tile-pos" if (profit_usdc is not None and profit_usdc > 0) else ("tile-neg" if (profit_usdc is not None and profit_usdc < 0) else "")

            # Card (social-note style): container + intro + bento
            with st.container(border=True):
                st.markdown(
                    f"""
<div class="discover-glass">
  <div style="display:flex; align-items:center; gap:12px;">
    {_pfp_html(addr)}
    <div style="min-width:0;">
      <div style="font-weight:900; font-size:1.15rem;">
        <span class="mono">{_esc(handle)}</span>
        <span class="alias">· {_esc(alias)}</span>
        <span class="muted" style="margin-left:0.6rem;">Signal {score}</span>
      </div>
      <div style="margin-top:0.55rem;">
        {" ".join([f'<span class="persona-pill">{_esc(p)}</span>' for p in personas])}
      </div>
      <div class="muted" style="margin-top:0.45rem;">{_esc(intro)}</div>
    </div>
  </div>
  <div class="bento">
    <div class="tile {profit_tile_cls}">
      <div class="tile-title">PnL</div>
      <div class="tile-big">{_esc("n/a" if profit_usdc is None else f"{profit_usdc:,.2f} USDC")}</div>
      <div class="tile-sub">ROI: {_esc(_fmt_pct(_safe_float(stats["roi"]) if stats is not None else None))} · Win: {_esc(_fmt_pct(_safe_float(stats["win_rate"]) if stats is not None else None))}</div>
    </div>
    <div class="tile">
      <div class="tile-title">Behavior</div>
      <div class="tile-sub">BUY%: <span class="mono">{_esc(_fmt_pct(buy_pct))}</span></div>
      <div class="tile-sub">Avg Size: <span class="mono">{_esc("n/a" if avg_size is None else f"{avg_size:,.2f} USDC")}</span></div>
    </div>
    <div class="tile" style="grid-column: 1 / span 2;">
      <div class="tile-title">Specialty</div>
      <div class="tile-sub">
        {" ".join([f'<span class="hash-tag">#{_esc(s)}</span>' for s in (sectors or ["Other"])])}
      </div>
    </div>
  </div>
</div>
                    """,
                    unsafe_allow_html=True,
                )

            _vspace(10)
            with st.expander("🔍 查看链上原始证据"):
                st.caption("每条记录都可点击 Explorer 验证（来源：Polygon `eth_getLogs` 解码）。")
                lines = _fetch_trade_proof_lines(conn, addr, limit=6)
                if not lines:
                    st.write("(no trades found)")
                for it in lines:
                    tx = it["tx"]
                    st.markdown(
                        f"- <span class='muted'>Block #{_esc(it['block'])} | {_esc(it['collateral'])} USDC | Prediction: {_esc(it['prediction'])}</span> · "
                        f"[Verify on Explorer]({_polygonscan_tx_url(tx)})",
                        unsafe_allow_html=True,
                    )

            _vspace(10)
            b1, b2, b3 = st.columns([1, 1, 1])
            if b1.button("⛔ Skip (Next)", use_container_width=True, key=f"discover_skip_{addr}_{idx}"):
                st.session_state["discover_target_idx"] = idx + 1
                st.rerun()
            if b2.button("💜 Follow", type="primary", use_container_width=True, key=f"discover_follow_{addr}_{idx}", disabled=not bool(follower_id)):
                _follow(conn, follower=follower_id, followee=addr)
                _upsert_swipe(conn, from_addr=follower_id, to_addr=addr, action="follow")
                st.session_state["discover_toast"] = "已成功订阅该博主动态"
                try:
                    st.toast("已成功订阅该博主动态", icon="✅")
                except Exception:
                    pass
                st.session_state["discover_target_idx"] = idx + 1
                st.rerun()
            if b3.button("👤 进入 Profile", use_container_width=True, key=f"discover_profile_{addr}_{idx}"):
                st.session_state["dating_profile_jump"] = addr
                st.rerun()

            following_count = len(_fetch_followees(conn, follower_id, limit=500)) if follower_id else 0
            st.caption(f"Batch progress: {idx+1}/{len(targets)} · Following: {following_count}")

        with st.expander("My Follows (from Discover)"):
            if not follower_id:
                st.write("Follow identity missing.")
            else:
                followees = _fetch_followees(conn, follower_id, limit=200)
                if not followees:
                    st.write("(empty) Tap Follow on a card above.")
                else:
                    st.caption("Tip: the full trade feed is on the `Following` page.")
                    for a in followees:
                        cols = st.columns([3, 1, 1])
                        cols[0].write(a if show_full_address else _short_addr(a))
                        if cols[1].button("Profile", key=f"dating_follow_profile_{follower_id}_{a}"):
                            st.session_state["dating_profile_jump"] = a
                            st.rerun()
                        if cols[2].button("Unfollow", key=f"dating_unfollow_{follower_id}_{a}"):
                            _unfollow(conn, follower=follower_id, followee=a)
                            st.rerun()

    elif nav == c["nav_about"]:
        st.subheader(c["page_about_title"])
        st.caption(c["page_about_subtitle"])
        st.markdown(
            """
## What this is

This project turns Polymarket (Polygon) on-chain `OrderFilled` events into:

- a **local SQLite** dataset you can regenerate,
- a **FastAPI** query layer,
- and a **Streamlit** UI for browsing leaderboards, profiles, and proof.

## Data sources
- **Trades (required)**: Polygon RPC `eth_getLogs` from Polymarket CTF Exchange contracts.
- **Market metadata (optional)**: Polymarket Gamma API for `tokenId → market` mapping and basic resolved-outcome inference.

## Notes / limitations
- PnL is a **best-effort MVP** realized PnL approximation (resolved markets only).
- Like/Pass in Dating is **local-only** and not persisted on-chain.
            """.strip()
        )
    else:
        st.info("Select a page from the sidebar.")

    conn.close()


if __name__ == "__main__":
    main()
