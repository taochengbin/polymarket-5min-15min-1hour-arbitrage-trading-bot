"""
Thread-safe snapshot + stop request for the web dashboard (same process as the bot).
"""
import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

_lock = threading.RLock()
_snapshot: Dict[str, Any] = {"status": "initializing"}
_stop_requested = False
_session_start: float = 0.0
_bot_context: Optional[Dict[str, Any]] = None


def set_session_start(ts: float) -> None:
    global _session_start
    with _lock:
        _session_start = ts


def set_bot_context(ctx: Optional[Dict[str, Any]]) -> None:
    """Register live bot handles for /api/trades and manual settlement."""
    global _bot_context
    with _lock:
        _bot_context = dict(ctx) if ctx else None


def get_bot_context() -> Optional[Dict[str, Any]]:
    with _lock:
        return dict(_bot_context) if _bot_context else None


_LIVE_COIN_KEYS = (
    "market_slug",
    "seconds_till_end",
    "up_ask",
    "down_ask",
    "confidence",
    "price",
    "market_start_price",
    "favorite",
)

_CONFIG_COIN_KEYS = ("trading_enabled", "trading_reason")


def _valid_ask(v: Any) -> bool:
    try:
        return float(v or 0) > 0
    except (TypeError, ValueError):
        return False


def _preserve_live_asks(row: Dict[str, Any], prev_row: Dict[str, Any]) -> Dict[str, Any]:
    """Keep previous UP/DN ask when a full snapshot patch has zeros."""
    out = dict(row)
    for key in ("up_ask", "down_ask"):
        if not _valid_ask(out.get(key)) and _valid_ask(prev_row.get(key)):
            out[key] = prev_row[key]
    return out


def set_snapshot(data: Dict[str, Any], *, merge_live_coins: bool = False) -> None:
    """Called from main trading loop."""
    global _snapshot
    with _lock:
        data = dict(data)
        prev_coins = dict((_snapshot or {}).get("coins") or {})
        new_coins = dict(data.get("coins") or {})
        for coin, row in new_coins.items():
            new_coins[coin] = _preserve_live_asks(dict(row), prev_coins.get(coin) or {})
        data["coins"] = new_coins
        data["_trades_fp"] = _trades_fingerprint(data.get("recent_trades") or [])
        data["updated_at"] = time.time()
        _snapshot = data


def get_snapshot() -> Dict[str, Any]:
    with _lock:
        return dict(_snapshot)


def _read_live_state(data_feed: Any, coin: str) -> Optional[Dict[str, Any]]:
    """Prefer lock-free UI cache; fall back to short try_get_state."""
    if hasattr(data_feed, "peek_ui_state"):
        st = data_feed.peek_ui_state(coin)
        if st:
            return st
    if hasattr(data_feed, "try_get_state"):
        return data_feed.try_get_state(coin, lock_timeout=0.25)
    return None


def _tick_wall_clock_row(data_feed: Any, coin: str, row: Dict[str, Any]) -> Dict[str, Any]:
    """Always refresh countdown from wall clock (no markets lock)."""
    out = dict(row)
    if hasattr(data_feed, "_current_slug"):
        out["market_slug"] = data_feed._current_slug(coin)
    if hasattr(data_feed, "_fresh_ste_for_coin"):
        out["seconds_till_end"] = int(data_feed._fresh_ste_for_coin(coin))
    return out


def inject_live_from_data_feed(
    snap: Dict[str, Any], data_feed: Any, coins: List[str]
) -> Dict[str, Any]:
    """Read ask/countdown from data_feed UI cache (non-blocking)."""
    if not data_feed or not coins:
        return snap
    out = dict(snap)
    merged = dict(out.get("coins") or {})
    book_ages: List[float] = []
    for coin in coins:
        prev_row = dict(merged.get(coin) or {})
        st = _read_live_state(data_feed, coin)
        row = dict(prev_row)
        if st:
            if hasattr(data_feed, "peek_book_age_sec"):
                book_ages.append(float(data_feed.peek_book_age_sec(coin)))
            elif hasattr(data_feed, "book_age_sec"):
                book_ages.append(float(data_feed.book_age_sec(coin)))
            ua = float(st.get("up_ask") or 0)
            da = float(st.get("down_ask") or 0)
            if st.get("market_slug"):
                row["market_slug"] = st.get("market_slug")
            row["seconds_till_end"] = int(st.get("seconds_till_end") or 0)
            if _valid_ask(ua):
                row["up_ask"] = ua
            if _valid_ask(da):
                row["down_ask"] = da
            if _valid_ask(row.get("up_ask")) and _valid_ask(row.get("down_ask")):
                row["confidence"] = float(st.get("confidence") or abs(da - ua))
                row["favorite"] = "UP" if float(row["up_ask"]) > float(row["down_ask"]) else "DOWN"
            if st.get("price"):
                row["price"] = float(st.get("price") or 0)
            if st.get("market_start_price"):
                row["market_start_price"] = float(st.get("market_start_price") or 0)
        else:
            row = _tick_wall_clock_row(data_feed, coin, row)
        merged[coin] = row
    out["coins"] = merged
    has_ask = any(
        _valid_ask((merged.get(c) or {}).get("up_ask"))
        and _valid_ask((merged.get(c) or {}).get("down_ask"))
        for c in coins
    )
    if has_ask:
        out["live_feed_ts"] = time.time()
        out["book_age_sec"] = round(max(book_ages), 2) if book_ages else None
    return out


def patch_live_coins(coin_blocks: Dict[str, Any], snapshot_ts: Optional[float] = None) -> None:
    """Refresh ask/countdown only — keep stats & position from last full snapshot."""
    global _snapshot
    ts = snapshot_ts if snapshot_ts is not None else time.time()
    with _lock:
        base = dict(_snapshot) if _snapshot else {"status": "running"}
        prev_coins = dict(base.get("coins") or {})
        merged: Dict[str, Any] = {}
        for coin, live in coin_blocks.items():
            row = dict(prev_coins.get(coin) or {})
            for key in _LIVE_COIN_KEYS:
                if key not in live:
                    continue
                val = live[key]
                if key in ("up_ask", "down_ask") and not _valid_ask(val):
                    if _valid_ask(row.get(key)):
                        continue
                row[key] = val
            for key in _CONFIG_COIN_KEYS:
                if key in live:
                    row[key] = live[key]
            merged[coin] = row
        for coin, row in prev_coins.items():
            if coin not in merged:
                merged[coin] = row
        base["coins"] = merged
        base["snapshot_ts"] = ts
        base["updated_at"] = time.time()
        _snapshot = base


def patch_trading_hours(
    trading_hours: Dict[str, Any], config_path: Optional[Path] = None
) -> None:
    """Refresh trading-hours panel + per-coin 交易状态 from config on disk."""
    global _snapshot
    with _lock:
        base = dict(_snapshot) if _snapshot else {"status": "running"}
        base["trading_hours"] = dict(trading_hours)
        if config_path is not None:
            from web_dashboard.snapshot_builder import apply_trading_status_to_coins

            base["coins"] = apply_trading_status_to_coins(
                base.get("coins") or {}, config_path
            )
        base["updated_at"] = time.time()
        _snapshot = base


def _trades_fingerprint(recent_trades: list) -> str:
    """Stable compare key — ignore sub-cent float noise on open rows."""
    parts = []
    for t in recent_trades or []:
        if not isinstance(t, dict):
            continue
        pnl = t.get("pnl_usd", t.get("pnl"))
        try:
            pnl_f = float(pnl) if pnl is not None else 0.0
        except (TypeError, ValueError):
            pnl_f = 0.0
        if t.get("is_open"):
            pnl_f = round(pnl_f, 1)
        else:
            pnl_f = round(pnl_f, 2)
        parts.append(
            "|".join(
                (
                    str(t.get("market_slug") or ""),
                    str(t.get("is_open")),
                    str(t.get("bet_side") or ""),
                    str(t.get("spot_at_entry") or ""),
                    str(t.get("entry_timestamp") or t.get("entry_time") or ""),
                    str(t.get("spot_start") or ""),
                    str(t.get("spot_end") or ""),
                    str(t.get("exit_label") or ""),
                    str(t.get("bet_result_label") or ""),
                    str(pnl_f),
                )
            )
        )
    return "\n".join(parts)


def patch_recent_trades(recent_trades: list) -> None:
    """Update trade table only — skip if unchanged (avoids UI flicker)."""
    global _snapshot
    with _lock:
        if not _snapshot:
            return
        fp = _trades_fingerprint(recent_trades)
        if _snapshot.get("_trades_fp") == fp:
            return
        base = dict(_snapshot)
        base["recent_trades"] = list(recent_trades)
        base["_trades_fp"] = fp
        base["updated_at"] = time.time()
        _snapshot = base


def request_stop() -> None:
    global _stop_requested
    with _lock:
        _stop_requested = True


def consume_stop_request() -> bool:
    """Main loop: if True, set stop_flag and clear request."""
    global _stop_requested
    with _lock:
        if _stop_requested:
            _stop_requested = False
            return True
        return False


def write_state_file(project_root: Path, data: Dict[str, Any]) -> None:
    """Optional: write logs/bot_state.json for read-only monitoring without shared memory."""
    path = project_root / "logs" / "bot_state.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        payload = dict(data)
        payload["updated_at"] = time.time()
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str)
        tmp.replace(path)
    except OSError:
        pass


def read_state_file(project_root: Path) -> Optional[Dict[str, Any]]:
    path = project_root / "logs" / "bot_state.json"
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
