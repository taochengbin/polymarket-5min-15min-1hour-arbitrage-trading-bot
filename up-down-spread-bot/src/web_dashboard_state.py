"""
Thread-safe snapshot + stop request for the web dashboard (same process as the bot).
"""
import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

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


def set_snapshot(data: Dict[str, Any]) -> None:
    """Called from main trading loop (every ~0.1s)."""
    global _snapshot
    with _lock:
        data = dict(data)
        data["_trades_fp"] = _trades_fingerprint(data.get("recent_trades") or [])
        data["updated_at"] = time.time()
        _snapshot = data


def get_snapshot() -> Dict[str, Any]:
    with _lock:
        return dict(_snapshot)


_LIVE_COIN_KEYS = (
    "market_slug",
    "seconds_till_end",
    "up_ask",
    "down_ask",
    "confidence",
    "price",
    "market_start_price",
    "favorite",
    "trading_enabled",
    "trading_reason",
)


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
