"""
Two-phase trade records for web + logs.

1) On entry: one row — bet side, spot at order, 持仓 / 持仓中, unrealized PnL.
2) On settlement: fill Chainlink open/close, set 押中/未中, final PnL.
"""
from __future__ import annotations

import time
from typing import Any, Dict, Optional

ENTRY_REASON_LABELS: Dict[str, str] = {
    "normal": "",
    "flip_reverse": "翻转补单",
    "second_entry": "第二单",
    "recovery": "回补",
}


def make_record_key(
    market_slug: str, entry_reason: Optional[str], entry_time: float
) -> str:
    """Unique row id: one open/closed record per entry (normal + flip_reverse)."""
    slug = str(market_slug or "").strip()
    er = (entry_reason or "normal").strip() or "normal"
    ts = int(float(entry_time or 0) * 1000)
    return f"{slug}#{er}#{ts}" if ts > 0 else f"{slug}#{er}"


def record_row_key(row: Dict[str, Any]) -> str:
    rk = row.get("record_key")
    if rk:
        return str(rk)
    return make_record_key(
        str(row.get("market_slug") or ""),
        row.get("entry_reason"),
        float(row.get("entry_time") or 0),
    )


def entry_label_for(entry_reason: Optional[str]) -> str:
    r = (entry_reason or "normal").strip() or "normal"
    return ENTRY_REASON_LABELS.get(r, r if r not in ("normal", "") else "")


def apply_entry_labels(record: Dict[str, Any]) -> None:
    er = record.get("entry_reason") or "normal"
    record["entry_reason"] = er
    record["entry_label"] = entry_label_for(er)


def build_open_record(
    *,
    market_slug: str,
    coin: str,
    bet_side: str,
    spot_at_entry: float,
    token_ask: float,
    contracts: float,
    size_usd: float,
    entry_time: float,
    entry_timestamp: str,
    unrealized_pnl: float,
    up_shares: float = 0,
    down_shares: float = 0,
    total_cost: float = 0,
    entry_reason: str = "normal",
    up_ask_at_entry: Optional[float] = None,
    down_ask_at_entry: Optional[float] = None,
    window_range_high: Optional[float] = None,
    window_range_low: Optional[float] = None,
) -> Dict[str, Any]:
    """Phase 1 — created at order fill."""
    ts = float(entry_time or time.time())
    er = (entry_reason or "normal").strip() or "normal"
    rec = {
        "record_key": make_record_key(market_slug, er, ts),
        "market_slug": market_slug,
        "coin": (coin or "btc").lower(),
        "bet_side": bet_side,
        "spot_at_entry": round(float(spot_at_entry), 2) if spot_at_entry else 0,
        "token_ask": round(float(token_ask), 4) if token_ask else 0,
        "entry_ask": round(float(token_ask), 4) if token_ask else 0,
        "up_ask_at_entry": round(float(up_ask_at_entry), 4) if up_ask_at_entry is not None else None,
        "down_ask_at_entry": round(float(down_ask_at_entry), 4) if down_ask_at_entry is not None else None,
        "contracts": round(float(contracts), 4),
        "size_usd": round(float(size_usd), 2),
        "total_cost": round(float(total_cost or size_usd), 2),
        "up_shares": round(float(up_shares), 4),
        "down_shares": round(float(down_shares), 4),
        "entry_time": ts,
        "entry_timestamp": entry_timestamp or time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(ts)
        ),
        "close_time": ts,
        "is_open": True,
        "bet_result_label": "持仓",
        "exit_label": "持仓中",
        "exit_reason": None,
        "exit_type": None,
        "bet_won": None,
        "settlement_winner": None,
        "settlement_pending": True,
        "spot_start": 0,
        "spot_end": 0,
        "btc_start": 0,
        "btc_final": 0,
        "btc_end": 0,
        "pnl": round(float(unrealized_pnl), 2),
        "pnl_usd": round(float(unrealized_pnl), 2),
        "price_source": "pending",
        "entry_reason": er,
        "entry_label": entry_label_for(er),
        "first_leg_ask_max": None,
        "second_entry_ask_threshold": None,
        "second_entry_would_trigger_ask": None,
        "window_range_high": (
            round(float(window_range_high), 2)
            if window_range_high is not None and float(window_range_high) > 0
            else None
        ),
        "window_range_low": (
            round(float(window_range_low), 2)
            if window_range_low is not None and float(window_range_low) > 0
            else None
        ),
    }
    return rec


def apply_first_leg_ask_analytics(
    record: Dict[str, Any],
    *,
    first_leg_ask_max: float,
    second_entry_ask_threshold: float = 0,
    hedge_ask_min: Optional[float] = None,
    hedge_ask_threshold: float = 0,
) -> None:
    """Stamp first-leg direction ask peak (live ask after first fill → window end)."""
    er = (record.get("entry_reason") or "normal").strip()
    if er in ("second_entry", "flip_reverse"):
        return
    if first_leg_ask_max and float(first_leg_ask_max) > 0:
        record["first_leg_ask_max"] = round(float(first_leg_ask_max), 4)
    thr = float(second_entry_ask_threshold or 0)
    record["second_entry_ask_threshold"] = round(thr, 4) if thr > 0 else None
    hthr = float(hedge_ask_threshold or 0)
    if hthr <= 0 and thr > 0:
        hthr = max(0.01, round(1.0 - thr, 4))
    if hthr > 0:
        record["second_entry_hedge_ask_threshold"] = round(hthr, 4)
    if hedge_ask_min is not None and float(hedge_ask_min) > 0:
        record["second_entry_hedge_ask_min"] = round(float(hedge_ask_min), 4)
    if hedge_ask_min is not None and hthr > 0:
        record["second_entry_would_trigger_ask"] = bool(
            float(hedge_ask_min) < hthr
        )


def refresh_open_unrealized(
    record: Dict[str, Any],
    *,
    unrealized_pnl: float,
    up_shares: float,
    down_shares: float,
    total_cost: float,
) -> None:
    """Update floating PnL while position is open."""
    if not record.get("is_open"):
        return
    record["pnl"] = round(float(unrealized_pnl), 2)
    record["pnl_usd"] = record["pnl"]
    record["up_shares"] = round(float(up_shares), 4)
    record["down_shares"] = round(float(down_shares), 4)
    record["total_cost"] = round(float(total_cost), 2)


def finalize_settlement(
    record: Dict[str, Any],
    *,
    spot_start: float,
    spot_end: float,
    settlement_winner: str,
) -> Dict[str, Any]:
    """Phase 2 — after market ends (Chainlink from Gamma)."""
    side = record.get("bet_side")
    winner = settlement_winner if settlement_winner in ("UP", "DOWN") else None
    ptb = float(spot_start or 0)
    fp = float(spot_end or 0)
    up_sh = float(record.get("up_shares") or 0)
    down_sh = float(record.get("down_shares") or 0)
    total_cost = float(record.get("total_cost") or record.get("size_usd") or 0)

    payout = 0.0
    if winner == "UP":
        payout = up_sh * 1.0
    elif winner == "DOWN":
        payout = down_sh * 1.0
    pnl = payout - total_cost

    bet_won = side in ("UP", "DOWN") and winner in ("UP", "DOWN") and side == winner

    record["is_open"] = False
    record["settlement_pending"] = False
    record["settlement_winner"] = winner
    record["winner"] = winner
    record["spot_start"] = round(ptb, 2) if ptb > 0 else 0
    record["spot_end"] = round(fp, 2) if fp > 0 else 0
    record["btc_start"] = record["spot_start"]
    record["btc_final"] = record["spot_end"]
    record["btc_end"] = record["spot_end"]
    record["price_source"] = (
        "polymarket_chainlink"
        if ptb > 0 and fp > 0
        else "polymarket_gamma"
        if winner in ("UP", "DOWN")
        else "pending"
    )
    record["exit_reason"] = "settlement"
    record["exit_type"] = "settlement"
    record["exit_label"] = "到期结算"
    record["bet_won"] = bet_won
    record["bet_result"] = "win" if bet_won else "loss"
    record["bet_result_label"] = "押中" if bet_won else "未中"
    record["bet_result_source"] = "polymarket_gamma"
    record["payout"] = round(payout, 2)
    record["pnl"] = round(pnl, 2)
    record["pnl_usd"] = record["pnl"]
    record["close_time"] = time.time()
    record["close_timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return record


def finalize_early_exit(
    record: Dict[str, Any],
    *,
    exit_reason: str,
    pnl: float,
    payout: float,
) -> Dict[str, Any]:
    """Early exit — realized PnL now; 标的起/止 filled later at settlement."""
    labels = {
        "stop_loss": "止损 stop_loss",
        "flip_stop": "翻转止损 flip_stop",
        "early_exit": "提前平仓",
    }
    record["is_open"] = False
    record["exit_reason"] = exit_reason
    record["exit_type"] = "early_exit"
    record["exit_label"] = labels.get(exit_reason, exit_reason or "提前平仓")
    record["bet_result_label"] = record.get("bet_result_label") or "—"
    record["bet_won"] = None
    record["settlement_pending"] = True
    record["payout"] = round(float(payout), 2)
    record["pnl"] = round(float(pnl), 2)
    record["pnl_usd"] = record["pnl"]
    record["close_time"] = time.time()
    record["close_timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
    return record


def apply_chainlink_labels(
    record: Dict[str, Any],
    *,
    spot_start: float,
    spot_end: float,
    settlement_winner: str,
) -> Dict[str, Any]:
    """After early exit: fill 标的起/止 + 押注结果; keep realized PnL from exit."""
    side = record.get("bet_side")
    winner = settlement_winner if settlement_winner in ("UP", "DOWN") else None
    ptb = float(spot_start or 0)
    fp = float(spot_end or 0)
    prev_start = float(record.get("spot_start") or record.get("btc_start") or 0)
    prev_end = float(record.get("spot_end") or record.get("btc_final") or 0)
    if ptb > 0:
        record["spot_start"] = round(ptb, 2)
    elif prev_start > 0:
        record["spot_start"] = round(prev_start, 2)
    else:
        record["spot_start"] = 0
    if fp > 0:
        record["spot_end"] = round(fp, 2)
    elif prev_end > 0:
        record["spot_end"] = round(prev_end, 2)
    else:
        record["spot_end"] = 0
    record["btc_start"] = record["spot_start"]
    record["btc_final"] = record["spot_end"]
    record["btc_end"] = record["spot_end"]
    complete = record["spot_start"] > 0 and record["spot_end"] > 0
    if complete:
        record["price_source"] = "polymarket_chainlink"
    record["settlement_winner"] = winner
    record["settlement_pending"] = not complete
    if side in ("UP", "DOWN") and winner in ("UP", "DOWN"):
        record["bet_won"] = side == winner
        record["bet_result_label"] = "押中" if record["bet_won"] else "未中"
    return record


def fetch_chainlink_settlement(
    market_slug: str, proxy_url: Optional[str] = None
) -> Dict[str, Any]:
    """Gamma Chainlink: priceToBeat, finalPrice, winner."""
    from polymarket_api import get_official_settlement

    return get_official_settlement(
        market_slug, timeout=8, proxy_url=proxy_url, use_cache=False
    )
