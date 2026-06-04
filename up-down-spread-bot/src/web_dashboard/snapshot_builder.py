"""
Build JSON-serializable dashboard snapshot from live trading objects.
"""
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from trader import (
    Trader,
    enrich_trade_record,
    _spot_at_entry_from_position,
    _entry_times_from_position,
    _entry_meta_from_position,
    _infer_spot_at_entry_from_trade,
)
from trade_record import apply_entry_labels
from trading_hours import (
    TradingHours,
    dashboard_payload,
    load_from_config_path,
    merge_trading_status,
)


def _hours_for_dashboard(
    config: Dict[str, Any], config_path: Optional[Path] = None
) -> TradingHours:
    if config_path is not None:
        try:
            return load_from_config_path(config_path)
        except OSError:
            pass
    return TradingHours.from_config(config)


def apply_trading_status_to_coins(
    coins: Dict[str, Any], config_path: Path
) -> Dict[str, Any]:
    """Recompute trading_enabled / trading_reason for every coin from config.json."""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except OSError:
        return dict(coins or {})
    hours = _hours_for_dashboard(cfg, config_path)
    out: Dict[str, Any] = {}
    for coin, row in (coins or {}).items():
        merged_row = dict(row)
        en, reason = _trading_flags(cfg, coin, hours=hours)
        merged_row["trading_enabled"] = en
        merged_row["trading_reason"] = reason
        out[coin] = merged_row
    return out


def _trading_flags(
    config: Dict[str, Any], coin: str, hours: Optional[TradingHours] = None
) -> Tuple[bool, str]:
    trading_cfg = config.get("trading", {}).get(coin, {})
    if hours is None:
        hours = TradingHours.from_config(config)
    return merge_trading_status(
        bool(trading_cfg.get("enabled", True)),
        str(trading_cfg.get("reason") or ""),
        hours,
    )


def _trade_sort_ts(trade: Dict) -> float:
    """Merge tie-break: prefer newer close / entry when deduping rows."""
    try:
        return float(trade.get("close_time") or trade.get("entry_time") or 0)
    except (TypeError, ValueError):
        return 0.0


def _entry_sort_ts(trade: Dict) -> float:
    """Web table order: newest order (entry) first."""
    try:
        et = float(trade.get("entry_time") or 0)
        if et > 0:
            return et
    except (TypeError, ValueError):
        pass
    ts = trade.get("entry_timestamp")
    if ts:
        try:
            return datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S").timestamp()
        except ValueError:
            pass
    try:
        return float(trade.get("close_time") or 0)
    except (TypeError, ValueError):
        return 0.0


def _row_score(row: Dict[str, Any]) -> int:
    """Prefer rows with settlement fields, not bare entry journal lines."""
    score = 0
    if not row.get("is_open"):
        score += 16
    if float(row.get("spot_start") or row.get("btc_start") or 0) > 0:
        score += 4
    if float(row.get("spot_end") or row.get("btc_final") or row.get("btc_end") or 0) > 0:
        score += 4
    if row.get("bet_result_label") and row.get("bet_result_label") not in ("持仓", "—", ""):
        score += 4
    if row.get("exit_label") and row.get("exit_label") not in ("持仓中", "—", ""):
        score += 2
    if row.get("settlement_winner") in ("UP", "DOWN"):
        score += 4
    return score


def _merge_trade_row(primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
    """Keep primary; fill missing settlement / price fields from secondary."""
    out = dict(primary)
    skip_zero = {
        "spot_start",
        "btc_start",
        "spot_end",
        "btc_final",
        "btc_end",
        "spot_at_entry",
    }
    for k, v in secondary.items():
        if k == "is_open":
            continue
        if v is None:
            continue
        if k in skip_zero:
            try:
                if float(v or 0) <= 0:
                    continue
            except (TypeError, ValueError):
                continue
        if v == "" or v == "—":
            continue
        if k in ("pnl", "pnl_usd") and not primary.get("is_open"):
            continue
        if k in ("pnl", "pnl_usd") and out.get("is_open") and float(out.get(k) or 0) != 0:
            continue
        if k in ("exit_label", "bet_result_label", "bet_won", "settlement_winner", "exit_reason", "exit_type"):
            if not primary.get("is_open") and primary.get(k) not in (None, "", "—"):
                continue
        if k == "spot_at_entry" and float(primary.get("spot_at_entry") or 0) > 0:
            continue
        if out.get(k) in (None, "", "—", 0, 0.0):
            out[k] = v
    # Primary row wins open/closed; never reopen a settled row via field merge.
    out["is_open"] = bool(primary.get("is_open"))
    return out


def _slug_has_open_position(multi_trader, market_slug: str) -> bool:
    slug = str(market_slug or "")
    if not slug:
        return False
    for tr in multi_trader.traders.values():
        pos = (getattr(tr, "positions", None) or {}).get(slug)
        if not pos or pos.get("status") == "CLOSED":
            continue
        up_sh = float((pos.get("UP") or {}).get("total_shares") or 0)
        down_sh = float((pos.get("DOWN") or {}).get("total_shares") or 0)
        if up_sh > 0 or down_sh > 0:
            return True
    return False


def _window_is_chainlink(w: Dict[str, Any]) -> bool:
    return (w or {}).get("price_source") in ("chainlink", "polymarket_chainlink")


def _row_price_source_chainlink(row: Dict[str, Any]) -> bool:
    ps = (row.get("price_source") or "").strip().lower()
    return ps in ("chainlink", "polymarket_chainlink", "polymarket_gamma")


def _market_start_px(
    coin: str,
    slug: str,
    row: Dict[str, Any],
    market_starts: Optional[Dict[str, Dict[str, float]]],
    market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]],
) -> float:
    """标的起 = Chainlink priceToBeat；不用 CoinGecko / 换盘现价。"""
    if market_windows:
        w = (market_windows.get(coin) or {}).get(slug) or {}
        if _window_is_chainlink(w):
            sp0 = float(w.get("spot_start") or 0)
            if sp0 > 0:
                return sp0
    if _row_price_source_chainlink(row):
        sp0 = float(row.get("spot_start") or row.get("btc_start") or 0)
        if sp0 > 0:
            return sp0
    if row.get("exit_reason") == "settlement":
        sp0 = float(row.get("spot_start") or row.get("btc_start") or 0)
        if sp0 > 0 and (row.get("price_source") or "") != "coingecko_fallback":
            return sp0
    return 0.0


def _market_end_px(
    coin: str,
    slug: str,
    row: Dict[str, Any],
    market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]],
) -> float:
    """标的止 = Chainlink finalPrice；禁止用平仓时 CoinGecko。"""
    if market_windows:
        w = (market_windows.get(coin) or {}).get(slug) or {}
        if _window_is_chainlink(w):
            sp1 = float(w.get("spot_end") or 0)
            if sp1 > 0:
                return sp1
    if _row_price_source_chainlink(row):
        sp1 = float(
            row.get("spot_end") or row.get("btc_final") or row.get("btc_end") or 0
        )
        if sp1 > 0:
            return sp1
    if row.get("exit_reason") == "settlement":
        sp1 = float(
            row.get("spot_end") or row.get("btc_final") or row.get("btc_end") or 0
        )
        if sp1 > 0 and (row.get("price_source") or "") != "coingecko_fallback":
            return sp1
    return 0.0


def _hydrate_row_prices(
    row: Dict[str, Any],
    coin: str,
    data_feed,
    market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
    market_starts: Optional[Dict[str, Dict[str, float]]] = None,
) -> None:
    """标的起/止仅来自 Chainlink 锁定价或已平仓记录，禁止用 feed 现价刷新。"""
    slug = str(row.get("market_slug") or "")
    if not slug or not coin:
        return
    sp0 = _market_start_px(coin, slug, row, market_starts, market_windows)
    if sp0 > 0:
        row["spot_start"] = sp0
        row["btc_start"] = sp0

    sp1 = _market_end_px(coin, slug, row, market_windows)
    if sp1 > 0:
        row["spot_end"] = sp1
        row["btc_final"] = sp1
        row["btc_end"] = sp1

    st = data_feed.get_state(coin) if data_feed else None
    on_live = bool(st and (st.get("market_slug") or "") == slug)

    _apply_settlement_from_spot_prices(row, sp0, sp1, on_live=on_live)


def _apply_settlement_from_spot_prices(
    row: Dict[str, Any],
    sp0: float,
    sp1: float,
    *,
    on_live: bool,
) -> None:
    """仅在有 Chainlink 起止价时补展示；已官方结算的记录不覆盖。"""
    if sp0 <= 0 or sp1 <= 0:
        return
    side = row.get("bet_side")
    if side not in ("UP", "DOWN"):
        return
    if row.get("is_open") and on_live:
        return
    er = row.get("exit_reason") or row.get("exit_type")
    if er in ("stop_loss", "flip_stop", "early_exit"):
        return
    if row.get("bet_result_source") == "polymarket_gamma":
        sw = row.get("settlement_winner") or row.get("winner")
        if sw in ("UP", "DOWN"):
            row["bet_won"] = side == sw
            row["bet_result_label"] = "押中" if row["bet_won"] else "未中"
        return
    if er == "settlement" and row.get("settlement_winner") in ("UP", "DOWN"):
        sw = row["settlement_winner"]
        row["bet_won"] = side == sw
        row["bet_result_label"] = "押中" if row["bet_won"] else "未中"
        row["settlement_pending"] = False
        return
    from spot_price import infer_up_down_winner

    w = infer_up_down_winner(sp0, sp1)
    if not w:
        return
    row["settlement_winner"] = w
    row["bet_won"] = side == w
    row["bet_result_label"] = "押中" if row["bet_won"] else "未中"
    row["settlement_pending"] = False
    if not row.get("exit_reason"):
        row["exit_label"] = "到期结算"
    row["is_open"] = False


def _finalize_row_for_web(
    row: Dict[str, Any],
    coin: str,
    data_feed,
    market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]],
) -> None:
    """持仓行保留标签；已结束盘补全 window 缓存里的标的止与结算展示。"""
    _hydrate_from_window_cache(row, coin, market_windows)
    if row.get("is_open"):
        row["exit_label"] = row.get("exit_label") or "持仓中"
        if row.get("bet_won") is None:
            lbl = row.get("bet_result_label") or ""
            if lbl not in ("押中", "未中", "待结算"):
                row["bet_result_label"] = "持仓"
        st = data_feed.get_state(coin) if data_feed else None
        slug = str(row.get("market_slug") or "")
        on_live = bool(st and (st.get("market_slug") or "") == slug)
        sp0 = float(row.get("spot_start") or row.get("btc_start") or 0)
        sp1 = float(row.get("spot_end") or row.get("btc_final") or 0)
        _apply_settlement_from_spot_prices(row, sp0, sp1, on_live=on_live)
        return
    if not row.get("exit_label") or row.get("exit_label") == "—":
        er = row.get("exit_reason") or row.get("exit_type")
        if er == "settlement":
            row["exit_label"] = "到期结算"
        elif er in ("stop_loss", "flip_stop", "early_exit"):
            row["exit_label"] = {
                "stop_loss": "止损 stop_loss",
                "flip_stop": "翻转止损 flip_stop",
                "early_exit": "提前平仓",
            }.get(er, er or "—")


def _hydrate_from_window_cache(
    row: Dict[str, Any], coin: str, market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]]
) -> None:
    if not market_windows:
        return
    slug = str(row.get("market_slug") or "")
    w = (market_windows.get(coin) or {}).get(slug) or {}
    if float(row.get("spot_start") or row.get("btc_start") or 0) <= 0:
        s = float(w.get("spot_start") or 0)
        if s > 0:
            row["spot_start"] = row["btc_start"] = s
    if float(row.get("spot_end") or row.get("btc_final") or row.get("btc_end") or 0) <= 0:
        e = float(w.get("spot_end") or 0)
        if e > 0:
            row["spot_end"] = row["btc_final"] = row["btc_end"] = e


def _put_trade_by_slug(by_slug: Dict[str, Dict[str, Any]], row: Dict[str, Any]) -> None:
    """One row per entry (record_key); same slug may have normal + flip_reverse."""
    from trade_record import record_row_key

    slug = str(row.get("market_slug") or "")
    if not slug:
        return
    row = dict(row)
    row["is_open"] = bool(row.get("is_open"))
    row_key = record_row_key(row)
    prev = by_slug.get(row_key)
    if prev is None:
        by_slug[row_key] = row
        return
    if prev.get("is_open") and not row.get("is_open"):
        merged = _merge_trade_row(row, prev)
        merged["is_open"] = False
        by_slug[row_key] = merged
        return
    if not prev.get("is_open") and row.get("is_open"):
        return
    if prev.get("is_open") and row.get("is_open"):
        if _row_score(prev) >= _row_score(row):
            by_slug[row_key] = _merge_trade_row(prev, row)
        else:
            by_slug[row_key] = _merge_trade_row(row, prev)
        return
    if _trade_sort_ts(row) >= _trade_sort_ts(prev):
        by_slug[row_key] = _merge_trade_row(row, prev)
    else:
        by_slug[row_key] = _merge_trade_row(prev, row)


def _collect_open_positions(
    multi_trader,
    strategy_base: str,
    coins: List[str],
    data_feed,
) -> List[Dict[str, Any]]:
    """Live positions — shown in web table immediately after entry (before close)."""
    rows: List[Dict[str, Any]] = []
    now = time.time()
    for coin in coins:
        tname = f"{strategy_base}_{coin}"
        tr = multi_trader.traders.get(tname)
        if not tr:
            continue
        positions = dict(getattr(tr, "positions", {}) or {})
        closed_slugs = getattr(tr, "closed_markets", set()) or set()
        for slug, pos in positions.items():
            if slug in closed_slugs:
                continue
            if not pos or pos.get("status") == "CLOSED":
                continue
            up_sh = float((pos.get("UP") or {}).get("total_shares") or 0)
            down_sh = float((pos.get("DOWN") or {}).get("total_shares") or 0)
            if up_sh <= 0 and down_sh <= 0:
                continue
            bet_side = "UP" if up_sh >= down_sh else "DOWN"
            spot_entry = _spot_at_entry_from_position(pos)
            entry_time, entry_timestamp = _entry_times_from_position(pos)
            entry_reason, entry_label = _entry_meta_from_position(pos)
            sort_ts = float(entry_time or now)
            spot_start = float(pos.get("spot_start") or 0) or None
            unreal = 0.0
            if data_feed:
                st = data_feed.get_state(coin)
                if st and (st.get("market_slug") or "") == slug:
                    ua = float(st.get("up_ask") or 0.5)
                    da = float(st.get("down_ask") or 0.5)
                    det = tr.get_market_detailed_stats(slug, ua, da)
                    if det:
                        unreal = float(det.get("unrealized_pnl") or 0)
            row = {
                "market_slug": slug,
                "strategy": tname,
                "coin": coin,
                "bet_side": bet_side,
                "spot_at_entry": spot_entry,
                "entry_time": entry_time,
                "entry_timestamp": entry_timestamp,
                "is_open": True,
                "close_time": sort_ts,
                "exit_label": "持仓中",
                "bet_result_label": "持仓",
                "settlement_pending": False,
                "btc_start": spot_start,
                "spot_start": spot_start,
                "pnl": unreal,
                "pnl_usd": unreal,
                "entry_reason": entry_reason,
                "entry_label": entry_label,
            }
            rows.append(row)
    return rows


def _load_entry_journal(log_dir: Path, strategy_name: str, coin: str) -> List[Dict[str, Any]]:
    """entries.jsonl — latest row per market_slug only."""
    path = log_dir / "entries.jsonl"
    if not path.is_file():
        return []
    by_slug: Dict[str, Dict[str, Any]] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if raw.get("type") != "entry" or not raw.get("market_slug"):
                    continue
                slug = str(raw.get("market_slug"))
                ts = float(raw.get("entry_time") or 0)
                prev = by_slug.get(slug)
                if prev is not None and float(prev.get("entry_time") or 0) > ts:
                    continue
                t = dict(raw)
                t["strategy"] = strategy_name
                t["coin"] = coin
                t["is_open"] = True
                sp = _infer_spot_at_entry_from_trade(t)
                if sp is not None:
                    t["spot_at_entry"] = sp
                t["close_time"] = ts
                t["exit_label"] = t.get("exit_label") or "持仓中"
                t["bet_result_label"] = t.get("bet_result_label") or "持仓"
                t["pnl"] = 0.0
                t["pnl_usd"] = 0.0
                by_slug[slug] = t
    except OSError:
        pass
    return list(by_slug.values())


def _collect_closed_trades(
    multi_trader, *, read_trade_files: bool = True
) -> List[Dict[str, Any]]:
    """Memory closed_trades first, then MySQL (or trades.jsonl fallback)."""
    by_slug: Dict[str, Dict[str, Any]] = {}

    def _merge(name: str, raw: Dict) -> None:
        coin = name.split("_")[-1] if "_" in name else ""
        t = dict(raw)
        t["strategy"] = name
        t["coin"] = coin
        t["is_open"] = False
        _put_trade_by_slug(by_slug, t)

    for name, tr in multi_trader.traders.items():
        for trade in list(getattr(tr, "closed_trades", []) or []):
            _merge(name, trade)
        if read_trade_files:
            from trade_storage import load_persisted_records

            for raw in load_persisted_records(tr, limit=0, config=getattr(tr, "config", None)):
                if "market_slug" not in raw:
                    continue
                slug = str(raw.get("market_slug") or "")
                if slug in by_slug:
                    by_slug[slug] = _merge_trade_row(by_slug[slug], raw)
                    continue
                _merge(name, raw)

    return list(by_slug.values())


def _merge_trade_rows(
    multi_trader,
    strategy_base: str,
    coins: List[str],
    data_feed,
    market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
    market_starts: Optional[Dict[str, Dict[str, float]]] = None,
    limit: Optional[int] = 40,
    read_trade_files: bool = True,
) -> List[Dict[str, Any]]:
    """One row per market_slug from trade_records (MySQL / memory)."""
    by_slug: Dict[str, Dict[str, Any]] = {}

    for name, tr in multi_trader.traders.items():
        coin = name.split("_")[-1] if "_" in name else ""
        if coin not in coins:
            continue
        for _rk, rec in dict(getattr(tr, "open_trade_records", {}) or {}).items():
            row = dict(rec)
            slug = str(row.get("market_slug") or "")
            row["strategy"] = name
            row["coin"] = coin
            if data_feed and row.get("is_open"):
                st = data_feed.get_state(coin)
                if st and (st.get("market_slug") or "") == slug:
                    ua = float(st.get("up_ask") or 0.5)
                    da = float(st.get("down_ask") or 0.5)
                    det = tr.get_market_detailed_stats(slug, ua, da)
                    if det:
                        from trade_record import refresh_open_unrealized

                        refresh_open_unrealized(
                            row,
                            unrealized_pnl=float(det.get("unrealized_pnl") or 0),
                            up_shares=float(det.get("up_shares") or 0),
                            down_shares=float(det.get("down_shares") or 0),
                            total_cost=float(det.get("total_invested") or 0),
                        )
            _put_trade_by_slug(by_slug, row)

        for trade in list(getattr(tr, "closed_trades", []) or []):
            row = dict(trade)
            row["strategy"] = name
            row["coin"] = coin
            row["is_open"] = False
            _put_trade_by_slug(by_slug, row)

        if read_trade_files:
            from trade_storage import load_persisted_records

            for raw in load_persisted_records(tr, limit=0, config=getattr(tr, "config", None)):
                if not raw.get("market_slug"):
                    continue
                raw["strategy"] = name
                raw["coin"] = coin
                _put_trade_by_slug(by_slug, raw)

    recent = sorted(by_slug.values(), key=_entry_sort_ts, reverse=True)
    if limit is not None:
        return recent[:limit]
    return recent


def build_fast_coin_snapshot(
    *,
    coins: List[str],
    config: Dict[str, Any],
    data_feed,
    config_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Lightweight coins block only — no trade merge, no detailed position stats."""
    hours = _hours_for_dashboard(config, config_path)
    coin_blocks: Dict[str, Any] = {}
    for coin in coins:
        st = None
        if data_feed and hasattr(data_feed, "peek_ui_state"):
            st = data_feed.peek_ui_state(coin)
        if not st and data_feed and hasattr(data_feed, "try_get_state"):
            st = data_feed.try_get_state(coin, lock_timeout=0.25)
        t_en, t_rs = _trading_flags(config, coin, hours=hours)
        if not st:
            row: Dict[str, Any] = {
                "trading_enabled": t_en,
                "trading_reason": t_rs,
                "stats": None,
                "position": None,
            }
            if data_feed and hasattr(data_feed, "_fresh_ste_for_coin"):
                row["market_slug"] = data_feed._current_slug(coin)
                row["seconds_till_end"] = int(data_feed._fresh_ste_for_coin(coin))
            coin_blocks[coin] = row
            continue
        ua = float(st.get("up_ask") or 0)
        da = float(st.get("down_ask") or 0)
        row: Dict[str, Any] = {
            "market_slug": st.get("market_slug") or "",
            "seconds_till_end": int(st.get("seconds_till_end") or 0),
            "confidence": float(st.get("confidence") or 0),
            "price": float(st.get("price") or 0),
            "market_start_price": float(st.get("market_start_price") or 0),
            "favorite": "UP" if ua > da else "DOWN",
            "trading_enabled": t_en,
            "trading_reason": t_rs,
            "stats": None,
            "position": None,
        }
        if ua > 0:
            row["up_ask"] = ua
        if da > 0:
            row["down_ask"] = da
        coin_blocks[coin] = row
    return coin_blocks


def _apply_display_labels(rows: List[Dict[str, Any]]) -> None:
    for t in rows:
        try:
            apply_entry_labels(t)
            if t.get("is_open"):
                t["exit_label"] = "持仓中"
                t["bet_result_label"] = "持仓"
            elif not t.get("exit_label"):
                er = t.get("exit_reason")
                if er == "settlement":
                    t["exit_label"] = "到期结算"
                elif er in ("stop_loss", "flip_stop"):
                    t["exit_label"] = {
                        "stop_loss": "止损 stop_loss",
                        "flip_stop": "翻转止损 flip_stop",
                    }.get(er, er)
        except Exception:
            pass


def _trim_rows_for_web(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _spot_px(v):
        if v is None:
            return None
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return None
        if fv == 0.0:
            return None
        return round(fv, 2)

    def _order_spot(t: Dict) -> Optional[float]:
        v = _infer_spot_at_entry_from_trade(t)
        if v is None:
            return None
        try:
            fv = float(v)
        except (TypeError, ValueError):
            return None
        if fv <= 0.0:
            return None
        return round(fv, 2)

    out: List[Dict[str, Any]] = []
    for t in rows:
        coin = t.get("coin") or ""
        spot_sym = coin.upper() or "—"
        pnl = round(float(t.get("pnl_usd", t.get("pnl", 0))), 2)
        bw = t.get("bet_won")
        out.append(
            {
                "strategy": t.get("strategy"),
                "coin": coin,
                "spot_label": spot_sym,
                "market_slug": t.get("market_slug"),
                "bet_side": t.get("bet_side") or "—",
                "entry_ask": round(float(t.get("entry_ask") or t.get("token_ask") or 0), 4) or None,
                "size_usd": round(float(t.get("size_usd") or t.get("total_cost") or 0), 2) or None,
                "up_ask_at_entry": t.get("up_ask_at_entry"),
                "down_ask_at_entry": t.get("down_ask_at_entry"),
                "entry_reason": t.get("entry_reason") or "normal",
                "entry_label": t.get("entry_label") or "",
                "spot_at_entry": _order_spot(t),
                "window_range_high": _spot_px(t.get("window_range_high")),
                "window_range_low": _spot_px(t.get("window_range_low")),
                "is_open": bool(t.get("is_open")),
                "entry_time": float(t.get("entry_time") or 0) or None,
                "entry_timestamp": t.get("entry_timestamp") or "",
                "settlement_winner": t.get("settlement_winner"),
                "settlement_pending": bool(t.get("settlement_pending")),
                "spot_start": _spot_px(t.get("btc_start") or t.get("spot_start")),
                "spot_end": _spot_px(t.get("btc_final") or t.get("spot_end")),
                "exit_label": t.get("exit_label") or "—",
                "bet_result_label": t.get("bet_result_label") or "—",
                "bet_won": bool(bw) if bw is not None else None,
                "pnl_usd": pnl,
                "pnl": pnl,
                "close_time": t.get("close_time"),
            }
        )
    return out


def build_recent_trades_trimmed(
    *,
    multi_trader,
    strategy_base: str,
    coins: List[str],
    data_feed,
    market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
    market_starts: Optional[Dict[str, Dict[str, float]]] = None,
    read_trade_files: bool = True,
    limit: int = 40,
) -> List[Dict[str, Any]]:
    """Merge memory + MySQL into web table rows."""
    recent = _merge_trade_rows(
        multi_trader,
        strategy_base,
        coins,
        data_feed,
        market_windows=market_windows,
        market_starts=market_starts,
        limit=limit,
        read_trade_files=read_trade_files,
    )
    _apply_display_labels(recent)
    return _trim_rows_for_web(recent)


def build_all_trades_trimmed(
    *,
    multi_trader,
    strategy_base: str,
    coins: List[str],
    data_feed,
    market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
    market_starts: Optional[Dict[str, Dict[str, float]]] = None,
    read_trade_files: bool = True,
) -> List[Dict[str, Any]]:
    """All trades (no row cap) for paginated API."""
    recent = _merge_trade_rows(
        multi_trader,
        strategy_base,
        coins,
        data_feed,
        market_windows=market_windows,
        market_starts=market_starts,
        limit=None,
        read_trade_files=read_trade_files,
    )
    _apply_display_labels(recent)
    return _trim_rows_for_web(recent)


def _day_bounds_local(date_str: str) -> Tuple[float, float]:
    """Inclusive local calendar day → unix [start, end)."""
    dt = datetime.strptime(date_str.strip()[:10], "%Y-%m-%d")
    start = dt.timestamp()
    end = start + 86400.0
    return start, end


def filter_trades_by_entry_date(
    rows: List[Dict[str, Any]],
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not date_from and not date_to:
        return list(rows)
    lo = 0.0
    hi = float("inf")
    if date_from:
        lo, _ = _day_bounds_local(date_from)
    if date_to:
        _, hi = _day_bounds_local(date_to)
    out: List[Dict[str, Any]] = []
    for t in rows:
        ts = _entry_sort_ts(t)
        if ts <= 0:
            continue
        if ts >= lo and ts < hi:
            out.append(t)
    return out


def query_trades_paginated(
    *,
    multi_trader,
    strategy_base: str,
    coins: List[str],
    data_feed,
    market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
    market_starts: Optional[Dict[str, Dict[str, float]]] = None,
    page: int = 1,
    page_size: int = 20,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    read_trade_files: bool = True,
) -> Dict[str, Any]:
    all_rows = build_all_trades_trimmed(
        multi_trader=multi_trader,
        strategy_base=strategy_base,
        coins=coins,
        data_feed=data_feed,
        market_windows=market_windows,
        market_starts=market_starts,
        read_trade_files=read_trade_files,
    )
    return _paginate_trade_rows(
        all_rows,
        page=page,
        page_size=page_size,
        date_from=date_from,
        date_to=date_to,
    )


def query_trades_from_db_paginated(
    *,
    config: Dict[str, Any],
    strategy_base: str,
    coins: List[str],
    page: int = 1,
    page_size: int = 20,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Offline/history query directly from MySQL."""
    from trade_storage import db_reads_enabled, load_records_for_strategies

    if not db_reads_enabled(config):
        return None
    names = [f"{strategy_base}_{c}" for c in coins]
    all_rows = load_records_for_strategies(config, names)
    if not all_rows:
        return None
    _apply_display_labels(all_rows)
    trimmed = _trim_rows_for_web(all_rows)
    return _paginate_trade_rows(
        trimmed,
        page=page,
        page_size=page_size,
        date_from=date_from,
        date_to=date_to,
    )


def _paginate_trade_rows(
    all_rows: List[Dict[str, Any]],
    *,
    page: int = 1,
    page_size: int = 20,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Dict[str, Any]:
    filtered = filter_trades_by_entry_date(all_rows, date_from, date_to)
    filtered.sort(key=_entry_sort_ts, reverse=True)
    total = len(filtered)
    page = max(1, int(page or 1))
    page_size = max(1, min(100, int(page_size or 20)))
    start = (page - 1) * page_size
    items = filtered[start : start + page_size]
    total_pages = max(1, (total + page_size - 1) // page_size)
    if page > total_pages:
        page = total_pages
        start = (page - 1) * page_size
        items = filtered[start : start + page_size]
    pending = sum(
        1
        for t in all_rows
        if t.get("is_open") or t.get("settlement_pending")
    )
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "pending_settlement_count": pending,
        "date_from": date_from or "",
        "date_to": date_to or "",
        "source": "mysql",
    }


def build_snapshot(
    *,
    coins: List[str],
    strategy_base: str,
    multi_trader,
    data_feed,
    wallet_balance: Optional[float],
    config: Dict[str, Any],
    config_path: Optional[Path] = None,
    session_start_time: float,
    dry_run: bool,
    markets_skipped: Dict[str, int],
    market_windows: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None,
    market_starts: Optional[Dict[str, Dict[str, float]]] = None,
    read_trade_files: bool = True,
    skip_trade_merge: bool = False,
    recent_trades_cached: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    now = time.time()
    uptime = now - session_start_time

    portfolio = multi_trader.get_portfolio_stats()
    tw = int(portfolio.get("total_wins", 0))
    tl = int(portfolio.get("total_losses", 0))
    tt = int(portfolio.get("total_trades", 0))
    port_wr = round((tw / tt * 100) if tt > 0 else 0.0, 2)
    hours = _hours_for_dashboard(config, config_path)

    coin_blocks: Dict[str, Any] = {}
    for coin in coins:
        trader_name = f"{strategy_base}_{coin}"
        st = data_feed.get_state(coin)
        trader = multi_trader.traders.get(trader_name)

        if not st:
            t_en, t_rs = _trading_flags(config, coin, hours=hours)
            ms = {
                "market_slug": "",
                "seconds_till_end": 0,
                "up_ask": 0.0,
                "down_ask": 0.0,
                "confidence": 0.0,
                "price": 0.0,
                "market_start_price": 0.0,
                "favorite": "—",
                "trading_enabled": t_en,
                "trading_reason": t_rs,
                "stats": None,
                "position": None,
            }
            coin_blocks[coin] = ms
            continue

        ms: Dict[str, Any] = {
            "market_slug": st.get("market_slug") or "",
            "seconds_till_end": int(st.get("seconds_till_end") or 0),
            "up_ask": float(st.get("up_ask") or 0),
            "down_ask": float(st.get("down_ask") or 0),
            "confidence": float(st.get("confidence") or 0),
            "price": float(st.get("price") or 0),
            "market_start_price": float(st.get("market_start_price") or 0),
        }
        ua, da = ms["up_ask"], ms["down_ask"]
        ms["favorite"] = "UP" if ua > da else "DOWN"

        ms["trading_enabled"], ms["trading_reason"] = _trading_flags(
            config, coin, hours=hours
        )

        pos_detail = None
        if trader:
            perf = trader.get_performance_stats()
            pnl_coin = trader.current_capital - trader.starting_capital
            slug = ms["market_slug"]
            ms["stats"] = {
                "pnl": round(pnl_coin, 2),
                "total_trades": perf.get("total_trades", 0),
                "wins": perf.get("wins", 0),
                "losses": perf.get("losses", 0),
                "win_rate": round(perf.get("win_rate", 0), 2),
            }
            if slug:
                pos = multi_trader.get_current_positions(trader_name, slug)
                if pos and (pos.get("up_shares", 0) > 0 or pos.get("down_shares", 0) > 0):
                    detailed = trader.get_market_detailed_stats(slug, ua, da)
                    if detailed:
                        pos_detail = {
                            "up_shares": detailed.get("up_shares", 0),
                            "down_shares": detailed.get("down_shares", 0),
                            "up_invested": round(detailed.get("up_invested", 0), 2),
                            "down_invested": round(detailed.get("down_invested", 0), 2),
                            "total_invested": round(detailed.get("total_invested", 0), 2),
                            "unrealized_pnl": round(detailed.get("unrealized_pnl", 0), 2),
                            "unrealized_pct": round(detailed.get("unrealized_pct", 0), 2),
                            "max_drawdown": round(detailed.get("max_drawdown", 0), 2),
                            "entries_count": detailed.get("entries_count", 0),
                            "our_side": "UP"
                            if detailed.get("up_shares", 0) > detailed.get("down_shares", 0)
                            else "DOWN",
                        }
                        pos_detail["if_up_wins"] = round(
                            (pos_detail["up_shares"] * 1.0) - pos_detail["total_invested"], 2
                        )
                        pos_detail["if_down_wins"] = round(
                            (pos_detail["down_shares"] * 1.0) - pos_detail["total_invested"], 2
                        )
        else:
            ms["stats"] = None

        ms["position"] = pos_detail
        coin_blocks[coin] = ms

    if skip_trade_merge and recent_trades_cached is not None:
        recent_trimmed = list(recent_trades_cached)
    else:
        recent_trimmed = build_recent_trades_trimmed(
            multi_trader=multi_trader,
            strategy_base=strategy_base,
            coins=coins,
            data_feed=data_feed,
            market_windows=market_windows,
            market_starts=market_starts,
            read_trade_files=read_trade_files,
            limit=40,
        )

    strat_cfg = config.get("strategy", {})
    safety_cfg = config.get("safety", {})
    exit_cfg = config.get("exit", {})
    pm = config.get("data_sources", {}).get("polymarket", {})
    market_interval_sec = int(pm.get("market_interval_sec", 900))
    return {
        "status": "running",
        "snapshot_ts": now,
        "uptime_sec": round(uptime, 1),
        "session_start": session_start_time,
        "wallet_balance": round(wallet_balance, 2) if wallet_balance is not None else None,
        "dry_run": dry_run,
        "markets_skipped": dict(markets_skipped),
        "portfolio": {
            "total_capital": round(portfolio.get("total_capital", 0), 2),
            "total_pnl": round(portfolio.get("total_pnl", 0), 2),
            "portfolio_roi": round(portfolio.get("portfolio_roi", 0), 2),
            "total_trades": tt,
            "total_wins": tw,
            "total_losses": tl,
            "win_rate": port_wr,
        },
        "market_interval_sec": market_interval_sec,
        "market_label": "5m" if market_interval_sec == 300 else ("15m" if market_interval_sec == 900 else f"{market_interval_sec}s"),
        "strategy_summary": {
            "entry_window_sec": strat_cfg.get("entry_window_sec"),
            "entry_frequency_sec": strat_cfg.get("entry_frequency_sec"),
            "min_confidence": strat_cfg.get("min_confidence"),
            "price_max": strat_cfg.get("price_max"),
            "max_spread": strat_cfg.get("max_spread"),
            "max_investment_per_market": strat_cfg.get("max_investment_per_market"),
            "sizing": strat_cfg.get("sizing", {}),
        },
        "safety_summary": {
            "max_order_size_usd": safety_cfg.get("max_order_size_usd"),
            "max_orders_per_minute": safety_cfg.get("max_orders_per_minute"),
            "max_total_investment": safety_cfg.get("max_total_investment"),
        },
        "flip_stop": exit_cfg.get("flip_stop", {}),
        "trading_hours": dashboard_payload(hours),
        "coins": coin_blocks,
        "recent_trades": recent_trimmed,
    }
