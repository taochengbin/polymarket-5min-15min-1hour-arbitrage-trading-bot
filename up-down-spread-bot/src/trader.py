"""
Position management with support for multiple entries per market
"""
import time
import json
import threading
from typing import Dict, List, Optional, Union
from pathlib import Path

from spot_price import fetch_coin_spot_usd, fetch_spot_usd
from polymarket_api import get_official_settlement


# Global dependencies (injected externally)
_order_executor = None
_data_feed = None  # ✅ For access to position_tracker (REAL data!)
_polymarket_proxy_url: Optional[str] = None
_token_ids_cache = {}  # {market_slug: {'UP': token_id, 'DOWN': token_id}}
_market_metadata_cache = {}  # {market_slug: {'condition_id': str, 'neg_risk': bool}}

# Persistent storage for metadata (critical for redeem after restart!)
_METADATA_FILE = Path("logs/market_metadata.json")
_CHAIN_TOKEN_DUST = 0.1


def set_order_executor(executor):
    """Inject OrderExecutor for real trading"""
    global _order_executor
    _order_executor = executor
    print("[TRADER] ✓ OrderExecutor injected")


def set_data_feed(data_feed):
    """Inject DataFeed for access to REAL positions"""
    global _data_feed
    _data_feed = data_feed
    print("[TRADER] ✅ DataFeed injected (REAL position tracking)")


def set_polymarket_proxy(proxy_url: Optional[str]) -> None:
    """HTTP(S) proxy for Gamma outcome lookups (same as data feed)."""
    global _polymarket_proxy_url
    _polymarket_proxy_url = (proxy_url or "").strip() or None


def _spot_at_entry_from_position(pos: Dict) -> Optional[float]:
    """Underlying spot USD when order was placed (BTC/ETH)."""
    try:
        v = float(pos.get("spot_at_entry") or 0)
        if v > 0:
            return round(v, 2)
    except (TypeError, ValueError):
        pass
    for e in reversed(pos.get("all_entries") or []):
        try:
            v = float(e.get("spot_at_entry") or 0)
            if v > 0:
                return round(v, 2)
        except (TypeError, ValueError):
            continue
    return None


def _token_ask_from_position(pos: Dict, bet_side: str = "") -> Optional[float]:
    """Polymarket outcome token ask (0–1) at order — not shown as 下单价 in UI."""
    try:
        op = float(pos.get("token_ask") or pos.get("order_price") or 0)
        if op > 0:
            return round(op, 4)
    except (TypeError, ValueError):
        pass
    for e in pos.get("all_entries") or []:
        if bet_side and e.get("side") != bet_side:
            continue
        try:
            v = float(e.get("token_ask") or e.get("order_price") or e.get("price") or 0)
            if 0 < v <= 1.0:
                return round(v, 4)
        except (TypeError, ValueError):
            continue
    return None


def _entry_price_from_position(pos: Dict, bet_side: str) -> Optional[float]:
    """Volume-weighted average fill price for bet_side (UP/DOWN)."""
    if bet_side not in ("UP", "DOWN"):
        return None
    entries = list((pos.get(bet_side) or {}).get("entries") or [])
    if not entries:
        for e in pos.get("all_entries") or []:
            if e.get("side") == bet_side:
                entries.append(e)
    if not entries:
        return None
    invested = sum(float(e.get("size_usd") or 0) for e in entries)
    shares = sum(float(e.get("shares") or 0) for e in entries)
    if shares > 0:
        return round(invested / shares, 4)
    try:
        p = float(entries[0].get("price") or 0)
        return round(p, 4) if p > 0 else None
    except (TypeError, ValueError):
        return None


def _infer_spot_at_entry_from_trade(trade: Dict) -> Optional[float]:
    """Spot USD at order time for web 下单价 column."""
    for key in ("spot_at_entry", "order_spot_usd", "entry_spot_usd"):
        try:
            v = trade.get(key)
            if v is not None:
                f = float(v)
                if f > 100:
                    return round(f, 2)
        except (TypeError, ValueError):
            pass
    return None


def _infer_entry_price_from_trade(trade: Dict) -> Optional[float]:
    """Alias: spot USD at order (not Polymarket token 0.xx)."""
    return _infer_spot_at_entry_from_trade(trade)


def _entry_times_from_position(pos: Dict) -> tuple:
    """Order fill time (unix + local string) from latest entry."""
    entries = pos.get("all_entries") or []
    if entries:
        first = entries[-1]
        t = float(first.get("time") or 0)
        ts = (first.get("timestamp") or "").strip()
        if t > 0:
            if not ts:
                ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
            return t, ts
    st = float(pos.get("start_time") or 0)
    if st > 0:
        return st, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(st))
    return 0.0, ""


def _entry_meta_from_position(pos: Dict) -> tuple:
    """Latest entry_reason + display label (e.g. flip_reverse → 翻转补单)."""
    from trade_record import entry_label_for

    entries = pos.get("all_entries") or []
    if entries:
        er = (entries[-1].get("entry_reason") or "normal").strip() or "normal"
        return er, entry_label_for(er)
    return "normal", ""


def _exit_reason_label(exit_reason: Optional[str], exit_type: Optional[str]) -> str:
    er = (exit_reason or exit_type or "").strip()
    labels = {
        "stop_loss": "止损 stop_loss",
        "flip_stop": "翻转止损 flip_stop",
        "natural_close": "到期结算",
        "settlement": "到期结算",
        "early_exit": "提前平仓",
    }
    return labels.get(er, er or "—")


def _apply_official_chainlink_prices(trade: Dict, price_to_beat: float, final_price: float) -> None:
    """Record Polymarket Chainlink open/close (Price to beat / final)."""
    if price_to_beat > 0:
        trade["btc_start"] = price_to_beat
        trade["spot_start"] = price_to_beat
    if final_price > 0:
        trade["btc_final"] = final_price
        trade["btc_end"] = final_price
        trade["spot_end"] = final_price
    if price_to_beat > 0 and final_price > 0:
        trade["price_source"] = "polymarket_chainlink"


def recalculate_settlement_pnl(trade: Dict) -> float:
    """
    Recompute payout/pnl for natural settlement using official winner.
    Returns capital adjustment (new_pnl - old_pnl).
    """
    if trade.get("exit_reason") != "settlement":
        return 0.0
    winner = trade.get("settlement_winner") or trade.get("winner")
    if winner not in ("UP", "DOWN"):
        return 0.0

    old_pnl = float(trade.get("pnl", 0) or 0)
    up_shares = float(trade.get("up_shares") or 0)
    down_shares = float(trade.get("down_shares") or 0)
    total_cost = float(trade.get("total_cost") or 0)
    if total_cost <= 0:
        total_cost = float(trade.get("up_invested") or 0) + float(trade.get("down_invested") or 0)

    winning_shares = up_shares if winner == "UP" else down_shares
    payout = winning_shares * 1.0
    pnl = payout - total_cost
    roi_pct = (pnl / total_cost * 100) if total_cost > 0 else 0.0

    trade["payout"] = payout
    trade["pnl"] = pnl
    trade["pnl_usd"] = round(pnl, 2)
    trade["roi_pct"] = roi_pct
    return pnl - old_pnl


def apply_official_bet_result(
    trade: Dict,
    coin: str,
    proxy_url: Optional[str] = None,
    force_refresh: bool = False,
) -> float:
    """
    Set bet_won / prices / settlement PnL from Polymarket Gamma only (Chainlink).
    Returns capital adjustment when settlement PnL was corrected (else 0).
    """
    bet_side = trade.get("bet_side")
    if not bet_side or bet_side not in ("UP", "DOWN"):
        up_s = float(trade.get("up_shares") or 0)
        dn_s = float(trade.get("down_shares") or 0)
        if up_s > dn_s:
            bet_side = "UP"
        elif dn_s > up_s:
            bet_side = "DOWN"
        elif up_s > 0:
            bet_side = "UP"
        elif dn_s > 0:
            bet_side = "DOWN"
        trade["bet_side"] = bet_side

    slug = trade.get("market_slug") or ""
    if not slug or bet_side not in ("UP", "DOWN"):
        trade["bet_won"] = None
        trade["bet_result"] = "unknown"
        trade["bet_result_label"] = "—"
        trade["settlement_pending"] = True
        return 0.0

    fully_official = (
        trade.get("bet_result_source") == "polymarket_gamma"
        and trade.get("settlement_winner") in ("UP", "DOWN")
        and trade.get("price_source") == "polymarket_chainlink"
        and trade.get("bet_won") is not None
    )
    if fully_official and not force_refresh:
        trade["bet_result"] = "win" if trade["bet_won"] else "loss"
        trade["bet_result_label"] = "押中" if trade["bet_won"] else "未中"
        trade["settlement_pending"] = False
        return 0.0

    px = proxy_url if proxy_url is not None else _polymarket_proxy_url
    if force_refresh:
        try:
            from polymarket_api import clear_outcome_cache

            clear_outcome_cache(slug)
        except Exception:
            pass
    api = get_official_settlement(slug, timeout=5, proxy_url=px)

    ptb = float(api.get("price_to_beat") or 0)
    fp = float(api.get("final_price") or 0)
    if ptb > 0 or fp > 0:
        _apply_official_chainlink_prices(trade, ptb, fp)

    if api.get("success") and api.get("winner") in ("UP", "DOWN"):
        official = api["winner"]
        trade["settlement_winner"] = official
        trade["winner"] = official
        trade["bet_won"] = bet_side == official
        trade["settlement_pending"] = False
        trade["bet_result_source"] = "polymarket_gamma"
        trade["bet_result"] = "win" if trade["bet_won"] else "loss"
        trade["bet_result_label"] = "押中" if trade["bet_won"] else "未中"
        if trade.get("is_open") and not trade.get("exit_reason"):
            trade["bet_result_label"] = "持仓"
            trade["exit_label"] = "持仓中"
            trade["settlement_pending"] = True
            return 0.0
        return recalculate_settlement_pnl(trade)

    if api.get("success"):
        if _restore_local_settlement_labels(trade, bet_side):
            return 0.0
        if trade.get("is_open") and not trade.get("exit_reason"):
            trade["bet_result_label"] = "持仓"
            trade["exit_label"] = "持仓中"
            trade["settlement_pending"] = True
            return 0.0
        trade["settlement_pending"] = True
        trade["bet_won"] = None
        trade["bet_result"] = "pending"
        trade["bet_result_label"] = "待结算"
        trade["bet_result_source"] = "polymarket_gamma_pending"
        return 0.0

    if _restore_local_settlement_labels(trade, bet_side):
        return 0.0
    trade["settlement_pending"] = True
    trade["bet_won"] = None
    trade["bet_result"] = "pending"
    trade["bet_result_label"] = "待结算"
    trade["bet_result_source"] = "api_unavailable"
    return 0.0


def _restore_local_settlement_labels(trade: Dict, bet_side: str) -> bool:
    """Keep close_market / spot-fallback result when Gamma is still pending."""
    if trade.get("exit_reason") != "settlement":
        return False
    if trade.get("bet_result_source") == "polymarket_gamma":
        return False
    sw = trade.get("settlement_winner")
    if sw not in ("UP", "DOWN") or bet_side not in ("UP", "DOWN"):
        return False
    bw = trade.get("bet_won")
    if bw is None:
        bw = bet_side == sw
    trade["bet_won"] = bool(bw)
    trade["bet_result"] = "win" if bw else "loss"
    trade["bet_result_label"] = "押中" if bw else "未中"
    trade["settlement_pending"] = False
    if not trade.get("bet_result_source"):
        trade["bet_result_source"] = "local_settlement"
    return True


def _apply_stored_bet_labels(trade: Dict) -> None:
    """Use fields already on the trade dict — no HTTP (safe for web snapshot hot path)."""
    bw = trade.get("bet_won")
    bet_side = trade.get("bet_side")
    if bw is None and bet_side in ("UP", "DOWN"):
        sw = trade.get("settlement_winner") or trade.get("winner")
        if sw in ("UP", "DOWN"):
            bw = bet_side == sw
            trade["bet_won"] = bw
    if bw is None and bet_side in ("UP", "DOWN"):
        sp0 = float(trade.get("btc_start") or trade.get("spot_start") or 0)
        sp1 = float(
            trade.get("btc_final") or trade.get("btc_end") or trade.get("spot_end") or 0
        )
        if sp0 > 0 and sp1 > 0:
            from spot_price import bet_won_direction

            inferred = bet_won_direction(bet_side, sp0, sp1)
            if inferred is not None:
                bw = inferred
                trade["bet_won"] = bw
                if not trade.get("settlement_winner"):
                    from spot_price import infer_up_down_winner

                    trade["settlement_winner"] = infer_up_down_winner(sp0, sp1)
    if bw is not None:
        trade["bet_result"] = "win" if bw else "loss"
        trade["bet_result_label"] = "押中" if bw else "未中"
        trade["settlement_pending"] = False
    elif trade.get("settlement_pending") or trade.get("bet_result") == "pending":
        trade["bet_result"] = "pending"
        trade["bet_result_label"] = trade.get("bet_result_label") or "待结算"
    elif trade.get("bet_result_label"):
        pass
    else:
        trade["bet_result_label"] = "—"


def enrich_trade_record(
    trade: Dict,
    coin: str,
    proxy_url: Optional[str] = None,
    refresh_official: bool = True,
    force_refresh: bool = False,
) -> float:
    """Normalize PnL, 押注成败 (Polymarket official), labels for logs and web UI.
    Returns capital adjustment when settlement PnL was corrected."""
    pnl = float(trade.get("pnl", 0) or 0)
    trade["pnl_usd"] = round(pnl, 2)

    sp0 = float(trade.get("btc_start") or trade.get("spot_start") or 0)
    sp1 = float(trade.get("btc_final") or trade.get("btc_end") or trade.get("spot_end") or 0)
    if sp0 > 0:
        trade["btc_start"] = sp0
        trade["spot_start"] = sp0
    if sp1 > 0:
        trade["btc_final"] = sp1
        trade["btc_end"] = sp1
        trade["spot_end"] = sp1

    cap_adj = 0.0
    if refresh_official:
        cap_adj = apply_official_bet_result(
            trade, coin, proxy_url=proxy_url, force_refresh=force_refresh
        )
    else:
        _apply_stored_bet_labels(trade)

    trade["pnl_usd"] = round(float(trade.get("pnl", 0) or 0), 2)

    prev_exit = (trade.get("exit_label") or "").strip()
    prev_bet_lbl = (trade.get("bet_result_label") or "").strip()
    if trade.get("is_open"):
        derived = _exit_reason_label(trade.get("exit_reason"), trade.get("exit_type"))
        if derived and derived != "—":
            trade["exit_label"] = derived
        elif prev_exit and prev_exit not in ("—", ""):
            trade["exit_label"] = prev_exit
        else:
            trade["exit_label"] = "持仓中"
        if trade.get("bet_won") is None:
            if prev_bet_lbl and prev_bet_lbl not in ("—", ""):
                trade["bet_result_label"] = prev_bet_lbl
            else:
                trade["bet_result_label"] = "持仓"
    else:
        derived = _exit_reason_label(trade.get("exit_reason"), trade.get("exit_type"))
        if derived and derived != "—":
            trade["exit_label"] = derived
        elif prev_exit and prev_exit not in ("—", ""):
            trade["exit_label"] = prev_exit
        else:
            trade["exit_label"] = "—"
        if not prev_bet_lbl and trade.get("bet_result_label") in (None, "", "—"):
            pass

    trade["coin"] = coin

    sp = _infer_spot_at_entry_from_trade(trade)
    if sp is not None:
        trade["spot_at_entry"] = sp

    from trade_record import apply_entry_labels

    apply_entry_labels(trade)

    et = trade.get("entry_time")
    if et is not None:
        try:
            et_f = float(et)
            if et_f > 0:
                trade["entry_time"] = et_f
                if not trade.get("entry_timestamp"):
                    trade["entry_timestamp"] = time.strftime(
                        "%Y-%m-%d %H:%M:%S", time.localtime(et_f)
                    )
        except (TypeError, ValueError):
            pass

    return cap_adj


def save_market_metadata_to_disk():
    """
    💾 Save metadata to disk (CRITICAL for redeem after restart!)
    
    Metadata includes:
    - token_ids (UP, DOWN) 
    - condition_id (for redeem)
    - neg_risk flag
    
    WITHOUT this redeem after restart is IMPOSSIBLE!
    """
    try:
        _METADATA_FILE.parent.mkdir(exist_ok=True)
        
        # Merge token_ids and metadata into one dict
        combined = {}
        for market_slug in _token_ids_cache:
            combined[market_slug] = {
                'token_ids': _token_ids_cache[market_slug],
                'metadata': _market_metadata_cache.get(market_slug, {})
            }
        
        with open(_METADATA_FILE, 'w') as f:
            json.dump(combined, f, indent=2)
        
        # print(f"[TRADER] 💾 Saved metadata for {len(combined)} markets to disk")
    except Exception as e:
        print(f"[TRADER] ⚠️ Failed to save metadata: {e}")


def load_market_metadata_from_disk():
    """
    📂 Load metadata from disk at startup
    
    This is critical for:
    - Redeeming positions after restart
    - EMERGENCY_SAVE positions (loaded from trades.jsonl)
    """
    global _token_ids_cache, _market_metadata_cache
    
    if not _METADATA_FILE.exists():
        print("[TRADER] ℹ️ No metadata file found (first run or clean start)")
        return
    
    try:
        with open(_METADATA_FILE, 'r') as f:
            combined = json.load(f)
        
        # Restore caches
        for market_slug, data in combined.items():
            if 'token_ids' in data:
                _token_ids_cache[market_slug] = data['token_ids']
            if 'metadata' in data:
                _market_metadata_cache[market_slug] = data['metadata']
        
        print(f"[TRADER] ✅ Loaded metadata for {len(combined)} markets from disk")
    except Exception as e:
        print(f"[TRADER] ⚠️ Failed to load metadata: {e}")


def set_token_ids(market_slug: str, up_token_id: str, down_token_id: str, 
                  condition_id: str = "", neg_risk: bool = True):
    """Cache token IDs and metadata for market + save to disk!"""
    global _token_ids_cache, _market_metadata_cache
    _token_ids_cache[market_slug] = {
        'UP': up_token_id,
        'DOWN': down_token_id
    }
    _market_metadata_cache[market_slug] = {
        'condition_id': condition_id,
        'neg_risk': neg_risk
    }
    
    # 💾 CRITICAL: Save to disk for redeem after restart!
    save_market_metadata_to_disk()


def get_token_ids(market_slug: str) -> dict:
    """Get token IDs for market"""
    return _token_ids_cache.get(market_slug, {})


def get_market_metadata(market_slug: str) -> dict:
    """Get metadata (condition_id, neg_risk) for market"""
    return _market_metadata_cache.get(market_slug, {})


class Trader:
    """Manage trading positions with detailed entry tracking"""
    
    def __init__(self, capital: float, log_dir: str = "logs", config: dict = None, coin: Optional[str] = None):
        self.starting_capital = capital
        self.current_capital = capital
        
        # Underlying symbol for this desk (btc, eth, …) — used for spot in snapshots / trades
        self.coin = (coin or "btc").lower()
        
        # Config for stop-loss checks
        self.config = config
        
        # Positions: {market_slug: {'UP': {...}, 'DOWN': {...}, 'entries': [...], ...}}
        self.positions = {}
        
        # Closed trades history
        self.closed_trades = []

        # Open rows keyed by record_key (slug#entry_reason#ts); multiple per market
        self.open_trade_records: Dict[str, Dict] = {}
        
        # Track closed markets to prevent re-entry after early exit
        self.closed_markets = set()  # Markets that were closed (early exit or normal)
        
        # 🛡️ THREAD SAFETY: Lock for async operations
        self.lock = threading.RLock()  # Reentrant lock (avoids deadlock)
        self._enter_in_flight: Dict[str, str] = {}
        
        # Market statistics tracking
        self.market_max_drawdown = {}  # {market_slug: max_dd_value}
        self.market_entries_count = {}  # {market_slug: count}
        
        # Logging
        self.log_dir = Path(log_dir)
        self.strategy_name = self.log_dir.name
        self.trades_file = self.log_dir / "trades.jsonl"
        self.session_file = self.log_dir / "session.json"
        self._trade_db = None
        try:
            from trade_db import get_trade_database

            self._trade_db = get_trade_database(config)
        except Exception as e:
            print(f"[TRADER] ⚠ MySQL 交易库未启用: {e}")
        
        print(f"[TRADER] Initialized with ${capital:,.2f} capital")
        
        # Load previous trades to restore statistics
        self.load_previous_trades()

    def adjust_capital(self, delta: float) -> None:
        """Apply PnL correction after official settlement refresh."""
        if delta:
            self.current_capital += float(delta)
    
    def _sync_trade_records_db(self, rows: list, *, async_write: bool = True) -> None:
        """Persist rows to MySQL; failures are logged only (never block trading)."""
        if not self._trade_db or not rows:
            return

        def _do_sync() -> None:
            try:
                from trade_storage import sync_trader_records_to_db

                sync_trader_records_to_db(self, rows)
            except Exception as e:
                print(f"[TRADER] ⚠ MySQL 同步失败: {e}")

        if async_write:
            threading.Thread(
                target=_do_sync,
                daemon=True,
                name=f"trade_db_sync_{self.strategy_name}",
            ).start()
        else:
            _do_sync()

    def _jsonl_backup_enabled(self) -> bool:
        try:
            from trade_storage import jsonl_backup_enabled

            return jsonl_backup_enabled(self.config)
        except Exception:
            return True

    def rewrite_trade_records_file(
        self, db_sync_rows: Optional[Union[Dict, List[Dict]]] = None
    ) -> bool:
        """Persist trade rows — incremental MySQL upsert; optional full jsonl backup."""
        rows = list(self.open_trade_records.values()) + list(self.closed_trades)
        rows.sort(
            key=lambda r: float(r.get("entry_time") or r.get("close_time") or 0),
            reverse=True,
        )
        if db_sync_rows is not None:
            if isinstance(db_sync_rows, dict):
                to_sync = [db_sync_rows]
            else:
                to_sync = list(db_sync_rows)
            if to_sync:
                self._sync_trade_records_db(to_sync)
        if not self._jsonl_backup_enabled():
            return True
        try:
            self.trades_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.trades_file.with_suffix(".jsonl.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                for t in rows:
                    f.write(json.dumps(t) + "\n")
            tmp.replace(self.trades_file)
            return True
        except OSError as e:
            print(f"[TRADER] ⚠ rewrite trades.jsonl failed: {e}")
            return False

    def rewrite_closed_trades_file(self) -> bool:
        return self.rewrite_trade_records_file()

    def _record_keys_for_slug(self, market_slug: str) -> List[str]:
        slug = str(market_slug or "")
        keys: List[str] = []
        for k, rec in self.open_trade_records.items():
            if k == slug or rec.get("market_slug") == slug:
                keys.append(k)
        return keys

    def _pop_all_open_records_for_slug(self, market_slug: str) -> List[Dict]:
        keys = self._record_keys_for_slug(market_slug)
        trades: List[Dict] = []
        for k in keys:
            t = self.open_trade_records.pop(k, None)
            if t is not None:
                trades.append(t)
        return trades

    def _leg_shares_for_record(
        self, side: str, contracts: float, entry_reason: str, pos: Dict
    ) -> tuple:
        """Per-entry row: only this leg's shares/cost (not whole hedge position)."""
        er = (entry_reason or "normal").strip() or "normal"
        c = round(float(contracts), 4)
        if er == "flip_reverse":
            up_sh = c if side == "UP" else 0.0
            down_sh = c if side == "DOWN" else 0.0
            return up_sh, down_sh
        up_sh = float((pos.get("UP") or {}).get("total_shares") or 0)
        down_sh = float((pos.get("DOWN") or {}).get("total_shares") or 0)
        if side == "UP":
            down_sh = 0.0
        elif side == "DOWN":
            up_sh = 0.0
        return up_sh, down_sh

    def _unrealized_pnl_from_asks(
        self, market_slug: str, up_ask: float, down_ask: float
    ) -> float:
        pos = self.positions.get(market_slug)
        if not pos:
            return 0.0
        ua = float(up_ask if up_ask is not None else 0.5)
        da = float(down_ask if down_ask is not None else 0.5)
        up_sh = float(pos["UP"]["total_shares"])
        down_sh = float(pos["DOWN"]["total_shares"])
        cost = float(pos["UP"]["total_invested"]) + float(pos["DOWN"]["total_invested"])
        return up_sh * ua + down_sh * da - cost

    def _save_open_trade_record(
        self,
        market_slug: str,
        side: str,
        spot_at_entry: float,
        token_ask: float,
        contracts: float,
        size_usd: float,
        entry_time: float,
        entry_timestamp: str,
        up_ask: float = None,
        down_ask: float = None,
        entry_reason: str = "normal",
        window_range_high: float = None,
        window_range_low: float = None,
    ) -> None:
        """Phase 1: one row per entry; flip_reverse does not overwrite first leg."""
        from trade_record import build_open_record, make_record_key

        pos = self.positions.get(market_slug) or {}
        up_sh, down_sh = self._leg_shares_for_record(
            side, contracts, entry_reason, pos
        )
        leg_cost = round(float(size_usd), 2)
        ua = float(up_ask if up_ask is not None else 0.5)
        da = float(down_ask if down_ask is not None else 0.5)
        unreal = up_sh * ua + down_sh * da - leg_cost
        rec = build_open_record(
            market_slug=market_slug,
            coin=self.coin,
            bet_side=side,
            spot_at_entry=spot_at_entry,
            token_ask=token_ask,
            contracts=contracts,
            size_usd=leg_cost,
            entry_time=entry_time,
            entry_timestamp=entry_timestamp,
            unrealized_pnl=unreal,
            up_shares=up_sh,
            down_shares=down_sh,
            total_cost=leg_cost,
            entry_reason=entry_reason,
            up_ask_at_entry=up_ask,
            down_ask_at_entry=down_ask,
            window_range_high=window_range_high,
            window_range_low=window_range_low,
        )
        record_key = rec.get("record_key") or make_record_key(
            market_slug, entry_reason, entry_time
        )
        self.open_trade_records[record_key] = rec
        self.rewrite_trade_records_file(rec)
        tag = rec.get("entry_label") or ""
        tag_s = f" | {tag}" if tag else ""
        wr_s = ""
        if window_range_high and window_range_low:
            wr_s = (
                f" | 前三分钟高${float(window_range_high):,.2f}"
                f" 低${float(window_range_low):,.2f}"
            )
        print(
            f"[TRADE-REC] 下单记录 {market_slug} | {side}{tag_s} | "
            f"现货@${spot_at_entry:,.2f}{wr_s} | 浮动盈亏 ${unreal:+.2f}"
        )

    def _upsert_closed_trade(self, trade: Dict) -> None:
        from trade_record import record_row_key

        row_key = record_row_key(trade)
        slug = str(trade.get("market_slug") or "")
        for i, t in enumerate(self.closed_trades):
            if record_row_key(t) == row_key:
                self.closed_trades[i] = trade
                return
            if not t.get("record_key") and t.get("market_slug") == slug:
                self.closed_trades[i] = trade
                return
        self.closed_trades.append(trade)

    def record_needs_phase2(self, market_slug: str) -> bool:
        """True if Chainlink settlement fields are still missing."""
        slug = str(market_slug or "")
        if not slug:
            return False
        for rec in self.open_trade_records.values():
            if rec.get("market_slug") != slug:
                continue
            if rec.get("is_open") or rec.get("settlement_pending"):
                sp0 = float(rec.get("spot_start") or rec.get("btc_start") or 0)
                sp1 = float(
                    rec.get("spot_end") or rec.get("btc_final") or rec.get("btc_end") or 0
                )
                if sp0 <= 0 or sp1 <= 0 or rec.get("bet_won") is None:
                    return True
        for t in reversed(self.closed_trades[-30:]):
            if t.get("market_slug") != slug:
                continue
            sp0 = float(t.get("spot_start") or t.get("btc_start") or 0)
            sp1 = float(
                t.get("spot_end") or t.get("btc_final") or t.get("btc_end") or 0
            )
            if sp0 > 0 and sp1 > 0 and not t.get("settlement_pending"):
                return False
            if not t.get("settlement_pending") and t.get("bet_won") is not None:
                if sp0 <= 0 or sp1 <= 0:
                    return True
            if t.get("settlement_pending") or t.get("bet_won") is None:
                return True
            sp0 = float(t.get("spot_start") or t.get("btc_start") or 0)
            sp1 = float(
                t.get("spot_end") or t.get("btc_final") or t.get("btc_end") or 0
            )
            if sp0 <= 0 or sp1 <= 0:
                return True
        return False

    def apply_chainlink_to_record(
        self, market_slug: str, spot_start: float, spot_end: float, winner: str
    ) -> Optional[Dict]:
        """Phase 2 for all rows of this market (open hold or early-exit closed)."""
        from trade_record import apply_chainlink_labels, finalize_settlement

        last: Optional[Dict] = None
        updated_rows: List[Dict] = []
        for key in self._record_keys_for_slug(market_slug):
            trade = self.open_trade_records.pop(key, None)
            if trade is None:
                continue
            if trade.get("exit_reason") in ("stop_loss", "flip_stop", "early_exit"):
                apply_chainlink_labels(
                    trade,
                    spot_start=spot_start,
                    spot_end=spot_end,
                    settlement_winner=winner,
                )
            else:
                finalize_settlement(
                    trade,
                    spot_start=spot_start,
                    spot_end=spot_end,
                    settlement_winner=winner,
                )
            self._upsert_closed_trade(trade)
            updated_rows.append(trade)
            last = trade
        for i, t in enumerate(self.closed_trades):
            if t.get("market_slug") != market_slug:
                continue
            if t.get("exit_reason") in ("stop_loss", "flip_stop", "early_exit"):
                apply_chainlink_labels(
                    t,
                    spot_start=spot_start,
                    spot_end=spot_end,
                    settlement_winner=winner,
                )
            else:
                finalize_settlement(
                    t,
                    spot_start=spot_start,
                    spot_end=spot_end,
                    settlement_winner=winner,
                )
            self.closed_trades[i] = t
            updated_rows.append(t)
            last = t
        if last is None:
            return None
        self.rewrite_trade_records_file(updated_rows)
        return last

    def load_previous_trades(self):
        """
        Load previous trades — merge MySQL + trades.jsonl; upsert only missing rows.
        """
        from trade_storage import (
            apply_merged_records_to_trader,
            db_reads_enabled,
            merge_trade_records,
            sync_missing_records_to_db,
        )

        db_rows: list = []
        if db_reads_enabled(self.config) and self._trade_db:
            try:
                db_rows = self._trade_db.load_records(
                    strategy_name=self.strategy_name,
                    limit=5000,
                )
            except Exception as e:
                print(f"[TRADER] ⚠ 从 MySQL 加载失败: {e}")

        jsonl_rows: list = []
        if self.trades_file.exists():
            try:
                with open(self.trades_file, "r", encoding="utf-8") as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            trade = json.loads(line)
                        except json.JSONDecodeError as e:
                            print(f"[WARNING] Corrupted JSON on line {line_num}: {e}")
                            continue
                        if trade.get("market_slug"):
                            jsonl_rows.append(trade)
            except OSError as e:
                print(f"[TRADER] ⚠ 读取 trades.jsonl 失败: {e}")

        merged = merge_trade_records(db_rows, jsonl_rows)
        if not merged:
            print(f"[TRADER] No previous trades found (first run)")
            return

        apply_merged_records_to_trader(self, merged)
        synced = sync_missing_records_to_db(self, db_rows, merged)
        if synced:
            print(f"[TRADER] ✓ 增量补全 MySQL {synced} 条缺失记录")

        loaded_count = len(merged)
        total_pnl = sum(float(t.get("pnl") or 0) for t in self.closed_trades)
        self.current_capital = self.starting_capital + total_pnl
        wins = sum(1 for t in self.closed_trades if self.infer_trade_outcome_win(t))
        closed_n = len(self.closed_trades)
        win_rate = (wins / closed_n * 100) if closed_n > 0 else 0

        source_bits = []
        if db_rows:
            source_bits.append(f"MySQL {len(db_rows)}")
        if jsonl_rows:
            source_bits.append(f"jsonl {len(jsonl_rows)}")
        print(
            f"[TRADER] ✓ 已加载 {loaded_count} 条交易 "
            f"({', '.join(source_bits) or 'memory'}) — "
            f"{len(self.open_trade_records)} open / {closed_n} closed"
        )
        print(f"[TRADER]   Cumulative PnL: ${total_pnl:+,.2f}")
        if closed_n:
            print(f"[TRADER]   Win Rate: {win_rate:.1f}% ({wins}/{closed_n})")
        print(f"[TRADER]   Current Capital: ${self.current_capital:,.2f}")
    
    
    def has_first_leg_for_flip_reverse(self, market_slug: str) -> bool:
        """True if market has a non-reverse open leg (eligible for flip reverse)."""
        with self.lock:
            pos = self.positions.get(market_slug)
            if not pos or pos.get("status") == "CLOSED":
                return False
            for ent in pos.get("all_entries") or []:
                if ent.get("entry_reason") == "flip_reverse":
                    return False
            up_sh = float(pos.get("UP", {}).get("total_shares") or 0)
            down_sh = float(pos.get("DOWN", {}).get("total_shares") or 0)
            return up_sh > 0 or down_sh > 0

    def snapshot_flip_position(self, market_slug: str) -> Optional[Dict]:
        """Snapshot open leg sizes before parallel flip sell (+ optional reverse buy)."""
        with self.lock:
            if market_slug not in self.positions:
                return None
            pos = self.positions[market_slug]
            return {
                "up_contracts": float(pos["UP"]["total_shares"]),
                "down_contracts": float(pos["DOWN"]["total_shares"]),
            }

    def flip_exchange_sell(
        self,
        market_slug: str,
        up_contracts: float,
        down_contracts: float,
        up_bid: Optional[float],
        down_bid: Optional[float],
    ) -> Dict:
        """Exchange-only sell for flip stop (no local bookkeeping)."""
        results: Dict = {}
        if not _order_executor or market_slug not in _token_ids_cache:
            return results
        token_ids = _token_ids_cache[market_slug]
        for side in ("UP", "DOWN"):
            contracts = up_contracts if side == "UP" else down_contracts
            if contracts <= 0:
                continue
            token_id = token_ids.get(side)
            if not token_id:
                continue
            bid = up_bid if side == "UP" else down_bid
            results[side] = _order_executor.sell_position(
                market_slug=market_slug,
                token_id=token_id,
                side=side,
                contracts=contracts,
                bid_price=bid,
            )
        return results

    def flip_exchange_buy(
        self,
        market_slug: str,
        side: str,
        contracts: float,
        up_ask: float,
        down_ask: float,
    ):
        """Exchange-only buy for flip reverse leg (no local bookkeeping)."""
        if not _order_executor or market_slug not in _token_ids_cache:
            return None
        token_ids = _token_ids_cache[market_slug]
        token_id = token_ids.get(side)
        ask_price = down_ask if side == "DOWN" else up_ask
        if not token_id or not ask_price:
            return None
        return _order_executor.place_buy_order(
            market_slug=market_slug,
            token_id=token_id,
            side=side,
            contracts=contracts,
            ask_price=ask_price,
            coin=self.coin,
        )

    def enter_position_contracts(self, market_slug: str, side: str, price: float, contracts: int,
                                 up_ask: float = None, down_ask: float = None,
                                 winner_ratio: float = 0.0, is_recovery: bool = False,
                                 entry_reason: str = 'normal',
                                 seconds_till_end: int = 0, time_from_start: int = 0,
                                 spot_at_entry: float = 0,
                                 market_spot_open: float = 0,
                                 prefill_buy_result=None,
                                 window_range_high: float = None,
                                 window_range_low: float = None) -> bool:
        """
        Enter a position by specifying number of contracts/shares
        🛡️ THREAD-SAFE: can be called from different threads
        
        Args:
            market_slug: Market identifier
            side: 'UP' or 'DOWN'
            price: Entry price
            contracts: Number of contracts/shares to buy
            up_ask: Current UP ask price (for detailed logging)
            down_ask: Current DOWN ask price (for detailed logging)
            winner_ratio: Current winner ratio (for detailed logging)
            is_recovery: Is this a recovery entry? (for detailed logging)
            entry_reason: Reason for entry (for detailed logging)
            seconds_till_end: Seconds until market end (for detailed logging)
            time_from_start: Seconds from market start (for detailed logging)
            
        Returns:
            True if entered successfully
        """
        # Skip if contracts is 0 (hedge with no position)
        if contracts == 0:
            return True  # Success, just didn't enter anything

        with self.lock:
            if market_slug in self.closed_markets:
                return False
            if market_slug in self._enter_in_flight:
                return False
            if entry_reason == "normal":
                pos = self.positions.get(market_slug)
                if pos:
                    for ent in pos.get("all_entries") or []:
                        if (ent.get("entry_reason") or "normal") != "flip_reverse":
                            if float(ent.get("shares") or 0) > 0:
                                return True
            self._enter_in_flight[market_slug] = entry_reason or "normal"

        try:
            return self._enter_position_contracts_locked(
                market_slug=market_slug,
                side=side,
                price=price,
                contracts=contracts,
                up_ask=up_ask,
                down_ask=down_ask,
                winner_ratio=winner_ratio,
                is_recovery=is_recovery,
                entry_reason=entry_reason,
                seconds_till_end=seconds_till_end,
                time_from_start=time_from_start,
                spot_at_entry=spot_at_entry,
                market_spot_open=market_spot_open,
                prefill_buy_result=prefill_buy_result,
                window_range_high=window_range_high,
                window_range_low=window_range_low,
            )
        finally:
            with self.lock:
                self._enter_in_flight.pop(market_slug, None)

    def _enter_position_contracts_locked(
        self,
        market_slug: str,
        side: str,
        price: float,
        contracts: int,
        up_ask: float = None,
        down_ask: float = None,
        winner_ratio: float = 0.0,
        is_recovery: bool = False,
        entry_reason: str = "normal",
        seconds_till_end: int = 0,
        time_from_start: int = 0,
        spot_at_entry: float = 0,
        market_spot_open: float = 0,
        prefill_buy_result=None,
        window_range_high: float = None,
        window_range_low: float = None,
    ) -> bool:
        """Place buy (I/O outside lock) then update positions under self.lock."""
        # Note: Market closure check now handled in main.py (market_start_prices)
        # This provides single source of truth and auto-cleanup on market switch
        
        # Calculate position size in USD
        size_usd = contracts * price
        shares = float(contracts)
        
        # Track entry count for ratio calculation
        if not hasattr(self, '_entry_count'):
            self._entry_count = 0
        self._entry_count += 1
        
        # 🔥 FIRST TRY TO BUY (if live mode)
        actual_contracts = shares
        actual_cost = size_usd
        
        if prefill_buy_result is not None:
            result = prefill_buy_result
            if result.success:
                actual_contracts = result.filled_size
                actual_cost = result.total_spent_usd
                print(
                    f"[TRADER] ✓ Flip reverse fill (parallel): "
                    f"{actual_contracts:.2f} contracts for ${actual_cost:.2f}"
                )
            elif not result.dry_run:
                print(
                    f"[TRADER] ❌ Flip reverse buy failed: {result.error} - position NOT created"
                )
                return False
        elif _order_executor and market_slug in _token_ids_cache:
            token_id = _token_ids_cache[market_slug][side]
            ask_price = up_ask if side == 'UP' else down_ask
            
            if token_id and ask_price:
                chain_before = _order_executor.get_blockchain_token_balance(token_id)
                if chain_before is None:
                    print(
                        f"[TRADER] ❌ RPC unavailable — skip buy this attempt "
                        f"({market_slug} {side})"
                    )
                    return False
                chain_before_val = chain_before
                mem_has_normal = False
                pos0 = self.positions.get(market_slug)
                if pos0:
                    for ent in pos0.get("all_entries") or []:
                        if (ent.get("entry_reason") or "normal") != "flip_reverse":
                            if float(ent.get("shares") or 0) > 0:
                                mem_has_normal = True
                                break

                if (
                    entry_reason == "normal"
                    and chain_before_val >= _CHAIN_TOKEN_DUST
                    and not mem_has_normal
                ):
                    actual_contracts = chain_before_val
                    actual_cost = round(chain_before_val * float(price), 2)
                    print(
                        f"[TRADER] 👻 Chain already holds {chain_before_val:.2f} {side} "
                        f"— recover position, skip new buy ({market_slug})"
                    )
                elif (
                    entry_reason == "normal"
                    and chain_before_val >= _CHAIN_TOKEN_DUST
                    and mem_has_normal
                ):
                    print(
                        f"[TRADER] ✓ Already holding {side} on chain+memory "
                        f"({chain_before_val:.2f}) — skip duplicate buy"
                    )
                    return True
                else:
                    print(f"[TRADER] ▶ {side:4s} @ ${price:.3f}  {shares:6.1f} contracts = ${size_usd:6.2f}  ({market_slug})")
                    
                    result = _order_executor.place_buy_order(
                        market_slug=market_slug,
                        token_id=token_id,
                        side=side,
                        contracts=contracts,
                        ask_price=ask_price,
                        coin=self.coin,
                    )
                    
                    if result.success:
                        actual_contracts = result.filled_size
                        actual_cost = result.total_spent_usd
                        
                        if actual_contracts != contracts:
                            print(f"[TRADER] ⚠ FAK partial fill: {actual_contracts:.2f}/{contracts} contracts")
                        
                        print(f"[TRADER] ✓ Order filled: {actual_contracts:.2f} contracts for ${actual_cost:.2f}")
                    
                    elif result.error == "ALREADY_HOLD_TOKEN_ON_CHAIN":
                        hold_bal = float(result.remaining_balance or chain_before_val)
                        if hold_bal >= _CHAIN_TOKEN_DUST:
                            actual_contracts = hold_bal
                            actual_cost = round(hold_bal * float(price), 2)
                            print(
                                f"[TRADER] 👻 Buy skipped — chain holds {hold_bal:.2f} "
                                f"{side}, recovering position"
                            )
                        else:
                            return False
                    
                    elif not result.dry_run:
                        chain_after = _order_executor.get_blockchain_token_balance(token_id)
                        chain_after_val = chain_after if chain_after is not None else 0.0
                        chain_delta = max(0.0, chain_after_val - chain_before_val)
                        if chain_delta >= _CHAIN_TOKEN_DUST:
                            actual_contracts = chain_delta
                            actual_cost = round(chain_delta * float(price), 2)
                            print(
                                f"[TRADER] 👻 Buy API failed but chain +{chain_delta:.2f} "
                                f"{side} — recovering position"
                            )
                        else:
                            print(f"[TRADER] ❌ Order FAILED for {side}: {result.error} - position NOT created")
                            return False
        else:
            # DRY_RUN or no executor - just print
            print(f"[TRADER] ▶ {side:4s} @ ${price:.3f}  {shares:6.1f} shares = ${size_usd:6.2f}  ({market_slug})")
        
        with self.lock:
            if market_slug in self.closed_markets and actual_contracts <= 0:
                return False
            if market_slug in self.closed_markets and actual_contracts > 0:
                print(
                    f"[TRADER] ⚠ Market {market_slug} marked closed but recording "
                    f"{actual_contracts:.2f} on-chain {side} fill"
                )
            # NOW create position with ACTUAL values (or paper values if DRY_RUN)
            if market_slug not in self.positions:
                self.positions[market_slug] = {
                    'UP': {
                        'entries': [],
                        'total_invested': 0.0,
                        'total_shares': 0.0
                    },
                    'DOWN': {
                        'entries': [],
                        'total_invested': 0.0,
                        'total_shares': 0.0
                    },
                    'all_entries': [],
                    'start_time': time.time(),
                    'status': 'OPEN'
                }
            
            token_ask = round(float(price), 4)
            fill_price = (
                round(actual_cost / actual_contracts, 4)
                if actual_contracts > 0
                else token_ask
            )
            entry_ts = time.time()
            entry_ts_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(entry_ts))
            # 下单时现货：下单瞬间单独拉价，不用 market_start_price（标的起）
            spot_px = 0.0
            if _data_feed and self.coin:
                try:
                    fresh = _data_feed.refresh_coin_spot(self.coin)
                    if fresh and fresh > 0:
                        spot_px = float(fresh)
                except Exception:
                    spot_px = 0.0
            if spot_px <= 0:
                spot_px = float(spot_at_entry or 0)
            if spot_px <= 0 and _data_feed and self.coin:
                try:
                    st = _data_feed.get_state(self.coin)
                    spot_px = float(st.get("price") or 0)
                except Exception:
                    spot_px = 0.0
            if spot_px <= 0 and self.coin:
                src = "coingecko"
                cl_feed = None
                if _data_feed is not None:
                    src = getattr(_data_feed, "spot_price_source", "coingecko")
                    cl_feed = getattr(_data_feed, "_chainlink_feed", None)
                spot_px = float(
                    fetch_spot_usd(
                        self.coin, source=src, chainlink_feed=cl_feed
                    )
                    or fetch_coin_spot_usd(self.coin)
                    or 0
                )
            spot_px = round(spot_px, 2) if spot_px > 0 else 0.0
            # Create entry with ACTUAL values
            entry = {
                'side': side,
                'token_ask': token_ask,
                'spot_at_entry': spot_px,
                'price': fill_price,
                'size_usd': actual_cost,
                'shares': actual_contracts,
                'time': entry_ts,
                'timestamp': entry_ts_str,
                'entry_reason': entry_reason,
                'actual_fill': (_order_executor is not None)  # Mark if real order
            }
            
            # Add to position
            pos = self.positions[market_slug]
            if spot_px > 0:
                pos["spot_at_entry"] = spot_px
            if not pos.get("token_ask"):
                pos["token_ask"] = token_ask
            pos['all_entries'].append(entry)
            pos[side]['entries'].append(entry)
            pos[side]['total_invested'] += actual_cost
            pos[side]['total_shares'] += actual_contracts
            try:
                sp0 = float(pos.get("spot_start") or 0)
                if sp0 <= 0 and market_spot_open > 0:
                    sp0 = float(market_spot_open)
                if sp0 <= 0 and _data_feed and self.coin:
                    st = _data_feed.get_state(self.coin)
                    if (st.get("market_slug") or "") == market_slug:
                        sp0 = float(st.get("market_start_price") or 0)
                if sp0 > 0:
                    pos["spot_start"] = sp0
            except Exception:
                pass
            
            # Update market statistics
            self._update_market_stats(market_slug)

            self._save_open_trade_record(
                market_slug=market_slug,
                side=side,
                spot_at_entry=spot_px,
                token_ask=token_ask,
                contracts=actual_contracts,
                size_usd=actual_cost,
                entry_time=entry_ts,
                entry_timestamp=entry_ts_str,
                up_ask=up_ask,
                down_ask=down_ask,
                entry_reason=entry_reason,
                window_range_high=window_range_high,
                window_range_low=window_range_low,
            )
        
        # Detailed logging for backtesting
        if up_ask is not None and down_ask is not None:
            try:
                self.log_entry_detailed(
                    market_slug=market_slug,
                    side=side,
                    contracts=actual_contracts,  # Log actual
                    price=price,
                    up_ask=up_ask,
                    down_ask=down_ask,
                    winner_ratio=winner_ratio,
                    is_recovery=is_recovery,
                    entry_reason=entry_reason,
                    seconds_till_end=seconds_till_end,
                    time_from_start=time_from_start
                )
            except Exception as e:
                # Don't fail the trade if logging fails
                print(f"[WARNING] Detailed logging failed: {e}")
        
        return True
    
    def enter_position(self, market_slug: str, side: str, price: float, size_pct: float) -> bool:
        """
        Enter a position
        
        Args:
            market_slug: Market identifier
            side: 'UP' or 'DOWN'
            price: Entry price
            size_pct: Position size as % of capital
            
        Returns:
            True if entered successfully
        """
        # Calculate position size
        size_usd = self.current_capital * (size_pct / 100.0)
        shares = size_usd / price if price > 0 else 0
        
        # Create market if doesn't exist
        if market_slug not in self.positions:
            self.positions[market_slug] = {
                'UP': {
                    'entries': [],
                    'total_invested': 0.0,
                    'total_shares': 0.0
                },
                'DOWN': {
                    'entries': [],
                    'total_invested': 0.0,
                    'total_shares': 0.0
                },
                'all_entries': [],
                'start_time': time.time(),
                'status': 'OPEN'
            }
        
        # Create entry
        entry = {
            'side': side,
            'price': price,
            'size_usd': size_usd,
            'shares': shares,
            'time': time.time(),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Add to position
        pos = self.positions[market_slug]
        pos['all_entries'].append(entry)
        pos[side]['entries'].append(entry)
        pos[side]['total_invested'] += size_usd
        pos[side]['total_shares'] += shares
        
        # Update market statistics
        self._update_market_stats(market_slug)
        
        # Calculate current ratio after this entry
        up_shares = pos['UP']['total_shares']
        down_shares = pos['DOWN']['total_shares']
        total_shares = up_shares + down_shares
        
        if total_shares > 0 and self._entry_count % 5 == 1:
            up_ratio = (up_shares / total_shares) * 100
            down_ratio = (down_shares / total_shares) * 100
            print(f"[TRADER] After entry: UP {up_shares:.1f} ({up_ratio:.1f}%) | DOWN {down_shares:.1f} ({down_ratio:.1f}%)")
        
        print(f"[TRADER] ▶ {side:4s} @ ${price:.3f}  {shares:6.1f} shares = ${size_usd:6.2f}  ({market_slug})")
        
        return True
    
    def close_market(
        self,
        market_slug: str,
        winner: str,
        btc_start: float,
        btc_final: float,
        *,
        skip_official_fetch: bool = False,
    ) -> Optional[Dict]:
        """
        Close all positions for a market
        
        Args:
            market_slug: Market identifier
            winner: 'UP' or 'DOWN'
            btc_start: Starting BTC price
            btc_final: Final BTC price
            
        Returns:
            Trade result dict
        """
        if market_slug not in self.positions:
            return None
        
        pos = self.positions[market_slug]
        
        # Calculate PnL
        winner_side = pos[winner]
        loser_side = pos['UP' if winner == 'DOWN' else 'DOWN']
        
        # Winner pays $1 per share
        payout = winner_side['total_shares'] * 1.0
        
        # Total cost
        total_cost = pos['UP']['total_invested'] + pos['DOWN']['total_invested']
        
        # PnL
        pnl = payout - total_cost
        roi_pct = (pnl / total_cost * 100) if total_cost > 0 else 0
        
        # Winner ratio
        total_shares = pos['UP']['total_shares'] + pos['DOWN']['total_shares']
        winner_ratio = (winner_side['total_shares'] / total_shares * 100) if total_shares > 0 else 50

        upc = pos['UP']['total_shares']
        dnc = pos['DOWN']['total_shares']
        if upc > dnc:
            bet_side = 'UP'
        elif dnc > upc:
            bet_side = 'DOWN'
        elif upc > 0:
            bet_side = 'UP'
        else:
            bet_side = 'DOWN'
        sp0 = float(btc_start or 0)
        sp1 = float(btc_final or 0)

        from trade_record import build_open_record, finalize_settlement, fetch_chainlink_settlement

        if skip_official_fetch and winner in ("UP", "DOWN"):
            ptb, fp, official_w = sp0, sp1, winner
            if ptb <= 0 or fp <= 0:
                api = fetch_chainlink_settlement(market_slug)
                ptb = float(api.get("price_to_beat") or 0) or ptb
                fp = float(api.get("final_price") or 0) or fp
                if api.get("winner") in ("UP", "DOWN"):
                    official_w = api["winner"]
        else:
            api = fetch_chainlink_settlement(market_slug)
            ptb = float(api.get("price_to_beat") or 0) or sp0
            fp = float(api.get("final_price") or 0) or sp1
            official_w = (
                api.get("winner") if api.get("winner") in ("UP", "DOWN") else winner
            )

        open_rows = self._pop_all_open_records_for_slug(market_slug)
        if not open_rows:
            entry_time, entry_timestamp = _entry_times_from_position(pos)
            spot_entry = _spot_at_entry_from_position(pos) or 0
            token_ask = _token_ask_from_position(pos, bet_side) or 0
            open_rows = [
                build_open_record(
                    market_slug=market_slug,
                    coin=self.coin,
                    bet_side=bet_side,
                    spot_at_entry=spot_entry,
                    token_ask=token_ask,
                    contracts=pos[bet_side]["total_shares"],
                    size_usd=total_cost,
                    entry_time=entry_time,
                    entry_timestamp=entry_timestamp,
                    unrealized_pnl=pnl,
                    up_shares=pos["UP"]["total_shares"],
                    down_shares=pos["DOWN"]["total_shares"],
                    total_cost=total_cost,
                )
            ]

        settled: List[Dict] = []
        total_pnl = 0.0
        for trade in open_rows:
            finalize_settlement(
                trade,
                spot_start=ptb,
                spot_end=fp,
                settlement_winner=official_w,
            )
            leg_cost = float(trade.get("total_cost") or trade.get("size_usd") or 0)
            trade["roi_pct"] = (
                (float(trade.get("pnl", 0)) / leg_cost * 100) if leg_cost > 0 else 0
            )
            trade["winner_ratio"] = winner_ratio
            trade["total_entries"] = len(pos["all_entries"])
            total_pnl += float(trade.get("pnl", 0) or 0)
            settled.append(trade)
        trade = settled[-1] if settled else None
        self.current_capital += total_pnl

        try:
            for row in settled:
                self.closed_trades.append(row)
            self.rewrite_trade_records_file(settled)
            
            # 2. Mark market as closed to prevent re-entry
            self.closed_markets.add(market_slug)
            
            # 3. NOW we can safely delete the position
            del self.positions[market_slug]
            
            # 4. Clean up market stats
            if market_slug in self.market_max_drawdown:
                del self.market_max_drawdown[market_slug]
            if market_slug in self.market_entries_count:
                del self.market_entries_count[market_slug]
                
        except Exception as e:
            # CRITICAL: If logging failed, DO NOT delete position!
            # Position will remain open and can be closed again
            print(f"[TRADER] ⚠️ FAILED TO CLOSE MARKET {market_slug}: {e}")
            print(f"[TRADER] ⚠️ Position kept open for retry!")
            return None
        
        # Print result
        status = "✓" if trade.get("bet_won") else "✗"
        fpnl = float(trade.get("pnl", 0))
        print(
            f"[TRADE-REC] 结算 {market_slug} | {trade.get('bet_result_label')} | "
            f"Chainlink ${trade.get('spot_start'):,.2f}→${trade.get('spot_end'):,.2f} | "
            f"PnL ${fpnl:+.2f}"
        )
        
        # ═══════════════════════════════════════════════════════════
        # 🔥 CRITICAL: Reset investment tracking for this market!
        # Now we can trade new market without limits!
        # ═══════════════════════════════════════════════════════════
        try:
            if _order_executor and hasattr(_order_executor, 'safety'):
                _order_executor.safety.reset_market(market_slug)
        except Exception as reset_err:
            print(f"[TRADER] ⚠ Failed to reset market tracking: {reset_err}")
        
        return trade
    
    def close_market_early_exit(self, market_slug: str, exit_price: float, exit_reason: str = 'early_exit',
                                up_bid: float = None, down_bid: float = None,
                                keep_market_open_for_reentry: bool = False,
                                skip_exchange_sell: bool = False,
                                parallel_sell_results: Optional[Dict] = None) -> Optional[Dict]:
        """
        Early exit: close position at current favorite price
        🛡️ THREAD-SAFE: can be called from different threads
        
        Args:
            market_slug: Market identifier
            exit_price: Current favorite price (e.g. 0.52)
            exit_reason: Reason for exit ('stop_loss', 'flip_stop', 'early_exit')
            up_bid: Current UP bid price (for selling UP tokens)
            down_bid: Current DOWN bid price (for selling DOWN tokens)
            keep_market_open_for_reentry: If True (flip reverse), do not add to closed_markets
            skip_exchange_sell: If True, sell was already submitted (e.g. parallel flip)
            parallel_sell_results: side -> OrderResult from flip_exchange_sell
        
        Returns:
            Trade result dict
        """
        with self.lock:
            # ✅ PROTECTION #1: Check that position exists
            if market_slug not in self.positions:
                return None
            
            # ✅ PROTECTION #2: Check market not closed (another thread could have closed)
            if market_slug in self.closed_markets and not keep_market_open_for_reentry:
                return None  # Already closed, skip silently
            
            pos = self.positions[market_slug]
            
            # Get contracts
            up_contracts = pos['UP']['total_shares']
            down_contracts = pos['DOWN']['total_shares']
            
            # Determine favorite (who has more contracts)
            if up_contracts > down_contracts:
                payout = up_contracts * exit_price + down_contracts * (1 - exit_price)
                exit_position_side = 'UP'
            else:
                payout = down_contracts * exit_price + up_contracts * (1 - exit_price)
                exit_position_side = 'DOWN'
            
            # Total cost
            total_cost = pos['UP']['total_invested'] + pos['DOWN']['total_invested']
            
            # PnL = payout - cost
            pnl = payout - total_cost
            roi_pct = (pnl / total_cost * 100) if total_cost > 0 else 0
            
            # Winner ratio (position concentration, not market result)
            total_shares = up_contracts + down_contracts
            winner_ratio = (
                (up_contracts / total_shares * 100)
                if exit_position_side == 'UP'
                else (down_contracts / total_shares * 100)
            )
            
            # Update capital
            self.current_capital += pnl
            
            # ═══════════════════════════════════════════════════════════
            # 📊 LOG FULL ORDERBOOK before selling (for analysis)
            # ═══════════════════════════════════════════════════════════
            if exit_reason in ['stop_loss', 'flip_stop']:
                try:
                    # Get current ask prices from data_feed
                    up_ask = 0.5
                    down_ask = 0.5
                    if _data_feed:
                        market_state = _data_feed.get_state(self.coin)
                        up_ask = market_state.get('up_ask', 0.5)
                        down_ask = market_state.get('down_ask', 0.5)
                    
                    self._last_orderbook_snapshot = self._capture_orderbook_snapshot(
                        market_slug, exit_reason,
                        up_bid if up_bid else (1 - exit_price),
                        down_bid if down_bid else exit_price,
                        up_ask, down_ask
                    )
                    self._log_exit_orderbook(self._last_orderbook_snapshot)
                except Exception as e:
                    print(f"[TRADER] ⚠ Failed to log orderbook: {e}")
                    self._last_orderbook_snapshot = None
            
            # 标的起/止：仅 Chainlink（pos / 窗口缓存），禁止 CoinGecko 现价写入平仓记录
            spot_start = float(pos.get("spot_start") or 0)
            spot_end = 0.0
            if up_contracts > down_contracts:
                bet_side = 'UP'
            elif down_contracts > up_contracts:
                bet_side = 'DOWN'
            elif up_contracts > 0:
                bet_side = 'UP'
            else:
                bet_side = 'DOWN'

            from trade_record import build_open_record, finalize_early_exit

            open_rows = self._pop_all_open_records_for_slug(market_slug)
            if not open_rows:
                entry_time, entry_timestamp = _entry_times_from_position(pos)
                open_rows = [
                    build_open_record(
                        market_slug=market_slug,
                        coin=self.coin,
                        bet_side=bet_side,
                        spot_at_entry=_spot_at_entry_from_position(pos) or 0,
                        token_ask=_token_ask_from_position(pos, bet_side) or 0,
                        contracts=up_contracts if bet_side == "UP" else down_contracts,
                        size_usd=total_cost,
                        entry_time=entry_time,
                        entry_timestamp=entry_timestamp,
                        unrealized_pnl=pnl,
                        up_shares=up_contracts,
                        down_shares=down_contracts,
                        total_cost=total_cost,
                    )
                ]

            closed_rows: List[Dict] = []
            for trade in open_rows:
                leg_cost = float(trade.get("total_cost") or trade.get("size_usd") or 0)
                leg_pnl = (
                    pnl * (leg_cost / total_cost)
                    if total_cost > 0 and leg_cost > 0
                    else pnl / max(len(open_rows), 1)
                )
                leg_payout = leg_cost + leg_pnl
                finalize_early_exit(
                    trade,
                    exit_reason=exit_reason,
                    pnl=leg_pnl,
                    payout=leg_payout,
                )
                trade["exit_position_side"] = exit_position_side
                trade["exit_price"] = exit_price
                trade["roi_pct"] = (
                    (leg_pnl / leg_cost * 100) if leg_cost > 0 else roi_pct
                )
                closed_rows.append(trade)
            trade = closed_rows[-1] if closed_rows else None

            try:
                for row in closed_rows:
                    self.closed_trades.append(row)
                self.rewrite_trade_records_file(closed_rows)
                
                # 3. Mark market as closed to prevent re-entry (skip if flip reverse follows)
                if not keep_market_open_for_reentry:
                    self.closed_markets.add(market_slug)
                
                # 4. NOW we can safely delete the position
                del self.positions[market_slug]
                
                # 5. Clean up market stats
                if market_slug in self.market_max_drawdown:
                    del self.market_max_drawdown[market_slug]
                if market_slug in self.market_entries_count:
                    del self.market_entries_count[market_slug]
                    
            except Exception as e:
                # CRITICAL: If logging failed, DO NOT delete position!
                # Position will remain open and can be closed again
                print(f"[TRADER] ⚠️ FAILED TO CLOSE MARKET {market_slug}: {e}")
                print(f"[TRADER] ⚠️ Position kept open for retry!")
                return None
            
            if trade:
                status = "✓" if trade.get("bet_won") else "🚨"
                n_entries = len(pos.get("all_entries") or [])
                print(
                    f"[TRADER] {status} EARLY EXIT {market_slug} @ ${exit_price:.2f}: "
                    f"{pnl:+.2f} ({roi_pct:+.1f}%) | {len(closed_rows)} record(s) | "
                    f"{trade.get('bet_result_label')} | {trade.get('exit_label')} | "
                    f"{n_entries} entries, ${total_cost:.0f} invested"
                )
            
            # 🔥 REAL SELL (if executor connected)
            # 📊 Collecting real payouts for accurate PnL
            real_payout = 0.0
            real_sells_executed = False
            
            if _order_executor and market_slug in _token_ids_cache:
                if skip_exchange_sell and parallel_sell_results is not None:
                    for side, result in parallel_sell_results.items():
                        if result and result.success:
                            real_payout += result.total_spent_usd
                            real_sells_executed = True
                        elif result and not result.dry_run:
                            print(f"[TRADER] ⚠ Failed to sell {side}: {result.error}")
                elif not skip_exchange_sell:
                    token_ids = _token_ids_cache[market_slug]
                    
                    # Sell both sides (UP and DOWN) using TRACKED contracts
                    for side in ['UP', 'DOWN']:
                        token_id = token_ids[side]
                        # Get tracked contract amount
                        side_contracts = up_contracts if side == 'UP' else down_contracts
                        
                        # Skip if no contracts
                        if side_contracts <= 0:
                            continue
                        
                        # Get bid price
                        bid = up_bid if side == 'UP' else down_bid
                        if bid is None:
                            # Fallback
                            bid = exit_price if side == 'UP' else (1 - exit_price)
                        
                        result = _order_executor.sell_position(
                            market_slug=market_slug,
                            token_id=token_id,
                            side=side,
                            contracts=side_contracts,  # TRACKED amount!
                            bid_price=bid
                        )
                        
                        if result.success:
                            # Accumulating REAL payout
                            real_payout += result.total_spent_usd
                            real_sells_executed = True
                        elif not result.dry_run:
                            print(f"[TRADER] ⚠ Failed to sell {side}: {result.error}")
                
                # ═══════════════════════════════════════════════════════════
                # 📊 SLIPPAGE ANALYSIS: Expected vs Actual
                # Compare estimated payout (by best BID) with real
                # ═══════════════════════════════════════════════════════════
                if real_sells_executed and real_payout > 0:
                    # Get orderbook snapshot (was captured BEFORE sell)
                    try:
                        if hasattr(self, '_last_orderbook_snapshot') and self._last_orderbook_snapshot:
                            snapshot = self._last_orderbook_snapshot
                            expected_payout = snapshot.get('expected_sale', {}).get('expected_payout_usd', payout)
                            expected_price = snapshot.get('expected_sale', {}).get('best_bid_price', exit_price)
                            
                            # Calculate slippage
                            slippage_usd = real_payout - expected_payout
                            slippage_pct = (slippage_usd / expected_payout * 100) if expected_payout > 0 else 0
                            
                            actual_avg_price = real_payout / (up_contracts + down_contracts) if (up_contracts + down_contracts) > 0 else 0
                            price_diff = actual_avg_price - expected_price
                            price_diff_pct = (price_diff / expected_price * 100) if expected_price > 0 else 0
                            
                            print(f"\n{'='*80}")
                            print(f"[SLIPPAGE ANALYSIS] {self.coin.upper()} - {exit_reason}")
                            print(f"{'='*80}")
                            print(f"📊 EXPECTED (based on BID at trigger):")
                            print(f"   Best BID price: ${expected_price:.4f}")
                            print(f"   Expected payout: ${expected_payout:.2f}")
                            print(f"   Expected PnL: ${pnl:.2f}")
                            print(f"")
                            print(f"💰 ACTUAL (from API response):")
                            print(f"   Avg fill price: ${actual_avg_price:.4f}")
                            print(f"   Actual payout: ${real_payout:.2f}")
                            print(f"   Actual PnL: ${real_pnl:.2f}")
                            print(f"")
                            print(f"📉 SLIPPAGE:")
                            print(f"   Payout difference: ${slippage_usd:+.2f} ({slippage_pct:+.1f}%)")
                            print(f"   Price difference: ${price_diff:+.4f} ({price_diff_pct:+.1f}%)")
                            
                            if slippage_usd < -1.0:
                                print(f"   ⚠️ NEGATIVE SLIPPAGE > $1 - investigating...")
                            elif abs(slippage_usd) < 0.5:
                                print(f"   ✅ Minimal slippage")
                            
                            print(f"{'='*80}\n")
                            
                            # Add to snapshot for logging
                            snapshot['actual_sale'] = {
                                'actual_payout': real_payout,
                                'actual_avg_price': actual_avg_price,
                                'actual_pnl': real_pnl,
                                'slippage_usd': slippage_usd,
                                'slippage_pct': slippage_pct,
                                'price_diff': price_diff,
                                'price_diff_pct': price_diff_pct
                            }
                            
                            # Overwrite snapshot with actual data
                            self._log_exit_orderbook(snapshot)
                            
                    except Exception as e:
                        print(f"[TRADER] ⚠ Slippage analysis error: {e}")
                
                # ═══════════════════════════════════════════════════════════
                # 📊 UPDATE TRADE RECORD with real data
                # Recalculate PnL based on REAL payout from blockchain
                # ═══════════════════════════════════════════════════════════
                if real_sells_executed and real_payout > 0:
                    # Recalculate PnL with real payout
                    real_pnl = real_payout - total_cost
                    real_roi_pct = (real_pnl / total_cost * 100) if total_cost > 0 else 0
                    
                    # Update trade record (returned and in memory)
                    trade['payout'] = real_payout
                    trade['pnl'] = real_pnl
                    trade['roi_pct'] = real_roi_pct
                    
                    # IMPORTANT: Also update last element in closed_trades
                    # (which was added before sell)
                    if self.closed_trades and self.closed_trades[-1]['market_slug'] == market_slug:
                        self.closed_trades[-1]['payout'] = real_payout
                        self.closed_trades[-1]['pnl'] = real_pnl
                        self.closed_trades[-1]['roi_pct'] = real_roi_pct
                    
                    # Log updated trade with real data
                    # (add second entry with updated=True flag for post-mortem analysis)
                    updated_trade = trade.copy()
                    updated_trade['updated'] = True
                    updated_trade['estimated_pnl'] = pnl
                    updated_trade['estimated_payout'] = payout
                    self._log_trade(updated_trade)
                    
                    # Update capital with real PnL (instead of estimated)
                    self.current_capital = self.current_capital - pnl + real_pnl
                    
                    print(f"[TRADER] 💰 Real payout: ${real_payout:.2f} (estimated: ${payout:.2f})")
                    if abs(real_pnl - pnl) > 0.5:
                        diff = real_pnl - pnl
                        print(f"[TRADER] ⚠️  PnL correction: {diff:+.2f} (real: {real_pnl:+.2f} vs estimated: {pnl:+.2f})")
            
            # ═══════════════════════════════════════════════════════════
            # 🔥 CRITICAL: Reset investment tracking for this market!
            # Now we can trade new market without limits!
            # ═══════════════════════════════════════════════════════════
            try:
                if _order_executor and hasattr(_order_executor, 'safety'):
                    _order_executor.safety.reset_market(market_slug)
            except Exception as reset_err:
                print(f"[TRADER] ⚠ Failed to reset market tracking: {reset_err}")
            
            return trade
    
    def _capture_orderbook_snapshot(self, market_slug: str, exit_reason: str, 
                                    up_bid: float, down_bid: float, up_ask: float, down_ask: float) -> Dict:
        """
        Capture full orderbook snapshot for exit analysis
        
        Returns dict with position + orderbook data
        """
        pos = self.positions.get(market_slug, {})
        
        # Determine which side we're selling
        up_shares = pos.get('UP', {}).get('total_shares', 0)
        down_shares = pos.get('DOWN', {}).get('total_shares', 0)
        
        if up_shares > down_shares:
            our_side = 'UP'
            sell_contracts = up_shares
            sell_bid_price = up_bid
        elif down_shares > 0:
            our_side = 'DOWN'
            sell_contracts = down_shares
            sell_bid_price = down_bid
        else:
            our_side = None
            sell_contracts = 0
            sell_bid_price = 0
        
        total_invested = pos.get('UP', {}).get('total_invested', 0) + pos.get('DOWN', {}).get('total_invested', 0)
        
        # Get full orderbook from data_feed
        up_bids_full = []
        down_bids_full = []
        up_asks_full = []
        down_asks_full = []
        
        if _data_feed:
            market_state = _data_feed.get_state(self.coin)
            up_bids_full = market_state.get('up_bids_full', [])
            down_bids_full = market_state.get('down_bids_full', [])
            up_asks_full = market_state.get('up_asks_full', [])
            down_asks_full = market_state.get('down_asks_full', [])
        
        snapshot = {
            'timestamp': time.time(),
            'datetime': time.strftime('%Y-%m-%d %H:%M:%S'),
            'coin': self.coin,
            'market_slug': market_slug,
            'exit_reason': exit_reason,
            'position': {
                'up_shares': up_shares,
                'down_shares': down_shares,
                'up_invested': pos.get('UP', {}).get('total_invested', 0),
                'down_invested': pos.get('DOWN', {}).get('total_invested', 0),
                'total_invested': total_invested,
                'our_side': our_side
            },
            'orderbook': {
                'UP': {
                    'best_bid': up_bid,
                    'best_ask': up_ask,
                    'spread': up_ask - up_bid if (up_ask and up_bid) else 0,
                    'bids_top5': [{'price': p, 'size': s} for p, s in up_bids_full[:5]],
                    'asks_top1': [{'price': p, 'size': s} for p, s in up_asks_full[:1]]
                },
                'DOWN': {
                    'best_bid': down_bid,
                    'best_ask': down_ask,
                    'spread': down_ask - down_bid if (down_ask and down_bid) else 0,
                    'bids_top5': [{'price': p, 'size': s} for p, s in down_bids_full[:5]],
                    'asks_top1': [{'price': p, 'size': s} for p, s in down_asks_full[:1]]
                }
            },
            'expected_sale': {
                'side': our_side,
                'contracts': sell_contracts,
                'best_bid_price': sell_bid_price,
                'expected_payout_usd': sell_contracts * sell_bid_price if sell_bid_price else 0,
                'invested_usd': total_invested,
                'expected_loss_usd': (sell_contracts * sell_bid_price - total_invested) if sell_bid_price else -total_invested
            }
        }
        
        return snapshot
    
    def _log_exit_orderbook(self, snapshot: Dict):
        """Write orderbook snapshot to log file for analysis"""
        import os
        
        log_dir = f"logs/{self.strategy_name}"
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = f"{log_dir}/exit_orderbooks.jsonl"
        
        with open(log_file, 'a') as f:
            f.write(json.dumps(snapshot) + '\n')
        
        # Print summary to console
        print(f"\n{'='*80}")
        print(f"[EXIT ORDERBOOK] {snapshot['coin'].upper()} - {snapshot['exit_reason']}")
        print(f"Market: {snapshot['market_slug']}")
        print(f"Our side: {snapshot['position']['our_side']}")
        print(f"Invested: ${snapshot['position']['total_invested']:.2f}")
        print(f"Best bid (sell price): {snapshot['expected_sale']['best_bid_price']:.4f}")
        print(f"Expected payout: ${snapshot['expected_sale']['expected_payout_usd']:.2f}")
        print(f"Expected loss: ${snapshot['expected_sale']['expected_loss_usd']:.2f}")
        print(f"UP: BID={snapshot['orderbook']['UP']['best_bid']:.4f} ASK={snapshot['orderbook']['UP']['best_ask']:.4f} SPREAD={snapshot['orderbook']['UP']['spread']:.4f}")
        print(f"DOWN: BID={snapshot['orderbook']['DOWN']['best_bid']:.4f} ASK={snapshot['orderbook']['DOWN']['best_ask']:.4f} SPREAD={snapshot['orderbook']['DOWN']['spread']:.4f}")
        
        # Print full orderbook of selling side
        our_side = snapshot['position']['our_side']
        if our_side:
            print(f"\n{our_side} Orderbook (we're selling here):")
            ob = snapshot['orderbook'][our_side]
            print(f"  Asks (top 1):")
            for level in ob['asks_top1']:
                print(f"    ${level['price']:.4f} × {level['size']:.2f}")
            print(f"  Bids (top 5):")
            for level in ob['bids_top5']:
                print(f"    ${level['price']:.4f} × {level['size']:.2f}")
        
        print(f"{'='*80}\n")
    
    def get_market_stats(self, market_slug: str, up_current: float = 0.5, down_current: float = 0.5) -> Optional[Dict]:
        """
        Get statistics for a specific market including unrealized PnL
        
        ✅ USES REAL DATA from trader.positions (updated via REST API takingAmount)!
        """
        if market_slug not in self.positions:
            return None
        
        pos = self.positions[market_slug]
        
        total_entries = len(pos['all_entries'])
        
        # ✅ USE REAL DATA from trader.positions (updated via REST API)
        total_invested = pos['UP']['total_invested'] + pos['DOWN']['total_invested']
        up_shares = pos['UP']['total_shares']
        down_shares = pos['DOWN']['total_shares']
        up_invested = pos['UP']['total_invested']
        down_invested = pos['DOWN']['total_invested']
        
        up_avg_price = (pos['UP']['total_invested'] / pos['UP']['total_shares']) if pos['UP']['total_shares'] > 0 else 0
        down_avg_price = (pos['DOWN']['total_invested'] / pos['DOWN']['total_shares']) if pos['DOWN']['total_shares'] > 0 else 0
        
        # Calculate unrealized PnL using current prices
        up_value = pos['UP']['total_shares'] * up_current
        down_value = pos['DOWN']['total_shares'] * down_current
        total_value = up_value + down_value
        unrealized_pnl = total_value - total_invested
        
        up_entries = len(pos['UP']['entries'])
        down_entries = len(pos['DOWN']['entries'])
        
        total_shares = up_shares + down_shares
        up_ratio = (up_shares / total_shares * 100) if total_shares > 0 else 0
        down_ratio = (down_shares / total_shares * 100) if total_shares > 0 else 0
        
        return {
            'total_entries': total_entries,
            'total_invested': total_invested,
            'total_cost': total_invested,  # Alias for compatibility
            'avg_per_entry': total_invested / total_entries if total_entries > 0 else 0,
            'up_entries': up_entries,
            'down_entries': down_entries,
            'up_invested': up_invested,  # ✅ REAL data
            'down_invested': down_invested,  # ✅ REAL data
            'up_shares': up_shares,  # ✅ REAL data
            'down_shares': down_shares,  # ✅ REAL data
            'up_avg_price': up_avg_price,
            'down_avg_price': down_avg_price,
            'up_ratio': up_ratio,
            'down_ratio': down_ratio,
            'unrealized_pnl': unrealized_pnl,  # ✅ REAL PnL from WebSocket!
            'exposure_pct': (total_invested / self.current_capital * 100) if self.current_capital > 0 else 0.0
        }
    
    @staticmethod
    def infer_trade_outcome_win(trade: Dict) -> bool:
        """True only when Polymarket official settlement says we bet the winning side."""
        if trade.get("bet_won") is not None:
            return bool(trade["bet_won"])
        return False

    def get_performance_stats(self) -> Dict:
        """Get overall performance statistics (no Gamma HTTP — uses stored trade fields)."""
        total_trades = len(self.closed_trades)
        resolved = [t for t in self.closed_trades if t.get("bet_won") is not None]
        wins = sum(1 for t in resolved if t.get("bet_won"))
        losses = len(resolved) - wins
        pending = total_trades - len(resolved)

        win_rate = (wins / len(resolved) * 100) if resolved else 0
        
        total_pnl = sum(t['pnl'] for t in self.closed_trades)
        avg_pnl = total_pnl / total_trades if total_trades > 0 else 0
        
        winning_trades = [t for t in resolved if t.get("bet_won")]
        losing_trades = [t for t in resolved if not t.get("bet_won")]
        
        best_win = max(winning_trades, key=lambda t: t['pnl']) if winning_trades else None
        worst_loss = min(losing_trades, key=lambda t: t['pnl']) if losing_trades else None
        
        total_wins = sum(t['pnl'] for t in winning_trades)
        total_losses = abs(sum(t['pnl'] for t in losing_trades))
        profit_factor = (total_wins / total_losses) if total_losses > 0 else 0
        
        avg_entries = sum(t.get('total_entries', 0) for t in self.closed_trades) / total_trades if total_trades > 0 else 0
        avg_invested = sum(t.get('total_cost', 0) for t in self.closed_trades) / total_trades if total_trades > 0 else 0
        
        return {
            'total_trades': total_trades,
            'wins': wins,
            'losses': losses,
            'pending_settlement': pending,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'best_win': best_win,
            'worst_loss': worst_loss,
            'profit_factor': profit_factor,
            'avg_entries': avg_entries,
            'avg_invested': avg_invested
        }
    
    def _update_market_stats(self, market_slug: str):
        """Update market statistics after entry"""
        # Update entries count
        if market_slug not in self.market_entries_count:
            self.market_entries_count[market_slug] = 0
        self.market_entries_count[market_slug] += 1
        
        # Initialize max drawdown if needed
        if market_slug not in self.market_max_drawdown:
            self.market_max_drawdown[market_slug] = 0.0
    
    def update_market_drawdown(self, market_slug: str, unrealized_pnl: float):
        """Update max drawdown for market if current is worse"""
        if market_slug not in self.market_max_drawdown:
            self.market_max_drawdown[market_slug] = 0.0
        
        if unrealized_pnl < self.market_max_drawdown[market_slug]:
            self.market_max_drawdown[market_slug] = unrealized_pnl
    
    def get_market_detailed_stats(self, market_slug: str, up_ask: float = 0.5, down_ask: float = 0.5) -> Optional[Dict]:
        """
        Get detailed statistics for a market
        
        Args:
            market_slug: Market identifier
            up_ask: Current UP ask price
            down_ask: Current DOWN ask price
            
        Returns:
            Dict with detailed stats or None
        """
        if market_slug not in self.positions:
            return None
        
        pos = self.positions[market_slug]
        
        up_shares = pos['UP']['total_shares']
        down_shares = pos['DOWN']['total_shares']
        up_invested = pos['UP']['total_invested']
        down_invested = pos['DOWN']['total_invested']
        total_invested = up_invested + down_invested
        
        # Current value (unrealized)
        current_value = (up_shares * up_ask) + (down_shares * down_ask)
        unrealized_pnl = current_value - total_invested
        unrealized_pct = (unrealized_pnl / total_invested * 100) if total_invested > 0 else 0
        
        # ═══════════════════════════════════════════════════════════
        # 🚨 CHECK STOP-LOSS RIGHT HERE (where PnL is calculated!)
        # ═══════════════════════════════════════════════════════════
        stop_loss_triggered = False
        stop_loss_threshold = None
        stop_loss_type = None
        
        # Get coin from market_slug (e.g., "btc-updown-15m-1768060800" -> "btc")
        coin = market_slug.split('-')[0] if '-' in market_slug else ''
        
        # Check if we have config for stop-loss
        if self.config and coin and total_invested > 0:
            sl_config = self.config.get('exit', {}).get('stop_loss', {}).get('per_coin', {}).get(coin, {})
            sl_enabled = sl_config.get('enabled', False)
            sl_type = sl_config.get('type', 'none')
            sl_value = sl_config.get('value', None)
            
            if sl_enabled and sl_value is not None:
                if sl_type == 'fixed':
                    # Fixed dollar amount (e.g., -$10)
                    stop_loss_threshold = sl_value
                    stop_loss_triggered = unrealized_pnl <= stop_loss_threshold
                    stop_loss_type = 'fixed'
                elif sl_type == 'percent':
                    # Percentage of invested capital (e.g., -15%)
                    stop_loss_threshold = total_invested * (sl_value / 100.0)
                    stop_loss_triggered = unrealized_pnl <= stop_loss_threshold
                    stop_loss_type = 'percent'
        
        # ═══════════════════════════════════════════════════════════
        # 🚨 CHECK FLIP-STOP (price reversal protection)
        # ═══════════════════════════════════════════════════════════
        flip_stop_triggered = False
        flip_stop_price = None
        flip_stop_side = None
        flip_stop_leg = None
        
        if self.config and coin and (up_shares > 0 or down_shares > 0):
            from strategy import check_flip_stop_trigger, resolve_flip_stop_target

            flip_cfg = self.config.get('exit', {}).get('flip_stop', {})
            max_spot_dist = float(flip_cfg.get('max_spot_distance_from_open_usd', 0) or 0)
            pos_flip = self.positions.get(market_slug) or {}
            target = resolve_flip_stop_target(
                flip_cfg=flip_cfg,
                up_ask=up_ask,
                down_ask=down_ask,
                all_entries=pos_flip.get("all_entries") or [],
            )
            if target:
                our_side = target["side"]
                our_price = float(target["price"])
                flip_stop_price = float(target["threshold"])
                flip_stop_leg = target.get("leg")
                flip_stop_side = our_side
                market_open = 0.0
                current_spot = 0.0
                if _data_feed and self.coin:
                    st = _data_feed.get_state(self.coin)
                    if st and (st.get('market_slug') or '') == market_slug:
                        market_open = float(st.get('market_start_price') or 0)
                        current_spot = float(st.get('price') or 0)

                if check_flip_stop_trigger(
                    our_price=our_price,
                    bet_side=our_side,
                    flip_stop_price=flip_stop_price,
                    market_open_spot=market_open,
                    current_spot=current_spot,
                    max_spot_distance_usd=max_spot_dist,
                ):
                    flip_stop_triggered = True
                    leg_label = "翻转补单腿" if flip_stop_leg == "reverse" else "首单腿"
                    spot_note = ""
                    if max_spot_dist > 0 and market_open > 0 and current_spot > 0:
                        if our_side == "UP":
                            spot_note = (
                                f" | spot ${current_spot:,.2f} < open+{max_spot_dist:.0f}"
                                f" (${market_open + max_spot_dist:,.2f})"
                            )
                        else:
                            spot_note = (
                                f" | spot ${current_spot:,.2f} > open-{max_spot_dist:.0f}"
                                f" (${market_open - max_spot_dist:,.2f})"
                            )
                    print(
                        f"[FLIP-STOP] 🚨 {coin.upper()} {leg_label} {our_side} @ ${our_price:.4f} "
                        f"<= ${flip_stop_price:.4f}{spot_note} TRIGGERED!"
                    )
                elif our_price <= flip_stop_price and max_spot_dist > 0:
                    if market_open > 0 and current_spot > 0:
                        if our_side == "UP":
                            need = f"spot < ${market_open + max_spot_dist:,.2f} (open+{max_spot_dist:.0f})"
                        else:
                            need = f"spot > ${market_open - max_spot_dist:,.2f} (open-{max_spot_dist:.0f})"
                        print(
                            f"[FLIP-STOP] ⏳ {coin.upper()} token<=${flip_stop_price:.2f} "
                            f"but {need}, now ${current_spot:,.2f}"
                        )
                elif our_price < flip_stop_price * 1.25:
                    print(
                        f"[FLIP-STOP] ⚠️  {coin.upper()} {our_side} @ ${our_price:.4f} "
                        f"close to ${flip_stop_price:.4f}"
                    )
        
        # Update drawdown with current unrealized PnL
        self.update_market_drawdown(market_slug, unrealized_pnl)
        
        # Max drawdown
        max_dd = self.market_max_drawdown.get(market_slug, 0.0)
        max_dd_pct = (max_dd / total_invested * 100) if total_invested > 0 else 0
        
        # Average entry prices
        avg_up_price = up_invested / up_shares if up_shares > 0 else 0
        avg_down_price = down_invested / down_shares if down_shares > 0 else 0
        
        # Entries count
        entries_count = self.market_entries_count.get(market_slug, len(pos['all_entries']))
        
        return {
            'up_shares': up_shares,
            'down_shares': down_shares,
            'up_invested': up_invested,
            'down_invested': down_invested,
            'total_invested': total_invested,
            'unrealized_pnl': unrealized_pnl,
            'unrealized_pct': unrealized_pct,
            'max_drawdown': max_dd,
            'max_drawdown_pct': max_dd_pct,
            'avg_up_price': avg_up_price,
            'avg_down_price': avg_down_price,
            'entries_count': entries_count,
            'stop_loss_triggered': stop_loss_triggered,
            'stop_loss_threshold': stop_loss_threshold,
            'stop_loss_type': stop_loss_type,
            'flip_stop_triggered': flip_stop_triggered,
            'flip_stop_price': flip_stop_price,
            'flip_stop_side': flip_stop_side,
            'flip_stop_leg': flip_stop_leg,
        }
    
    def _append_entry_journal(
        self,
        *,
        market_slug: str,
        side: str,
        spot_at_entry: float,
        token_ask: float,
        contracts: float,
        size_usd: float,
        entry_time: float,
        entry_timestamp: str,
    ) -> None:
        """Persist open entry for web dashboard (rewrite same slug = latest order time)."""
        try:
            path = self.log_dir / "entries.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            row = {
                "type": "entry",
                "market_slug": market_slug,
                "bet_side": side,
                "spot_at_entry": round(float(spot_at_entry), 2) if spot_at_entry else 0,
                "token_ask": round(float(token_ask), 4) if token_ask else 0,
                "contracts": round(float(contracts), 4),
                "size_usd": round(float(size_usd), 2),
                "entry_time": float(entry_time),
                "entry_timestamp": entry_timestamp,
                "coin": self.coin,
            }
            # Keep only latest line per market_slug so times do not stick to old orders
            existing: List[Dict] = []
            if path.is_file():
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            existing.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            kept = [r for r in existing if r.get("market_slug") != market_slug]
            kept.append(row)
            with open(path, "w", encoding="utf-8") as f:
                for r in kept[-200:]:
                    f.write(json.dumps(r) + "\n")
                f.flush()
        except OSError as e:
            print(f"[TRADER] ⚠️ entry journal write failed: {e}")

    def _sync_one_trade_db(self, trade: Dict) -> None:
        """Background-safe single-row MySQL upsert; never raises."""
        if not self._trade_db:
            return
        try:
            payload = dict(trade)
            payload.setdefault("strategy_name", self.strategy_name)
            self._trade_db.upsert_record(payload, strategy_name=self.strategy_name)
        except Exception as e:
            print(f"[TRADER] ⚠ MySQL 单行同步失败: {e}")

    def _log_trade(self, trade: Dict):
        """
        Log trade to file with maximum fault tolerance.
        MySQL sync is best-effort and must not block or fail the trading path.
        """
        try:
            self.trades_file.parent.mkdir(parents=True, exist_ok=True)

            if self._jsonl_backup_enabled():
                with open(self.trades_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps(trade) + "\n")
                    f.flush()
        except PermissionError as e:
            print(f"[TRADER] ⚠️ PERMISSION ERROR logging trade: {e}")
            print(f"[TRADER] ⚠️ Trade data: {trade}")
            print(f"[TRADER] ⚠️ File: {self.trades_file}")
            raise  # Re-raise to prevent position deletion
            
        except OSError as e:
            print(f"[TRADER] ⚠️ DISK ERROR logging trade: {e}")
            print(f"[TRADER] ⚠️ Trade data: {trade}")
            print(f"[TRADER] ⚠️ Check disk space: df -h")
            raise  # Re-raise to prevent position deletion
            
        except Exception as e:
            print(f"[TRADER] ⚠️ UNKNOWN ERROR logging trade: {e}")
            print(f"[TRADER] ⚠️ Trade data: {trade}")
            import traceback
            traceback.print_exc()
            raise  # Re-raise to prevent position deletion

        if self._trade_db:
            threading.Thread(
                target=self._sync_one_trade_db,
                args=(trade,),
                daemon=True,
                name=f"trade_db_one_{self.strategy_name}",
            ).start()

    def save_session(self):
        """Save current session state"""
        try:
            session = {
                'starting_capital': self.starting_capital,
                'current_capital': self.current_capital,
                'total_pnl': self.current_capital - self.starting_capital,
                'roi_pct': ((self.current_capital / self.starting_capital) - 1) * 100,
                'open_positions': len(self.positions),
                'closed_trades': len(self.closed_trades),
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            with open(self.session_file, 'w') as f:
                json.dump(session, f, indent=2)
                
        except Exception as e:
            print(f"[TRADER] Error saving session: {e}")
    
    def log_entry_detailed(self, market_slug: str, side: str, contracts: int, 
                           price: float, up_ask: float, down_ask: float,
                           winner_ratio: float, is_recovery: bool, 
                           entry_reason: str, seconds_till_end: int,
                           time_from_start: int):
        """
        Log detailed entry for backtesting analysis
        
        Args:
            market_slug: Full market slug
            side: 'UP' or 'DOWN'
            contracts: Number of contracts
            price: Entry price
            up_ask: Current UP ask price
            down_ask: Current DOWN ask price
            winner_ratio: Current winner ratio (0.0-1.0)
            is_recovery: Is this a recovery entry after WR < 40%?
            entry_reason: 'normal' or 'recovery'
            seconds_till_end: Seconds until market end
            time_from_start: Seconds from market start
        """
        import os
        
        # Create detailed logs directory
        detailed_dir = str(self.log_dir).replace('/logs/', '/logs_detailed/')
        Path(detailed_dir).mkdir(parents=True, exist_ok=True)
        
        # Get position data
        if market_slug not in self.positions:
            return
        
        pos = self.positions[market_slug]
        
        # Calculate current metrics
        up_contracts = pos['UP']['total_shares']
        down_contracts = pos['DOWN']['total_shares']
        up_invested = pos['UP']['total_invested']
        down_invested = pos['DOWN']['total_invested']
        total_invested = up_invested + down_invested
        total_contracts = up_contracts + down_contracts
        entries_count = len(pos['all_entries'])
        
        # Calculate CORRECT unrealized PnL based on current market prices
        current_value = (up_contracts * up_ask) + (down_contracts * down_ask)
        unrealized_pnl = current_value - total_invested
        unrealized_pnl_pct = (unrealized_pnl / total_invested * 100) if total_invested > 0 else 0
        
        # Update max drawdown with current unrealized PnL BEFORE reading it
        self.update_market_drawdown(market_slug, unrealized_pnl)
        
        # Calculate PnL scenarios if market resolves
        if_up_wins = (up_contracts * 1.0) - total_invested
        if_down_wins = (down_contracts * 1.0) - total_invested
        
        # Average prices
        avg_up_price = (up_invested / up_contracts) if up_contracts > 0 else 0
        avg_down_price = (down_invested / down_contracts) if down_contracts > 0 else 0
        
        # Get max drawdown for this market (after updating it above)
        max_dd = self.market_max_drawdown.get(market_slug, 0.0)
        max_dd_pct = (max_dd / total_invested * 100) if total_invested > 0 else 0
        
        # Build entry data
        entry_data = {
            "timestamp": int(time.time()),
            "market_slug": market_slug,
            "seconds_till_end": seconds_till_end,
            "time_from_start": time_from_start,
            
            "market_prices": {
                "up_ask": round(up_ask, 3),
                "down_ask": round(down_ask, 3),
                "confidence": round(abs(down_ask - up_ask), 3)
            },
            
            "entry": {
                "side": side,
                "contracts": contracts,
                "price": round(price, 3),
                "cost": round(contracts * price, 2)
            },
            
            "position_after": {
                "up_contracts": int(up_contracts),
                "down_contracts": int(down_contracts),
                "up_invested": round(up_invested, 2),
                "down_invested": round(down_invested, 2),
                "total_invested": round(total_invested, 2),
                "total_contracts": int(total_contracts),
                "entries_count": entries_count
            },
            
            "pnl_metrics": {
                "unrealized_pnl": round(unrealized_pnl, 2),
                "unrealized_pnl_pct": round(unrealized_pnl_pct, 2),
                "max_drawdown": round(max_dd, 2),
                "max_drawdown_pct": round(max_dd_pct, 2),
                "if_up_wins": round(if_up_wins, 2),
                "if_down_wins": round(if_down_wins, 2),
                "avg_up_price": round(avg_up_price, 3),
                "avg_down_price": round(avg_down_price, 3)
            },
            
            "strategy_state": {
                "winner_ratio": round(winner_ratio, 3),
                "is_recovery": is_recovery,
                "entry_reason": entry_reason
            }
        }
        
        # Filename based on market slug
        filename = f"{market_slug}_entries.jsonl"
        filepath = os.path.join(detailed_dir, filename)
        
        # Append entry
        with open(filepath, 'a') as f:
            f.write(json.dumps(entry_data) + '\n')


