"""
Multi-Market data feed: Polymarket orderbook for 4 coins
"""
import json
import time
import threading
import websocket
import subprocess
import requests
import os
import hmac
import hashlib
import base64
from typing import Optional, Dict, Any, List, Tuple
from urllib.parse import urlparse
import trader as trader_module
from position_tracker import PositionTracker
from proxy_env import (
    requests_proxies,
    websocket_proxy_kwargs,
    resolve_proxy_url,
    normalize_proxy_url,
)
from spot_price import fetch_coin_spot_usd, fetch_spot_usd, spot_price_source_from_config
from chainlink_rtds import ChainlinkRtdsFeed, RTDS_WS_URL
from market_config import enabled_coins_from_config
from trading_hours import TradingHours

_MIN_TOKEN_PX = 0.001
_MAX_TOKEN_PX = 1.0


class DataFeed:
    """Polymarket orderbooks for BTC, ETH, SOL, XRP (configurable 5m or 15m windows)."""
    
    def __init__(self, config: Dict, config_path=None):
        self.config = config
        self._config_path = None
        if config_path is not None:
            from pathlib import Path

            self._config_path = Path(config_path).resolve()

        from market_config import apply_market_window_settings

        apply_market_window_settings(self.config)

        # ✅ POSITION TRACKER - single source of truth for positions!
        self.position_tracker = PositionTracker()
        
        # API credentials for authenticated WebSocket
        self.api_key = os.getenv('POLYMARKET_API_KEY')
        self.api_secret = os.getenv('POLYMARKET_API_SECRET')
        self.api_passphrase = os.getenv('POLYMARKET_API_PASSPHRASE')
        
        pm = config.get("data_sources", {}).get("polymarket", {})
        strategy_cfg = config.get("strategy") or {}
        self.enabled_coins = enabled_coins_from_config(config)
        if not self.enabled_coins:
            raise ValueError("No coins enabled in config trading section")
        self.spot_price_source = spot_price_source_from_config(config)
        _min_wr = float(strategy_cfg.get("min_window_range_usd", 0) or 0)
        self._chainlink_feed: Optional[ChainlinkRtdsFeed] = None
        if self.spot_price_source == "chainlink" or _min_wr > 0:
            rtds_url = str(pm.get("rtds_ws_url") or RTDS_WS_URL).strip() or RTDS_WS_URL
            self._chainlink_feed = ChainlinkRtdsFeed(
                ws_url=rtds_url,
                proxy_url_override=None,  # set below after proxy resolved
                on_price_update=self._on_chainlink_spot_update,
                coins=list(self.enabled_coins),
            )
        raw_px = (pm.get("http_proxy") or pm.get("https_proxy") or "").strip()
        self._proxy_url_override: Optional[str] = (
            normalize_proxy_url(raw_px) if raw_px else None
        )
        # Polymarket WS: optional best_bid_ask / new_market stream. Default off — some proxies or
        # server paths misbehave with custom_feature_enabled; book + price_change work without it.
        self._ws_custom_features: bool = bool(pm.get("websocket_custom_features", False))
        self._market_ws_enabled: bool = bool(pm.get("market_ws_enabled", True))
        self._clob_host = os.getenv("CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
        try:
            self._clob_poll_sec = float(pm.get("clob_poll_interval_sec", 0.35))
        except (TypeError, ValueError):
            self._clob_poll_sec = 0.35
        if self._clob_poll_sec < 0:
            self._clob_poll_sec = 0.0
        if not self._market_ws_enabled and self._clob_poll_sec <= 0:
            print(
                "[DATA] ⚠ market_ws_enabled=false requires clob_poll_interval_sec > 0"
            )
        _soft_raw = pm.get("watchdog_soft_rest_sec")
        if _soft_raw is None and self._clob_poll_sec <= 0:
            self._watchdog_soft_rest_sec = 5.0
        else:
            try:
                self._watchdog_soft_rest_sec = max(0.0, float(_soft_raw or 0))
            except (TypeError, ValueError):
                self._watchdog_soft_rest_sec = 0.0
        self._watchdog_soft_rest_mono: Dict[str, float] = {
            c: 0.0 for c in self.enabled_coins
        }

        self.market_interval_sec = int(pm.get("market_interval_sec", 900))
        if self.market_interval_sec <= 0:
            self.market_interval_sec = 900
        # Slug: {coin}-updown-5m-{slot} or {coin}-updown-15m-{slot}
        if self.market_interval_sec == 300:
            self.market_slug_suffix = "5m"
        elif self.market_interval_sec == 900:
            self.market_slug_suffix = "15m"
        else:
            self.market_slug_suffix = (
                f"{self.market_interval_sec // 60}m"
                if self.market_interval_sec % 60 == 0
                else "15m"
            )
            print(
                f"[DATA] Warning: market_interval_sec={self.market_interval_sec} "
                f"(standard Polymarket crypto up/down uses 300 or 900). Slug suffix={self.market_slug_suffix}"
            )
        
        iv = self.market_interval_sec
        tnow = int(time.time())
        self.markets = {}
        for coin in self.enabled_coins:
            self.markets[coin] = {
                "slug": "",
                "up_ask": 0.5,
                "down_ask": 0.5,
                "up_bid": 0.5,
                "down_bid": 0.5,
                "up_ask_timestamp": 0.0,
                "down_ask_timestamp": 0.0,
                "up_bid_timestamp": 0.0,
                "down_bid_timestamp": 0.0,
                "up_bids_full": [],
                "down_bids_full": [],
                "up_asks_full": [],
                "down_asks_full": [],
                "tokens": {},
                "seconds_till_end": iv,
                "market_end_time": tnow + iv,
                "market_start_price": 0.0,
            }
        
        # Current prices (only BTC and ETH have price feeds)
        self.btc_price = 0.0
        self.eth_price = 0.0
        
        # Thread safety - per-coin locks for enabled coins only
        self.locks = {c: threading.Lock() for c in self.enabled_coins}
        self.stop_event = threading.Event()
        
        # Threads
        self.threads = []
        
        # Event-driven callbacks for price updates
        self.price_callbacks = []

        # PM market WS watchdog (A: slug/window drift, B: stale ask) — no REST poll
        pm_cfg = config.get("data_sources", {}).get("polymarket", {}) or {}
        self._ws_watchdog_interval_sec = max(
            0.5, float(pm_cfg.get("ws_watchdog_interval_sec", 1.0) or 1.0)
        )
        self._ws_stale_ask_sec = max(
            5.0, float(pm_cfg.get("ws_stale_ask_sec", 15.0) or 15.0)
        )
        self._ws_window_ended_grace_sec = max(
            2.0, float(pm_cfg.get("ws_window_ended_grace_sec", 5.0) or 5.0)
        )
        self._ws_force_close_cooldown_sec = max(
            3.0, float(pm_cfg.get("ws_force_close_cooldown_sec", 8.0) or 8.0)
        )
        self._pm_ws_lock = {c: threading.Lock() for c in self.enabled_coins}
        self._pm_ws_app: Dict[str, Any] = {c: None for c in self.enabled_coins}
        self._pm_force_close_mono: Dict[str, float] = {c: 0.0 for c in self.enabled_coins}
        self._pm_hours_resume_mono: Dict[str, float] = {c: 0.0 for c in self.enabled_coins}
        self._clob_rest_fail_streak = 0
        self._last_clob_rest_ok_mono = time.monotonic()
        self._watchdog_stale_streak: Dict[str, int] = {
            c: 0 for c in self.enabled_coins
        }
        self._watchdog_callback_mono: Dict[str, float] = {
            c: 0.0 for c in self.enabled_coins
        }
        self._last_book_ok_mono: Dict[str, float] = {
            c: 0.0 for c in self.enabled_coins
        }
        self._last_pm_ws_msg_mono: Dict[str, float] = {
            c: 0.0 for c in self.enabled_coins
        }
        self._ui_rest_mono: Dict[str, float] = {c: 0.0 for c in self.enabled_coins}
        self._ui_rest_interval_sec = max(
            6.0, float(pm_cfg.get("ui_book_rest_interval_sec", 8.0) or 8.0)
        )
        self._resync_throttle_mono: Dict[str, float] = {
            c: 0.0 for c in self.enabled_coins
        }
        self._resync_throttle_sec = max(
            3.0, float(pm_cfg.get("market_resync_throttle_sec", 5.0) or 5.0)
        )
        self._ask_frozen_sec = max(
            6.0, float(pm_cfg.get("ask_price_frozen_sec", 8.0) or 8.0)
        )
        self._last_ask_pair: Dict[str, Tuple[float, float]] = {}
        self._last_ask_change_mono: Dict[str, float] = {
            c: time.monotonic() for c in self.enabled_coins
        }
        # Lock-free UI read cache — published on each book write (HTTP / 8Hz patch).
        self._ui_state_cache: Dict[str, Dict[str, Any]] = {}
        self._ui_state_cache_lock = threading.Lock()
        # UI/trading read path: markets only (clob_poll + WS write here).
        self._ws_post_hours_grace_sec = max(
            10.0, float(pm_cfg.get("ws_post_hours_grace_sec", 45.0) or 45.0)
        )
        self.trading_hours = TradingHours.from_config(config)
        self._hours_outside_logged: set = set()
        self._hours_feeds_paused_logged = False

        eff = resolve_proxy_url(self._proxy_url_override)
        if self._chainlink_feed is not None:
            self._chainlink_feed._proxy_url_override = self._proxy_url_override
        if eff:
            pe = urlparse(eff)
            src = "config.json data_sources.polymarket.http_proxy" if self._proxy_url_override else "HTTPS_PROXY/HTTP_PROXY env"
            h, p = pe.hostname or "?", pe.port or ("443" if (pe.scheme or "").lower() == "https" else "80")
            print(f"[DATA] Polymarket proxy active ({src}) → host {h} port {p} (REST + WS MARKET)")
        _ws_px = websocket_proxy_kwargs(self._proxy_url_override)
        if eff and not _ws_px:
            print("[DATA] ⚠️ Proxy URL set but WebSocket kwargs empty (check scheme: use http://127.0.0.1:PORT).")
        print(
            f"[DATA] Spot price source for order logic: {self.spot_price_source.upper()}"
            + (" (Polymarket RTDS crypto_prices_chainlink)" if self.spot_price_source == "chainlink" else " (CoinGecko REST)")
        )
        if _min_wr > 0 and self.spot_price_source != "chainlink":
            print(
                f"[DATA] Chainlink RTDS also started for window range filter "
                f"(min_window_range_usd=${_min_wr:,.0f})"
            )
        print(
            f"[DATA] Active market feeds: {', '.join(c.upper() for c in self.enabled_coins)}"
        )
        if self.trading_hours.enabled:
            print(
                f"[DATA] Trading hours active (local): {self.trading_hours.ranges_summary()} "
                f"— no Polymarket/Gamma outside windows; "
                f"watchdog resumes after {self._ws_post_hours_grace_sec:.0f}s grace"
            )
    
    def _on_chainlink_spot_update(self, coin: str, value: float) -> None:
        """RTDS push → update in-memory spot used by strategy / flip-stop."""
        if coin not in self.locks or value <= 0:
            return
        with self.locks[coin]:
            if coin == "btc":
                self.btc_price = value
            elif coin == "eth":
                self.eth_price = value

    def _fetch_spot_for_coin(self, coin: str, timeout: float = 2.0) -> Optional[float]:
        return fetch_spot_usd(
            coin,
            self.spot_price_source,
            timeout=timeout,
            chainlink_feed=self._chainlink_feed,
        )

    def get_chainlink_spot(self, coin: str) -> float:
        """Chainlink RTDS price only (for window range sampling)."""
        if self._chainlink_feed is None:
            return 0.0
        px = self._chainlink_feed.get_price(coin)
        return float(px) if px and px > 0 else 0.0

    def _pause_external_feeds_for_hours(self) -> None:
        """Stop RTDS (and ensure PM WS closed) while outside trading windows."""
        if self._chainlink_feed is not None:
            self._chainlink_feed.stop()
        for coin in self.enabled_coins:
            with self._pm_ws_lock[coin]:
                ws = self._pm_ws_app.get(coin)
            if ws is not None:
                self._close_pm_ws(coin, "outside trading window", source="hours")

    def _resume_external_feeds_for_hours(self) -> None:
        if self._chainlink_feed is not None:
            self._chainlink_feed.start()

    def _reload_trading_hours_from_disk(self) -> None:
        if self._config_path is None or not self._config_path.exists():
            return
        try:
            from trading_hours import load_from_config_path

            new_th = load_from_config_path(self._config_path)
        except Exception:
            return
        old = self.trading_hours.ranges_summary()
        new = new_th.ranges_summary()
        self.trading_hours = new_th
        if old != new:
            print(f"[HOURS] trading_hours reloaded from disk: {new}")

    def _trading_hours_supervisor(self) -> None:
        """Start/stop Chainlink RTDS with trading windows (PM workers self-pause)."""
        while not self.stop_event.is_set():
            self._reload_trading_hours_from_disk()
            if not self.trading_hours.enabled:
                time.sleep(30.0)
                continue
            if self.trading_hours.operations_active():
                if self._hours_feeds_paused_logged:
                    nxt = self.trading_hours.active_range_label() or "window"
                    print(
                        f"[HOURS] ▶ Trading window open ({nxt}) — "
                        f"resuming RTDS + PM feeds"
                    )
                    self._hours_feeds_paused_logged = False
                self._resume_external_feeds_for_hours()
                time.sleep(5.0)
            else:
                if not self._hours_feeds_paused_logged:
                    when = self.trading_hours.next_window_start()
                    when_s = when.strftime("%H:%M") if when else "?"
                    print(
                        f"[HOURS] ⏸ Outside trading window "
                        f"({self.trading_hours.ranges_summary()}) — "
                        f"all external feeds paused until {when_s}"
                    )
                    self._hours_feeds_paused_logged = True
                self._pause_external_feeds_for_hours()
                self.trading_hours.sleep_until_allowed(self.stop_event, max_sleep=60.0)

    def start(self):
        """Start data streams for enabled coins only."""
        if self._chainlink_feed is not None:
            if self.trading_hours.operations_active():
                self._chainlink_feed.start()
                print("[DATA] Started Chainlink RTDS spot feed")
            else:
                print("[DATA] Chainlink RTDS deferred until trading window opens")

        if self.trading_hours.enabled:
            sup = threading.Thread(
                target=self._trading_hours_supervisor,
                daemon=True,
                name="trading_hours_supervisor",
            )
            sup.start()
            self.threads.append(sup)

        if self._market_ws_enabled:
            for coin in self.enabled_coins:
                pm_thread = threading.Thread(
                    target=self._polymarket_worker, args=(coin,), daemon=True
                )
                pm_thread.start()
                self.threads.append(pm_thread)
                print(f"[DATA] Started Polymarket WS feed for {coin.upper()}")
        else:
            print(
                "[DATA] PM market WebSocket OFF — POST /prices poll drives "
                "entry + exit checks"
            )

        # ❌ USER CHANNEL DISABLED - WebSocket auth doesn't work
        # Using REST API takingAmount/makingAmount instead!
        print(f"[DATA] ℹ️  Position tracking via REST API responses")
        
        # Start local timer update (fixes timer freeze)
        timer_thread = threading.Thread(target=self._timer_worker, daemon=True)
        timer_thread.start()
        self.threads.append(timer_thread)

        watchdog_thread = threading.Thread(
            target=self._pm_ws_watchdog_worker,
            daemon=True,
            name="pm_ws_watchdog",
        )
        watchdog_thread.start()
        self.threads.append(watchdog_thread)
        if self._market_ws_enabled:
            if self._clob_poll_sec > 0:
                print(
                    f"[DATA] PM WebSocket watchdog every {self._ws_watchdog_interval_sec:g}s "
                    f"(stale ask>{self._ws_stale_ask_sec:g}s → WS reconnect + on-demand REST)"
                )
            else:
                print(
                    f"[DATA] CLOB REST poll OFF — ask via WS; watchdog every "
                    f"{self._ws_watchdog_interval_sec:g}s "
                    f"(soft REST @ {self._watchdog_soft_rest_sec:g}s stale, "
                    f"hard reconnect @ {self._ws_stale_ask_sec:g}s)"
                )

        if self._clob_poll_sec > 0:
            poll_thread = threading.Thread(
                target=self._clob_poll_worker, daemon=True, name="clob_poll"
            )
            poll_thread.start()
            self.threads.append(poll_thread)
            eff_poll = max(0.25, self._clob_poll_sec)
            print(
                f"[DATA] CLOB POST /prices poll every {eff_poll:g}s "
                f"(clob_poll_interval_sec={self._clob_poll_sec:g}; 0=disabled)"
            )

        print(
            f"[DATA] All feeds started: {len(self.enabled_coins)} Polymarket orderbook(s) "
            f"({self.market_slug_suffix} / {self.market_interval_sec}s windows)"
        )
        for coin in self.enabled_coins:
            self._publish_ui_state(coin)
    
    def stop(self):
        """Stop all data streams"""
        print("[DATA] Stopping feeds...")
        self.stop_event.set()
        if self._chainlink_feed is not None:
            self._chainlink_feed.stop()

        # Give threads time to cleanup
        for t in self.threads:
            if t.is_alive():
                t.join(timeout=1)
        
        print("[DATA] Feeds stopped")
    
    def _poll_drives_entry(self) -> bool:
        """Poll-only mode: no PM market WS; CLOB /prices triggers strategy callbacks."""
        return not self._market_ws_enabled and self._clob_poll_sec > 0

    def _register_market_tokens(
        self, coin: str, market_slug: str, tokens: Dict[str, Any]
    ) -> None:
        """Register CLOB token IDs for order execution (required on each new window)."""
        self.position_tracker.register_market(
            market_slug=market_slug,
            up_token_id=tokens["up"],
            down_token_id=tokens["down"],
        )
        trader_module.set_token_ids(
            market_slug=market_slug,
            up_token_id=tokens["up"],
            down_token_id=tokens["down"],
            condition_id=tokens.get("condition_id", ""),
            neg_risk=tokens.get("neg_risk", True),
        )

    def _sync_market_for_current_slug(self, coin: str, *, quiet: bool = False) -> bool:
        """Align slug + CLOB token IDs with current window."""
        expected = self._current_slug(coin)
        with self.locks[coin]:
            stored_slug = (self.markets[coin].get("slug") or "").strip()
            raw = self.markets[coin].get("tokens") or {}
            have_tokens = bool(
                self._norm_token_id(raw.get("up"))
                and self._norm_token_id(raw.get("down"))
            )
        if stored_slug == expected and have_tokens:
            return True
        new_tokens = self._fetch_tokens(coin)
        if not new_tokens:
            return False
        now = int(time.time())
        iv = self.market_interval_sec
        market_end = ((now // iv) * iv) + iv
        with self.locks[coin]:
            if stored_slug != expected:
                self.markets[coin]["market_start_price"] = 0.0
                self.markets[coin]["up_ask_timestamp"] = 0.0
                self.markets[coin]["down_ask_timestamp"] = 0.0
            self.markets[coin]["slug"] = expected
            self.markets[coin]["tokens"] = new_tokens
            self.markets[coin]["market_end_time"] = market_end
            self.markets[coin]["seconds_till_end"] = max(0, market_end - now)
            if (
                self.spot_price_source != "chainlink"
                and self.markets[coin]["market_start_price"] == 0.0
            ):
                if coin == "btc":
                    self.markets[coin]["market_start_price"] = self.btc_price
                elif coin == "eth":
                    self.markets[coin]["market_start_price"] = self.eth_price
            self._mark_book_fresh(coin)
        self._register_market_tokens(coin, expected, new_tokens)
        if not quiet:
            print(
                f"[PM-{coin.upper()}] Token sync → {expected[-24:]} "
                f"(was {stored_slug[-24:] if stored_slug else 'empty'})"
            )
        return True

    def book_age_sec(self, coin: str) -> float:
        """Seconds since last UP/DN ask timestamp in markets (for dashboard stale badge)."""
        if hasattr(self, "peek_book_age_sec"):
            age = float(self.peek_book_age_sec(coin))
            if age < 9000.0:
                return age
        if coin not in self.markets:
            return 9999.0
        if not self.locks[coin].acquire(timeout=0.15):
            return 9999.0
        try:
            up_ts = float(self.markets[coin].get("up_ask_timestamp") or 0)
            down_ts = float(self.markets[coin].get("down_ask_timestamp") or 0)
        finally:
            self.locks[coin].release()
        ts = max(up_ts, down_ts)
        return max(0.0, time.time() - ts) if ts > 0 else 9999.0

    def _fresh_ste_for_coin(self, coin: str) -> int:
        """Wall-clock countdown — safe without markets lock."""
        now = int(time.time())
        slug = self._current_slug(coin)
        try:
            slot = int(slug.rsplit("-", 1)[-1])
            return max(0, slot + self.market_interval_sec - now)
        except (ValueError, TypeError):
            pass
        if coin in self.markets:
            market_end = int(self.markets[coin].get("market_end_time") or 0)
            if market_end > 0:
                return max(0, market_end - now)
        return 0

    def _publish_ui_state_unlocked(self, coin: str) -> None:
        """Copy markets → lock-free UI cache. Caller must hold locks[coin]."""
        st = self._get_state_unlocked(coin)
        if not st:
            return
        snap = dict(st)
        snap["market_slug"] = self._current_slug(coin)
        snap["seconds_till_end"] = self._fresh_ste_for_coin(coin)
        with self._ui_state_cache_lock:
            self._ui_state_cache[coin] = snap

    def _publish_ui_state(self, coin: str) -> None:
        """Best-effort publish when caller does not already hold the coin lock."""
        if coin not in self.markets:
            return
        if not self.locks[coin].acquire(timeout=0.5):
            return
        try:
            self._publish_ui_state_unlocked(coin)
        finally:
            self.locks[coin].release()

    def peek_ui_state(self, coin: str) -> Optional[Dict]:
        """Non-blocking dashboard read — ste refreshed from wall clock each call."""
        with self._ui_state_cache_lock:
            cached = self._ui_state_cache.get(coin)
            if not cached:
                return None
            out = dict(cached)
        out["market_slug"] = self._current_slug(coin)
        out["seconds_till_end"] = self._fresh_ste_for_coin(coin)
        ua = float(out.get("up_ask") or 0)
        da = float(out.get("down_ask") or 0)
        if ua > 0 and da > 0:
            out["confidence"] = abs(da - ua)
        return out

    def peek_book_age_sec(self, coin: str) -> float:
        """Age since last successful book write (_last_book_ok_mono, no markets lock)."""
        last = float(self._last_book_ok_mono.get(coin) or 0.0)
        if last > 0:
            return max(0.0, time.monotonic() - last)
        with self._ui_state_cache_lock:
            st = self._ui_state_cache.get(coin) or {}
            up_ts = float(st.get("up_ask_timestamp") or 0)
            down_ts = float(st.get("down_ask_timestamp") or 0)
        ts = max(up_ts, down_ts)
        return max(0.0, time.time() - ts) if ts > 0 else 9999.0

    def _ws_socket_open(self, coin: str) -> Tuple[bool, bool]:
        """Return (ws_app_present, sock_connected)."""
        with self._pm_ws_lock[coin]:
            ws = self._pm_ws_app.get(coin)
        if ws is None:
            return False, False
        sock = getattr(ws, "sock", None)
        if sock is None:
            return True, False
        try:
            return True, bool(getattr(sock, "connected", True))
        except Exception:
            return True, True

    def feed_connectivity_status(self, coins: Optional[List[str]] = None) -> Dict[str, Any]:
        """Per-coin WS + book freshness for /api/health (read-only)."""
        coins = coins or list(self.enabled_coins)
        now_m = time.monotonic()
        out: Dict[str, Any] = {}
        for coin in coins:
            if coin not in self.enabled_coins:
                continue
            present, sock_ok = self._ws_socket_open(coin)
            last_ws = float(self._last_pm_ws_msg_mono.get(coin) or 0.0)
            ws_msg_age = round(now_m - last_ws, 2) if last_ws > 0 else None
            book_age = round(self.peek_book_age_sec(coin), 2)
            st = self.peek_ui_state(coin) or {}
            if self._market_ws_enabled:
                ws_alive = bool(
                    present
                    and sock_ok
                    and ws_msg_age is not None
                    and ws_msg_age < 30.0
                )
            else:
                ws_alive = False
                present = False
                sock_ok = False
            poll_ok = book_age < max(3.0, self._clob_poll_sec * 3)
            out[coin] = {
                "ws_app_present": present,
                "ws_sock_connected": sock_ok,
                "ws_last_msg_age_sec": ws_msg_age,
                "ws_alive": ws_alive,
                "market_ws_enabled": self._market_ws_enabled,
                "poll_drives_entry": self._poll_drives_entry(),
                "book_age_sec": book_age,
                "clob_poll_ok": poll_ok,
                "up_ask": st.get("up_ask"),
                "down_ask": st.get("down_ask"),
                "market_slug": (st.get("market_slug") or "")[-24:],
            }
        return out

    def try_get_state(self, coin: str = "btc", *, lock_timeout: float = 0.5) -> Optional[Dict]:
        """Non-blocking get_state for HTTP handlers — skip if feed lock is held."""
        if coin not in self.markets:
            return None
        if not self.locks[coin].acquire(timeout=lock_timeout):
            return None
        try:
            return self._get_state_unlocked(coin)
        finally:
            self.locks[coin].release()

    def get_state(self, coin: str = 'btc') -> Dict:
        """Get current market state for specified coin (thread-safe)"""
        if coin not in self.markets:
            return None
        with self.locks[coin]:
            return self._get_state_unlocked(coin)

    def _get_state_unlocked(self, coin: str) -> Optional[Dict]:
        """Caller must hold locks[coin]."""
        market = self.markets.get(coin)
        if not market:
            return None

        if coin == 'btc':
            price = self.btc_price
        elif coin == 'eth':
            price = self.eth_price
        else:
            price = 0.0

        up_ask = market.get('up_ask') or 0.0
        down_ask = market.get('down_ask') or 0.0
        confidence = abs(down_ask - up_ask) if (up_ask > 0 and down_ask > 0) else 0.0

        now = int(time.time())
        slug = self._current_slug(coin)
        try:
            slot = int(slug.rsplit("-", 1)[-1])
            ste = max(0, slot + self.market_interval_sec - now)
        except (ValueError, TypeError):
            market_end = int(market.get("market_end_time") or 0)
            ste = max(0, market_end - now) if market_end > 0 else 0

        return {
            'up_ask': up_ask,
            'down_ask': down_ask,
            'up_ask_timestamp': market.get('up_ask_timestamp') or 0.0,
            'down_ask_timestamp': market.get('down_ask_timestamp') or 0.0,
            'price': price,
            'market_start_price': market['market_start_price'],
            'seconds_till_end': ste,
            'market_slug': slug,
            'confidence': confidence,
            'coin': coin,
            'spot_price_source': self.spot_price_source,
            'market_interval_sec': self.market_interval_sec,
            'market_slug_suffix': self.market_slug_suffix,
        }
    
    def register_price_callback(self, callback):
        """Register callback function for price updates (event-driven)"""
        self.price_callbacks.append(callback)
    
    def _current_slug(self, coin: str) -> str:
        """Calculate current market slug (5m or 15m per config)."""
        iv = self.market_interval_sec
        current_slot = int(time.time()) // iv * iv
        return f"{coin}-updown-{self.market_slug_suffix}-{current_slot}"

    def _set_pm_ws(self, coin: str, ws: Any) -> None:
        with self._pm_ws_lock[coin]:
            self._pm_ws_app[coin] = ws

    def _clear_pm_ws(self, coin: str) -> None:
        with self._pm_ws_lock[coin]:
            self._pm_ws_app[coin] = None

    def _close_pm_ws(
        self, coin: str, reason: str, *, source: str = "watchdog", force: bool = False
    ) -> None:
        """
        Close market WS. Watchdog and trading-hours use separate paths so they
        do not share cooldown or fight over the same connection.
        """
        with self._pm_ws_lock[coin]:
            ws = self._pm_ws_app.get(coin)
        if ws is None:
            return
        if source == "watchdog" and not force:
            now = time.monotonic()
            if now - self._pm_force_close_mono.get(coin, 0.0) < self._ws_force_close_cooldown_sec:
                return
            self._pm_force_close_mono[coin] = now
            tag = "Watchdog"
        else:
            tag = "Hours"
        try:
            if hasattr(ws, "keep_running"):
                ws.keep_running = False
        except Exception:
            pass
        try:
            sock = getattr(ws, "sock", None)
            if sock is not None:
                sock.close()
        except Exception:
            pass
        try:
            ws.close()
        except Exception:
            pass
        self._clear_pm_ws(coin)
        print(f"[PM-{coin.upper()}] {tag}: closing WS ({reason})")

    def _force_pm_ws_reconnect(self, coin: str, reason: str, *, force: bool = False) -> None:
        """Watchdog-only: close hung WS so _polymarket_worker can re-subscribe."""
        self._close_pm_ws(coin, reason, source="watchdog", force=force)
        if self.trading_hours.operations_active():
            self._refresh_coin_book_rest(coin, fire_callbacks=False)

    def _mark_book_fresh(self, coin: str) -> None:
        """Mark book OK + publish UI cache. Caller must hold locks[coin]."""
        self._last_book_ok_mono[coin] = time.monotonic()
        self._publish_ui_state_unlocked(coin)

    def _record_ask_pair(self, coin: str) -> None:
        """Track when displayed UP/DN ask values actually change (not just timestamps)."""
        with self.locks[coin]:
            pair = (
                round(float(self.markets[coin].get("up_ask") or 0), 4),
                round(float(self.markets[coin].get("down_ask") or 0), 4),
            )
        if self._last_ask_pair.get(coin) != pair:
            self._last_ask_pair[coin] = pair
            self._last_ask_change_mono[coin] = time.monotonic()

    def _ask_price_frozen_sec(self, coin: str) -> float:
        last = self._last_ask_change_mono.get(coin, 0.0)
        if last <= 0:
            return 0.0
        return time.monotonic() - last

    def _ask_stale_age_sec(self, coin: str) -> float:
        """
        Seconds since last fresh UP/DN book.
        Wall timestamps + monotonic fallback; slug set but ts=0 => stale (not 0).
        """
        now_wall = int(time.time())
        last_mono = self._last_book_ok_mono.get(coin, 0.0)
        mono_age = (time.monotonic() - last_mono) if last_mono > 0 else 0.0
        with self.locks[coin]:
            up_ts = float(self.markets[coin].get("up_ask_timestamp") or 0)
            down_ts = float(self.markets[coin].get("down_ask_timestamp") or 0)
            slug = (self.markets[coin].get("slug") or "").strip()
        if up_ts > 0 or down_ts > 0:
            wall_age = max(0.0, float(now_wall - max(up_ts, down_ts)))
            return max(wall_age, mono_age)
        if slug:
            return max(mono_age, 999.0) if last_mono <= 0 else max(mono_age, self._watchdog_soft_rest_sec + 1.0)
        return mono_age

    def _recover_feeds_after_network(self, *, reason: str = "network_recovered") -> None:
        """REST refresh + WS kick after proxy/VPN blip — unfreeze dashboard asks."""
        if not self.trading_hours.operations_active():
            return
        print(
            f"[PM-RECOVER] {reason} — resyncing markets"
            + ("" if self._market_ws_enabled else " (poll-only)")
        )
        self._clob_rest_fail_streak = 0
        fire = self._poll_drives_entry()
        for coin in self.enabled_coins:
            try:
                expected = self._current_slug(coin)
                with self.locks[coin]:
                    stored = (self.markets[coin].get("slug") or "").strip()
                if stored != expected or not stored:
                    self._resync_market_from_gamma(coin, reason, force=True)
                else:
                    self._refresh_coin_book_rest(coin, fire_callbacks=fire)
                    if self._market_ws_enabled:
                        _present, ws_up = self._ws_socket_open(coin)
                        if _present and not ws_up:
                            self._close_pm_ws(
                                coin, "zombie_sock_dead", source="watchdog", force=True
                            )
                        elif not ws_up:
                            self._force_pm_ws_reconnect(coin, reason, force=True)
            except Exception as exc:
                print(f"[PM-RECOVER] {coin.upper()} failed: {exc}")

    def _resync_market_from_gamma(
        self, coin: str, reason: str, *, force: bool = False
    ) -> bool:
        """
        Watchdog path: pull current Gamma tokens + slug when PM worker is stuck
        on an old window (zombie WS — close() alone may not return run_forever).
        """
        if not self.trading_hours.operations_active():
            return False
        now_m = time.monotonic()
        if not force and (
            now_m - self._resync_throttle_mono.get(coin, 0.0)
        ) < self._resync_throttle_sec:
            return False
        tokens = self._fetch_tokens(coin)
        if not tokens:
            return False
        market_slug = self._current_slug(coin)
        current_time = int(time.time())
        iv = self.market_interval_sec
        market_end = ((current_time // iv) * iv) + iv
        with self.locks[coin]:
            prev_slug = self.markets[coin].get("slug") or ""
            if prev_slug != market_slug:
                self.markets[coin]["market_start_price"] = 0.0
                self.markets[coin]["up_ask_timestamp"] = 0.0
                self.markets[coin]["down_ask_timestamp"] = 0.0
            self.markets[coin]["slug"] = market_slug
            self.markets[coin]["market_end_time"] = market_end
            self.markets[coin]["tokens"] = tokens
            self.markets[coin]["seconds_till_end"] = max(
                0, market_end - current_time
            )
            if (
                self.spot_price_source != "chainlink"
                and self.markets[coin]["market_start_price"] == 0.0
            ):
                if coin == "btc":
                    self.markets[coin]["market_start_price"] = self.btc_price
                elif coin == "eth":
                    self.markets[coin]["market_start_price"] = self.eth_price
        self._register_market_tokens(coin, market_slug, tokens)
        self._resync_throttle_mono[coin] = now_m
        fire = self._poll_drives_entry()
        self._refresh_coin_book_rest(coin, fire_callbacks=fire)
        self._record_ask_pair(coin)
        if self._market_ws_enabled:
            self._force_pm_ws_reconnect(coin, f"resync {reason}", force=True)
        print(
            f"[PM-{coin.upper()}] Market resync → {market_slug} "
            f"(ste={max(0, market_end - current_time)}s, {reason})"
        )
        return True

    def _watchdog_may_run(self, coin: str) -> bool:
        """False during non-trading hours or grace after resuming (avoid vs hours pause)."""
        if not self.trading_hours.operations_active():
            return False
        resumed = self._pm_hours_resume_mono.get(coin, 0.0)
        if resumed > 0 and (time.monotonic() - resumed) < self._ws_post_hours_grace_sec:
            return False
        return True

    def _pm_ws_watchdog_check(self, coin: str) -> None:
        """
        A) slug behind current slot, or market window ended but WS worker stuck.
        B) active window (seconds_till_end>0) but UP/DN ask timestamps too old.
        Skipped outside trading_hours and shortly after hours resume.
        """
        if not self._watchdog_may_run(coin):
            return
        expected_slug = self._current_slug(coin)
        now_wall = int(time.time())

        with self.locks[coin]:
            stored_slug = (self.markets[coin].get("slug") or "").strip()
            ste = int(self.markets[coin].get("seconds_till_end") or 0)
            market_end = int(self.markets[coin].get("market_end_time") or 0)

        ws_present, ws_sock_ok = self._ws_socket_open(coin)
        if ws_present and not ws_sock_ok:
            print(
                f"[PM-{coin.upper()}] Watchdog: zombie WS (app present, sock dead) "
                f"— clearing for reconnect"
            )
            self._close_pm_ws(coin, "zombie_sock_dead", source="watchdog", force=True)
            ws_present, ws_sock_ok = False, False
        ws_up = ws_sock_ok

        if not stored_slug:
            if expected_slug:
                if not self._resync_market_from_gamma(coin, "empty_slug"):
                    if ws_up:
                        self._force_pm_ws_reconnect(
                            coin, "empty_slug", force=True
                        )
                    else:
                        print(
                            f"[PM-{coin.upper()}] Watchdog: slug empty, waiting for worker "
                            f"(expected {expected_slug[-20:]})"
                        )
            return

        stale_age = self._ask_stale_age_sec(coin)
        rest_stale_thresh = self._ws_stale_ask_sec

        if stored_slug != expected_slug:
            self._watchdog_stale_streak[coin] = 0
            drift_reason = (
                f"slug_drift stored={stored_slug[-13:]} "
                f"expected={expected_slug[-13:]}"
            )
            if not self._resync_market_from_gamma(coin, drift_reason):
                self._force_pm_ws_reconnect(coin, drift_reason, force=True)
            return

        if market_end > 0 and now_wall > market_end + self._ws_window_ended_grace_sec:
            self._watchdog_stale_streak[coin] = 0
            end_reason = f"window_ended ste={ste} end={market_end}"
            if not self._resync_market_from_gamma(coin, end_reason):
                self._force_pm_ws_reconnect(coin, end_reason, force=True)
            return

        # WS down: on-demand REST only when ask is stale (not every watchdog tick).
        if not ws_up:
            if stale_age >= rest_stale_thresh:
                self._watchdog_stale_streak[coin] += 1
                self._watchdog_handle_stale_ask(coin, stale_age, ste, ws_was_up=False)
            elif self._watchdog_soft_rest(coin, stale_age):
                pass
            return

        if stale_age < rest_stale_thresh:
            if self._watchdog_soft_rest(coin, stale_age):
                self._watchdog_stale_streak[coin] = 0
                return
            self._watchdog_stale_streak[coin] = 0
            return

        self._watchdog_stale_streak[coin] += 1
        self._watchdog_handle_stale_ask(coin, stale_age, ste, ws_was_up=True)

    def _poll_only_watchdog_check(self, coin: str) -> None:
        """Poll-only: slug/window resync + stale /prices refresh (no PM WS)."""
        if not self._watchdog_may_run(coin):
            return
        expected_slug = self._current_slug(coin)
        now_wall = int(time.time())
        with self.locks[coin]:
            stored_slug = (self.markets[coin].get("slug") or "").strip()
            ste = int(self.markets[coin].get("seconds_till_end") or 0)
            market_end = int(self.markets[coin].get("market_end_time") or 0)

        if not stored_slug or stored_slug != expected_slug:
            reason = (
                "empty_slug"
                if not stored_slug
                else f"slug_drift {stored_slug[-13:]}→{expected_slug[-13:]}"
            )
            self._resync_market_from_gamma(coin, reason, force=not stored_slug)
            return

        if market_end > 0 and now_wall > market_end + self._ws_window_ended_grace_sec:
            self._resync_market_from_gamma(
                coin, f"window_ended ste={ste} end={market_end}", force=True
            )
            return

        stale_age = self._ask_stale_age_sec(coin)
        fire = self._poll_drives_entry()
        if stale_age >= self._ws_stale_ask_sec:
            if self._refresh_coin_book_rest(coin, fire_callbacks=fire):
                self._clob_rest_fail_streak = 0
            else:
                self._clob_rest_fail_streak += 1
            return

        frozen = self._ask_price_frozen_sec(coin)
        if ste > 0 and frozen >= self._ask_frozen_sec:
            before = self._last_ask_pair.get(coin) or (0.0, 0.0)
            if self._refresh_coin_book_rest(coin, fire_callbacks=fire):
                self._record_ask_pair(coin)
                if self._last_ask_pair.get(coin) != before:
                    return
            if frozen >= self._ask_frozen_sec * 2:
                self._resync_market_from_gamma(
                    coin, f"ask_frozen {frozen:.0f}s", force=True
                )

    def _watchdog_handle_frozen_prices(self, coin: str) -> None:
        """
        WS zombie can keep timestamps fresh while ask values freeze on the UI.
        Force REST/resync when UP/DN prices are unchanged too long.
        """
        if not self._watchdog_may_run(coin):
            return
        with self.locks[coin]:
            ste = int(self.markets[coin].get("seconds_till_end") or 0)
        if ste <= 0:
            return
        frozen = self._ask_price_frozen_sec(coin)
        if frozen < self._ask_frozen_sec:
            return
        before = self._last_ask_pair.get(coin) or (0.0, 0.0)
        if self._refresh_coin_book_rest(coin, fire_callbacks=False):
            self._record_ask_pair(coin)
            if self._last_ask_pair.get(coin) != before:
                return
        if frozen >= self._ask_frozen_sec * 2:
            print(
                f"[PM-{coin.upper()}] Watchdog: ask frozen {frozen:.0f}s "
                f"({before[0]:.3f}/{before[1]:.3f}) — resync"
            )
            self._resync_market_from_gamma(
                coin, f"ask_frozen {frozen:.0f}s", force=True
            )
        elif frozen >= self._ask_frozen_sec and self._clob_rest_fail_streak >= 3:
            self._recover_feeds_after_network(
                reason=f"ask_frozen {frozen:.0f}s rest_fail"
            )

    def _maybe_ui_book_refresh(self, coin: str) -> None:
        """Periodic REST when CLOB poll is off — keeps dashboard ask moving."""
        if self._clob_poll_sec > 0 or not self.trading_hours.operations_active():
            return
        expected = self._current_slug(coin)
        with self.locks[coin]:
            stored = (self.markets[coin].get("slug") or "").strip()
        if stored and stored != expected:
            self._resync_market_from_gamma(
                coin, f"ui_drift {stored[-13:]}→{expected[-13:]}"
            )
            return
        frozen = self._ask_price_frozen_sec(coin)
        interval = self._ui_rest_interval_sec
        if frozen >= self._ask_frozen_sec:
            interval = min(3.0, interval)
        now_m = time.monotonic()
        if (now_m - self._ui_rest_mono.get(coin, 0.0)) < interval:
            return
        if self._refresh_coin_book_rest(coin, fire_callbacks=False):
            self._record_ask_pair(coin)
            self._ui_rest_mono[coin] = now_m

    def _watchdog_soft_rest(self, coin: str, stale_age: float) -> bool:
        """
        Light REST refresh for UI when CLOB poll is off (saves traffic vs 0.35s poll).
        Throttled per coin; does not force WS reconnect.
        """
        soft = self._watchdog_soft_rest_sec
        if soft <= 0 or stale_age < soft:
            return False
        now_m = time.monotonic()
        if (now_m - self._watchdog_soft_rest_mono.get(coin, 0.0)) < soft:
            return False
        if self._refresh_coin_book_rest(coin, fire_callbacks=False):
            self._watchdog_soft_rest_mono[coin] = now_m
            self._clob_rest_fail_streak = 0
            if stale_age >= soft * 2:
                print(
                    f"[PM-{coin.upper()}] Watchdog: soft REST refresh "
                    f"(ask stale {stale_age:.0f}s, UI/trading unfreeze)"
                )
            return True
        self._clob_rest_fail_streak += 1
        if self._clob_rest_fail_streak >= 3:
            self._recover_feeds_after_network(
                reason=f"soft_rest_fail x{self._clob_rest_fail_streak}"
            )
        return False

    def _watchdog_handle_stale_ask(
        self,
        coin: str,
        stale_age: float,
        ste: int,
        *,
        ws_was_up: bool,
    ) -> None:
        """On-demand REST + WS reconnect after network blip; throttled strategy callback."""
        touched = self._refresh_coin_book_rest(coin, fire_callbacks=False)
        now_m = time.monotonic()
        if touched:
            self._clob_rest_fail_streak = 0
            if (now_m - self._watchdog_callback_mono.get(coin, 0.0)) >= 2.0:
                self._fire_price_callbacks(coin)
                self._watchdog_callback_mono[coin] = now_m
        else:
            self._clob_rest_fail_streak += 1
        tag = "ws_down" if not ws_was_up else "stale_ask"
        if ws_was_up:
            self._force_pm_ws_reconnect(
                coin,
                f"{tag} {stale_age:.0f}s (ste={ste})"
                + ("" if touched else ", rest_failed"),
                force=(not touched or stale_age >= self._ws_stale_ask_sec * 2),
            )
        elif self._watchdog_stale_streak.get(coin, 0) >= 2:
            print(
                f"[PM-{coin.upper()}] Watchdog: WS down, stale {stale_age:.0f}s "
                f"— PM worker reconnecting"
            )
        if self._watchdog_stale_streak.get(coin, 0) >= 3:
            self._recover_feeds_after_network(
                reason=f"watchdog_stale x{self._watchdog_stale_streak[coin]} "
                f"({tag} {stale_age:.0f}s)"
            )
            self._watchdog_stale_streak[coin] = 0

    def _pm_ws_watchdog_worker(self) -> None:
        while not self.stop_event.is_set():
            if not self.trading_hours.operations_active():
                self.trading_hours.sleep_until_allowed(self.stop_event, max_sleep=60.0)
                continue
            try:
                for coin in self.enabled_coins:
                    if self._market_ws_enabled:
                        self._pm_ws_watchdog_check(coin)
                        self._watchdog_handle_frozen_prices(coin)
                        self._maybe_ui_book_refresh(coin)
                    else:
                        self._poll_only_watchdog_check(coin)
            except Exception as exc:
                print(f"[PM-WATCHDOG] {exc}")
            self.stop_event.wait(self._ws_watchdog_interval_sec)

    def _fetch_tokens(self, coin: str) -> Optional[Dict]:
        """Fetch current market tokens from Polymarket for specified coin"""
        try:
            gamma_api = self.config['data_sources']['polymarket']['gamma_api']
            slug = self._current_slug(coin)
            
            # Use events API with specific slug
            url = f"{gamma_api}/events?slug={slug}"
            proxies = requests_proxies(self._proxy_url_override)
            req_kw: Dict[str, Any] = {"timeout": (3.0, 10)}
            if proxies:
                req_kw["proxies"] = proxies
            resp = requests.get(url, **req_kw)
            resp.raise_for_status()
            
            events = resp.json()
            if not events:
                # Market not found - may not be open yet
                current_time = int(time.time())
                iv = self.market_interval_sec
                next_market = ((current_time // iv) + 1) * iv
                wait_time = next_market - current_time
                print(f"[PM-{coin.upper()}] Market {slug} not found (may not be open yet, next in {wait_time}s)")
                return None
            
            # Get first market
            market = events[0]["markets"][0]
            clob_token_ids = market.get("clobTokenIds", [])
            outcomes = market.get("outcomes", [])
            condition_id = market.get("conditionId", "")
            neg_risk = market.get("negRisk", True)
            
            # Parse if string format
            if isinstance(clob_token_ids, str):
                clob_token_ids = json.loads(clob_token_ids)
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            
            # Find Up and Down indices
            up_idx = outcomes.index("Up") if "Up" in outcomes else 0
            down_idx = outcomes.index("Down") if "Down" in outcomes else 1
            
            out = {
                'up': self._norm_token_id(clob_token_ids[up_idx]),
                'down': self._norm_token_id(clob_token_ids[down_idx]),
                'condition_id': condition_id,
                'neg_risk': neg_risk
            }
            # Spot price fetched async — must NOT block WS connect (CoinGecko can hang/slow)

            def _spot_bg(c: str, slug: str) -> None:
                if not self.trading_hours.operations_active():
                    return
                try:
                    spot = self._fetch_spot_for_coin(c, timeout=2.0)
                    if spot and spot > 0:
                        with self.locks[c]:
                            if self.markets[c].get("slug") == slug:
                                # 标的起由 Gamma Chainlink 锁定；chainlink 源不写 CoinGecko/RTDS 现价
                                if (
                                    self.spot_price_source != "chainlink"
                                    and float(self.markets[c].get("market_start_price") or 0) <= 0
                                ):
                                    self.markets[c]["market_start_price"] = spot
                            if c == "btc":
                                self.btc_price = spot
                            elif c == "eth":
                                self.eth_price = spot
                except Exception:
                    pass

            threading.Thread(
                target=_spot_bg, args=(coin, slug), daemon=True, name=f"spot_{coin}"
            ).start()

            return out
            
        except Exception as e:
            print(f"[PM-{coin.upper()}] Error fetching tokens: {e}")
        return None

    def refresh_coin_spot(self, coin: str) -> float:
        """Update live underlying spot for coin; returns price or 0."""
        if coin not in self.markets:
            return 0.0
        if not self.trading_hours.operations_active():
            with self.locks[coin]:
                if coin == "btc":
                    return float(self.btc_price or 0)
                if coin == "eth":
                    return float(self.eth_price or 0)
            return 0.0
        spot = self._fetch_spot_for_coin(coin)
        if not spot or spot <= 0:
            return 0.0
        with self.locks[coin]:
            if coin == 'btc':
                self.btc_price = spot
            elif coin == 'eth':
                self.eth_price = spot
        return spot
    
    def _wait_trading_hours(self, coin: str) -> bool:
        """Return False if stop requested; skip Polymarket traffic until window opens."""
        if self.trading_hours.operations_active():
            if coin in self._hours_outside_logged:
                self._pm_hours_resume_mono[coin] = time.monotonic()
                self._hours_outside_logged.discard(coin)
            return True
        with self._pm_ws_lock[coin]:
            ws = self._pm_ws_app.get(coin)
        if ws is not None:
            self._close_pm_ws(coin, "outside trading window", source="hours")
        if coin not in self._hours_outside_logged:
            nxt = self.trading_hours.next_window_start()
            when = nxt.strftime("%H:%M") if nxt else "?"
            print(
                f"[HOURS] Outside trading window ({self.trading_hours.ranges_summary()}) "
                f"— pausing PM feed for {coin.upper()} until {when} (watchdog idle)"
            )
            self._hours_outside_logged.add(coin)
        self.trading_hours.sleep_until_allowed(self.stop_event, max_sleep=60.0)
        return not self.stop_event.is_set()

    def _polymarket_worker(self, coin: str):
        """Polymarket WebSocket worker for specified coin"""
        while not self.stop_event.is_set():
            if not self._wait_trading_hours(coin):
                break
            # Fetch tokens
            tokens = self._fetch_tokens(coin)
            if not tokens:
                time.sleep(5)
                continue
            
            with self.locks[coin]:
                self.markets[coin]['tokens'] = tokens
            
            # Save token IDs to trader module for real trading
            market_slug = self._current_slug(coin)
            trader_module.set_token_ids(
                market_slug=market_slug,
                up_token_id=tokens['up'],
                down_token_id=tokens['down'],
                condition_id=tokens.get('condition_id', ''),
                neg_risk=tokens.get('neg_risk', True)
            )
            
            # Calculate reconnect time
            current_time = int(time.time())
            iv = self.market_interval_sec
            market_end = ((current_time // iv) * iv) + iv
            reconnect_in = market_end - current_time + 2
            
            # Get market slug
            market_slug = self._current_slug(coin)
            
            with self.locks[coin]:
                prev_slug = self.markets[coin].get("slug") or ""
                if prev_slug != market_slug:
                    self.markets[coin]["market_start_price"] = 0.0
                    # Avoid watchdog B firing on previous market's stale timestamps
                    self.markets[coin]["up_ask_timestamp"] = 0.0
                    self.markets[coin]["down_ask_timestamp"] = 0.0
                self.markets[coin]['slug'] = market_slug
                self.markets[coin]['market_end_time'] = market_end
                self.markets[coin]['tokens'] = tokens
                
                # ✅ Register market in PositionTracker for tracking via WebSocket
                self.position_tracker.register_market(
                    market_slug=market_slug,
                    up_token_id=tokens['up'],
                    down_token_id=tokens['down']
                )
                
                # Set market start price only for BTC/ETH when using CoinGecko source
                if (
                    self.spot_price_source != "chainlink"
                    and self.markets[coin]['market_start_price'] == 0.0
                ):
                    if coin == 'btc':
                        self.markets[coin]['market_start_price'] = self.btc_price
                    elif coin == 'eth':
                        self.markets[coin]['market_start_price'] = self.eth_price
                    # SOL/XRP: leave at 0.0 (no price feed needed)
            
            print(f"[PM-{coin.upper()}] Connected to {market_slug}, reconnect in {reconnect_in}s")
            self._refresh_coin_book_rest(coin, fire_callbacks=False)

            # Connect WebSocket
            try:
                ws_url = self.config['data_sources']['polymarket']['ws_url']
                ws_ref = [None]  # Store ws reference for closing
                self._clear_pm_ws(coin)

                ws = websocket.WebSocketApp(
                    ws_url,
                    on_message=lambda ws, msg: self._on_pm_message(msg, tokens, coin),
                    on_error=lambda ws, err, _c=coin: (
                        print(f"[PM-{_c.upper()}] WebSocket error: {err!r}") if err else None
                    ),
                    on_close=lambda ws, code, reason, _c=coin: (
                        print(f"[PM-{_c.upper()}] WebSocket closed code={code} reason={reason!r}")
                        if code is not None and int(code) != 1000
                        else None
                    ),
                )
                
                ws_ref[0] = ws
                self._set_pm_ws(coin, ws)

                def on_open(ws):
                    # Polymarket docs: type must be lowercase "market" (not "MARKET").
                    sub_msg: Dict[str, Any] = {
                        "type": "market",
                        "assets_ids": [
                            self._norm_token_id(tokens["up"]),
                            self._norm_token_id(tokens["down"]),
                        ],
                    }
                    if self._ws_custom_features:
                        sub_msg["custom_feature_enabled"] = True
                    ws.send(json.dumps(sub_msg))
                
                ws.on_open = on_open
                
                # Auto-reconnect timer
                timer = threading.Timer(reconnect_in, lambda: ws.close())
                timer.start()
                
                # Stop checker thread
                def check_stop():
                    while not self.stop_event.is_set():
                        time.sleep(0.5)
                    if ws_ref[0]:
                        ws_ref[0].close()
                
                stop_checker = threading.Thread(target=check_stop, daemon=True)
                stop_checker.start()

                _px = websocket_proxy_kwargs(self._proxy_url_override)
                try:
                    ws.run_forever(
                        ping_interval=20,
                        ping_timeout=10,
                        skip_utf8_validation=True,
                        **_px,
                    )
                finally:
                    timer.cancel()
                    self._clear_pm_ws(coin)

                # Stop immediately if stop_event is set
                if self.stop_event.is_set():
                    break

            except Exception as e:
                self._clear_pm_ws(coin)
                print(f"[PM-{coin.upper()}] Error: {e}")
                time.sleep(5)
    
    @staticmethod
    def _norm_token_id(tid) -> str:
        if tid is None:
            return ""
        return str(tid).strip()

    def _live_tokens(self, coin: str, fallback: Optional[Dict] = None) -> Dict[str, str]:
        with self.locks[coin]:
            raw = dict(self.markets[coin].get("tokens") or fallback or {})
        up = self._norm_token_id(raw.get("up"))
        down = self._norm_token_id(raw.get("down"))
        if not up or not down:
            return {}
        out = {"up": up, "down": down}
        if raw.get("condition_id"):
            out["condition_id"] = raw["condition_id"]
        return out

    def _token_side(self, asset_id: str, tokens: Dict) -> Optional[str]:
        aid = self._norm_token_id(asset_id)
        if not aid:
            return None
        if aid == self._norm_token_id(tokens.get("up")):
            return "up"
        if aid == self._norm_token_id(tokens.get("down")):
            return "down"
        return None

    @staticmethod
    def _parse_best_float(val) -> Optional[float]:
        if val is None:
            return None
        s = str(val).strip()
        if not s:
            return None
        try:
            f = float(s)
        except ValueError:
            return None
        if f <= 0.0 or f > _MAX_TOKEN_PX:
            return None
        return f

    def _price_side_from_block(
        self, block: Any, side: str
    ) -> Optional[float]:
        if not isinstance(block, dict):
            return None
        raw = block.get(side) or block.get(side.upper()) or block.get(side.lower())
        return self._parse_best_float(raw)

    def _prices_block_for_token(self, data: Any, token_id: str) -> Optional[Dict]:
        tid = self._norm_token_id(token_id)
        if not tid or not isinstance(data, dict):
            return None
        block = data.get(tid) or data.get(str(tid))
        if isinstance(block, dict):
            return block
        for key, val in data.items():
            if self._norm_token_id(key) == tid and isinstance(val, dict):
                return val
        return None

    def _fetch_clob_prices_batch(
        self, up_token: str, down_token: str, timeout: float = 2.5
    ) -> Dict[str, Tuple[Optional[float], Optional[float]]]:
        """
        POST /prices — one request for UP/DOWN best bid (SELL) and ask (BUY).
        """
        up_tid = self._norm_token_id(up_token)
        down_tid = self._norm_token_id(down_token)
        if not up_tid or not down_tid:
            return {}
        body = [
            {"token_id": up_tid, "side": "BUY"},
            {"token_id": up_tid, "side": "SELL"},
            {"token_id": down_tid, "side": "BUY"},
            {"token_id": down_tid, "side": "SELL"},
        ]
        try:
            proxies = requests_proxies(self._proxy_url_override)
            req_kw: Dict[str, Any] = {"timeout": (2.0, timeout)}
            if proxies:
                req_kw["proxies"] = proxies
            resp = requests.post(
                f"{self._clob_host}/prices",
                json=body,
                **req_kw,
            )
            resp.raise_for_status()
            data = resp.json()
            up_block = self._prices_block_for_token(data, up_tid)
            dn_block = self._prices_block_for_token(data, down_tid)
            return {
                "up": (
                    self._price_side_from_block(up_block, "SELL"),
                    self._price_side_from_block(up_block, "BUY"),
                ),
                "down": (
                    self._price_side_from_block(dn_block, "SELL"),
                    self._price_side_from_block(dn_block, "BUY"),
                ),
            }
        except Exception:
            return {}

    def _refresh_coin_book_rest(self, coin: str, fire_callbacks: bool = False) -> bool:
        """Pull best bid/ask via CLOB POST /prices for current market tokens."""
        if not self.trading_hours.operations_active():
            return False
        self._sync_market_for_current_slug(coin, quiet=True)
        tokens = self._live_tokens(coin)
        if not tokens:
            return False
        book = self._fetch_clob_prices_batch(tokens["up"], tokens["down"], timeout=2.5)
        if not book:
            return False
        up_bid, up_ask = book.get("up", (None, None))
        down_bid, down_ask = book.get("down", (None, None))
        changed = False
        touched = False
        now = time.time()
        with self.locks[coin]:
            if up_ask is not None:
                touched = True
                if self.markets[coin]["up_ask"] != up_ask:
                    changed = True
                self.markets[coin]["up_ask"] = up_ask
                self.markets[coin]["up_ask_timestamp"] = now
            if down_ask is not None:
                touched = True
                if self.markets[coin]["down_ask"] != down_ask:
                    changed = True
                self.markets[coin]["down_ask"] = down_ask
                self.markets[coin]["down_ask_timestamp"] = now
            if up_bid is not None:
                touched = True
                if self.markets[coin]["up_bid"] != up_bid:
                    changed = True
                self.markets[coin]["up_bid"] = up_bid
                self.markets[coin]["up_bid_timestamp"] = now
            if down_bid is not None:
                touched = True
                if self.markets[coin]["down_bid"] != down_bid:
                    changed = True
                self.markets[coin]["down_bid"] = down_bid
                self.markets[coin]["down_bid_timestamp"] = now
            if touched:
                self._mark_book_fresh(coin)
        if touched:
            self._record_ask_pair(coin)
        if fire_callbacks and touched:
            self._fire_price_callbacks(coin)
        return touched

    def _clob_poll_worker(self) -> None:
        """Periodic POST /prices — poll-only mode also drives entry/exit callbacks."""
        interval = max(0.25, self._clob_poll_sec)
        drive_entry = self._poll_drives_entry()
        logged_ok = False
        while not self.stop_event.is_set():
            if not self.trading_hours.operations_active():
                self.trading_hours.sleep_until_allowed(self.stop_event, max_sleep=60.0)
                continue
            any_ok = False
            for coin in self.enabled_coins:
                if self.stop_event.is_set():
                    break
                try:
                    if self._refresh_coin_book_rest(
                        coin, fire_callbacks=drive_entry
                    ):
                        any_ok = True
                except Exception:
                    pass
            if any_ok:
                if self._clob_rest_fail_streak >= 3:
                    self._recover_feeds_after_network(
                        reason=f"clob_rest_back after {self._clob_rest_fail_streak} failures"
                    )
                self._clob_rest_fail_streak = 0
                self._last_clob_rest_ok_mono = time.monotonic()
                if not logged_ok:
                    for coin in self.enabled_coins:
                        st = self.get_state(coin)
                        if st and float(st.get("up_ask") or 0) > 0:
                            print(
                                f"[DATA] CLOB /prices OK — {coin.upper()} ask "
                                f"{float(st['up_ask']):.3f}/{float(st['down_ask']):.3f}"
                            )
                            logged_ok = True
                            break
            else:
                self._clob_rest_fail_streak += 1
            time.sleep(interval)

    def _apply_best_bid_ask(
        self,
        coin: str,
        tokens: Dict,
        asset_id: str,
        best_bid: Any,
        best_ask: Any,
    ) -> bool:
        """
        Apply top-of-book from price_change / best_bid_ask payloads.
        Returns True if up/down ask or bid changed (for callbacks).
        """
        bid_f = self._parse_best_float(best_bid)
        ask_f = self._parse_best_float(best_ask)
        if bid_f is None and ask_f is None:
            return False
        live = self._live_tokens(coin, fallback=tokens)
        side = self._token_side(asset_id, live)
        if not side:
            return False

        price_changed = False
        now = time.time()
        with self.locks[coin]:
            if ask_f is not None:
                key = f"{side}_ask"
                if self.markets[coin][key] != ask_f:
                    self.markets[coin][key] = ask_f
                    self.markets[coin][f"{side}_ask_timestamp"] = now
                    price_changed = True
            if bid_f is not None:
                key = f"{side}_bid"
                if self.markets[coin][key] != bid_f:
                    self.markets[coin][key] = bid_f
                    self.markets[coin][f"{side}_bid_timestamp"] = now
                    price_changed = True
            if price_changed:
                self._mark_book_fresh(coin)
        if price_changed:
            self._record_ask_pair(coin)

        return price_changed

    def _fire_price_callbacks(self, coin: str) -> None:
        """Run registered callbacks with a snapshot of the coin's market (lock held briefly)."""
        with self.locks[coin]:
            up_ask = self.markets[coin]["up_ask"]
            down_ask = self.markets[coin]["down_ask"]
            if up_ask is None or down_ask is None:
                return
            up_bid = self.markets[coin]["up_bid"]
            down_bid = self.markets[coin]["down_bid"]
            market_slug = self.markets[coin]["slug"]
            seconds_till_end = self.markets[coin]["seconds_till_end"]
            if coin == "btc":
                market_price = self.btc_price
            elif coin == "eth":
                market_price = self.eth_price
            else:
                market_price = 0.0
            market_start_price = self.markets[coin]["market_start_price"]
            market_state = {
                "up_ask": up_ask,
                "down_ask": down_ask,
                "up_bid": up_bid,
                "down_bid": down_bid,
                "up_ask_timestamp": self.markets[coin]["up_ask_timestamp"],
                "down_ask_timestamp": self.markets[coin]["down_ask_timestamp"],
                "up_bid_timestamp": self.markets[coin]["up_bid_timestamp"],
                "down_bid_timestamp": self.markets[coin]["down_bid_timestamp"],
                "price": market_price,
                "market_start_price": market_start_price,
                "seconds_till_end": seconds_till_end,
                "market_slug": market_slug,
                "confidence": abs(down_ask - up_ask),
                "coin": coin,
            }
            callbacks_to_call = list(self.price_callbacks)

        for callback in callbacks_to_call:
            try:

                def safe_callback_wrapper(
                    cb=callback, ms=market_state, c=coin
                ):
                    try:
                        cb(c, ms)
                    except Exception as e:
                        print(f"[CALLBACK ERROR] {c}: {e}")
                        import traceback

                        traceback.print_exc()

                threading.Thread(
                    target=safe_callback_wrapper,
                    daemon=True,
                    name=f"cb_{coin}_{int(time.time() * 1000)}",
                ).start()
            except Exception as e:
                print(f"[CALLBACK ERROR] Failed to start callback for {coin}: {e}")

    def _on_pm_message(self, message: str, tokens: Dict, coin: str):
        """Parse Polymarket orderbook message for specified coin"""
        try:
            self._last_pm_ws_msg_mono[coin] = time.monotonic()
            parsed = json.loads(message)
            events = parsed if isinstance(parsed, list) else [parsed]
            live = self._live_tokens(coin, fallback=tokens)
            if not live:
                return
            for data in events:
                if isinstance(data, dict):
                    self._process_pm_event(data, live, coin)
        except Exception:
            pass

    def _process_pm_event(self, data: Dict, tokens: Dict, coin: str) -> None:
        """Handle one WS market event (book / price_change / best_bid_ask)."""
        event_type = data.get("event_type", "unknown")

        if event_type == "price_change":
            price_changed = False
            for ch in data.get("price_changes") or []:
                if not isinstance(ch, dict):
                    continue
                aid = ch.get("asset_id")
                if aid and self._apply_best_bid_ask(
                    coin, tokens, aid, ch.get("best_bid"), ch.get("best_ask")
                ):
                    price_changed = True
            if price_changed:
                self._fire_price_callbacks(coin)
            return

        if event_type == "best_bid_ask":
            aid = data.get("asset_id")
            if aid and self._apply_best_bid_ask(
                coin, tokens, aid, data.get("best_bid"), data.get("best_ask")
            ):
                self._fire_price_callbacks(coin)
            return

        if event_type != "book":
            return

        asks_raw = data.get("asks", [])
        bids_raw = data.get("bids", [])

        asks: List[Tuple[float, float]] = []
        for ask in asks_raw or []:
            if isinstance(ask, dict):
                price = float(ask.get("price", 0))
                size = float(ask.get("size", 0))
            else:
                price = float(ask[0]) if len(ask) > 0 else 0
                size = float(ask[1]) if len(ask) > 1 else 0
            if _MIN_TOKEN_PX <= price <= _MAX_TOKEN_PX and size > 0:
                asks.append((price, size))

        bids: List[Tuple[float, float]] = []
        for bid in bids_raw or []:
            if isinstance(bid, dict):
                price = float(bid.get("price", 0))
                size = float(bid.get("size", 0))
            else:
                price = float(bid[0]) if len(bid) > 0 else 0
                size = float(bid[1]) if len(bid) > 1 else 0
            if _MIN_TOKEN_PX <= price <= _MAX_TOKEN_PX and size > 0:
                bids.append((price, size))

        asks.sort(key=lambda x: x[0])
        bids.sort(key=lambda x: x[0], reverse=True)

        best_ask = asks[0] if asks else None
        best_bid = bids[0] if bids else None
        live = self._live_tokens(coin, fallback=tokens)
        side = self._token_side(data.get("asset_id", ""), live)
        if not side:
            return

        with self.locks[coin]:
            price_changed = False
            old_up_ask = self.markets[coin]["up_ask"]
            old_down_ask = self.markets[coin]["down_ask"]
            old_up_bid = self.markets[coin]["up_bid"]
            old_down_bid = self.markets[coin]["down_bid"]
            now = time.time()

            if best_ask:
                price, _size = best_ask
                if side == "up":
                    if price != old_up_ask:
                        self.markets[coin]["up_ask"] = price
                        self.markets[coin]["up_ask_timestamp"] = now
                        price_changed = True
                    self.markets[coin]["up_asks_full"] = asks[:1]
                    self.markets[coin]["up_bids_full"] = bids[:5]
                else:
                    if price != old_down_ask:
                        self.markets[coin]["down_ask"] = price
                        self.markets[coin]["down_ask_timestamp"] = now
                        price_changed = True
                    self.markets[coin]["down_asks_full"] = asks[:1]
                    self.markets[coin]["down_bids_full"] = bids[:5]

            if best_bid:
                price, _size = best_bid
                if side == "up":
                    if price != old_up_bid:
                        self.markets[coin]["up_bid"] = price
                        self.markets[coin]["up_bid_timestamp"] = now
                        price_changed = True
                    if not self.markets[coin]["up_bids_full"]:
                        self.markets[coin]["up_bids_full"] = bids[:5]
                else:
                    if price != old_down_bid:
                        self.markets[coin]["down_bid"] = price
                        self.markets[coin]["down_bid_timestamp"] = now
                        price_changed = True
                    if not self.markets[coin]["down_bids_full"]:
                        self.markets[coin]["down_bids_full"] = bids[:5]
            if price_changed:
                self._mark_book_fresh(coin)

        if price_changed:
            self._record_ask_pair(coin)

        if price_changed:
            self._fire_price_callbacks(coin)

    def _timer_worker(self):
        """Update seconds_till_end every second (local clock; no network)."""
        while not self.stop_event.is_set():
            current_time = int(time.time())
            for coin in self.enabled_coins:
                with self.locks[coin]:
                    market_end_time = int(self.markets[coin].get("market_end_time") or 0)
                    if market_end_time > 0:
                        self.markets[coin]["seconds_till_end"] = max(
                            0, market_end_time - current_time
                        )
            if not self.trading_hours.operations_active():
                self.trading_hours.sleep_until_allowed(self.stop_event, max_sleep=30.0)
                continue
            time.sleep(1)
    
    def _user_channel_worker(self):
        """
        WebSocket User Channel - source of ALL position data!
        
        Connects to authenticated channel and receives:
        - ORDER events (with size_matched - real amount!)
        - TRADE events (transaction confirmations)
        
        THIS IS THE SINGLE SOURCE OF TRUTH!
        """
        reconnect_delay = 5
        
        while not self.stop_event.is_set():
            try:
                ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
                
                print("[USER-WS] 🔌 Connecting to User Channel...")
                
                ws = websocket.WebSocketApp(
                    ws_url,
                    on_message=lambda ws, msg: self._on_user_message(msg),
                    on_error=lambda ws, err: print(f"[USER-WS] ❌ Error: {err}") if err else None,
                    on_close=lambda ws, code, reason: print(f"[USER-WS] 🔌 Disconnected (code={code})")
                )
                
                def on_open(ws):
                    """Send authenticated subscription request"""
                    try:
                        # Create signature for authentication
                        timestamp = str(int(time.time()))
                        message = timestamp
                        signature = hmac.new(
                            self.api_secret.encode('utf-8'),
                            message.encode('utf-8'),
                            hashlib.sha256
                        ).digest()
                        signature_b64 = base64.b64encode(signature).decode('utf-8')
                        
                        sub_msg = {
                            "auth": {
                                "apikey": self.api_key,
                                "secret": signature_b64,
                                "passphrase": self.api_passphrase,
                                "timestamp": timestamp
                            },
                            "type": "user"
                        }
                        ws.send(json.dumps(sub_msg))
                        print("[USER-WS] ✅ Authenticated & subscribed to user channel")
                    except Exception as e:
                        print(f"[USER-WS] ⚠️  Auth failed: {e}")
                
                ws.on_open = on_open
                
                # Run forever (blocking call)
                _px = websocket_proxy_kwargs(self._proxy_url_override)
                ws.run_forever(**_px)
                
            except Exception as e:
                print(f"[USER-WS] ⚠️  Exception: {e}")
            
            # Reconnect delay
            if not self.stop_event.is_set():
                print(f"[USER-WS] ⏳ Reconnecting in {reconnect_delay}s...")
                time.sleep(reconnect_delay)
    
    def _on_user_message(self, message: str):
        """
        Process all USER events - SINGLE source of truth!
        
        Event types:
        - order: ORDER events (PLACEMENT/UPDATE/CANCELLATION)
        - trade: TRADE events (MATCHED/MINED/CONFIRMED)
        
        All events are passed to PositionTracker!
        """
        try:
            data = json.loads(message)
            event_type = data.get("event_type")
            
            if event_type == "order":
                # ✅ ORDER EVENT - update position via tracker
                self.position_tracker.on_order_event(data)
            
            elif event_type == "trade":
                # ✅ TRADE EVENT - confirm trade
                self.position_tracker.on_trade_event(data)
            
            else:
                # Other event types (e.g., heartbeat)
                pass
        
        except json.JSONDecodeError:
            # Not JSON message (e.g., connection established)
            pass
        except Exception as e:
            print(f"[USER-WS] ⚠️  Parse error: {e}")
