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

_MIN_TOKEN_PX = 0.001
_MAX_TOKEN_PX = 1.0


class DataFeed:
    """Polymarket orderbooks for BTC, ETH, SOL, XRP (configurable 5m or 15m windows)."""
    
    def __init__(self, config: Dict):
        self.config = config
        
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
        self._clob_host = os.getenv("CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
        self._clob_poll_sec = float(pm.get("clob_poll_interval_sec", 0.35))

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

    def start(self):
        """Start data streams for enabled coins only."""
        if self._chainlink_feed is not None:
            self._chainlink_feed.start()
            print("[DATA] Started Chainlink RTDS spot feed")

        for coin in self.enabled_coins:
            pm_thread = threading.Thread(target=self._polymarket_worker, args=(coin,), daemon=True)
            pm_thread.start()
            self.threads.append(pm_thread)
            print(f"[DATA] Started Polymarket feed for {coin.upper()}")
        
        # ❌ USER CHANNEL DISABLED - WebSocket auth doesn't work
        # Using REST API takingAmount/makingAmount instead!
        print(f"[DATA] ℹ️  Position tracking via REST API responses")
        
        # Start local timer update (fixes timer freeze)
        timer_thread = threading.Thread(target=self._timer_worker, daemon=True)
        timer_thread.start()
        self.threads.append(timer_thread)

        # REST top-of-book poll — keeps UP/DN ask fresh when WS is quiet or proxy drops events
        poll_thread = threading.Thread(target=self._clob_poll_worker, daemon=True, name="clob_poll")
        poll_thread.start()
        self.threads.append(poll_thread)
        
        print(
            f"[DATA] All feeds started: {len(self.enabled_coins)} Polymarket orderbook(s) "
            f"({self.market_slug_suffix} / {self.market_interval_sec}s windows)"
        )
    
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
    
    def get_state(self, coin: str = 'btc') -> Dict:
        """Get current market state for specified coin (thread-safe)"""
        if coin not in self.markets:
            return None
        with self.locks[coin]:
            market = self.markets.get(coin)
            if not market:
                return None
            
            # Price only for BTC and ETH (SOL/XRP don't have price feeds)
            if coin == 'btc':
                price = self.btc_price
            elif coin == 'eth':
                price = self.eth_price
            else:
                price = 0.0  # SOL and XRP don't need price
            
            # Safe handling of None values
            up_ask = market.get('up_ask') or 0.0
            down_ask = market.get('down_ask') or 0.0
            confidence = abs(down_ask - up_ask) if (up_ask > 0 and down_ask > 0) else 0.0
            
            return {
                'up_ask': up_ask,
                'down_ask': down_ask,
                'up_ask_timestamp': market.get('up_ask_timestamp') or 0.0,
                'down_ask_timestamp': market.get('down_ask_timestamp') or 0.0,
                'price': price,
                'market_start_price': market['market_start_price'],
                'seconds_till_end': market['seconds_till_end'],
                'market_slug': market['slug'],
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
    
    def _fetch_tokens(self, coin: str) -> Optional[Dict]:
        """Fetch current market tokens from Polymarket for specified coin"""
        try:
            gamma_api = self.config['data_sources']['polymarket']['gamma_api']
            slug = self._current_slug(coin)
            
            # Use events API with specific slug
            url = f"{gamma_api}/events?slug={slug}"
            proxies = requests_proxies(self._proxy_url_override)
            req_kw: Dict[str, Any] = {"timeout": 10}
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
        spot = self._fetch_spot_for_coin(coin)
        if not spot or spot <= 0:
            return 0.0
        with self.locks[coin]:
            if coin == 'btc':
                self.btc_price = spot
            elif coin == 'eth':
                self.eth_price = spot
        return spot
    
    def _polymarket_worker(self, coin: str):
        """Polymarket WebSocket worker for specified coin"""
        while not self.stop_event.is_set():
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
                ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                    skip_utf8_validation=True,
                    **_px,
                )
                timer.cancel()
                
                # Stop immediately if stop_event is set
                if self.stop_event.is_set():
                    break
                
            except Exception as e:
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

    def _fetch_clob_top_of_book(
        self, token_id: str, timeout: float = 2.5
    ) -> Tuple[Optional[float], Optional[float]]:
        """REST fallback: best bid / best ask for one outcome token."""
        tid = self._norm_token_id(token_id)
        if not tid:
            return None, None
        try:
            proxies = requests_proxies(self._proxy_url_override)
            req_kw: Dict[str, Any] = {"timeout": timeout}
            if proxies:
                req_kw["proxies"] = proxies
            url = f"{self._clob_host}/book"
            resp = requests.get(url, params={"token_id": tid}, **req_kw)
            resp.raise_for_status()
            data = resp.json()
            asks: List[Tuple[float, float]] = []
            bids: List[Tuple[float, float]] = []
            for ask in data.get("asks") or []:
                if isinstance(ask, dict):
                    p = float(ask.get("price", 0))
                    s = float(ask.get("size", 0))
                else:
                    p = float(ask[0]) if len(ask) > 0 else 0
                    s = float(ask[1]) if len(ask) > 1 else 0
                if _MIN_TOKEN_PX <= p <= _MAX_TOKEN_PX and s > 0:
                    asks.append((p, s))
            for bid in data.get("bids") or []:
                if isinstance(bid, dict):
                    p = float(bid.get("price", 0))
                    s = float(bid.get("size", 0))
                else:
                    p = float(bid[0]) if len(bid) > 0 else 0
                    s = float(bid[1]) if len(bid) > 1 else 0
                if _MIN_TOKEN_PX <= p <= _MAX_TOKEN_PX and s > 0:
                    bids.append((p, s))
            asks.sort(key=lambda x: x[0])
            bids.sort(key=lambda x: x[0], reverse=True)
            best_ask = asks[0][0] if asks else None
            best_bid = bids[0][0] if bids else None
            return best_bid, best_ask
        except Exception:
            return None, None

    def _refresh_coin_book_rest(self, coin: str, fire_callbacks: bool = False) -> bool:
        """Pull top-of-book via CLOB REST for current market tokens."""
        tokens = self._live_tokens(coin)
        if not tokens:
            return False
        book: Dict[str, Tuple[Optional[float], Optional[float]]] = {}

        def _pull(side: str, tid: str) -> None:
            book[side] = self._fetch_clob_top_of_book(tid, timeout=1.2)

        t_up = threading.Thread(target=_pull, args=("up", tokens["up"]), daemon=True)
        t_dn = threading.Thread(target=_pull, args=("down", tokens["down"]), daemon=True)
        t_up.start()
        t_dn.start()
        t_up.join(timeout=2.5)
        t_dn.join(timeout=2.5)
        up_bid, up_ask = book.get("up", (None, None))
        down_bid, down_ask = book.get("down", (None, None))
        changed = False
        now = time.time()
        with self.locks[coin]:
            if up_ask is not None and self.markets[coin]["up_ask"] != up_ask:
                self.markets[coin]["up_ask"] = up_ask
                self.markets[coin]["up_ask_timestamp"] = now
                changed = True
            if down_ask is not None and self.markets[coin]["down_ask"] != down_ask:
                self.markets[coin]["down_ask"] = down_ask
                self.markets[coin]["down_ask_timestamp"] = now
                changed = True
            if up_bid is not None and self.markets[coin]["up_bid"] != up_bid:
                self.markets[coin]["up_bid"] = up_bid
                self.markets[coin]["up_bid_timestamp"] = now
                changed = True
            if down_bid is not None and self.markets[coin]["down_bid"] != down_bid:
                self.markets[coin]["down_bid"] = down_bid
                self.markets[coin]["down_bid_timestamp"] = now
                changed = True
        if changed and fire_callbacks:
            self._fire_price_callbacks(coin)
        return changed

    def _clob_poll_worker(self) -> None:
        """Periodic REST refresh for web UI + get_state (no strategy callbacks — avoids thread storms)."""
        interval = max(0.25, self._clob_poll_sec)
        while not self.stop_event.is_set():
            for coin in self.enabled_coins:
                if self.stop_event.is_set():
                    break
                try:
                    self._refresh_coin_book_rest(coin, fire_callbacks=False)
                except Exception:
                    pass
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
        side = self._token_side(asset_id, tokens)
        if not side:
            return False

        price_changed = False
        with self.locks[coin]:
            if ask_f is not None:
                key = f"{side}_ask"
                if self.markets[coin][key] != ask_f:
                    self.markets[coin][key] = ask_f
                    self.markets[coin][f"{side}_ask_timestamp"] = time.time()
                    price_changed = True
            if bid_f is not None:
                key = f"{side}_bid"
                if self.markets[coin][key] != bid_f:
                    self.markets[coin][key] = bid_f
                    self.markets[coin][f"{side}_bid_timestamp"] = time.time()
                    price_changed = True

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
        side = self._token_side(data.get("asset_id", ""), tokens)
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
                    self.markets[coin]["up_ask"] = price
                    self.markets[coin]["up_ask_timestamp"] = now
                    self.markets[coin]["up_asks_full"] = asks[:1]
                    self.markets[coin]["up_bids_full"] = bids[:5]
                    if price != old_up_ask:
                        price_changed = True
                else:
                    self.markets[coin]["down_ask"] = price
                    self.markets[coin]["down_ask_timestamp"] = now
                    self.markets[coin]["down_asks_full"] = asks[:1]
                    self.markets[coin]["down_bids_full"] = bids[:5]
                    if price != old_down_ask:
                        price_changed = True

            if best_bid:
                price, _size = best_bid
                if side == "up":
                    self.markets[coin]["up_bid"] = price
                    self.markets[coin]["up_bid_timestamp"] = now
                    if not self.markets[coin]["up_bids_full"]:
                        self.markets[coin]["up_bids_full"] = bids[:5]
                    if price != old_up_bid:
                        price_changed = True
                else:
                    self.markets[coin]["down_bid"] = price
                    self.markets[coin]["down_bid_timestamp"] = now
                    if not self.markets[coin]["down_bids_full"]:
                        self.markets[coin]["down_bids_full"] = bids[:5]
                    if price != old_down_bid:
                        price_changed = True

        if price_changed:
            self._fire_price_callbacks(coin)

    def _timer_worker(self):
        """Update timer every second locally for all markets (per-coin locks)"""
        while not self.stop_event.is_set():
            current_time = int(time.time())
            # Update each coin's timer independently (fully parallel)
            for coin in self.enabled_coins:
                with self.locks[coin]:
                    market_end_time = self.markets[coin]['market_end_time']
                    self.markets[coin]['seconds_till_end'] = max(0, market_end_time - current_time)
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
