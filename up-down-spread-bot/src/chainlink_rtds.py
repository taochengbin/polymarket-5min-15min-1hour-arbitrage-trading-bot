"""
Polymarket RTDS WebSocket — Chainlink crypto_prices_chainlink stream.
Free, no auth. Same oracle used for Polymarket crypto up/down settlement.
"""
import json
import threading
import time
from typing import Callable, Dict, List, Optional, Set

import websocket

from proxy_env import websocket_proxy_kwargs

RTDS_WS_URL = "wss://ws-live-data.polymarket.com"
PING_INTERVAL_SEC = 5.0

_COIN_SYMBOLS: Dict[str, str] = {
    "btc": "btc/usd",
    "eth": "eth/usd",
    "sol": "sol/usd",
    "xrp": "xrp/usd",
}
_SYMBOL_TO_COIN = {v: k for k, v in _COIN_SYMBOLS.items()}


class ChainlinkRtdsFeed:
    """Real-time Chainlink USD prices via Polymarket RTDS."""

    def __init__(
        self,
        ws_url: str = RTDS_WS_URL,
        proxy_url_override: Optional[str] = None,
        on_price_update: Optional[Callable[[str, float], None]] = None,
        coins: Optional[List[str]] = None,
    ):
        self._ws_url = ws_url or RTDS_WS_URL
        self._proxy_url_override = proxy_url_override
        self._on_price_update = on_price_update
        raw = [str(c).strip().lower() for c in (coins or ["btc"])]
        self._coins: Set[str] = {c for c in raw if c in _COIN_SYMBOLS} or {"btc"}
        self._prices: Dict[str, tuple] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def get_price(self, coin: str) -> Optional[float]:
        c = (coin or "").strip().lower()
        with self._lock:
            row = self._prices.get(c)
        if not row:
            return None
        return row[0]

    def get_price_age_sec(self, coin: str) -> Optional[float]:
        c = (coin or "").strip().lower()
        with self._lock:
            row = self._prices.get(c)
        if not row:
            return None
        return time.monotonic() - row[1]

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._worker, daemon=True, name="chainlink_rtds"
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _set_price(self, coin: str, value: float) -> None:
        if value <= 0:
            return
        now = time.monotonic()
        with self._lock:
            self._prices[coin] = (value, now)
        if self._on_price_update:
            try:
                self._on_price_update(coin, value)
            except Exception:
                pass

    def _subscribe_msg(self) -> str:
        # Subscribe to all Chainlink symbols; per-symbol filters only send a snapshot
        # without reliable live ticks — filter client-side instead.
        return json.dumps(
            {
                "action": "subscribe",
                "subscriptions": [
                    {
                        "topic": "crypto_prices_chainlink",
                        "type": "*",
                        "filters": "",
                    }
                ],
            }
        )

    def _on_message(self, _ws, msg) -> None:
        if msg is None:
            return
        if isinstance(msg, bytes):
            msg = msg.decode("utf-8", errors="replace")
        if not str(msg).strip():
            return
        try:
            data = json.loads(msg)
        except (json.JSONDecodeError, TypeError):
            return
        topic = str(data.get("topic") or "")
        # Filtered chainlink subs may arrive as topic crypto_prices (slash symbols).
        if topic not in ("crypto_prices_chainlink", "crypto_prices"):
            return
        payload = data.get("payload") or {}
        sym = (payload.get("symbol") or "").lower()
        if topic == "crypto_prices" and "/" not in sym:
            return
        coin = _SYMBOL_TO_COIN.get(sym)
        if not coin or coin not in self._coins:
            return
        if "value" in payload:
            try:
                val = float(payload.get("value") or 0)
            except (TypeError, ValueError):
                return
            self._set_price(coin, val)
            return
        rows = payload.get("data")
        if isinstance(rows, list) and rows:
            if not coin and len(self._coins) == 1:
                coin = next(iter(self._coins))
            if not coin or coin not in self._coins:
                return
            last = rows[-1]
            if isinstance(last, dict):
                try:
                    val = float(last.get("value") or 0)
                except (TypeError, ValueError):
                    return
                self._set_price(coin, val)

    def _worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_session()
            except Exception as exc:
                print(f"[RTDS-CL] Error: {exc}")
            if self._stop_event.is_set():
                break
            time.sleep(3)

    def _run_session(self) -> None:
        ws_ref = [None]
        ping_stop = threading.Event()

        def ping_loop() -> None:
            while not ping_stop.is_set() and not self._stop_event.is_set():
                time.sleep(PING_INTERVAL_SEC)
                try:
                    if ws_ref[0]:
                        ws_ref[0].send("PING")
                except Exception:
                    pass

        ping_thread = threading.Thread(
            target=ping_loop, daemon=True, name="rtds_cl_ping"
        )

        def on_open(ws) -> None:
            ws.send(self._subscribe_msg())
            syms = ", ".join(_COIN_SYMBOLS[c] for c in sorted(self._coins) if c in _COIN_SYMBOLS)
            print(f"[RTDS-CL] Subscribed crypto_prices_chainlink (all, client filter: {syms})")
            if not ping_thread.is_alive():
                ping_thread.start()

        def on_close(_ws, code, reason) -> None:
            ping_stop.set()
            if code is not None and int(code) != 1000:
                print(f"[RTDS-CL] WebSocket closed code={code} reason={reason!r}")

        ws = websocket.WebSocketApp(
            self._ws_url,
            on_open=on_open,
            on_message=self._on_message,
            on_error=lambda _w, err: (
                print(f"[RTDS-CL] WebSocket error: {err!r}") if err else None
            ),
            on_close=on_close,
        )
        ws_ref[0] = ws

        def check_stop() -> None:
            self._stop_event.wait()
            ping_stop.set()
            try:
                ws.close()
            except Exception:
                pass

        threading.Thread(target=check_stop, daemon=True).start()

        px = websocket_proxy_kwargs(self._proxy_url_override)
        ws.run_forever(
            ping_interval=20,
            ping_timeout=10,
            skip_utf8_validation=True,
            **px,
        )
        ping_stop.set()
