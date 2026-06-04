"""
Track Chainlink spot high/low after window open for low-volatility (coin-flip) filter.
Samples every N seconds before the entry window; blocks entry if range < threshold.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class _WindowRangeState:
    open_price: float
    high: float
    low: float
    open_locked_at: float
    samples: List[float] = field(default_factory=list)
    sample_timestamps: List[float] = field(default_factory=list)
    skip_logged: bool = False
    monitoring_done: bool = False


class WindowRangeTracker:
    """
    After Chainlink open (priceToBeat) is locked:
    - Every sample_interval_sec, record Chainlink spot until entry window OR monitor_sec elapsed
    - At entry: range = high - low (includes open in high/low)
    """

    def __init__(
        self,
        *,
        min_range_usd: float = 0.0,
        sample_interval_sec: float = 5.0,
        monitor_sec: float = 180.0,
        log_to_trades: bool = True,
    ):
        self.min_range_usd = float(min_range_usd or 0)
        self.sample_interval_sec = max(1.0, float(sample_interval_sec or 5))
        self.monitor_sec = max(0.0, float(monitor_sec or 0))
        self.log_to_trades = bool(log_to_trades)
        self._states: Dict[str, Dict[str, _WindowRangeState]] = {}
        self._lock = threading.Lock()
        self._last_sample_mono: Dict[Tuple[str, str], float] = {}
        self._open_not_locked_logged: set = set()

    @property
    def enabled(self) -> bool:
        """Run Chainlink sampling (filter and/or trade log fields)."""
        return self.min_range_usd > 0 or self.log_to_trades

    @property
    def filter_enabled(self) -> bool:
        return self.min_range_usd > 0

    def on_open_locked(self, coin: str, slug: str, open_price: float) -> None:
        if not slug or open_price <= 0:
            return
        c = (coin or "").strip().lower()
        with self._lock:
            bucket = self._states.setdefault(c, {})
            if slug in bucket:
                st = bucket[slug]
                if st.open_price <= 0:
                    st.open_price = float(open_price)
                    st.high = float(open_price)
                    st.low = float(open_price)
                return
            bucket[slug] = _WindowRangeState(
                open_price=float(open_price),
                high=float(open_price),
                low=float(open_price),
                open_locked_at=time.time(),
            )
        print(
            f"[RANGE] {c.upper()} {slug} Chainlink open ${open_price:,.2f} — "
            f"sampling every {self.sample_interval_sec:.0f}s (max {self.monitor_sec:.0f}s, "
            f"min range ${self.min_range_usd:,.0f})"
        )

    def _get_state(self, coin: str, slug: str) -> Optional[_WindowRangeState]:
        c = (coin or "").strip().lower()
        with self._lock:
            return self._states.get(c, {}).get(slug)

    def has_open_locked(self, coin: str, slug: str) -> bool:
        st = self._get_state(coin, slug)
        return st is not None and st.open_price > 0

    def get_range(self, coin: str, slug: str) -> float:
        st = self._get_state(coin, slug)
        if not st or st.open_price <= 0:
            return 0.0
        return max(0.0, st.high - st.low)

    def sample_count(self, coin: str, slug: str) -> int:
        st = self._get_state(coin, slug)
        return len(st.samples) if st else 0

    def should_sample(
        self,
        coin: str,
        slug: str,
        *,
        seconds_till_end: int,
        entry_window_sec: int,
    ) -> bool:
        """Sample only before entry window and within monitor duration after open lock."""
        if not self.enabled or not slug:
            return False
        st = self._get_state(coin, slug)
        if not st or st.open_price <= 0 or st.monitoring_done:
            return False
        if seconds_till_end <= entry_window_sec:
            with self._lock:
                st.monitoring_done = True
            return False
        elapsed = time.time() - st.open_locked_at
        if self.monitor_sec > 0 and elapsed >= self.monitor_sec:
            with self._lock:
                st.monitoring_done = True
            return False
        key = ((coin or "").strip().lower(), slug)
        now = time.monotonic()
        last = self._last_sample_mono.get(key, 0.0)
        if now - last < self.sample_interval_sec:
            return False
        return True

    def record_sample(self, coin: str, slug: str, price: float) -> None:
        if price <= 0 or not slug:
            return
        c = (coin or "").strip().lower()
        key = (c, slug)
        with self._lock:
            st = self._states.get(c, {}).get(slug)
            if not st:
                return
            px = float(price)
            st.samples.append(px)
            st.sample_timestamps.append(time.time())
            st.high = max(st.high, px)
            st.low = min(st.low, px)
            self._last_sample_mono[key] = time.monotonic()

    def entry_allowed(
        self,
        coin: str,
        slug: str,
        *,
        in_entry_window: bool,
    ) -> Tuple[bool, str]:
        """
        Returns (allowed, reason).
        Before entry window: always allowed (not evaluated yet).
        In entry window: require range >= min_range_usd when enabled.
        """
        if not self.filter_enabled:
            return True, "disabled"
        if not in_entry_window:
            return True, "before_entry_window"
        st = self._get_state(coin, slug)
        if not st or st.open_price <= 0:
            key = ((coin or "").strip().lower(), slug)
            if key not in self._open_not_locked_logged:
                self._open_not_locked_logged.add(key)
                print(
                    f"[RANGE-SKIP] {(coin or '').upper()} {slug} | "
                    f"open not locked (waiting Gamma priceToBeat or RTDS fallback)"
                )
            return False, "open_not_locked"
        rng = max(0.0, st.high - st.low)
        if rng < self.min_range_usd:
            if not st.skip_logged:
                with self._lock:
                    st.skip_logged = True
                print(
                    f"[RANGE-SKIP] {(coin or '').upper()} {slug} | "
                    f"Chainlink range ${rng:,.2f} < ${self.min_range_usd:,.0f} "
                    f"(open ${st.open_price:,.2f}, high ${st.high:,.2f}, low ${st.low:,.2f}, "
                    f"samples={len(st.samples)}) — skip entry (coin-flip filter)"
                )
            return False, f"range_{rng:.2f}_lt_{self.min_range_usd:.0f}"
        return True, f"range_{rng:.2f}_ok"

    def snapshot_for_state(self, coin: str, slug: str) -> Dict[str, float]:
        st = self._get_state(coin, slug)
        if not st:
            return {"window_range_usd": 0.0, "window_range_samples": 0}
        return {
            "window_range_usd": max(0.0, st.high - st.low),
            "window_range_samples": float(len(st.samples)),
            "window_range_high": st.high,
            "window_range_low": st.low,
        }

    def fields_for_trade_record(
        self, coin: str, slug: str, *, spot_now: float = 0.0
    ) -> Dict[str, Optional[float]]:
        """
        Chainlink high/low since open lock (up to monitor_sec, default 3 min).
        Optionally include spot_now so entry-time price is in the range.
        """
        if spot_now > 0:
            self.record_sample(coin, slug, spot_now)
        st = self._get_state(coin, slug)
        if not st or st.open_price <= 0:
            return {"window_range_high": None, "window_range_low": None}
        return {
            "window_range_high": round(float(st.high), 2),
            "window_range_low": round(float(st.low), 2),
        }

    def remove_market(self, coin: str, slug: str) -> None:
        c = (coin or "").strip().lower()
        with self._lock:
            self._states.get(c, {}).pop(slug, None)
            self._last_sample_mono.pop((c, slug), None)
            self._open_not_locked_logged.discard((c, slug))

    @classmethod
    def from_config(cls, config: dict) -> WindowRangeTracker:
        sc = config.get("strategy") or {}
        min_range = float(sc.get("min_window_range_usd", 0) or 0)
        log_cfg = sc.get("log_window_range_in_trades")
        if log_cfg is None:
            log_to_trades = min_range > 0
        else:
            log_to_trades = bool(log_cfg)
        return cls(
            min_range_usd=min_range,
            sample_interval_sec=float(sc.get("window_range_sample_sec", 5) or 5),
            monitor_sec=float(sc.get("window_range_monitor_sec", 180) or 180),
            log_to_trades=log_to_trades,
        )
