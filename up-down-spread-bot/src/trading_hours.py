"""
Trading-hours gate: restrict bot operations to configured local-time windows.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class _TimeRange:
    label: str
    start_min: int  # minutes since midnight
    end_min: int

    def contains(self, minute: int) -> bool:
        if self.start_min <= self.end_min:
            return self.start_min <= minute <= self.end_min
        # spans midnight
        return minute >= self.start_min or minute <= self.end_min


def _parse_hhmm(value: str) -> int:
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"invalid time: {value!r}")
    hour = int(parts[0])
    minute = int(parts[1])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"invalid time: {value!r}")
    return hour * 60 + minute


def _parse_ranges(raw: Any) -> List[_TimeRange]:
    out: List[_TimeRange] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, str) or "-" not in item:
            continue
        start_s, end_s = item.split("-", 1)
        try:
            out.append(
                _TimeRange(
                    label=item.strip(),
                    start_min=_parse_hhmm(start_s),
                    end_min=_parse_hhmm(end_s),
                )
            )
        except ValueError:
            continue
    return out


class CallableStopEvent:
    """Adapter so redeem collector can pass a lambda instead of threading.Event."""

    def __init__(self, is_stopped: Callable[[], bool]) -> None:
        self._is_stopped = is_stopped

    def is_set(self) -> bool:
        return bool(self._is_stopped())


class TradingHours:
    def __init__(self, enabled: bool, ranges: List[_TimeRange]) -> None:
        self.enabled = bool(enabled)
        self._ranges = list(ranges)

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "TradingHours":
        th = config.get("trading_hours") or {}
        return cls(
            enabled=bool(th.get("enabled", False)),
            ranges=_parse_ranges(th.get("ranges")),
        )

    def ranges_summary(self) -> str:
        if not self._ranges:
            return "none"
        return ", ".join(r.label for r in self._ranges)

    def _now_local(self) -> datetime:
        return datetime.now()

    def _minute_of_day(self, when: Optional[datetime] = None) -> int:
        dt = when or self._now_local()
        return dt.hour * 60 + dt.minute

    def _active_range(self, when: Optional[datetime] = None) -> Optional[_TimeRange]:
        if not self.enabled or not self._ranges:
            return None
        minute = self._minute_of_day(when)
        for rng in self._ranges:
            if rng.contains(minute):
                return rng
        return None

    def operations_active(self, when: Optional[datetime] = None) -> bool:
        if not self.enabled:
            return True
        if not self._ranges:
            return True
        return self._active_range(when) is not None

    def active_range_label(self, when: Optional[datetime] = None) -> Optional[str]:
        active = self._active_range(when)
        return active.label if active else None

    def next_window_start(self, when: Optional[datetime] = None) -> Optional[datetime]:
        if not self.enabled or not self._ranges:
            return None
        now = when or self._now_local()
        minute = self._minute_of_day(now)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)

        candidates: List[datetime] = []
        for rng in self._ranges:
            start_dt = today + timedelta(minutes=rng.start_min)
            if rng.contains(minute):
                return start_dt
            if rng.start_min > minute:
                candidates.append(start_dt)
            else:
                candidates.append(start_dt + timedelta(days=1))

        if not candidates:
            return None
        return min(candidates)

    def seconds_until_allowed(self, when: Optional[datetime] = None) -> float:
        if self.operations_active(when):
            return 0.0
        nxt = self.next_window_start(when)
        if nxt is None:
            return 60.0
        now = when or self._now_local()
        return max(0.0, (nxt - now).total_seconds())

    def _sleep_chunk(self, stop_event: Any, seconds: float) -> bool:
        if seconds <= 0:
            return bool(getattr(stop_event, "is_set", lambda: False)())
        if hasattr(stop_event, "wait"):
            stop_event.wait(seconds)
            return bool(stop_event.is_set())
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if stop_event.is_set():
                return True
            time.sleep(min(0.5, deadline - time.monotonic()))
        return bool(stop_event.is_set())

    def sleep_until_allowed(self, stop_event: Any, max_sleep: float = 60.0) -> None:
        while not stop_event.is_set() and not self.operations_active():
            remaining = min(max_sleep, self.seconds_until_allowed())
            if self._sleep_chunk(stop_event, max(0.5, remaining)):
                return

    def wait_until_operations_active(self, stop_event: Any, max_sleep: float = 60.0) -> None:
        self.sleep_until_allowed(stop_event, max_sleep=max_sleep)

    def status_for_dashboard(self, when: Optional[datetime] = None) -> Tuple[bool, str]:
        if not self.enabled:
            return True, "24h"
        if self.operations_active(when):
            label = self.active_range_label(when) or "window"
            return True, f"in {label}"
        nxt = self.next_window_start(when)
        when_s = nxt.strftime("%H:%M") if nxt else "?"
        return False, f"paused until {when_s}"


def load_from_config_path(config_path: Path) -> TradingHours:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return TradingHours.from_config(cfg)


def dashboard_payload(hours: TradingHours) -> Dict[str, Any]:
    allowed, reason = hours.status_for_dashboard()
    return {
        "enabled": hours.enabled,
        "ranges": [r.label for r in hours._ranges],
        "summary": hours.ranges_summary(),
        "allowed_now": allowed,
        "status_reason": reason,
        "local_time": hours._now_local().strftime("%H:%M:%S"),
    }


def merge_trading_status(
    coin_enabled: bool,
    coin_reason: str,
    hours: TradingHours,
) -> Tuple[bool, str]:
    if not coin_enabled:
        return False, coin_reason or "disabled in config"
    if not hours.operations_active():
        _, reason = hours.status_for_dashboard()
        return False, reason
    return True, ""
