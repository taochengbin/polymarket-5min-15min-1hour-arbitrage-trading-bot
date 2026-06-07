"""
Track and log entry-window skip reasons once per strategy/market/reason.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Set, Tuple


class EntrySkipTracker:
    def __init__(self) -> None:
        self._logged: Set[Tuple[str, str, str]] = set()
        self._entered: Set[Tuple[str, str]] = set()

    def _key(self, strategy_name: str, market_slug: str) -> Tuple[str, str]:
        return (strategy_name, market_slug)

    def note(
        self,
        strategy_name: str,
        coin: str,
        market_slug: str,
        reason: str,
        snap: Optional[Dict[str, Any]] = None,
        *,
        in_entry_window: bool = False,
    ) -> None:
        if not in_entry_window:
            return
        if self._key(strategy_name, market_slug) in self._entered:
            return
        reason = (reason or "unknown").strip()
        log_key = (strategy_name, market_slug, reason)
        if log_key in self._logged:
            return
        self._logged.add(log_key)

        extra = ""
        if snap:
            ste = snap.get("ste")
            conf = snap.get("conf")
            up = snap.get("up")
            dn = snap.get("dn")
            if ste is not None or conf is not None:
                extra = (
                    f" | ste={ste} conf={conf} up={up} dn={dn}"
                    if ste is not None
                    else f" | up={up} dn={dn}"
                )
        print(
            f"[ENTRY-SKIP] {(coin or '').upper()} {market_slug} "
            f"[{strategy_name}] {reason}{extra}"
        )

    def mark_entered(self, strategy_name: str, market_slug: str) -> None:
        key = self._key(strategy_name, market_slug)
        self._entered.add(key)
        self._logged = {k for k in self._logged if k[0] != strategy_name or k[1] != market_slug}

    def flush_all_for_slug(
        self,
        coin: str,
        market_slug: str,
        strategy_names: list,
    ) -> None:
        for strategy_name in strategy_names:
            key = self._key(strategy_name, market_slug)
            self._entered.discard(key)
            self._logged = {
                k for k in self._logged if k[0] != strategy_name or k[1] != market_slug
            }
