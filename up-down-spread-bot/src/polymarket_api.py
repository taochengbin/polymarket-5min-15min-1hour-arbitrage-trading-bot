"""
Polymarket API integration for market outcome verification
"""

import time
import requests
import json
from typing import Optional, Dict, Tuple

from proxy_env import requests_proxies, requests_proxies_from_environ

GAMMA_API = "https://gamma-api.polymarket.com"

# slug -> (cached_at, winner, closed, resolved, price_to_beat, final_price)
_outcome_cache: Dict[str, Tuple[float, Optional[str], bool, bool, float, float]] = {}
_CACHE_RESOLVED_SEC = 300.0
_CACHE_PENDING_SEC = 15.0


def clear_outcome_cache(slug: Optional[str] = None) -> None:
    if slug:
        _outcome_cache.pop(slug, None)
    else:
        _outcome_cache.clear()


def _parse_event_metadata(event: Dict) -> Tuple[float, float]:
    em = event.get("eventMetadata") or {}
    try:
        ptb = float(em.get("priceToBeat") or 0)
    except (TypeError, ValueError):
        ptb = 0.0
    try:
        fp = float(em.get("finalPrice") or 0)
    except (TypeError, ValueError):
        fp = 0.0
    return ptb, fp


def _winner_from_prices(prices) -> Optional[str]:
    if not prices or len(prices) < 2:
        return None
    try:
        price_up = float(prices[0])
        price_down = float(prices[1])
    except (TypeError, ValueError):
        return None
    if price_up > 0.99:
        return "UP"
    if price_down > 0.99:
        return "DOWN"
    return None


def market_slug_window_end_ts(slug: str, interval_sec: int = 300) -> float:
    """Unix time when the up/down window ends (slug suffix = window start)."""
    try:
        start = int(str(slug).rsplit("-", 1)[-1])
        return float(start + int(interval_sec))
    except (ValueError, IndexError):
        return 0.0


def _winner_from_chainlink_prices(price_to_beat: float, final_price: float) -> Optional[str]:
    """Poly rule: Up if end >= open (Chainlink), else Down."""
    if price_to_beat <= 0 or final_price <= 0:
        return None
    if final_price >= price_to_beat:
        return "UP"
    return "DOWN"


def get_market_outcome(slug: str, timeout: int = 5, proxy_url: Optional[str] = None) -> Dict:
    """
    Get market outcome from Polymarket Gamma API.

    Returns winner, resolved/closed flags, and Chainlink open/close (priceToBeat / finalPrice).
    """
    try:
        url = f"{GAMMA_API}/events?slug={slug}"
        proxies = requests_proxies(proxy_url) if proxy_url else requests_proxies_from_environ()
        req_kw = {"timeout": timeout}
        if proxies:
            req_kw["proxies"] = proxies
        resp = None
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                resp = requests.get(url, **req_kw)
                resp.raise_for_status()
                break
            except requests.exceptions.RequestException as exc:
                last_err = exc
                if attempt < 2:
                    time.sleep(0.4 * (attempt + 1))
                    continue
                return {"success": False, "error": f"API request failed: {str(exc)}"}
        if resp is None:
            return {
                "success": False,
                "error": f"API request failed: {str(last_err or 'unknown')}",
            }

        events = resp.json()
        if not events:
            return {"success": False, "error": f"Market not found in API: {slug}"}

        event = events[0]
        markets = event.get("markets", [])
        if not markets:
            return {"success": False, "error": f"No markets in event: {slug}"}

        market = markets[0]
        outcomes = market.get("outcomes", [])
        prices = market.get("outcomePrices", [])

        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        if isinstance(prices, str):
            prices = json.loads(prices)

        closed = bool(market.get("closed", False))
        uma = (market.get("umaResolutionStatus") or "").strip().lower()
        resolved = bool(market.get("resolved")) or uma == "resolved"

        price_to_beat, final_price = _parse_event_metadata(event)

        # BTC/ETH up-down: Chainlink open/close is authoritative when present.
        cl_winner = _winner_from_chainlink_prices(price_to_beat, final_price)
        price_winner = _winner_from_prices(prices)
        past_end = (
            market_slug_window_end_ts(slug) > 0
            and time.time() >= market_slug_window_end_ts(slug) + 10
        )
        winner = None
        if cl_winner:
            winner = cl_winner
        elif price_winner and (closed or resolved):
            winner = price_winner
        elif price_winner and past_end:
            winner = price_winner
            closed = True

        return {
            "success": True,
            "winner": winner,
            "resolved": resolved,
            "closed": closed,
            "outcomes": outcomes,
            "prices": prices,
            "price_to_beat": price_to_beat,
            "final_price": final_price,
        }

    except requests.exceptions.Timeout:
        return {"success": False, "error": f"API timeout for {slug}"}
    except requests.exceptions.RequestException as e:
        return {"success": False, "error": f"API request failed: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": f"Unexpected error: {str(e)}"}


def get_official_settlement(
    slug: str,
    timeout: int = 5,
    proxy_url: Optional[str] = None,
    use_cache: bool = True,
) -> Dict:
    """
    Official settlement: UP/DOWN winner + Chainlink open/close from Gamma.

    Returns: success, winner, closed, resolved, price_to_beat, final_price, error, cached
    """
    if not slug:
        return {
            "success": False,
            "winner": None,
            "closed": False,
            "resolved": False,
            "price_to_beat": 0.0,
            "final_price": 0.0,
            "error": "empty slug",
        }

    now = time.time()
    if use_cache:
        cached = _outcome_cache.get(slug)
        if cached:
            ts, winner, closed, resolved, ptb, fp = cached
            ttl = _CACHE_RESOLVED_SEC if winner in ("UP", "DOWN") else _CACHE_PENDING_SEC
            if now - ts < ttl:
                return {
                    "success": True,
                    "winner": winner,
                    "closed": closed,
                    "resolved": resolved,
                    "price_to_beat": ptb,
                    "final_price": fp,
                    "cached": True,
                }

    api = get_market_outcome(slug, timeout=timeout, proxy_url=proxy_url)
    if not api.get("success"):
        return {
            "success": False,
            "winner": None,
            "closed": False,
            "resolved": False,
            "price_to_beat": 0.0,
            "final_price": 0.0,
            "error": api.get("error", "api failed"),
        }

    closed = bool(api.get("closed"))
    resolved = bool(api.get("resolved"))
    ptb = float(api.get("price_to_beat") or 0)
    fp = float(api.get("final_price") or 0)
    winner = api.get("winner") if api.get("winner") in ("UP", "DOWN") else None
    cl_w = _winner_from_chainlink_prices(ptb, fp)
    if cl_w:
        winner = cl_w
    _outcome_cache[slug] = (now, winner, closed, resolved, ptb, fp)

    return {
        "success": True,
        "winner": winner,
        "closed": closed,
        "resolved": resolved,
        "price_to_beat": ptb,
        "final_price": fp,
        "cached": False,
    }


def get_official_settlement_winner(
    slug: str,
    timeout: int = 5,
    proxy_url: Optional[str] = None,
) -> Dict:
    """Backward-compatible wrapper (winner + flags only)."""
    s = get_official_settlement(slug, timeout=timeout, proxy_url=proxy_url)
    return {
        "success": s.get("success"),
        "winner": s.get("winner"),
        "closed": s.get("closed"),
        "resolved": s.get("resolved"),
        "cached": s.get("cached", False),
        "error": s.get("error"),
    }


def chainlink_window_prices(
    slug: str,
    timeout: int = 6,
    proxy_url: Optional[str] = None,
    use_cache: bool = True,
) -> Dict:
    """
    Polymarket 5m/15m 窗口 Chainlink 标的起/止（Gamma eventMetadata）。
    非 CoinGecko 现价。
    """
    s = get_official_settlement(
        slug, timeout=timeout, proxy_url=proxy_url, use_cache=use_cache
    )
    return {
        "success": bool(s.get("success")),
        "spot_start": float(s.get("price_to_beat") or 0),
        "spot_end": float(s.get("final_price") or 0),
        "closed": bool(s.get("closed")),
        "resolved": bool(s.get("resolved")),
        "winner": s.get("winner"),
    }


def wait_for_official_settlement(
    slug: str,
    proxy_url: Optional[str] = None,
    max_wait: float = 90.0,
    poll_interval: float = 3.0,
    request_timeout: int = 8,
) -> Dict:
    """Poll Gamma until official winner is known or max_wait elapses."""
    deadline = time.time() + max_wait
    last: Dict = {"success": False, "winner": None, "error": "timeout"}

    while time.time() < deadline:
        last = get_official_settlement(
            slug,
            timeout=request_timeout,
            proxy_url=proxy_url,
            use_cache=False,
        )
        if last.get("success") and last.get("winner") in ("UP", "DOWN"):
            ptb = float(last.get("price_to_beat") or 0)
            fp = float(last.get("final_price") or 0)
            if ptb > 0 and fp > 0:
                return last
            # Gamma outcome resolved but Chainlink finalPrice may lag — still usable.
            if last.get("closed") or last.get("resolved"):
                return last
        time.sleep(poll_interval)

    if not last.get("success"):
        last = get_official_settlement(
            slug, timeout=request_timeout, proxy_url=proxy_url, use_cache=False
        )
    return last
