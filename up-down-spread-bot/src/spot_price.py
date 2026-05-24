"""
Fetch underlying spot USD prices for crypto up/down markets (BTC, ETH, SOL, XRP).
Used for entry / flip-stop spot move filters (not settlement — that uses Gamma Chainlink).
"""
import time
from typing import Any, Dict, Optional

import requests

VALID_SPOT_SOURCES = ("coingecko", "chainlink")

# CoinGecko simple/price ids
_CG_IDS: Dict[str, str] = {
    "btc": "bitcoin",
    "eth": "ethereum",
    "sol": "solana",
    "xrp": "ripple",
}

_cache: Dict[str, tuple] = {}  # coin -> (price, monotonic_ts)
_CACHE_TTL_SEC = 8.0


def normalize_spot_price_source(raw: Any) -> str:
    """coingecko | chainlink — unknown values fall back to coingecko."""
    s = str(raw or "coingecko").strip().lower()
    if s not in VALID_SPOT_SOURCES:
        return "coingecko"
    return s


def spot_price_source_from_config(config: Dict) -> str:
    """Read data_sources.spot_price_source (or legacy polymarket.spot_price_source)."""
    ds = config.get("data_sources") or {}
    raw = ds.get("spot_price_source")
    if raw is None:
        raw = (ds.get("polymarket") or {}).get("spot_price_source")
    return normalize_spot_price_source(raw)


def fetch_spot_usd(
    coin: str,
    source: str = "coingecko",
    timeout: float = 2.0,
    chainlink_feed: Any = None,
) -> Optional[float]:
    """
    Unified spot fetch for order logic.
    chainlink: Polymarket RTDS cache (no HTTP). coingecko: REST API.
    """
    src = normalize_spot_price_source(source)
    if src == "chainlink":
        feed = chainlink_feed
        if feed is None:
            return None
        return feed.get_price(coin)
    return fetch_coin_spot_usd(coin, timeout=timeout)


def fetch_coin_spot_usd(coin: str, timeout: float = 2.0) -> Optional[float]:
    """Current USD spot for coin (btc/eth/sol/xrp). Returns None on failure."""
    c = (coin or "").strip().lower()
    cg_id = _CG_IDS.get(c)
    if not cg_id:
        return None

    now = time.monotonic()
    cached = _cache.get(c)
    if cached and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[0]

    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": cg_id, "vs_currencies": "usd"},
            timeout=timeout,
        )
        if r.status_code != 200:
            return cached[0] if cached else None
        px = r.json().get(cg_id, {}).get("usd")
        if px is None:
            return cached[0] if cached else None
        val = float(px)
        if val > 0:
            _cache[c] = (val, now)
            return val
    except Exception:
        pass
    return cached[0] if cached else None


def infer_up_down_winner(spot_start: float, spot_end: float) -> Optional[str]:
    """UP if close >= open, DOWN if close < open."""
    if spot_start <= 0 or spot_end <= 0:
        return None
    if spot_end >= spot_start:
        return "UP"
    return "DOWN"


def bet_won_direction(bet_side: str, spot_start: float, spot_end: float) -> Optional[bool]:
    w = infer_up_down_winner(spot_start, spot_end)
    if w is None or bet_side not in ("UP", "DOWN"):
        return None
    return bet_side == w
