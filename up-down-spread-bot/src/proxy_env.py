"""
Read HTTP(S) proxy from optional config override or environment (HTTPS_PROXY, etc.)
for requests and websocket-client — same convention as curl / Invoke-WebRequest.
"""
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse


def normalize_proxy_url(url: str) -> str:
    """
    Ensure a parseable URL for requests / websocket-client.
    Values like ``127.0.0.1:58591`` (no scheme) yield hostname=None in urlparse
    and break WebSocket proxy — prepend http://.
    """
    u = str(url).strip()
    if not u or "://" in u:
        return u
    return "http://" + u


def proxy_url_from_environ() -> Optional[str]:
    """First non-empty proxy URL from common env vars."""
    for key in (
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ):
        val = os.environ.get(key)
        if val and str(val).strip():
            return normalize_proxy_url(str(val).strip())
    return None


def resolve_proxy_url(override: Optional[str] = None) -> Optional[str]:
    """Prefer non-empty `override` (e.g. from config.json), else environment."""
    if override is not None and str(override).strip():
        return normalize_proxy_url(str(override).strip())
    return proxy_url_from_environ()


def requests_proxies(override: Optional[str] = None) -> Optional[Dict[str, str]]:
    u = resolve_proxy_url(override)
    if not u:
        return None
    return {"http": u, "https": u}


def websocket_proxy_kwargs(override: Optional[str] = None) -> Dict[str, Any]:
    """
    Map HTTP(S) proxy URL to websocket-client run_forever() args.
    Sets proxy_type='http' for CONNECT tunneling (required by many clients for wss://).
    SOCKS URLs are skipped here (use http:// mixed port or TUN).
    """
    u = resolve_proxy_url(override)
    if not u:
        return {}
    parsed = urlparse(u)
    scheme = (parsed.scheme or "").lower()
    if scheme in ("socks5", "socks5h", "socks4", "socks4a"):
        print(
            "[PROXY] SOCKS* proxy: Polymarket WS path expects http:// (e.g. Clash mixed port) or TUN/VPN."
        )
        return {}
    if not parsed.hostname:
        print(
            "[PROXY] Invalid proxy URL (missing host). Use full URL, e.g. http://127.0.0.1:7890"
        )
        return {}
    port = parsed.port
    if port is None:
        port = 443 if scheme == "https" else 80
    out: Dict[str, Any] = {
        "http_proxy_host": parsed.hostname,
        "http_proxy_port": int(port),
        "proxy_type": "http",
    }
    if parsed.username is not None or parsed.password is not None:
        out["http_proxy_auth"] = (parsed.username or "", parsed.password or "")
    return out


# Backwards-compatible names (DataFeed / polymarket_api import these)
def requests_proxies_from_environ() -> Optional[Dict[str, str]]:
    return requests_proxies(None)


def websocket_proxy_kwargs_from_environ() -> Dict[str, Any]:
    return websocket_proxy_kwargs(None)
