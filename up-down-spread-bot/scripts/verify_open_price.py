#!/usr/bin/env python3
"""Verify Gamma priceToBeat + RTDS fallback for open price locking."""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from main import load_config
from polymarket_api import chainlink_window_prices
from data_feed import DataFeed
from window_range_tracker import WindowRangeTracker


def _current_btc_slug(interval_sec: int = 300) -> str:
    slot = int(time.time()) // interval_sec * interval_sec
    return f"btc-updown-5m-{slot}"


def main() -> int:
    config, _config_path = load_config(str(ROOT / "config" / "config.json"))
    proxy = (
        config.get("data_sources", {}).get("polymarket", {}).get("http_proxy") or ""
    ).strip() or None
    iv = int(config.get("data_sources", {}).get("polymarket", {}).get("market_interval_sec", 300))

    slug = _current_btc_slug(iv)
    print(f"=== Open price verification ===")
    print(f"slug: {slug}")
    print(f"proxy: {proxy or '(none)'}")
    print()

    print("1) Gamma chainlink_window_prices:")
    cl = chainlink_window_prices(slug, timeout=8, proxy_url=proxy, use_cache=False)
    print(json.dumps(cl, indent=2))
    print()

    print("2) Chainlink RTDS (15s):")
    feed = DataFeed(config)
    feed.start()
    px = 0.0
    for _ in range(30):
        time.sleep(0.5)
        px = feed.get_chainlink_spot("btc")
        if px > 0:
            break
    print(f"   RTDS btc price: ${px:,.2f}" if px > 0 else "   RTDS btc price: 0 (check proxy / WS)")
    feed.stop()
    print()

    closed_slug = "btc-updown-5m-1779611100"
    print(f"4) Closed market Gamma reference ({closed_slug}):")
    cl_closed = chainlink_window_prices(
        closed_slug, timeout=8, proxy_url=proxy, use_cache=False
    )
    print(json.dumps(cl_closed, indent=2))
    print()

    print("3) Simulated fallback + range tracker:")
    tracker = WindowRangeTracker.from_config(config)
    open_px = float(cl.get("spot_start") or 0)
    source = "chainlink"
    if open_px <= 0 and px > 0:
        open_px = px
        source = "chainlink_rtds"
    if open_px <= 0:
        print("   FAIL: no Gamma ptb and no RTDS price — check RTDS WS / proxy")
        if float(cl_closed.get("spot_start") or 0) > 0:
            print("   (Gamma works for closed markets; live ptb may appear only after close)")
        return 1
    tracker.on_open_locked("btc", slug, open_px)
    ok, reason = tracker.entry_allowed("btc", slug, in_entry_window=True)
    snap = tracker.snapshot_for_state("btc", slug)
    print(f"   source: {source}")
    print(f"   open: ${open_px:,.2f}")
    print(f"   entry_allowed: {ok} ({reason})")
    print(f"   snapshot: {snap}")
    print()
    if ok or reason.startswith("range_"):
        print("OK: open locked — range filter can evaluate (may skip if amplitude low)")
        return 0
    print(f"FAIL: still blocked ({reason})")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
