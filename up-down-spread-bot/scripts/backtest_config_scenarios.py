#!/usr/bin/env python3
"""Estimate PnL under different config scenarios using trades.jsonl history."""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from strategy import side_entry_price_allowed, _parse_side_price_filters

DEFAULT_LOG = ROOT / "logs" / "late_v3_btc" / "trades.jsonl"

SCENARIOS: Dict[str, Dict[str, Any]] = {
    "旧配置(实际)": {
        "price_max": 0.92,
        "reverse_entry_enabled": True,
        "include_flip_reverse": True,
        "only_normal": False,
    },
    "0.4方案(无方向过滤)": {
        "price_max": 0.92,
        "min_confidence": 0.40,
        "reverse_entry_enabled": False,
        "include_flip_reverse": False,
        "only_normal": True,
        "flip_stop_price": 0.40,
        "simulate_flip_stop_pnl": True,
    },
    "数据优化(当前config)": {
        "price_max": 0.95,
        "min_confidence": 0.48,
        "reverse_entry_enabled": False,
        "include_flip_reverse": False,
        "only_normal": True,
        "flip_stop_price": 0.40,
        "simulate_flip_stop_pnl": True,
        "side_price_filters": {
            "UP": {"price_max": 0.95, "skip_ask_ranges": [[0.85, 0.90]]},
            "DOWN": {"price_max": 0.84, "skip_ask_ranges": [[0.70, 0.75], [0.85, 0.90]]},
        },
    },
    "中等(保留补仓)": {
        "price_max": 0.75,
        "reverse_entry_enabled": True,
        "reverse_entry_price": 0.30,
        "include_flip_reverse": True,
        "only_normal": False,
        "skip_reverse_if_normal_ask_above": 0.75,
        "filter_reverse_by_trigger": True,
    },
}

DEFAULT_SPREAD = 1.02


def estimated_confidence(token_ask: float, spread: float = DEFAULT_SPREAD) -> float:
    """Favorite-side ask → |up_ask - down_ask| when up+down≈spread."""
    return abs(2.0 * float(token_ask) - spread)


def simulated_stop_pnl(row: dict, stop_price: float) -> float:
    """Sell all contracts at stop_price when token hits threshold."""
    ask = float(row.get("token_ask") or 0)
    size = float(row.get("size_usd") or row.get("total_cost") or 5)
    if ask <= 0:
        return float(row.get("pnl") or 0)
    return round(size * (stop_price / ask - 1.0), 2)


def row_pnl(row: dict, cfg: Dict[str, Any]) -> float:
    actual = float(row.get("pnl") or row.get("pnl_usd") or 0)
    if not cfg.get("simulate_flip_stop_pnl"):
        return actual

    stop_px = float(cfg.get("flip_stop_price", 0.40))
    label = row.get("bet_result_label") or ""
    exit_reason = row.get("exit_reason") or ""

    if label == "押中":
        return actual
    if label in ("未中", "—") or exit_reason in ("flip_stop", "stop_loss", "settlement"):
        return simulated_stop_pnl(row, stop_px)
    return actual


def _parse_ts(row: dict) -> Optional[datetime]:
    ts = (row.get("entry_timestamp") or "")[:19]
    if not ts:
        return None
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def load_rows(path: Path, since: Optional[str] = None) -> List[dict]:
    rows: List[dict] = []
    start = datetime.strptime(since, "%Y-%m-%d %H:%M:%S") if since else None
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        dt = _parse_ts(row)
        if start and (not dt or dt < start):
            continue
        rows.append(row)
    return rows


def simulate(rows: List[dict], cfg: Dict[str, Any]) -> Dict[str, Any]:
    price_max = float(cfg.get("price_max", 0.92))
    min_conf = float(cfg.get("min_confidence", 0) or 0)
    include_rev = bool(cfg.get("include_flip_reverse", True))
    only_normal = bool(cfg.get("only_normal", False))
    rev_enabled = bool(cfg.get("reverse_entry_enabled", True))
    rev_trigger = float(cfg.get("reverse_entry_price", 0.40))
    skip_rev_if_normal_above = float(cfg.get("skip_reverse_if_normal_ask_above", 999))
    side_filters = _parse_side_price_filters(
        {"side_price_filters": cfg.get("side_price_filters") or {}},
        price_max,
    )

    kept: List[dict] = []
    skipped: List[dict] = []

    for row in rows:
        er = row.get("entry_reason") or "normal"
        ask = float(row.get("token_ask") or 0)

        if only_normal and er != "normal":
            skipped.append(row)
            continue

        if er == "normal" and ask > price_max:
            skipped.append(row)
            continue

        if er == "normal" and min_conf > 0:
            if estimated_confidence(ask) < min_conf:
                skipped.append(row)
                continue

        if er == "normal" and cfg.get("side_price_filters"):
            side = (row.get("bet_side") or "").upper()
            if not side_entry_price_allowed(
                side,
                ask,
                global_price_max=price_max,
                side_filters=side_filters,
            ):
                skipped.append(row)
                continue

        if er == "flip_reverse":
            if not include_rev or not rev_enabled:
                skipped.append(row)
                continue
            if cfg.get("filter_reverse_by_trigger"):
                slug = row.get("market_slug")
                normals = [
                    r
                    for r in rows
                    if r.get("market_slug") == slug
                    and (r.get("entry_reason") or "normal") == "normal"
                ]
                if normals:
                    n_ask = float(normals[0].get("token_ask") or 0)
                    if n_ask > skip_rev_if_normal_above:
                        skipped.append(row)
                        continue
                    if n_ask > rev_trigger:
                        skipped.append(row)
                        continue

        kept.append(row)

    pnl = sum(row_pnl(r, cfg) for r in kept)
    wins = sum(1 for r in kept if r.get("bet_result_label") == "押中")
    losses = sum(1 for r in kept if r.get("bet_result_label") == "未中")
    n = len(kept)

    actual_pnl = sum(float(r.get("pnl") or r.get("pnl_usd") or 0) for r in kept)

    return {
        "trades": n,
        "skipped": len(skipped),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / n * 100, 1) if n else 0.0,
        "pnl": round(pnl, 2),
        "actual_pnl": round(actual_pnl, 2),
        "avg_pnl": round(pnl / n, 2) if n else 0.0,
    }


def main() -> None:
    since = sys.argv[1] if len(sys.argv) > 1 else "2026-05-22 19:20:00"
    log_path = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_LOG
    if not log_path.is_file():
        print(f"Missing {log_path}")
        sys.exit(1)

    rows = load_rows(log_path, since=since)
    print(f"数据源: {log_path}")
    print(f"时间窗口: >= {since}")
    print(f"原始记录: {len(rows)} 笔\n")
    print(f"{'方案':<22} {'笔数':>5} {'跳过':>5} {'胜率':>7} {'模拟PnL':>10} {'原PnL':>10} {'均PnL':>8}")
    print("-" * 72)

    for name, cfg in SCENARIOS.items():
        s = simulate(rows, cfg)
        orig = s.get("actual_pnl", s["pnl"])
        print(
            f"{name:<22} {s['trades']:>5} {s['skipped']:>5} "
            f"{s['win_rate']:>6.1f}% {s['pnl']:>+10.2f} {orig:>+10.2f} {s['avg_pnl']:>+8.2f}"
        )

    print("\n说明:")
    print("- 数据优化: 关补仓 + conf>=0.48 + flip_stop@0.40 + UP/DOWN 方向价位过滤")
    print("- UP 跳过 ask 0.85-0.90; DOWN 跳过 0.70-0.75 与 0.85-0.90, DOWN max 0.84")
    print("- min_confidence 用 2*ask-spread 估算; 输单按 0.40 止损重算 PnL")


if __name__ == "__main__":
    main()
