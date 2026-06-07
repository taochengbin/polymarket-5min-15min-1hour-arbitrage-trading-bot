"""
Trade record merge + persistence helpers (MySQL + optional jsonl backup).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from trade_record import record_row_key


def _db_section(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return (config or {}).get("database") or {}


def db_reads_enabled(config: Optional[Dict[str, Any]]) -> bool:
    sec = _db_section(config)
    return bool(sec.get("enabled")) and bool(sec.get("read_from_db", True))


def jsonl_backup_enabled(config: Optional[Dict[str, Any]]) -> bool:
    sec = _db_section(config)
    if not sec.get("enabled"):
        return True
    return bool(sec.get("jsonl_backup", False))


def _row_sort_ts(row: Dict[str, Any]) -> float:
    try:
        return float(row.get("close_time") or row.get("entry_time") or 0)
    except (TypeError, ValueError):
        return 0.0


def merge_trade_records(
    db_rows: List[Dict[str, Any]],
    jsonl_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    by_key: Dict[str, Dict[str, Any]] = {}
    for row in db_rows + jsonl_rows:
        if not row.get("market_slug"):
            continue
        key = record_row_key(row)
        prev = by_key.get(key)
        if prev is None or _row_sort_ts(row) >= _row_sort_ts(prev):
            by_key[key] = dict(row)
    merged = list(by_key.values())
    merged.sort(key=_row_sort_ts, reverse=True)
    return merged


def apply_merged_records_to_trader(trader: Any, merged: List[Dict[str, Any]]) -> None:
    trader.open_trade_records.clear()
    trader.closed_trades.clear()
    for row in merged:
        rec = dict(row)
        key = record_row_key(rec)
        rec["record_key"] = key
        if rec.get("is_open"):
            trader.open_trade_records[key] = rec
        else:
            trader.closed_trades.append(rec)


def _existing_db_keys(db_rows: List[Dict[str, Any]]) -> Set[str]:
    return {record_row_key(r) for r in db_rows if r.get("market_slug")}


def sync_missing_records_to_db(
    trader: Any,
    db_rows: List[Dict[str, Any]],
    merged: List[Dict[str, Any]],
) -> int:
    db = getattr(trader, "_trade_db", None)
    if not db:
        return 0
    have = _existing_db_keys(db_rows)
    synced = 0
    for row in merged:
        key = record_row_key(row)
        if key in have:
            continue
        try:
            db.upsert_record(row, strategy_name=trader.strategy_name)
            synced += 1
        except Exception:
            continue
    return synced


def sync_trader_records_to_db(trader: Any, rows: List[Dict[str, Any]]) -> None:
    db = getattr(trader, "_trade_db", None)
    if not db or not rows:
        return
    for row in rows:
        db.upsert_record(dict(row), strategy_name=trader.strategy_name)


def _load_jsonl(path: Path, *, pending_only: bool, limit: int) -> List[Dict[str, Any]]:
    if not path.is_file():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if pending_only and not row.get("is_open") and not row.get(
                    "settlement_pending"
                ):
                    continue
                if row.get("market_slug"):
                    rows.append(row)
    except OSError:
        return []
    rows.sort(key=_row_sort_ts, reverse=True)
    if limit and limit > 0:
        rows = rows[:limit]
    return rows


def load_persisted_records(
    trader: Any,
    *,
    limit: int = 0,
    config: Optional[Dict[str, Any]] = None,
    pending_only: bool = False,
) -> List[Dict[str, Any]]:
    cfg = config or getattr(trader, "config", None)
    db = getattr(trader, "_trade_db", None)
    if db_reads_enabled(cfg) and db:
        try:
            return db.load_records(
                strategy_name=trader.strategy_name,
                limit=limit,
                pending_only=pending_only,
            )
        except Exception:
            pass
    path = Path(getattr(trader, "trades_file", "") or "")
    return _load_jsonl(path, pending_only=pending_only, limit=limit)


def load_records_for_strategies(
    config: Dict[str, Any],
    strategy_names: List[str],
) -> List[Dict[str, Any]]:
    if not db_reads_enabled(config):
        return []
    try:
        from trade_db import get_trade_database

        db = get_trade_database(config)
    except Exception:
        return []
    rows: List[Dict[str, Any]] = []
    for name in strategy_names:
        try:
            rows.extend(db.load_records(strategy_name=name, limit=5000))
        except Exception:
            continue
    rows.sort(key=_row_sort_ts, reverse=True)
    return rows
