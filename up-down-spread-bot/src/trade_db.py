"""
MySQL persistence for trade records (optional).
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from trade_record import record_row_key


def _db_cfg(config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cfg = (config or {}).get("database") or {}
    return {
        "enabled": bool(cfg.get("enabled", False)),
        "host": os.getenv("MYSQL_HOST") or cfg.get("host") or "localhost",
        "port": int(os.getenv("MYSQL_PORT") or cfg.get("port") or 3306),
        "user": os.getenv("MYSQL_USER") or cfg.get("user") or "root",
        "password": os.getenv("MYSQL_PASSWORD") or cfg.get("password") or "",
        "database": os.getenv("MYSQL_DATABASE") or cfg.get("database") or "fy",
        "table": cfg.get("table") or "trade_records",
        "connect_timeout_sec": float(cfg.get("connect_timeout_sec") or 3),
    }


class TradeDatabase:
    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        self._cfg = _db_cfg(config)
        self._table = self._cfg["table"]
        import pymysql

        self._conn = pymysql.connect(
            host=self._cfg["host"],
            port=self._cfg["port"],
            user=self._cfg["user"],
            password=self._cfg["password"],
            database=self._cfg["database"],
            charset="utf8mb4",
            connect_timeout=int(self._cfg["connect_timeout_sec"]),
            autocommit=True,
        )
        self._ensure_table()

    def _ensure_table(self) -> None:
        sql = f"""
        CREATE TABLE IF NOT EXISTS `{self._table}` (
            record_key VARCHAR(255) NOT NULL,
            strategy_name VARCHAR(64) NOT NULL,
            market_slug VARCHAR(255) NOT NULL,
            coin VARCHAR(16) DEFAULT NULL,
            is_open TINYINT(1) NOT NULL DEFAULT 0,
            entry_time DOUBLE DEFAULT NULL,
            close_time DOUBLE DEFAULT NULL,
            payload JSON NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (record_key),
            KEY idx_strategy (strategy_name),
            KEY idx_slug (market_slug)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)

    def _row_to_record(self, payload: Any) -> Dict[str, Any]:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        if isinstance(payload, str):
            return json.loads(payload)
        if isinstance(payload, dict):
            return dict(payload)
        raise ValueError("invalid payload type")

    def load_records(
        self,
        *,
        strategy_name: str,
        limit: int = 5000,
        pending_only: bool = False,
    ) -> List[Dict[str, Any]]:
        clauses = ["strategy_name = %s"]
        params: List[Any] = [strategy_name]
        if pending_only:
            clauses.append(
                "(is_open = 1 OR JSON_EXTRACT(payload, '$.settlement_pending') = true)"
            )
        where = " AND ".join(clauses)
        lim = f" LIMIT {int(limit)}" if limit and limit > 0 else ""
        sql = (
            f"SELECT payload FROM `{self._table}` WHERE {where} "
            f"ORDER BY entry_time DESC{lim}"
        )
        rows: List[Dict[str, Any]] = []
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            for (payload,) in cur.fetchall():
                try:
                    rows.append(self._row_to_record(payload))
                except (json.JSONDecodeError, ValueError):
                    continue
        return rows

    def upsert_record(self, record: Dict[str, Any], *, strategy_name: str) -> None:
        row = dict(record)
        row.setdefault("strategy_name", strategy_name)
        key = record_row_key(row)
        row["record_key"] = key
        payload = json.dumps(row, ensure_ascii=False)
        sql = f"""
        INSERT INTO `{self._table}`
            (record_key, strategy_name, market_slug, coin, is_open, entry_time, close_time, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            strategy_name = VALUES(strategy_name),
            market_slug = VALUES(market_slug),
            coin = VALUES(coin),
            is_open = VALUES(is_open),
            entry_time = VALUES(entry_time),
            close_time = VALUES(close_time),
            payload = VALUES(payload)
        """
        with self._conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    key,
                    strategy_name,
                    str(row.get("market_slug") or ""),
                    str(row.get("coin") or "").lower() or None,
                    1 if row.get("is_open") else 0,
                    float(row.get("entry_time") or 0) or None,
                    float(row.get("close_time") or row.get("entry_time") or 0) or None,
                    payload,
                ),
            )


def get_trade_database(config: Optional[Dict[str, Any]] = None) -> Optional[TradeDatabase]:
    cfg = _db_cfg(config)
    if not cfg["enabled"]:
        raise RuntimeError("database.enabled=false")
    return TradeDatabase(config)
