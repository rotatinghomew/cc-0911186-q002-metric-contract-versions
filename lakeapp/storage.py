from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_batches (
    source TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (source, batch_id)
);
CREATE TABLE IF NOT EXISTS source_progress (
    source TEXT PRIMARY KEY,
    cursor INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS source_period_coverage (
    source TEXT NOT NULL,
    period TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    PRIMARY KEY (source, period)
);
CREATE TABLE IF NOT EXISTS facts (
    source TEXT NOT NULL,
    business_key TEXT NOT NULL,
    payload TEXT NOT NULL,
    source_batch TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (source, business_key)
);
CREATE TABLE IF NOT EXISTS metric_definitions (
    metric TEXT NOT NULL,
    version INTEGER NOT NULL,
    effective_from TEXT NOT NULL,
    effective_until TEXT,
    status TEXT NOT NULL,
    params TEXT NOT NULL,
    registered_at TEXT NOT NULL,
    activated_at TEXT,
    PRIMARY KEY (metric, version)
);
CREATE TABLE IF NOT EXISTS definition_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric TEXT NOT NULL,
    version INTEGER NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS metric_facts (
    metric TEXT NOT NULL,
    source TEXT NOT NULL,
    business_key TEXT NOT NULL,
    period TEXT NOT NULL,
    counted_value REAL,
    raw_value REAL NOT NULL,
    occurred_at TEXT NOT NULL,
    metric_version INTEGER,
    status TEXT NOT NULL,
    reason TEXT,
    source_batch TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (metric, source, business_key, period)
);
CREATE TABLE IF NOT EXISTS manifests (
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL,
    metric TEXT,
    period_from TEXT,
    period_to TEXT,
    frozen_at TEXT,
    PRIMARY KEY (name, version)
);
"""

# 既有数据库可能缺少后加的列，打开时补齐，已有数据保留。
MANIFEST_COLUMNS = {
    "metric": "TEXT",
    "period_from": "TEXT",
    "period_to": "TEXT",
    "frozen_at": "TEXT",
}


def open_store(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA)
    _ensure_columns(connection, "manifests", MANIFEST_COLUMNS)
    connection.commit()
    return connection


def _ensure_columns(connection: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}
    for name, ddl in columns.items():
        if name not in existing:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


@contextmanager
def transaction(connection: sqlite3.Connection):
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def encode(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode(value: str):
    return json.loads(value)
