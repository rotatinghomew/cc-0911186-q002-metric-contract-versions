from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from .derive import compute_periods, metric_facts
from .storage import decode, encode, transaction


def publish(connection, name: str, version: int, rows: list[dict]) -> None:
    payload = {"rows": rows, "row_count": len(rows)}
    with transaction(connection):
        connection.execute(
            "INSERT INTO manifests(name,version,payload,status) VALUES(?,?,?,?)",
            (name, version, encode(payload), "ready"),
        )


def publish_metric(connection, name: str, version: int, metric: str, period_from: str, period_to: str) -> dict:
    """把指定期间范围快照为 ADS 版本。任何期间不完整时整版标记 incomplete，
    不完整版本不可冻结，避免被当作最终数字。已存在的 (name, version) 不可改写。"""
    periods = compute_periods(connection, metric, period_from, period_to)
    rows = metric_facts(connection, metric, period_from, period_to)
    status = "ready" if all(entry["status"] == "current" for entry in periods) else "incomplete"
    payload = {
        "metric": metric,
        "period_from": period_from,
        "period_to": period_to,
        "periods": periods,
        "rows": rows,
        "row_count": len(rows),
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    try:
        with transaction(connection):
            connection.execute(
                "INSERT INTO manifests(name,version,payload,status,metric,period_from,period_to) VALUES(?,?,?,?,?,?,?)",
                (name, version, encode(payload), status, metric, period_from, period_to),
            )
    except sqlite3.IntegrityError:
        raise ValueError("版本已存在，已发布或已冻结的内容不能被改写")
    return {"name": name, "version": version, "status": status, "row_count": len(rows)}


def freeze(connection, name: str, version: int) -> dict:
    """冻结为最终结果。只有数据完整的 ready 版本可冻结；冻结单向不可逆。"""
    row = connection.execute("SELECT status FROM manifests WHERE name=? AND version=?", (name, version)).fetchone()
    if not row:
        raise KeyError("版本不存在")
    if row["status"] == "frozen":
        raise ValueError("版本已冻结")
    if row["status"] != "ready":
        raise ValueError("数据尚不完整，不能冻结")
    with transaction(connection):
        connection.execute(
            "UPDATE manifests SET status='frozen', frozen_at=datetime('now') WHERE name=? AND version=?",
            (name, version),
        )
    return {"name": name, "version": version, "status": "frozen"}


def read_manifest(connection, name: str, version: int) -> dict:
    row = connection.execute(
        "SELECT payload,status FROM manifests WHERE name=? AND version=?", (name, version)
    ).fetchone()
    if not row:
        raise KeyError("版本不可读")
    payload = decode(row["payload"])
    payload["status"] = row["status"]
    return payload


def frozen_manifest_for(connection, metric: str, period: str) -> dict | None:
    """查找覆盖该指标该期间的冻结版本；冻结结果优先于任何重算。"""
    rows = connection.execute(
        "SELECT name,version,payload,frozen_at FROM manifests"
        " WHERE metric=? AND status='frozen' AND period_from<=? AND period_to>=?"
        " ORDER BY frozen_at DESC, version DESC",
        (metric, period, period),
    )
    for row in rows:
        payload = decode(row["payload"])
        for entry in payload.get("periods", []):
            if entry.get("period") == period:
                return {
                    "name": row["name"],
                    "version": row["version"],
                    "frozen_at": row["frozen_at"],
                    "metric_version": entry.get("metric_version"),
                    "value": entry.get("value"),
                }
    return None
