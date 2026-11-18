from __future__ import annotations

from .contract import Event, parse_event
from .ingest import coverage
from .semantic import definition_for, measure, metric_source, month_range
from .storage import decode, encode, transaction


def apply_events(connection, events: list[Event]) -> int:
    changed = 0
    with transaction(connection):
        for event in events:
            if event.kind == "retract":
                connection.execute("DELETE FROM facts WHERE source=? AND business_key=?", (event.source, event.business_key))
            else:
                connection.execute(
                    "INSERT INTO facts(source,business_key,payload,source_batch,version) VALUES(?,?,?,?,1) "
                    "ON CONFLICT(source,business_key) DO UPDATE SET payload=excluded.payload,source_batch=excluded.source_batch,version=facts.version+1",
                    (event.source, event.business_key, encode({"value": event.value, "occurred_at": event.occurred_at}), event.batch_id),
                )
            changed += 1
    return changed


def facts(connection) -> list[dict]:
    return [dict(row) for row in connection.execute("SELECT source,business_key,payload,source_batch,version FROM facts ORDER BY source,business_key")]


def apply_metric_events(connection, metric: str, events: list[Event]) -> int:
    """把新到事件按发生期间适用的已激活定义计入 DWS；每条事实记录所用定义版本。
    无生效定义或字段缺失时保留不完整标记，等待补齐后重算。"""
    changed = 0
    with transaction(connection):
        for event in events:
            period = event.occurred_at[:7]
            if event.kind == "retract":
                connection.execute(
                    "DELETE FROM metric_facts WHERE metric=? AND source=? AND business_key=?",
                    (metric, event.source, event.business_key),
                )
            else:
                definition = definition_for(connection, metric, period)
                counted, status, reason, version = measure(definition, event)
                connection.execute(
                    "INSERT INTO metric_facts(metric,source,business_key,period,counted_value,raw_value,occurred_at,"
                    "metric_version,status,reason,source_batch,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,datetime('now')) "
                    "ON CONFLICT(metric,source,business_key,period) DO UPDATE SET counted_value=excluded.counted_value,"
                    "raw_value=excluded.raw_value,occurred_at=excluded.occurred_at,metric_version=excluded.metric_version,"
                    "status=excluded.status,reason=excluded.reason,source_batch=excluded.source_batch,updated_at=excluded.updated_at",
                    (metric, event.source, event.business_key, period, counted, event.value, event.occurred_at,
                     version, status, reason, event.batch_id),
                )
            changed += 1
    return changed


def metric_facts(connection, metric: str, period_from: str | None = None, period_to: str | None = None) -> list[dict]:
    sql = (
        "SELECT metric,source,business_key,period,counted_value,raw_value,occurred_at,metric_version,status,reason,"
        "source_batch,updated_at FROM metric_facts WHERE metric=?"
    )
    args: list = [metric]
    if period_from is not None:
        sql += " AND period>=?"
        args.append(period_from)
    if period_to is not None:
        sql += " AND period<=?"
        args.append(period_to)
    sql += " ORDER BY period,source,business_key"
    return [dict(row) for row in connection.execute(sql, args)]


def compute_periods(connection, metric: str, period_from: str, period_to: str) -> list[dict]:
    """按期间汇总 DWS 当前状态。数据不完整时状态为 pending，只给出 partial_value，
    绝不给出貌似最终的 value。"""
    source = metric_source(connection, metric)
    periods = []
    for period in month_range(period_from, period_to):
        applicable = definition_for(connection, metric, period)
        raw_count = coverage(connection, source, period)
        rows = [
            dict(row)
            for row in connection.execute(
                "SELECT counted_value,metric_version,status FROM metric_facts WHERE metric=? AND period=?",
                (metric, period),
            )
        ]
        reasons = []
        if raw_count == 0:
            reasons.append("raw_missing")
        if applicable is None:
            reasons.append("no_active_definition")
        if applicable is not None and any(row["metric_version"] != applicable.version for row in rows):
            reasons.append("stale_version")
        if any(row["status"] != "complete" for row in rows):
            reasons.append("facts_incomplete")
        if raw_count > 0 and not rows:
            reasons.append("awaiting_derivation")
        entry = {
            "period": period,
            "metric_version": applicable.version if applicable else None,
            "fact_count": len(rows),
            "raw_event_count": raw_count,
            "reasons": reasons,
        }
        if reasons:
            entry["status"] = "pending"
            usable = [
                row["counted_value"]
                for row in rows
                if row["status"] == "complete" and applicable is not None and row["metric_version"] == applicable.version
            ]
            entry["partial_value"] = sum(usable) if usable else None
        else:
            entry["status"] = "current"
            entry["value"] = sum(row["counted_value"] for row in rows)
        periods.append(entry)
    return periods


def recompute(connection, metric: str, period_from: str, period_to: str) -> dict:
    """按各期间当前适用的定义从 ODS 重放重算。冻结月份跳过不改写；
    原始事件缺失的期间保留现有事实并显式标注。"""
    source = metric_source(connection, metric)
    periods = month_range(period_from, period_to)
    wanted = set(periods)
    frozen = {}
    for row in connection.execute(
        "SELECT name,version,period_from,period_to FROM manifests WHERE metric=? AND status='frozen'", (metric,)
    ):
        for period in month_range(row["period_from"], row["period_to"]):
            if period in wanted:
                frozen[period] = {"name": row["name"], "version": row["version"]}
    events_by_period = {period: [] for period in periods}
    for row in connection.execute(
        "SELECT batch_id,cursor,payload FROM raw_batches WHERE source=? ORDER BY cursor", (source,)
    ):
        payload = decode(row["payload"])
        for record in payload["records"]:
            event = parse_event(source, row["batch_id"], row["cursor"], record)
            period = event.occurred_at[:7]
            if period in wanted:
                events_by_period[period].append(event)
    results = []
    for period in periods:
        if period in frozen:
            results.append({"period": period, "action": "frozen_skipped", "manifest": frozen[period]})
            continue
        events = events_by_period[period]
        if not events:
            results.append({"period": period, "action": "raw_missing", "detail": "原始事件缺失，保留现有事实"})
            continue
        connection.execute("DELETE FROM metric_facts WHERE metric=? AND period=?", (metric, period))
        apply_metric_events(connection, metric, events)
        rows = metric_facts(connection, metric, period, period)
        applicable = definition_for(connection, metric, period)
        results.append({
            "period": period,
            "action": "recomputed",
            "metric_version": applicable.version if applicable else None,
            "event_count": len(events),
            "fact_count": len(rows),
            "incomplete_count": sum(1 for row in rows if row["status"] != "complete"),
        })
    return {"metric": metric, "period_from": period_from, "period_to": period_to, "periods": results}
