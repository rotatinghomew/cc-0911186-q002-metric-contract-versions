from __future__ import annotations

from .derive import compute_periods
from .publish import frozen_manifest_for
from .semantic import metric_source, month_range
from .storage import decode


def metric_versions(connection, name: str) -> list[int]:
    rows = connection.execute("SELECT version FROM manifests WHERE name=? ORDER BY version", (name,))
    return [row[0] for row in rows]


def metric_report(connection, metric: str, period_from: str, period_to: str) -> dict:
    """按期间给出指标结果。每月状态三选一：frozen（冻结结果）、current（当前可用）、
    pending（等待补齐，只给 partial_value 与原因，不给貌似最终的数字）。"""
    metric_source(connection, metric)
    computed = {entry["period"]: entry for entry in compute_periods(connection, metric, period_from, period_to)}
    periods = []
    for period in month_range(period_from, period_to):
        frozen = frozen_manifest_for(connection, metric, period)
        if frozen:
            periods.append({
                "period": period,
                "status": "frozen",
                "value": frozen["value"],
                "metric_version": frozen["metric_version"],
                "manifest": {"name": frozen["name"], "version": frozen["version"]},
                "reasons": [],
            })
        else:
            periods.append(computed[period])
    status = "incomplete" if any(entry["status"] == "pending" for entry in periods) else "complete"
    return {"metric": metric, "period_from": period_from, "period_to": period_to, "status": status, "periods": periods}


def fact_lineage(connection, source: str, business_key: str, metric: str | None = None) -> dict:
    """从 ODS 到 ADS 追踪一条事实：原始批次、DWD 当前状态、DWS 计入值与定义版本、
    以及引用了它的 ADS 版本及其定义版本。"""
    ods = []
    for row in connection.execute(
        "SELECT batch_id,cursor,payload,received_at FROM raw_batches WHERE source=? ORDER BY cursor", (source,)
    ):
        payload = decode(row["payload"])
        matches = [record for record in payload["records"] if record.get("business_key") == business_key]
        if matches:
            ods.append({
                "batch_id": row["batch_id"],
                "cursor": row["cursor"],
                "received_at": row["received_at"],
                "record_count": len(matches),
                "periods": sorted({record["occurred_at"][:7] for record in matches if "occurred_at" in record}),
            })
    dwd = [
        {
            "source": row["source"],
            "business_key": row["business_key"],
            "payload": decode(row["payload"]),
            "source_batch": row["source_batch"],
            "version": row["version"],
        }
        for row in connection.execute("SELECT * FROM facts WHERE source=? AND business_key=?", (source, business_key))
    ]
    dws_sql = (
        "SELECT metric,period,counted_value,metric_version,status,reason,source_batch,updated_at"
        " FROM metric_facts WHERE source=? AND business_key=?"
    )
    dws_args: list = [source, business_key]
    if metric:
        dws_sql += " AND metric=?"
        dws_args.append(metric)
    dws_sql += " ORDER BY metric,period"
    dws = [dict(row) for row in connection.execute(dws_sql, dws_args)]
    ads = []
    for row in connection.execute("SELECT name,version,status,metric,period_from,period_to,payload FROM manifests ORDER BY name,version"):
        payload = decode(row["payload"])
        hits = [
            entry for entry in payload.get("rows", [])
            if entry.get("source") == source and entry.get("business_key") == business_key
        ]
        if hits:
            ads.append({
                "name": row["name"],
                "version": row["version"],
                "status": row["status"],
                "metric": row["metric"] or payload.get("metric"),
                "period_from": row["period_from"],
                "period_to": row["period_to"],
                "rows": [
                    {
                        "period": entry.get("period"),
                        "counted_value": entry.get("counted_value"),
                        "metric_version": entry.get("metric_version"),
                        "status": entry.get("status"),
                    }
                    for entry in hits
                ],
            })
    return {"source": source, "business_key": business_key, "ods": ods, "dwd": dwd, "dws": dws, "ads": ads}
