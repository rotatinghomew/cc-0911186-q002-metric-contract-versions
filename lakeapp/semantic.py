from __future__ import annotations

import re
from dataclasses import dataclass

from .storage import decode, encode, transaction

# page_open: 按页面打开时长计入;active_segments: 只计连续互动片段。
MODES = {"page_open", "active_segments"}
PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
OPEN_END = "9999-12"


@dataclass(frozen=True)
class MetricDefinition:
    metric: str
    version: int
    effective_from: str
    effective_until: str | None
    status: str
    params: dict

    def covers(self, period: str) -> bool:
        return self.effective_from <= period and (self.effective_until is None or period < self.effective_until)


def check_period(period: str, field: str) -> None:
    if not isinstance(period, str) or not PERIOD_RE.match(period):
        raise ValueError(f"期间格式应为 YYYY-MM: {field}={period!r}")


def month_range(period_from: str, period_to: str) -> list[str]:
    check_period(period_from, "period_from")
    check_period(period_to, "period_to")
    if period_from > period_to:
        raise ValueError("期间范围无效")
    year, month = int(period_from[:4]), int(period_from[5:7])
    end_year, end_month = int(period_to[:4]), int(period_to[5:7])
    periods = []
    while (year, month) <= (end_year, end_month):
        periods.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return periods


def _to_definition(row) -> MetricDefinition:
    return MetricDefinition(
        row["metric"], row["version"], row["effective_from"], row["effective_until"], row["status"], decode(row["params"])
    )


def _history(connection, metric: str, version: int, action: str, detail: dict) -> None:
    connection.execute(
        "INSERT INTO definition_history(metric,version,action,detail,created_at) VALUES(?,?,?,?,datetime('now'))",
        (metric, version, action, encode(detail)),
    )


def register_definition(
    connection,
    metric: str,
    version: int,
    effective_from: str,
    effective_until: str | None = None,
    params: dict | None = None,
) -> MetricDefinition:
    """登记新口径，初始为草稿；草稿不参与任何计算，激活后才生效。"""
    params = dict(params or {})
    if not metric:
        raise ValueError("指标名不能为空")
    if int(version) < 1:
        raise ValueError("版本必须为正整数")
    check_period(effective_from, "effective_from")
    if effective_until is not None:
        check_period(effective_until, "effective_until")
        if effective_until <= effective_from:
            raise ValueError("effective_until 必须晚于 effective_from")
    if not params.get("source"):
        raise ValueError("定义缺少来源 source")
    if params.get("mode") not in MODES:
        raise ValueError("未知的计算方式: " + str(params.get("mode")))
    existing = connection.execute(
        "SELECT 1 FROM metric_definitions WHERE metric=? AND version=?", (metric, version)
    ).fetchone()
    if existing:
        raise ValueError("指标版本已存在")
    with transaction(connection):
        connection.execute(
            "INSERT INTO metric_definitions(metric,version,effective_from,effective_until,status,params,registered_at)"
            " VALUES(?,?,?,?,'draft',?,datetime('now'))",
            (metric, int(version), effective_from, effective_until, encode(params)),
        )
        _history(connection, metric, int(version), "register", {
            "effective_from": effective_from, "effective_until": effective_until, "params": params,
        })
    return MetricDefinition(metric, int(version), effective_from, effective_until, "draft", params)


def _overlaps(a: MetricDefinition, b: MetricDefinition) -> bool:
    return a.effective_from < (b.effective_until or OPEN_END) and b.effective_from < (a.effective_until or OPEN_END)


def activate_definition(connection, metric: str, version: int) -> MetricDefinition:
    """激活草稿版本。生效起点不得早于任何已激活版本（防止倒签改写历史口径）；
    被截断的旧版本自动收尾并写入历史。激活后仍与已激活版本重叠则拒绝。"""
    row = connection.execute(
        "SELECT * FROM metric_definitions WHERE metric=? AND version=?", (metric, version)
    ).fetchone()
    if not row:
        raise KeyError("指标版本不存在")
    if row["status"] != "draft":
        raise ValueError("仅草稿状态可激活")
    new = _to_definition(row)
    actives = [
        _to_definition(r)
        for r in connection.execute("SELECT * FROM metric_definitions WHERE metric=? AND status='active'", (metric,))
    ]
    for other in actives:
        if new.effective_from < other.effective_from:
            raise ValueError("新版本生效起点不能早于已激活版本")
    with transaction(connection):
        for other in actives:
            if not _overlaps(other, new):
                continue
            if other.effective_until is None or other.effective_until > new.effective_from:
                connection.execute(
                    "UPDATE metric_definitions SET effective_until=? WHERE metric=? AND version=?",
                    (new.effective_from, metric, other.version),
                )
                _history(connection, metric, other.version, "close_range", {
                    "effective_until": new.effective_from, "by_version": version,
                })
                other = MetricDefinition(other.metric, other.version, other.effective_from, new.effective_from, other.status, other.params)
            if _overlaps(other, new):
                raise ValueError("生效范围与已激活版本重叠")
        connection.execute(
            "UPDATE metric_definitions SET status='active', activated_at=datetime('now') WHERE metric=? AND version=?",
            (metric, version),
        )
        _history(connection, metric, version, "activate", {
            "effective_from": new.effective_from, "effective_until": new.effective_until,
        })
    return MetricDefinition(metric, version, new.effective_from, new.effective_until, "active", new.params)


def definition_for(connection, metric: str, period: str) -> MetricDefinition | None:
    """返回该期间当前生效（已激活）的定义；草稿与未生效版本不参与。"""
    check_period(period, "period")
    rows = connection.execute(
        "SELECT * FROM metric_definitions WHERE metric=? AND status='active'"
        " AND effective_from<=? AND (effective_until IS NULL OR effective_until>?)"
        " ORDER BY effective_from DESC",
        (metric, period, period),
    )
    for row in rows:
        definition = _to_definition(row)
        if definition.covers(period):
            return definition
    return None


def definitions(connection, metric: str | None = None) -> dict:
    """注册表与变更历史，供追溯每次定义的生效范围。"""
    sql = "SELECT * FROM metric_definitions"
    args: list = []
    if metric:
        sql += " WHERE metric=?"
        args.append(metric)
    sql += " ORDER BY metric,version"
    defs = [
        {
            "metric": row["metric"],
            "version": row["version"],
            "effective_from": row["effective_from"],
            "effective_until": row["effective_until"],
            "status": row["status"],
            "params": decode(row["params"]),
            "registered_at": row["registered_at"],
            "activated_at": row["activated_at"],
        }
        for row in connection.execute(sql, args)
    ]
    hsql = "SELECT * FROM definition_history"
    hargs: list = []
    if metric:
        hsql += " WHERE metric=?"
        hargs.append(metric)
    hsql += " ORDER BY id"
    history = [
        {
            "metric": row["metric"],
            "version": row["version"],
            "action": row["action"],
            "detail": decode(row["detail"]),
            "created_at": row["created_at"],
        }
        for row in connection.execute(hsql, hargs)
    ]
    return {"definitions": defs, "history": history}


def metric_source(connection, metric: str) -> str:
    for row in connection.execute("SELECT params FROM metric_definitions WHERE metric=? ORDER BY version", (metric,)):
        source = decode(row["params"]).get("source")
        if source:
            return source
    raise ValueError("指标未注册: " + metric)


def metrics_for_source(connection, source: str) -> list[str]:
    found: list[str] = []
    for row in connection.execute("SELECT metric,params FROM metric_definitions ORDER BY metric"):
        if decode(row["params"]).get("source") == source and row["metric"] not in found:
            found.append(row["metric"])
    return found


def measure(definition: MetricDefinition | None, event) -> tuple[float | None, str, str | None, int | None]:
    """按适用定义计算计入值。返回 (计入值, 状态, 原因, 定义版本)。
    无生效定义或事件缺少所需字段时不得编造数字，标记为不完整。"""
    if definition is None:
        return None, "incomplete", "no_active_definition", None
    mode = definition.params.get("mode")
    if mode == "page_open":
        return event.value, "complete", None, definition.version
    if mode == "active_segments":
        field = definition.params.get("field", "active_value")
        if field in event.attributes:
            return float(event.attributes[field]), "complete", None, definition.version
        return None, "incomplete", f"missing_field:{field}", definition.version
    raise ValueError("未知的计算方式: " + str(mode))
