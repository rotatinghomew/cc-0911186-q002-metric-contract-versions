from __future__ import annotations

import argparse
import json

from .derive import apply_events, apply_metric_events, facts, metric_facts, recompute
from .ingest import progress, receive
from .publish import freeze, publish, publish_metric, read_manifest
from .query import fact_lineage, metric_report
from .semantic import activate_definition, definitions, metrics_for_source, register_definition
from .storage import open_store

ACTIONS = [
    "receive", "facts", "progress", "publish", "read",
    "define", "activate", "definitions", "metric-facts", "recompute",
    "report", "publish-report", "freeze", "lineage",
]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="本地分层数据服务")
    parser.add_argument("database")
    parser.add_argument("action", choices=ACTIONS)
    parser.add_argument("payload", nargs="?")
    args = parser.parse_args(argv)
    connection = open_store(args.database)
    try:
        if args.action == "receive":
            data = json.loads(args.payload or "{}")
            events = receive(connection, data["source"], data["batch_id"], data["cursor"], data["records"])
            changed = apply_events(connection, events)
            derived = 0
            for metric in metrics_for_source(connection, data["source"]):
                derived += apply_metric_events(connection, metric, events)
            print(json.dumps({"received": len(events), "changed": changed, "metric_facts": derived}, ensure_ascii=False))
        elif args.action == "facts":
            print(json.dumps(facts(connection), ensure_ascii=False))
        elif args.action == "progress":
            print(json.dumps(progress(connection), ensure_ascii=False))
        elif args.action == "publish":
            data = json.loads(args.payload or "{}")
            publish(connection, data["name"], data["version"], facts(connection))
            print("ok")
        elif args.action == "read":
            data = json.loads(args.payload or "{}")
            print(json.dumps(read_manifest(connection, data["name"], data["version"]), ensure_ascii=False))
        elif args.action == "define":
            data = json.loads(args.payload or "{}")
            params = data.get("params") or {key: data[key] for key in ("source", "mode", "field") if key in data}
            definition = register_definition(
                connection, data["metric"], data["version"], data["effective_from"], data.get("effective_until"), params
            )
            print(json.dumps(definition.__dict__, ensure_ascii=False))
        elif args.action == "activate":
            data = json.loads(args.payload or "{}")
            definition = activate_definition(connection, data["metric"], data["version"])
            print(json.dumps(definition.__dict__, ensure_ascii=False))
        elif args.action == "definitions":
            data = json.loads(args.payload or "{}")
            print(json.dumps(definitions(connection, data.get("metric")), ensure_ascii=False))
        elif args.action == "metric-facts":
            data = json.loads(args.payload or "{}")
            print(json.dumps(metric_facts(connection, data["metric"], data.get("period_from"), data.get("period_to")), ensure_ascii=False))
        elif args.action == "recompute":
            data = json.loads(args.payload or "{}")
            print(json.dumps(recompute(connection, data["metric"], data["period_from"], data["period_to"]), ensure_ascii=False))
        elif args.action == "report":
            data = json.loads(args.payload or "{}")
            print(json.dumps(metric_report(connection, data["metric"], data["period_from"], data["period_to"]), ensure_ascii=False))
        elif args.action == "publish-report":
            data = json.loads(args.payload or "{}")
            print(json.dumps(
                publish_metric(connection, data["name"], data["version"], data["metric"], data["period_from"], data["period_to"]),
                ensure_ascii=False,
            ))
        elif args.action == "freeze":
            data = json.loads(args.payload or "{}")
            print(json.dumps(freeze(connection, data["name"], data["version"]), ensure_ascii=False))
        else:
            data = json.loads(args.payload or "{}")
            print(json.dumps(fact_lineage(connection, data["source"], data["business_key"], data.get("metric")), ensure_ascii=False))
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
