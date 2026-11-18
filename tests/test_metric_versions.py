import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from lakeapp.cli import main as cli_main
from lakeapp.derive import apply_events, apply_metric_events, metric_facts, recompute
from lakeapp.ingest import receive
from lakeapp.publish import freeze, publish_metric
from lakeapp.query import fact_lineage, metric_report
from lakeapp.semantic import activate_definition, definition_for, definitions, register_definition
from lakeapp.storage import open_store

METRIC = "effective_learning_duration"


def learning_event(key, occurred_at, value, **attrs):
    record = {"business_key": key, "kind": "upsert", "value": value, "occurred_at": occurred_at}
    record.update(attrs)
    return record


class MetricVersionTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.connection = open_store(Path(self.tempdir.name) / "data.db")

    def tearDown(self):
        self.connection.close()
        self.tempdir.cleanup()

    def define(self, version, effective_from, mode, effective_until=None, field=None):
        params = {"source": "learning", "mode": mode}
        if field:
            params["field"] = field
        register_definition(self.connection, METRIC, version, effective_from, effective_until, params)

    def activate(self, version):
        return activate_definition(self.connection, METRIC, version)

    def feed(self, batch_id, cursor, records):
        events = receive(self.connection, "learning", batch_id, cursor, records)
        apply_events(self.connection, events)
        apply_metric_events(self.connection, METRIC, events)

    def test_activation_is_traceable_and_draft_stays_inactive(self):
        self.define(1, "2026-01", "page_open")
        self.activate(1)
        self.define(2, "2026-12", "active_segments")
        # 草稿不参与计算：2026-12 仍适用 v1
        self.assertEqual(definition_for(self.connection, METRIC, "2026-12").version, 1)
        self.activate(2)
        self.assertEqual(definition_for(self.connection, METRIC, "2026-11").version, 1)
        self.assertEqual(definition_for(self.connection, METRIC, "2026-12").version, 2)
        registry = definitions(self.connection, METRIC)
        v1 = next(d for d in registry["definitions"] if d["version"] == 1)
        self.assertEqual(v1["effective_until"], "2026-12")
        actions = [(h["version"], h["action"]) for h in registry["history"]]
        self.assertIn((1, "register"), actions)
        self.assertIn((1, "close_range"), actions)
        self.assertIn((2, "activate"), actions)

    def test_activation_rejects_backdated_version(self):
        self.define(1, "2026-01", "page_open")
        self.activate(1)
        self.define(2, "2025-06", "active_segments")
        with self.assertRaises(ValueError):
            self.activate(2)

    def test_new_events_use_applicable_definition(self):
        self.define(1, "2026-01", "page_open")
        self.activate(1)
        self.define(2, "2026-12", "active_segments")
        self.activate(2)
        self.feed("b1", 1, [learning_event("s1", "2026-11-10T09:00:00", 100)])
        self.feed("b2", 2, [learning_event("s2", "2026-12-01T09:00:00", 100, active_value=40)])
        self.feed("b3", 3, [learning_event("s3", "2026-12-02T09:00:00", 80)])
        rows = {row["business_key"]: row for row in metric_facts(self.connection, METRIC)}
        self.assertEqual((rows["s1"]["counted_value"], rows["s1"]["metric_version"]), (100.0, 1))
        self.assertEqual((rows["s2"]["counted_value"], rows["s2"]["metric_version"]), (40.0, 2))
        self.assertEqual(rows["s3"]["status"], "incomplete")
        self.assertEqual(rows["s3"]["reason"], "missing_field:active_value")
        report = metric_report(self.connection, METRIC, "2026-11", "2026-12")
        nov, dec = report["periods"]
        self.assertEqual((nov["status"], nov["value"]), ("current", 100.0))
        self.assertEqual(dec["status"], "pending")
        self.assertIn("facts_incomplete", dec["reasons"])
        self.assertEqual(dec["partial_value"], 40.0)
        self.assertNotIn("value", dec)
        self.assertEqual(report["status"], "incomplete")

    def test_draft_definition_not_mixed_into_current_month(self):
        self.define(1, "2026-01", "page_open")
        self.activate(1)
        # 新口径已登记但尚未正式生效，本月审核中的数据不得提前混入
        self.define(2, "2026-11", "active_segments")
        self.feed("b1", 1, [learning_event("s1", "2026-11-05T09:00:00", 60)])
        rows = metric_facts(self.connection, METRIC)
        self.assertEqual((rows[0]["counted_value"], rows[0]["metric_version"]), (60.0, 1))
        report = metric_report(self.connection, METRIC, "2026-11", "2026-11")
        self.assertEqual(report["periods"][0]["status"], "current")

    def test_recompute_historical_range_with_corresponding_version(self):
        self.define(1, "2026-01", "page_open")
        self.activate(1)
        self.feed("b1", 1, [learning_event("s1", "2026-10-05T09:00:00", 90, active_value=33)])
        # 换版追溯自 2026-10 生效，旧事实成为等待补齐的重算
        self.define(2, "2026-10", "active_segments")
        self.activate(2)
        report = metric_report(self.connection, METRIC, "2026-10", "2026-10")
        period = report["periods"][0]
        self.assertEqual(period["status"], "pending")
        self.assertIn("stale_version", period["reasons"])
        self.assertNotIn("value", period)
        result = recompute(self.connection, METRIC, "2026-10", "2026-10")
        self.assertEqual(result["periods"][0]["action"], "recomputed")
        self.assertEqual(result["periods"][0]["metric_version"], 2)
        report = metric_report(self.connection, METRIC, "2026-10", "2026-10")
        period = report["periods"][0]
        self.assertEqual((period["status"], period["value"], period["metric_version"]), ("current", 33.0, 2))

    def test_frozen_month_is_never_rewritten(self):
        self.define(1, "2026-01", "page_open")
        self.activate(1)
        self.feed("b1", 1, [learning_event("s1", "2026-09-03T09:00:00", 50)])
        self.feed("b2", 2, [learning_event("s2", "2026-10-02T09:00:00", 70)])
        outcome = publish_metric(self.connection, "monthly", 1, METRIC, "2026-09", "2026-09")
        self.assertEqual(outcome["status"], "ready")
        freeze(self.connection, "monthly", 1)
        # 换版追溯到冻结月份也不能改写冻结结果
        self.define(2, "2026-09", "active_segments")
        self.activate(2)
        result = recompute(self.connection, METRIC, "2026-09", "2026-10")
        actions = {entry["period"]: entry["action"] for entry in result["periods"]}
        self.assertEqual(actions["2026-09"], "frozen_skipped")
        self.assertEqual(actions["2026-10"], "recomputed")
        report = metric_report(self.connection, METRIC, "2026-09", "2026-10")
        sep, oct_ = report["periods"]
        self.assertEqual(sep["status"], "frozen")
        self.assertEqual((sep["value"], sep["metric_version"]), (50.0, 1))
        self.assertEqual(sep["manifest"], {"name": "monthly", "version": 1})
        self.assertEqual(oct_["status"], "pending")
        with self.assertRaises(ValueError):
            freeze(self.connection, "monthly", 1)
        with self.assertRaises(ValueError):
            publish_metric(self.connection, "monthly", 1, METRIC, "2026-09", "2026-09")

    def test_missing_raw_events_are_flagged_not_final(self):
        self.define(1, "2026-01", "page_open")
        self.activate(1)
        report = metric_report(self.connection, METRIC, "2026-08", "2026-08")
        period = report["periods"][0]
        self.assertEqual(period["status"], "pending")
        self.assertIn("raw_missing", period["reasons"])
        self.assertIsNone(period["partial_value"])
        self.assertNotIn("value", period)
        result = recompute(self.connection, METRIC, "2026-08", "2026-08")
        self.assertEqual(result["periods"][0]["action"], "raw_missing")

    def test_incomplete_report_cannot_be_frozen(self):
        self.define(1, "2026-01", "page_open")
        self.activate(1)
        self.define(2, "2026-11", "active_segments")
        self.activate(2)
        self.feed("b1", 1, [learning_event("s1", "2026-11-01T09:00:00", 80)])
        outcome = publish_metric(self.connection, "monthly", 1, METRIC, "2026-11", "2026-11")
        self.assertEqual(outcome["status"], "incomplete")
        with self.assertRaises(ValueError):
            freeze(self.connection, "monthly", 1)

    def test_lineage_traces_definition_version_from_ods_to_ads(self):
        self.define(1, "2026-01", "page_open")
        self.activate(1)
        self.define(2, "2026-12", "active_segments")
        self.activate(2)
        self.feed("b1", 1, [learning_event("s1", "2026-11-10T09:00:00", 100)])
        self.feed("b2", 2, [learning_event("s2", "2026-12-01T09:00:00", 100, active_value=40)])
        publish_metric(self.connection, "monthly", 1, METRIC, "2026-11", "2026-11")
        freeze(self.connection, "monthly", 1)
        lineage = fact_lineage(self.connection, "learning", "s2")
        self.assertEqual([b["batch_id"] for b in lineage["ods"]], ["b2"])
        self.assertEqual(lineage["dws"][0]["metric_version"], 2)
        self.assertEqual(lineage["dws"][0]["counted_value"], 40.0)
        lineage_nov = fact_lineage(self.connection, "learning", "s1")
        self.assertEqual(lineage_nov["dws"][0]["metric_version"], 1)
        self.assertEqual(lineage_nov["ads"][0]["status"], "frozen")
        self.assertEqual(lineage_nov["ads"][0]["rows"][0]["metric_version"], 1)


class CliTest(unittest.TestCase):
    def test_cli_define_receive_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "cli.db")
            cli_main([db, "define", json.dumps({
                "metric": METRIC, "version": 1, "effective_from": "2026-01",
                "source": "learning", "mode": "page_open",
            })])
            cli_main([db, "activate", json.dumps({"metric": METRIC, "version": 1})])
            out = io.StringIO()
            with redirect_stdout(out):
                cli_main([db, "receive", json.dumps({
                    "source": "learning", "batch_id": "b1", "cursor": 1,
                    "records": [learning_event("s1", "2026-11-01T08:00:00", 30)],
                })])
            self.assertEqual(json.loads(out.getvalue())["metric_facts"], 1)
            out = io.StringIO()
            with redirect_stdout(out):
                cli_main([db, "report", json.dumps({
                    "metric": METRIC, "period_from": "2026-11", "period_to": "2026-11",
                })])
            report = json.loads(out.getvalue())
            self.assertEqual(report["status"], "complete")
            self.assertEqual(report["periods"][0]["value"], 30.0)


if __name__ == "__main__":
    unittest.main()
