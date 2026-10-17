"""Real Schwab producer/export -> pinned QRS intake, merge, aggregate, render."""
import copy
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from application.runtime_report_summary import summarize_execution_cycle_result
from notifications import unified
from test_unified_notifications import MemoryStore

SNAPSHOT = Path(__file__).parent / "qrs_snapshot"
QRS_SHA = "160933eaa376bd4297616e8affc946430be69771"
EXPORT_AT = datetime(2026, 10, 16, 23, tzinfo=timezone.utc)
SOURCE_AT = EXPORT_AT - timedelta(hours=4)


@pytest.fixture
def producer(monkeypatch):
    target = dict(platform_id="schwab", strategy_profile="strategy-a", service_name="synthetic-service",
        account_scope="synthetic", account_selector=["synthetic"], deployment_selector="synthetic-slot", dry_run_only=False)
    env = {unified.FLAG: "true", "RUNTIME_TARGET_ENABLED": "true", "RUNTIME_TARGET_JSON": json.dumps(target),
        "DIGEST_STORE_TARGET": "synthetic-slot", "DIGEST_OPAQUE_ACCOUNT_UID": "opaque-a", "DIGEST_TARGET_ID": "target-a",
        "EXECUTION_REPORT_GCS_URI": "gs://synthetic-store/archive", "DIGEST_CANDIDATES_GCS_URI": "gs://synthetic-store/candidates"}
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("QSL_DIGEST_ROUTINE_ORDERS", raising=False)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return EXPORT_AT.astimezone(tz)
    monkeypatch.setattr(unified, "datetime", Clock)
    store, reports, uploads, sends = MemoryStore(), [], [], []
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda *a, **kw: store)

    def run(run_id, orders=()):
        source = dict(schema_version="runtime_report.v1", platform="schwab", strategy_profile="strategy-a",
            service_name="synthetic-service", runtime_target=copy.deepcopy(target), dry_run=False,
            status="ok", run_id=run_id, started_at=SOURCE_AT.isoformat())
        result = SimpleNamespace(execution=dict(execution_status="completed" if orders else "no_action",
            broker_submission_done=bool(orders)), submitted_orders=orders, trade_logs=())
        with unified.notification_context(source):
            unified.publish_cycle(result, config=SimpleNamespace(dry_run_only=False, strategy_profile="strategy-a"),
                legacy=lambda: sends.append(run_id) or True)
        source["summary"] = summarize_execution_cycle_result(result, dry_run=False)
        reports.append(source)
        return source

    def read(args):
        if args[0] == "ls":
            if "/notification-events/" in args[-1]:
                return [{"url": f"gs://synthetic-store/candidates/schwab/notification-events/2026-10-16/{key}.json"}
                    for key in store.facts]
            return [{"url": f"gs://synthetic-store/archive/schwab/strategy-a/2026-10/{i}.json",
                "metadata": {"updated": SOURCE_AT.isoformat()}} for i in range(len(reports))]
        key = args[-1].rsplit("/", 1)[-1].split(".")[0]
        if "/notification-events/" in args[-1]:
            return {"event": copy.deepcopy(store.facts[key][0])}
        return copy.deepcopy(reports[int(key)])

    real_export = unified.emit_completed_candidates
    def export(_env, **kwargs):
        return real_export(env, now=EXPORT_AT, read=read, upload=lambda uri, body: uploads.append(json.loads(body)), **kwargs)
    monkeypatch.setattr(unified, "emit_completed_candidates", export)
    return SimpleNamespace(run=run, export=export, store=store, reports=reports, uploads=uploads, sends=sends, env=env)


def qrs_roundtrip(monkeypatch, tmp_path, payloads):
    monkeypatch.syspath_prepend(str(SNAPSHOT))
    monkeypatch.setenv("QRS_UNIFIED_NOTIFICATION_ENABLED", "true")
    monkeypatch.setenv("DIGEST_CANDIDATES_STORE_STATUS", "loaded")
    expected = [dict(platform_id="schwab", strategy_profile="strategy-a", opaque_account_uid="opaque-a", target_id="target-a")]
    monkeypatch.setenv("DIGEST_EXPECTED_TARGETS_JSON", json.dumps(expected))
    import merge_digest_candidates as intake
    import notification_candidate_adapter as adapter
    import daily_digest_aggregator as aggregator
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    for i, payload in enumerate(payloads):
        (inputs / f"observation-{i}.json").write_text(json.dumps(payload))
    merged, stats = intake.merge(None, inputs)
    current, stale = adapter.partition(intake._rows(merged), "2026-10-16")
    assert not stale
    adapter.validate_expected(expected)
    runs, summary = adapter.aggregate(current, expected)
    merged_path = tmp_path / "merged.json"
    merged_path.write_text(json.dumps(merged))
    config = tmp_path / "platform-config.json"
    config.write_text(json.dumps({"notifications": {"quant_sentinel": {"daily_digest": {}}}, "platforms": {}}))
    payload, text, collected = aggregator.build_digest_payload(business_day="2026-10-16", locale="zh",
        candidates_path=merged_path, include_github=False, config_path=config)
    receipt = aggregator.build_receipt(payload, collected, message_chars=len(text))
    return runs, summary, text, receipt, stats


@pytest.mark.parametrize("with_order", [False, True])
def test_real_producer_qrs_roundtrip_two_runs_zero_orders_order_and_duplicate(monkeypatch, tmp_path, producer, with_order):
    first = producer.run("business-a")
    orders = ({"status": "filled", "broker_order_id": "synthetic-order",
        "fills": [{"execution_id": "synthetic-fill-a"}, {"execution_id": "synthetic-fill-b"}]},) if with_order else ()
    producer.run("business-b", orders)
    producer.reports.append(copy.deepcopy(first))  # repeated archive observation
    for _ in range(2):
        unified.publish_receipt(lambda: pytest.fail("healthy business receipt must be covered"),
            kind="healthy_heartbeat", reason="receipt_observed", source_report=first)
    output = producer.uploads[-1]
    assert len(output["runs"]) == 2
    assert {row["source_run_id"] for row in output["runs"]} == {"business-a", "business-b"}
    for row in output["runs"]:
        assert row["cycle_count"] == 1 and "source_run_ids" not in row
        assert all(fact["source_run_id"] == row["source_run_id"] for fact in row["notification_events"])
    runs, summary, text, receipt, stats = qrs_roundtrip(monkeypatch, tmp_path, [output, copy.deepcopy(output)])
    assert stats["files"] == 2
    assert len(runs) == receipt["run_count"] == summary["source_inventory"]["schwab"]["runs"] == 2
    assert summary["event_count"] == 3
    assert summary["counts_unknown"] is False and summary["counts_scope_unknown"] is False
    assert summary["source_inventory"]["schwab"]["covered_targets"] == 1
    assert sum(row["order_count"] for row in runs) == int(with_order)
    assert sum(row["fill_count"] for row in runs) == 2 * int(with_order)
    zero = next(row for row in runs if row["source_run_id"] == "business-a")
    assert zero["order_count"] == zero["fill_count"] == 0
    assert zero["field_status"]["order_count"] == "known"
    assert "全窗口统计未知" not in text and "覆盖不完整" not in text
    assert f"订单：{int(with_order)}" in text
    assert producer.sends == (["business-b"] if with_order else [])
    assert receipt["total_fills_status"] == "known"


def test_notification_snapshots_match_recorded_source_hashes():
    provenance = json.loads((SNAPSHOT / "provenance.json").read_text())
    assert provenance["source_commit"] == QRS_SHA
    for name, metadata in provenance["files"].items():
        assert hashlib.sha256((SNAPSHOT / name).read_bytes()).hexdigest() == metadata["sha256"]
    snapshot = Path(unified.__file__).parent / "digest_snapshot"
    provenance = json.loads((snapshot / "provenance.json").read_text())
    assert provenance["source_commit"] == "07e24c0c804a9b47316b6ec3c75bb63820d8f421"
    for name, metadata in provenance["files"].items():
        assert hashlib.sha256((snapshot / name).read_bytes()).hexdigest() == metadata["sha256"]


def test_observed_business_run_must_be_in_the_actual_export_before_heartbeat_deferral(producer):
    producer.run("business-a")
    with pytest.raises(ValueError, match="observed_run_not_covered"):
        producer.export(producer.env, required_runs=[("schwab", "opaque-a", "target-a", "strategy-a", "missing-business")])
    assert not producer.uploads


def test_real_zero_order_producer_renders_this_run_zero_not_unknown(monkeypatch, tmp_path, producer):
    producer.run("business-zero")
    assert producer.export(producer.env)["uploaded"] is True
    _, summary, text, receipt, _ = qrs_roundtrip(monkeypatch, tmp_path, producer.uploads)
    assert summary["source_inventory"]["schwab"]["runs"] == 1
    assert summary["counts_unknown"] is False
    assert "本轮 0 单" in text and "统计未知" not in text
    assert receipt["run_count"] == 1 and receipt["total_fills"] == 0
