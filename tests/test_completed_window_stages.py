"""Completed live archive projection excludes identifiable non-execution stages."""
from copy import deepcopy
from datetime import datetime, timezone
import json

import pytest

from notifications import unified


def archive():
    target = dict(platform_id="schwab", strategy_profile="synthetic", service_name="synthetic-service",
                  account_scope="synthetic", account_selector=["synthetic"], deployment_selector="synthetic", dry_run_only=False)
    env = dict(RUNTIME_TARGET_ENABLED="true", RUNTIME_TARGET_JSON=json.dumps(target),
               EXECUTION_REPORT_GCS_URI="gs://synthetic/archive", DIGEST_CANDIDATES_GCS_URI="gs://synthetic/candidates",
               DIGEST_STORE_TARGET="synthetic", DIGEST_OPAQUE_ACCOUNT_UID="synthetic", DIGEST_TARGET_ID="synthetic")
    live = dict(schema_version="runtime_report.v1", platform="schwab", strategy_profile="synthetic",
                service_name="synthetic-service", runtime_target=target, dry_run=False, status="ok",
                run_id="live", started_at="2026-10-30T19:45:00+00:00", summary={"notification_digest": {
                    "schema_version": "qsl.notification_cycle.v1", "order_count": 0, "fill_count": None,
                    "routine": True, "dry_run": False}})
    preview = deepcopy(live)
    preview.update(run_id="preview", dry_run=True)
    preview["summary"]["notification_digest"].update(routine=False, dry_run=True)
    probe = deepcopy(live)
    probe.update(run_id="probe", summary={"account_observation": {}, "buying_power": 1, "total_equity": 1})
    return env, live, preview, probe


def export(env, reports, *, now=None, persist=False, **kwargs):
    def read(args):
        if args[0] == "ls":
            return [{"url": f"gs://synthetic/archive/charles_schwab/synthetic/2026-10/{i}.json",
                     "metadata": {"updated": r["started_at"]}} for i, r in enumerate(reports)]
        return deepcopy(reports[int(args[-1].rsplit("/", 1)[1].split(".")[0])])
    uploads = []
    unified.emit_completed_candidates(env, now=now or datetime(2026, 10, 31, 1, tzinfo=timezone.utc),
        read=read, upload=None if persist else lambda uri, body: uploads.append(json.loads(body)), **kwargs)
    return uploads


def test_live_window_with_preview_and_probes_covers_only_live_run():
    env, live, preview, probe = archive()
    uploads = export(env, [probe, preview, live])
    rows = uploads[0]["runs"]
    assert len(rows) == 1 and rows[0]["source_run_id"] == "live"
    assert rows[0]["cycle_count"] == 1 and rows[0]["fill_count"] is None


def test_missing_live_evidence_is_not_treated_as_a_probe():
    env, live, _, probe = archive()
    live["summary"] = {}
    rows = export(env, [probe, live])[0]["runs"]
    assert len(rows) == 1 and rows[0]["source_run_id"] == "live"
    assert rows[0]["status"] == "alert" and rows[0]["order_count"] is None


def test_excluded_preview_cannot_satisfy_required_live_coverage():
    env, live, preview, probe = archive()
    required = ("schwab", "synthetic", "synthetic", "synthetic", "preview")
    with pytest.raises(ValueError, match="notification_observed_run_not_covered"):
        export(env, [probe, preview, live], required_runs=[required])


def weekend(stage):
    env, live, preview, probe = archive()
    report = deepcopy({"probe": probe, "preview": preview}[stage])
    report["started_at"] = "2026-11-01T12:00:00+00:00"
    return env, report, datetime(2026, 11, 2, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize("stage", ["probe", "preview"])
def test_weekend_non_live_only_needs_schedule_evidence(stage):
    env, report, now = weekend(stage)
    with pytest.raises(ValueError, match="candidate_source_absent"):
        export(env, [report], now=now)


@pytest.mark.parametrize("stage", ["probe", "preview"])
@pytest.mark.parametrize("schedule", [{}, {"scheduler": {"main_time": "0 12 * * *", "timezone": "UTC"}}])
def test_empty_live_window_with_unknown_or_due_schedule_is_not_idle(stage, schedule):
    env, report, now = weekend(stage)
    env["DIGEST_NOTIFICATION_SCHEDULE_JSON"] = json.dumps(schedule)
    with pytest.raises(ValueError, match="candidate_due_source_absent"):
        export(env, [report], now=now)


@pytest.mark.parametrize("stage", ["probe", "preview"])
def test_weekend_not_due_is_target_evidence_without_a_run_or_zero_trades(stage):
    env, report, now = weekend(stage)
    env["DIGEST_NOTIFICATION_SCHEDULE_JSON"] = json.dumps({"scheduler": {
        "main_time": "45 15 * * 1-5", "timezone": "America/New_York"}})
    row = export(env, [report], now=now)[0]["runs"][0]
    assert row["actually_ran"] is False and row["scheduled_idle"] is True
    assert row["reason_code"] == "not_due" and row["business_day"] == "2026-11-01"
    assert not row.get("source_run_id") and not row.get("notification_events")
    assert row.get("order_count") is None and row.get("fill_count") is None


def test_market_closed_skip_keeps_real_source_and_unknown_trade_counts():
    env, live, _, _ = archive()
    live.update(status="skipped", summary={}, diagnostics={"skip_reason": "market_closed"})
    row = export(env, [live])[0]["runs"][0]
    assert row["actually_ran"] is False and row["cycle_count"] == 0
    assert row["source_run_id"] == "live" and row["reason_code"] == "market_closed"
    assert row["order_count"] is None and row["fill_count"] is None


def test_probe_shaped_report_with_errors_is_kept_as_alert():
    env, _, _, probe = archive()
    probe["error_summary"] = {"errors": ["synthetic probe error"]}
    row = export(env, [probe])[0]["runs"][0]
    assert row["source_run_id"] == "probe" and row["status"] == "alert"
    assert row["order_count"] is None and row["fill_count"] is None


@pytest.mark.parametrize("conflict", ["errors", "submitted", "pending"])
def test_market_closed_skip_with_conflicting_execution_evidence_is_rejected(conflict):
    env, live, _, _ = archive()
    live.update(status="skipped", summary={}, diagnostics={"skip_reason": "market_closed"})
    if conflict == "errors":
        live["error_summary"] = {"errors": ["synthetic"]}
    else:
        live["summary"]["orders_submitted_count" if conflict == "submitted" else "orders_pending_count"] = 1
    with pytest.raises(ValueError, match="candidate_non_execution_conflict"):
        export(env, [live])


def test_not_due_row_cannot_satisfy_required_live_run_before_persistence(monkeypatch):
    env, report, now = weekend("probe")
    env["DIGEST_NOTIFICATION_SCHEDULE_JSON"] = json.dumps({"scheduler": {
        "main_time": "45 15 * * 1-5", "timezone": "America/New_York"}})
    def forbidden(*args, **kwargs):
        pytest.fail("uncovered required run must stop before storage")
    monkeypatch.setattr(unified, "persist_projected_candidate", forbidden)
    required = ("schwab", "synthetic", "synthetic", "synthetic", "missing-live")
    with pytest.raises(ValueError, match="notification_observed_run_not_covered"):
        export(env, [report], now=now, persist=True, required_runs=[required])


@pytest.mark.parametrize("kind", ["idle", "market_closed"])
def test_non_execution_rows_survive_real_persistence_contract(monkeypatch, kind):
    from test_unified_notification_storage import fake_gcs
    objects, calls, _, _ = fake_gcs.__wrapped__(monkeypatch)
    env, report, now = weekend("probe")
    env["DIGEST_NOTIFICATION_SCHEDULE_JSON"] = json.dumps({"scheduler": {
        "main_time": "45 15 * * 1-5", "timezone": "America/New_York"}})
    if kind == "market_closed":
        report.update(status="skipped", summary={}, diagnostics={"skip_reason": "market_closed"})
    store = unified.GCSNotificationStore(env, client=object())
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda env=None: store)
    export(env, [report], now=now, persist=True)
    before = deepcopy(objects)
    export(env, [report], now=now, persist=True)
    assert objects == before and len(objects) == 1
    saved = next(json.loads(body) for _, body in objects.values())["runs"][0]
    assert saved["actually_ran"] is False
    assert not saved.get("source_run_id") if kind == "idle" else saved["source_run_id"] == report["run_id"]
    assert saved.get("order_count") is None and saved.get("fill_count") is None
    writes = [kw for method, _, kw in calls if method == "upload"]
    assert len(writes) == 1 and writes[0]["if_generation_match"] == 0 and writes[0]["retry"] is None


@pytest.mark.parametrize("kind", ["idle", "market_closed"])
def test_shared_qrs_snapshot_never_turns_non_execution_into_zero_fills(monkeypatch, kind):
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).parent / "qrs_snapshot"))
    monkeypatch.setenv("QRS_UNIFIED_NOTIFICATION_ENABLED", "true")
    import daily_digest_aggregator as aggregator
    import daily_digest_notify as digest
    import merge_digest_candidates as intake
    import notification_candidate_adapter as adapter

    env, report, now = weekend("probe")
    env["DIGEST_NOTIFICATION_SCHEDULE_JSON"] = json.dumps({"scheduler": {
        "main_time": "45 15 * * 1-5", "timezone": "America/New_York"}})
    if kind == "market_closed":
        report.update(status="skipped", summary={}, diagnostics={"skip_reason": "market_closed"})
    exported = export(env, [report], now=now)[0]
    row = exported["runs"][0]
    current, stale = adapter.partition(intake._rows(exported), row["business_day"])
    assert not stale
    expected = [{key: row[key] for key in adapter.IDENTITY[:4]}]
    runs, summary = adapter.aggregate(current, expected)
    assert runs == [] and summary["source_inventory"]["schwab"]["runs"] == 0
    assert summary["source_inventory"]["schwab"]["covered_targets"] == 1
    coverage = digest.EvidenceCoverage(candidates_loaded=True)
    payload = digest.DailyDigestInput(business_day=row["business_day"], runs=[],
        evidence_coverage=coverage, notification_summary=summary)
    collected = aggregator.CollectResult(runs=[], coverage=coverage, merged_rows=[], notification_summary=summary)
    receipt = aggregator.build_receipt(payload, collected, message_chars=0)
    assert receipt["run_count"] == 0
    assert receipt["total_fills"] is None and receipt["total_fills_status"] == "unknown"


def test_shared_qrs_patch_reverses_exactly_to_recorded_snapshot_base(tmp_path):
    import hashlib
    from pathlib import Path
    import subprocess
    snapshot = Path(__file__).parent / "qrs_snapshot"
    provenance = json.loads((snapshot / "provenance.json").read_text())
    metadata = provenance["files"]["notification_candidate_adapter.py"]
    patch = snapshot / metadata["patch"]
    assert hashlib.sha256(patch.read_bytes()).hexdigest() == metadata["patch_sha256"]
    target = tmp_path / "notification_candidate_adapter.py"
    target.write_bytes((snapshot / target.name).read_bytes())
    result = subprocess.run(["git", "apply", "--reverse", "-p3", str(patch.resolve())],
        cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0
    assert hashlib.sha256(target.read_bytes()).hexdigest() == metadata["base_sha256"]
