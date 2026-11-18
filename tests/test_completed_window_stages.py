"""Completed live archive projection excludes identifiable non-execution stages."""
from copy import deepcopy
from datetime import datetime, timezone
import json

import pytest

from charles_schwab_platform.notifications import unified


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


def _with_observation(target, report, positions, *, scope="strategy_symbols_only", complete=True):
    target["runtime_risk_limits"] = {"binding": {"account_hash": "synthetic-account-hash"}}
    report["runtime_target"] = deepcopy(target)
    report["finished_at"] = "2026-10-30T19:46:00+00:00"
    obs = {
        "account_hash": "synthetic-account-hash",
        "observed_at": "2026-10-30T19:45:30+00:00",
        "net_assets": "123.45",
        "net_assets_source": "liquidationValue",
        "net_assets_currency": "USD",
        "net_assets_currency_source": "owner_confirmed",
        "broker_reported_positions": positions,
        "broker_reported_positions_scope": scope,
        "broker_reported_positions_observed_at": "2026-10-30T19:45:30+00:00",
    }
    if complete is not None:
        obs["broker_reported_positions_complete"] = complete
    report["summary"]["account_observation"] = obs


def test_account_snapshots_supports_empty_account_positions():
    env, live, _, _ = archive()
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    _with_observation(target, live, [], scope="strategy_symbols_only", complete=True)
    snaps = unified._account_snapshots(unified._notification_report(live), target, datetime(2026, 11, 2, 2, tzinfo=timezone.utc))
    assert len(snaps) == 1
    assert snaps[0]["holdings"] == []
    assert snaps[0]["holdings_complete"] is True


def test_digest_archive_7day_carryover_on_scheduled_idle(monkeypatch):
    from datetime import timedelta
    from test_unified_notification_storage import fake_gcs
    from charles_schwab_platform.notifications.digest_snapshot.digest_archive import emit

    objects, _, _, _ = fake_gcs.__wrapped__(monkeypatch)
    env, live, _, _ = archive()
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    _with_observation(
        target,
        live,
        [{"symbol": "SOXL", "quantity": "3", "market_value": "90.00", "currency": "USD"}],
        scope="strategy_symbols_only",
        complete=True,
    )
    env["RUNTIME_TARGET_JSON"] = json.dumps(target)
    prior_started = datetime.fromisoformat(live["started_at"].replace("Z", "+00:00"))
    idle_now = prior_started + timedelta(days=2, hours=3)
    env["QSL_ROUTINE_NOTIFICATION_MODE"] = "daily_digest"
    env["DIGEST_NOTIFICATION_SCHEDULE_JSON"] = json.dumps({"scheduler": {
        "main_time": "45 15 * * 1-5", "timezone": "America/New_York"}})
    env["EXECUTION_REPORT_GCS_URI"] = "gs://synthetic-archive/reports"
    env["DIGEST_CANDIDATES_GCS_URI"] = "gs://synthetic-candidates/digest"
    prefix = f"reports/charles_schwab/synthetic/{prior_started:%Y-%m}/prior.json"
    objects[("synthetic-archive", prefix)] = json.dumps(live).encode()

    def read(args):
        if args[0] == "ls":
            return [{"url": f"gs://synthetic-archive/{prefix}",
                     "metadata": {"updated": live["finished_at"]}}]
        return json.loads(objects[("synthetic-archive", prefix)].decode())

    uploads = []
    res = emit(env, now=idle_now, read=read, upload=lambda *a: uploads.append(a))
    assert res["uploaded"] is True
    row = json.loads(uploads[0][1])["runs"][0]
    assert row["scheduled_idle"] is True
    assert row["actually_ran"] is False
    snap = row["account_daily_facts"]["snapshots"][0]
    assert snap["holdings"] == [{"symbol": "SOXL", "quantity": 3.0, "asset_type": "security"}]


def test_digest_archive_carryover_excludes_probe_observed_after_cutoff(monkeypatch):
    import quant_platform_kit.common.runtime_heartbeat_policy as policy
    from charles_schwab_platform.notifications.digest_snapshot.digest_archive import emit

    env, live, _, probe = archive()
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    _with_observation(target, probe, [], scope="strategy_symbols_only", complete=True)
    # Business day 2026-10-30 runs until 2026-10-30T22:00:00Z; probe starts before
    # cutoff at 21:59:50Z while its observation happens after cutoff at 22:00:10Z.
    probe["started_at"] = "2026-10-30T21:59:50+00:00"
    probe["finished_at"] = "2026-10-30T22:00:20+00:00"
    probe["summary"]["account_observation"]["observed_at"] = "2026-10-30T22:00:10+00:00"
    probe["summary"]["account_observation"]["broker_reported_positions_observed_at"] = "2026-10-30T22:00:10+00:00"
    env["RUNTIME_TARGET_JSON"] = json.dumps(target)
    env["QSL_ROUTINE_NOTIFICATION_MODE"] = "daily_digest"
    env["DIGEST_NOTIFICATION_SCHEDULE_JSON"] = json.dumps({"scheduler": {
        "main_time": "45 15 * * 1-5", "timezone": "America/New_York"}})
    env["EXECUTION_REPORT_GCS_URI"] = "gs://synthetic/archive"
    env["DIGEST_CANDIDATES_GCS_URI"] = "gs://synthetic/candidates"
    monkeypatch.setattr(policy, "filter_due_targets", lambda *a, **kw: ([], True))

    def read(args):
        if args[0] == "ls":
            return [{"url": "gs://synthetic/archive/charles_schwab/synthetic/2026-10/probe.json",
                     "metadata": {"updated": probe["finished_at"]}}]
        return deepcopy(probe)

    uploads = []
    emit(env, now=datetime(2026, 10, 31, 1, tzinfo=timezone.utc), read=read, upload=lambda *a: uploads.append(a))
    row = json.loads(uploads[0][1])["runs"][0]
    assert "account_daily_facts" not in row


def test_persist_projected_candidate_upgrades_legacy_snapshot_with_empty_holdings(monkeypatch):
    from test_unified_notification_storage import fake_gcs

    objects, _, _, _ = fake_gcs.__wrapped__(monkeypatch)
    env, live, _, _ = archive()
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    _with_observation(target, live, [], scope="strategy_symbols_only", complete=True)
    env["RUNTIME_TARGET_JSON"] = json.dumps(target)
    store = unified.GCSNotificationStore(env, client=object())
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda env=None: store)

    # First persist a legacy equity-only snapshot (without empty holdings metadata).
    payload = unified._project_runs([live], expected_target=target, opaque_account_uid="synthetic",
                                    target_id="synthetic", now=datetime(2026, 10, 31, 1, tzinfo=timezone.utc),
                                    archived_risk_blocks=set())
    legacy_payload = deepcopy(payload)
    legacy_payload["runs"][0]["account_daily_facts"]["snapshots"] = [
        {"as_of": "2026-10-30T19:45:30+00:00", "equity": 123.45, "equity_currency": "USD"}
    ]
    assert unified.persist_projected_candidate(legacy_payload, env=env, store=store) is True

    # Replaying the enriched empty-holdings projection upgrades cleanly without CandidateConflictError.
    assert unified.persist_projected_candidate(payload, env=env, store=store) is True
    saved = next(json.loads(body) for _, body in objects.values())["runs"][0]
    assert saved["account_daily_facts"]["snapshots"][0]["holdings"] == []
    assert saved["account_daily_facts"]["snapshots"][0]["holdings_complete"] is True

    # Real equity disagreements must still fail closed.
    conflicting = deepcopy(payload)
    conflicting["runs"][0]["account_daily_facts"]["snapshots"][0]["equity"] = 999.99
    with pytest.raises(unified.CandidateConflictError):
        unified.persist_projected_candidate(conflicting, env=env, store=store)


def test_digest_archive_carryover_prefers_latest_observation_time(monkeypatch):
    from datetime import timedelta
    from test_unified_notification_storage import fake_gcs
    from charles_schwab_platform.notifications.digest_snapshot.digest_archive import emit

    fake_gcs.__wrapped__(monkeypatch)
    env, older_report, _, newer_report = archive()
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    _with_observation(
        target,
        older_report,
        [{"symbol": "OLD", "quantity": "1", "market_value": "50.00", "currency": "USD"}],
        scope="strategy_symbols_only",
        complete=True,
    )
    _with_observation(
        target,
        newer_report,
        [{"symbol": "NEW", "quantity": "2", "market_value": "100.00", "currency": "USD"}],
        scope="strategy_symbols_only",
        complete=True,
    )
    older_started = datetime(2026, 10, 28, 19, 45, tzinfo=timezone.utc)
    newer_started = datetime(2026, 10, 29, 19, 45, tzinfo=timezone.utc)
    older_report["started_at"] = older_started.isoformat()
    older_report["finished_at"] = (older_started + timedelta(minutes=1)).isoformat()
    older_report["summary"]["account_observation"]["observed_at"] = (older_started + timedelta(seconds=30)).isoformat()
    older_report["summary"]["account_observation"]["broker_reported_positions_observed_at"] = (
        older_started + timedelta(seconds=30)
    ).isoformat()
    newer_report["started_at"] = newer_started.isoformat()
    newer_report["finished_at"] = (newer_started + timedelta(minutes=1)).isoformat()
    newer_report["summary"]["account_observation"]["observed_at"] = (newer_started + timedelta(seconds=30)).isoformat()
    newer_report["summary"]["account_observation"]["broker_reported_positions_observed_at"] = (
        newer_started + timedelta(seconds=30)
    ).isoformat()

    env["RUNTIME_TARGET_JSON"] = json.dumps(target)
    env["QSL_ROUTINE_NOTIFICATION_MODE"] = "daily_digest"
    env["DIGEST_NOTIFICATION_SCHEDULE_JSON"] = json.dumps({"scheduler": {
        "main_time": "45 15 * * 1-5", "timezone": "America/New_York"}})
    env["EXECUTION_REPORT_GCS_URI"] = "gs://synthetic-archive/reports"
    env["DIGEST_CANDIDATES_GCS_URI"] = "gs://synthetic-candidates/digest"
    idle_now = datetime(2026, 11, 1, 1, 0, tzinfo=timezone.utc)

    def read(args):
        if args[0] == "ls":
            return [
                {"url": "gs://synthetic-archive/reports/charles_schwab/synthetic/2026-10/older.json",
                 "metadata": {"updated": "2026-10-30T10:00:00Z"}},
                {"url": "gs://synthetic-archive/reports/charles_schwab/synthetic/2026-10/newer.json",
                 "metadata": {"updated": "2026-10-29T19:46:00Z"}},
            ]
        return deepcopy(older_report) if args[1].endswith("older.json") else deepcopy(newer_report)

    uploads = []
    res = emit(env, now=idle_now, read=read, upload=lambda *a: uploads.append(a))
    assert res["uploaded"] is True
    snap = json.loads(uploads[0][1])["runs"][0]["account_daily_facts"]["snapshots"][0]
    assert snap["holdings"] == [{"symbol": "NEW", "quantity": 2.0, "asset_type": "security"}]
