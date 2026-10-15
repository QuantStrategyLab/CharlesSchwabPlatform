from copy import deepcopy
from datetime import datetime, timezone
import pytest
from notifications.digest_snapshot.digest_candidates import cycle_evidence, project_reports

TARGET = dict(platform_id="synthetic", strategy_profile="core", service_name="test-service",
    account_scope="test-scope", account_selector=["test-selector"], deployment_selector="test-slot", dry_run_only=False)
NOW = datetime(2026, 10, 16, 2, tzinfo=timezone.utc)


def report(run_id="cycle-1", orders=()):
    return dict(schema_version="runtime_report.v1", platform="synthetic", strategy_profile="core",
        service_name="test-service", runtime_target=deepcopy(TARGET), dry_run=False, status="ok", run_id=run_id,
        started_at="2026-10-16T01:00:00Z", summary={"notification_digest": cycle_evidence(
            {"execution_status": "completed", "broker_submission_done": bool(orders)}, orders)})


def project(reports):
    return project_reports(reports, expected_target=TARGET, opaque_account_uid="synthetic-uid",
        target_id="synthetic-target", now=NOW)["runs"]


def test_all_cycles_aggregate_without_inventing_fills():
    source = [report(), report("cycle-2", [{"status": "filled"}])]
    original = deepcopy(source)
    row = project(source)[0]
    assert (row["order_count"], row["fill_count"], row["cycle_count"], row["status"]) == (1, 1, 2, "ok")
    assert source == original
    assert "account_scope" not in row


def test_missing_evidence_is_unknown_not_zero():
    source = report()
    source["summary"] = {}
    row = project([source])[0]
    assert row["fill_count"] is None and row["order_count"] is None and row["status"] == "alert"


@pytest.mark.parametrize("mutation", ["identity", "future", "duplicate", "error"])
def test_bad_source_stays_visible_or_rejects(mutation):
    source = report()
    if mutation == "error":
        source["status"] = "error"
        assert project([source])[0]["status"] == "alert"
        return
    if mutation == "identity": source["runtime_target"]["account_scope"] = "other"
    if mutation == "future": source["started_at"] = "2026-10-17T00:00:00Z"
    with pytest.raises(ValueError):
        project([source, deepcopy(source)] if mutation == "duplicate" else [source])


def test_archive_default_off_and_disabled_target_never_read_or_upload():
    from notifications.digest_snapshot.digest_archive import emit
    def forbidden(*args): raise AssertionError("unexpected I/O")
    assert emit({}, read=forbidden, upload=forbidden)["status"] == "disabled"
    assert emit({"QSL_ROUTINE_NOTIFICATION_MODE": "daily_digest", "RUNTIME_TARGET_ENABLED": "false"},
        read=forbidden, upload=forbidden)["status"] == "target_disabled"


def test_archive_aggregates_window_and_uploads_once_without_private_output():
    import json
    from notifications.digest_snapshot.digest_archive import emit
    env = dict(QSL_ROUTINE_NOTIFICATION_MODE="daily_digest", RUNTIME_TARGET_ENABLED="true",
        RUNTIME_TARGET_JSON=json.dumps(TARGET), DIGEST_STORE_TARGET="synthetic-slot",
        DIGEST_OPAQUE_ACCOUNT_UID="synthetic-uid", DIGEST_TARGET_ID="synthetic-target",
        EXECUTION_REPORT_GCS_URI="gs://synthetic-reports/archive",
        DIGEST_CANDIDATES_GCS_URI="gs://synthetic-reports/candidates")
    reports = [report("first", [{"status": "filled"}]), report("second")]
    for source in reports: source["started_at"] = "2026-10-15T20:00:00Z"
    def read(args):
        if args[0] == "ls":
            return [{"url": f"gs://synthetic-reports/archive/synthetic/core/2026-10/{i}.json",
                     "metadata": {"updated": "2026-10-15T20:01:00Z"}} for i in range(2)]
        return reports[int(args[1].rsplit("/", 1)[-1].split(".")[0])]
    uploads = []
    result = emit(env, now=NOW, read=read, upload=lambda *args: uploads.append(args))
    assert result["runs"] == 2 and len(uploads) == 1
    assert "/2026-10-15/" in uploads[0][0]
    row = json.loads(uploads[0][1])["runs"][0]
    assert (row["order_count"], row["fill_count"], row["cycle_count"]) == (1, 1, 2)
    calls = []
    def failed_upload(*args):
        calls.append(args)
        raise RuntimeError("unknown write outcome")
    with pytest.raises(RuntimeError): emit(env, now=NOW, read=read, upload=failed_upload)
    assert len(calls) == 1


def test_configured_closed_session_proves_idle_without_inventing_runs(monkeypatch):
    import json
    import quant_platform_kit.common.runtime_heartbeat_policy as policy
    from notifications.digest_snapshot.digest_archive import emit
    env = dict(QSL_ROUTINE_NOTIFICATION_MODE="daily_digest", RUNTIME_TARGET_ENABLED="true",
        RUNTIME_TARGET_JSON=json.dumps(TARGET), DIGEST_STORE_TARGET="synthetic-slot",
        DIGEST_OPAQUE_ACCOUNT_UID="synthetic-uid", DIGEST_TARGET_ID="synthetic-target",
        EXECUTION_REPORT_GCS_URI="gs://synthetic-reports/archive",
        DIGEST_CANDIDATES_GCS_URI="gs://synthetic-reports/candidates",
        DIGEST_NOTIFICATION_SCHEDULE_JSON=json.dumps({"scheduler": {"main_time": "0 20 * * 1-5", "timezone": "UTC"}}))
    monkeypatch.setattr(policy, "filter_due_targets", lambda *args, **kwargs: ([], True))
    uploads = []
    emit(env, now=NOW, read=lambda args: [], upload=lambda *args: uploads.append(args))
    row = json.loads(uploads[0][1])["runs"][0]
    assert row["actually_ran"] is False and "order_count" not in row and "fill_count" not in row
    monkeypatch.setattr(policy, "filter_due_targets", lambda *args, **kwargs: ([{}], True))
    with pytest.raises(ValueError, match="candidate_due_source_absent"):
        emit(env, now=NOW, read=lambda args: [], upload=lambda *args: uploads.append(args))
    assert len(uploads) == 1


def test_multi_target_bindings_use_each_explicit_enablement_without_a_global_target():
    import json
    from notifications.digest_snapshot.digest_archive import emit
    result = emit({"QSL_ROUTINE_NOTIFICATION_MODE": "daily_digest",
        "DIGEST_TARGET_BINDINGS_JSON": json.dumps([{"RUNTIME_TARGET_ENABLED": "false"}])},
        read=lambda args: pytest.fail("disabled target must not be read"),
        upload=lambda *args: pytest.fail("disabled target must not be uploaded"))
    assert result == {"status": "projected", "uploaded": False, "targets": 1}
