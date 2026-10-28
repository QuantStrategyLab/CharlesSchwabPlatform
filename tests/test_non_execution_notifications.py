"""Admission-blocked HTTP calls remain non-runs; no broker or notification calls."""
import ast
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from notifications import unified
from test_unified_notification_storage import fake_gcs as _fake_gcs


@pytest.fixture
def candidate_store(monkeypatch):
    return _fake_gcs.__wrapped__(monkeypatch)

NOW = datetime(2026, 10, 28, 2, tzinfo=timezone.utc)
TARGET = dict(platform_id="schwab", strategy_profile="core", service_name="test-service",
              account_scope="test-scope", account_selector=["test-selector"],
              deployment_selector="test-slot", dry_run_only=False)


def skipped_report():
    return dict(schema_version="runtime_report.v1", platform="schwab", strategy_profile="core",
                service_name="test-service", runtime_target=deepcopy(TARGET), dry_run=False,
                run_id="blocked-request", started_at=NOW.isoformat(), status="skipped", summary={},
                diagnostics={"skip_reason": "runtime_execution_not_admitted"})


def project(report):
    return unified._project_runs([report], expected_target=TARGET, opaque_account_uid="opaque-test",
                                 target_id="target-test", now=NOW)


def test_admission_block_projects_explicit_non_run_with_unknown_counts():
    row = project(skipped_report())["runs"][0]
    assert row["actually_ran"] is False
    assert row["cycle_count"] == 0
    assert row["order_count"] is None and row["fill_count"] is None
    assert row["source_run_id"] == "blocked-request"


@pytest.mark.parametrize("change", ["dry_run", "reason", "counts", "target"])
def test_non_run_does_not_relax_source_or_execution_evidence(change):
    report = skipped_report()
    if change == "dry_run":
        report["dry_run"] = True
    elif change == "reason":
        report["diagnostics"]["skip_reason"] = "unverified"
    elif change == "counts":
        report["summary"]["notification_digest"] = {"order_count": 1}
    else:
        report["runtime_target"]["account_scope"] = "other"
    with pytest.raises(ValueError):
        project(report)


def test_non_run_is_generation_checked_and_idempotent(candidate_store):
    objects, calls, _, base = candidate_store
    env = {**base, "DIGEST_STORE_TARGET": "primary-live"}
    store = unified.GCSNotificationStore(env, client=object())
    payload = project(skipped_report())
    assert unified.persist_projected_candidate(payload, env=env, store=store)
    writes = sum(method == "upload" for method, _, _ in calls)
    assert unified.persist_projected_candidate(payload, env=env, store=store)
    assert sum(method == "upload" for method, _, _ in calls) == writes
    saved = next(json.loads(body) for _, body in objects.values())
    assert len(saved["runs"]) == 1 and saved["runs"][0]["actually_ran"] is False


def test_non_run_conflict_is_not_overwritten(candidate_store):
    _, _, _, base = candidate_store
    env = {**base, "DIGEST_STORE_TARGET": "primary-live"}
    store = unified.GCSNotificationStore(env, client=object())
    payload = project(skipped_report())
    assert unified.persist_projected_candidate(payload, env=env, store=store)
    changed = deepcopy(payload)
    changed["runs"][0].update(actually_ran=True, cycle_count=1)
    with pytest.raises(ValueError):
        unified.persist_projected_candidate(changed, env=env, store=store)


@pytest.mark.parametrize("enabled,persist_fails", [(False, False), (True, False), (True, True)])
def test_actual_early_guard_persists_only_when_enabled_and_never_builds_client(monkeypatch, enabled, persist_fails):
    source = Path(__file__).resolve().parents[1] / "main.py"
    node = next(x for x in ast.parse(source.read_text()).body if isinstance(x, ast.FunctionDef) and x.name == "_handle_schwab_cycle")
    calls = []
    monkeypatch.setattr(unified, "enabled", lambda: enabled)
    monkeypatch.setattr(unified, "persist_completed_report", lambda report: calls.append(("candidate", deepcopy(report))))

    def persist(report):
        calls.append(("archive", deepcopy(report)))
        if persist_fails:
            raise RuntimeError("private archive failure")
        return "synthetic-archive-ack"

    reporting = SimpleNamespace(build_log_context=lambda: object())
    namespace = {
        "RUNTIME_SETTINGS": SimpleNamespace(runtime_target_enabled=False),
        "build_composer": lambda: SimpleNamespace(build_reporting_adapters=lambda: reporting),
        "build_execution_report": lambda context: skipped_report(),
        "finalize_runtime_report": lambda report, **values: report.update(values),
        "_runtime_revision_diagnostics": lambda **values: values,
        "persist_execution_report": persist,
        "run_strategy_core": lambda *args, **kwargs: pytest.fail("guard called strategy"),
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), namespace)
    assert namespace["_handle_schwab_cycle"]() == ("Runtime Target Disabled", 200)
    assert [kind for kind, _ in calls] == ([] if not enabled else ["archive"] if persist_fails else ["archive", "candidate"])
    if enabled:
        assert calls[0][1]["status"] == "skipped"
