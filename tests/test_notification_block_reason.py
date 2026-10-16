"""Synthetic decision -> report -> candidate regressions; no external operations."""
import copy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from notifications.block_reason import decision_block_reason, latest_block_reason
from application.runtime_report_summary import summarize_execution_cycle_result
from scripts.project_digest_candidates import project_digest_candidates
from scripts.runtime_daily_report_projection import project_daily_runtime
from test_runtime_daily_report_projection import report, TARGET, NOW
from test_nasdaq_dca_prefetched_market_history import _adapters


def decision(observed=6, limit=5):
    return SimpleNamespace(risk_flags=("rejected:circuit_breaker",), diagnostics={
        "risk_gate": "REJECT", "consecutive_losses": observed,
        "reason": f"连续亏损 {observed} 笔 > {limit} 熔断"})


def blocked_report(observed=6, limit=5):
    summary = summarize_execution_cycle_result(SimpleNamespace(
        execution={"execution_status": "blocked", "broker_submission_done": False,
                   "notification_block_reason": decision_block_reason(decision(observed, limit))},
        submitted_orders=(), trade_logs=()), dry_run=False)
    return report("risk_blocked", summary=summary)


def project(sources):
    return project_daily_runtime(target=TARGET, reports=sources, observed_at=NOW)


def candidate(sources):
    return project_digest_candidates(daily_projection=project(sources), notification_reports=sources)["runs"][0]


@pytest.mark.parametrize("observed,limit", [(6, 5), (13, 12), (1, 0)])
def test_values_come_from_completed_gate_and_totals_stay_unknown(observed, limit):
    sources = [blocked_report(observed, limit)]
    original = copy.deepcopy(sources)
    row = candidate(sources)
    assert row["block_reason"] == {"reason_code": "consecutive_loss_limit_exceeded",
        "observed_value": observed, "limit": limit, "order_submitted_this_run": False}
    assert row["order_count"] is None and row["fill_count"] is None
    assert sources == original


@pytest.mark.parametrize("change", [
    {"risk_gate": "APPROVE"}, {"consecutive_losses": True}, {"consecutive_losses": 7},
    {"reason": "private arbitrary text"}, {"reason": "连续亏损 5 笔 > 5 熔断"},
])
def test_unverified_or_malformed_reason_is_not_copied(change):
    value = decision()
    value.diagnostics.update(change)
    assert decision_block_reason(value) == {}


def test_plan_annotation_preserves_decision_mapper_arguments_and_existing_plan():
    value = decision()
    before = copy.deepcopy(value)
    original_plan = {"execution": {"execution_status": "blocked"},
                     "allocation": {"target_mode": "value"}, "account_hash": "synthetic"}
    mapper = Mock(return_value=copy.deepcopy(original_plan))
    adapters = replace(_adapters(), strategy_runtime=SimpleNamespace(
        evaluate=Mock(return_value=SimpleNamespace(decision=value, metadata={"marker": "synthetic"}))),
        map_strategy_decision_to_plan_fn=mapper)
    snapshot = object()
    plan = adapters.resolve_rebalance_plan(qqq_history={}, snapshot=snapshot)
    assert mapper.call_args.args == (value,)
    assert mapper.call_args.kwargs == {"snapshot": snapshot, "strategy_profile": adapters.strategy_profile,
        "runtime_metadata": {"marker": "synthetic", "schwab_execution_policy": {
            "reserved_cash_floor_usd": 0.0, "reserved_cash_ratio": 0.0, "cash_only_execution": True}}}
    assert plan["execution"].pop("notification_block_reason") == decision_block_reason(value)
    assert plan == original_plan and value == before


@pytest.mark.parametrize("change", [
    {"receipt_state": "invalid"}, {"receipt_outcome": "failed"},
    {"receipt_broker_confirmation": "not_observed"}, {"broker_submission_done": True},
    {"broker_submission_done": None}, {"orders_pending_count": 1},
    {"orders_pending_count": False}, {"errors_present": True}, {"action_done": True},
])
def test_submission_fact_requires_existing_valid_run_receipt(change):
    sources = [blocked_report()]
    daily = project(sources)
    daily["records"][0]["runs"][0]["evidence"].update(change)
    row = project_digest_candidates(daily_projection=daily, notification_reports=sources)["runs"][0]
    assert "block_reason" not in row
    assert row["order_count"] is None and row["fill_count"] is None


def test_old_report_shows_only_verified_run_fact_and_no_invented_limit():
    source = blocked_report()
    del source["summary"]["notification_block_reason"]
    assert candidate([source])["block_reason"] == {"reason_code": "strategy_risk_rejected",
        "observed_value": None, "limit": None, "order_submitted_this_run": False}


def test_later_submission_and_conflicting_copies_cannot_claim_no_submission():
    source = blocked_report()
    later = report("submitted", run_id="later-synthetic", started_at="2026-10-06T20:03:00Z",
        finished_at="2026-10-06T20:04:00Z", summary={"broker_submission_done": True})
    assert "block_reason" not in candidate([source, later])
    conflict = copy.deepcopy(source)
    conflict["summary"]["notification_block_reason"]["limit"] = 4
    # The existing daily receiver body remains byte-for-byte unchanged by copy.
    daily = project([source, conflict])
    assert "block_reason" not in project_digest_candidates(
        daily_projection=daily, notification_reports=[source, conflict])["runs"][0]
    record = project([source])["records"][0]
    record["conflicts"] = ["synthetic"]
    assert latest_block_reason(record, [source]) == {}


def test_notification_only_metadata_does_not_change_runtime_daily_contract():
    source = blocked_report()
    legacy = copy.deepcopy(source)
    del legacy["summary"]["notification_block_reason"]
    assert project([source]) == project([legacy])


def test_emitter_passes_only_reports_admitted_for_source_and_account(tmp_path, monkeypatch):
    from test_emit_digest_candidates import _valid_report_payload, _base_environ, _fact_reader, PREFIX, REVISION
    from scripts import emit_digest_candidates as emit
    from scripts.publish_runtime_daily_from_reports import ReadBatch
    from quant_platform_kit.common.execution_receipts import build_execution_receipt
    import datetime as dt
    import json

    source = _valid_report_payload()
    source["dry_run"] = False
    source["account_scope"] = "live"
    source["runtime_target"]["account_scope"] = "live"
    source["runtime_target"]["execution_mode"] = "live"
    source["runtime_release_receipt"] = blocked_report()["runtime_release_receipt"]
    source["summary"].update(blocked_report()["summary"])
    source["execution_receipt"] = build_execution_receipt(platform="schwab",
        strategy_profile=TARGET["strategy_profile"], strategy_revision="a" * 40,
        execution_mode="live", outcome="risk_blocked", observed_at=source["finished_at"])
    monkeypatch.setattr(emit.caller, "_select_identity", lambda _env: (
        "SYNTHETIC-HASH-NOT-A-REAL-ACCOUNT", {"enabled": True}))
    out = tmp_path / "candidate.json"
    env = _base_environ(out)
    env["SCHWAB_ACCOUNT_FACTS_SERVICE_NAME"] = TARGET["service"]
    batch = ReadBatch(entries=[{"payload": source, "object_uri": PREFIX + "2026-10/20261008T200100Z.json"}])
    result = emit.emit_digest_candidates(env, observed_at=dt.datetime(2026, 10, 8, 21, tzinfo=dt.timezone.utc),
        fact_reader=_fact_reader(), archive_reader=lambda **_kw: batch)
    assert result["status"] == "candidates_written"
    assert json.loads(out.read_text())["runs"][0]["block_reason"]["limit"] == 5


def test_unified_notification_projector_carries_same_run_facts():
    from notifications.digest_snapshot.digest_candidates import project_reports
    source = blocked_report()
    target = {**source["runtime_target"], "platform_id": "schwab", "service_name": TARGET["service"],
              "deployment_selector": "synthetic", "dry_run_only": False}
    source.update(platform="schwab", runtime_target=target)
    row = project_reports([source], expected_target=target, opaque_account_uid="synthetic",
        target_id="schwab/synthetic", now=NOW)["runs"][0]
    assert row["block_reason"]["order_submitted_this_run"] is False
    assert row["block_reason"]["observed_value"] == 6
    assert row["fill_count"] is None and row["order_count"] is None


def test_candidate_cli_works_outside_repo_without_pythonpath_or_install(tmp_path):
    import json
    import subprocess
    import sys
    from pathlib import Path

    source = tmp_path / "daily.json"
    output = tmp_path / "candidate.json"
    source.write_text(json.dumps(project([blocked_report()])))
    command = Path(__file__).resolve().parents[1] / "scripts/project_digest_candidates.py"
    result = subprocess.run([sys.executable, "-I", str(command), "--daily-projection", str(source),
                             "--output", str(output)], cwd=tmp_path, env={}, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    row = json.loads(output.read_text())["runs"][0]
    assert row["order_count"] is None and row["fill_count"] is None
