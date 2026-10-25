"""Offline fault acceptance: real Schwab seams, synthetic broker boundary only."""

from __future__ import annotations

import json

import pytest

from application.offline_fault_acceptance import (
    RESPONSE_LOSS_MARKER_KEY,
    run_offline_fault_acceptance,
)

_BASE = "fba4896d21efbf5b9c2bdf0db75823150e1427af"
_IDS = (
    "accepted_submit_response_loss",
    "partial_fill_cancel_late_duplicate_reports",
    "restart_around_claim",
    "query_429_disconnect_unknown",
    "same_boxx_multi_owner",
    "risk_pause",
)
_CLASSIFICATIONS = {"supported", "verified-safe-reject", "unsupported"}


@pytest.fixture
def acceptance(tmp_path, monkeypatch):
    monkeypatch.setattr("application.runtime_broker_adapters.time.sleep", lambda _seconds: None)
    return run_offline_fault_acceptance(tmp_path)


def _scenario(result, scenario_id):
    matches = [item for item in result["scenarios"] if item["scenario_id"] == scenario_id]
    assert len(matches) == 1
    return matches[0]


def test_aggregate_is_serializable_offline_research_result(acceptance):
    json.dumps(acceptance)
    assert acceptance["schema_version"] == "schwab_offline_fault_acceptance.v1"
    assert acceptance["base_commit"] == _BASE
    assert acceptance["research_only"] is True
    assert acceptance["offline"] is True
    assert acceptance["no_account_connection"] is True
    assert [item["scenario_id"] for item in acceptance["scenarios"]] == list(_IDS)
    assert {item["classification"] for item in acceptance["scenarios"]} <= _CLASSIFICATIONS


def test_each_scenario_names_a_real_call_path(acceptance):
    for scenario in acceptance["scenarios"]:
        assert scenario["classification"] in _CLASSIFICATIONS
        assert scenario["call_path"]
        assert all(path.startswith("application.") or path.startswith("quant_platform_kit.") for path in scenario["call_path"])
        assert scenario["limitations"]


def test_response_loss_is_safe_reject_without_fabricated_order_identity(acceptance, tmp_path):
    scenario = _scenario(acceptance, "accepted_submit_response_loss")
    assertions = scenario["assertions"]
    assert scenario["classification"] == "verified-safe-reject"
    assert "application.execution_claim.claim_execution_marker" in scenario["call_path"]
    assert "application.execution_service.execute_rebalance_cycle" in scenario["call_path"]
    assert assertions["first_submit_count"] == 1
    assert assertions["second_submit_count"] == 0
    assert assertions["broker_order_id"] is None
    assert assertions["order_status_query_count"] == 0
    assert assertions["execution_status"] == "pending_reconciliation"
    assert assertions["broker_submission_done"] is False
    payloads = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in tmp_path.rglob("*.json")
    ]
    claim_payloads = [
        payload for payload in payloads if payload.get("schema_version") == "execution_claim.v1"
    ]
    assert claim_payloads
    assert any(payload.get("marker_key") == RESPONSE_LOSS_MARKER_KEY for payload in claim_payloads)
    assert all(payload["state"] == "claimed" for payload in claim_payloads)
    assert all("ttl" not in payload and "expires_at" not in payload for payload in claim_payloads)


def test_response_loss_follows_claim_seam_instead_of_hardcoded_dedup(tmp_path, monkeypatch):
    from application import offline_fault_acceptance as mod

    real_claim = mod.claim_execution_marker
    seen = []

    def claim(store, marker_key, **kwargs):
        if marker_key == RESPONSE_LOSS_MARKER_KEY:
            seen.append(marker_key)
            if len(seen) >= 2:
                return True
        return real_claim(store, marker_key, **kwargs)

    monkeypatch.setattr(mod, "claim_execution_marker", claim)
    monkeypatch.setattr("application.runtime_broker_adapters.time.sleep", lambda _seconds: None)
    result = mod.run_offline_fault_acceptance(tmp_path)
    scenario = _scenario(result, "accepted_submit_response_loss")
    assert len(seen) >= 2
    assert scenario["assertions"]["second_submit_count"] == 1
    assert scenario["classification"] == "unsupported"


def test_partial_fill_cancel_and_duplicate_reports_are_unsupported(acceptance):
    scenario = _scenario(acceptance, "partial_fill_cancel_late_duplicate_reports")
    assertions = scenario["assertions"]
    assert scenario["classification"] == "unsupported"
    assert "application.broker_reconciliation.collect_read_only_reconciliation_observations" in scenario["call_path"]
    assert assertions["recent_executions_complete"] is False
    assert assertions["recent_executions_count"] == 0
    assert "order_fill_quantity_is_not_a_timestamped_execution_stream" in assertions["reason_codes"]
    assert assertions["cancel_request_status"] == "PENDING_CANCEL"
    assert assertions["cancel_request_remains_open"] is True
    assert assertions["duplicate_open_order_count"] >= 2
    assert assertions["exactly_once_fill_proven"] is False
    assert assertions["fee_exactly_once_proven"] is False
    assert assertions["normalized_order_has_fee"] is False


def test_restart_reconciles_before_continue_and_does_not_expire_claim(acceptance):
    scenario = _scenario(acceptance, "restart_around_claim")
    assertions = scenario["assertions"]
    assert scenario["classification"] == "verified-safe-reject"
    assert scenario["call_path"].index(
        "application.execution_claim.claim_execution_marker"
    ) < scenario["call_path"].index(
        "application.broker_reconciliation.collect_read_only_reconciliation_observations"
    )
    assert assertions["restarted_claim_acquired"] is False
    assert assertions["claim_has_ttl"] is False
    assert assertions["claim_file_exists_after_reconcile"] is True
    assert assertions["recent_executions_complete"] is False
    assert assertions["continue_submit_count"] == 0
    assert assertions["assumed_unfilled"] is False
    assert scenario["events"].index("reconcile") < scenario["events"].index("continue_decision")


def test_query_retry_stays_on_account_seam_and_unknown_freezes_new_risk(tmp_path, monkeypatch):
    sleeps = []
    monkeypatch.setattr(
        "application.runtime_broker_adapters.time.sleep",
        lambda seconds: sleeps.append(seconds),
    )
    result = run_offline_fault_acceptance(tmp_path)
    scenario = _scenario(result, "query_429_disconnect_unknown")
    assertions = scenario["assertions"]
    assert scenario["classification"] == "verified-safe-reject"
    assert "application.runtime_broker_adapters.SchwabRuntimeBrokerAdapters.fetch_managed_snapshot" in scenario["call_path"]
    assert "application.runtime_broker_adapters.SchwabRuntimeBrokerAdapters.build_order_status_fetcher" in scenario["call_path"]
    assert "application.execution_service.execute_rebalance_cycle" in scenario["call_path"]
    assert sleeps == [1.0, 2.0]
    assert assertions["account_query_attempts"] == 3
    assert assertions["disconnect_query_attempts"] == 1
    assert assertions["disconnect_snapshot_cash"] is None
    assert assertions["order_query_attempts_on_429"] == 1
    assert assertions["order_query_bounded_retry"] is False
    assert assertions["unknown_filled_quantity"] is None
    assert assertions["unknown_treated_as_zero"] is False
    assert assertions["new_risk_submit_count"] == 0
    assert any("order query" in item.lower() or "订单查询" in item for item in scenario["limitations"])


def test_boxx_multi_owner_attribution_is_unsupported_and_cash_is_not_duplicated(acceptance):
    scenario = _scenario(acceptance, "same_boxx_multi_owner")
    assertions = scenario["assertions"]
    assert scenario["classification"] == "unsupported"
    assert "application.broker_reconciliation.collect_read_only_reconciliation_observations" in scenario["call_path"]
    assert assertions["broker_net_quantity"] == 25.0
    assert assertions["owner_attribution_supported"] is False
    assert "owner" not in assertions["position_fact_keys"]
    assert assertions["aggregate_cash"] == 100.0
    assert assertions["cash_recorded_assignments"] == 1
    assert assertions["owner_cash_allocations"] is None


def test_risk_pause_calls_execution_path_and_submits_no_new_risk(acceptance):
    scenario = _scenario(acceptance, "risk_pause")
    phases = scenario["phases"]
    assertions = scenario["assertions"]
    assert scenario["classification"] == "supported"
    assert "application.execution_service.execute_rebalance_cycle" in scenario["call_path"]
    assert phases["request"]["seq"] < phases["receive"]["seq"] < phases["effective_prevention"]["seq"]
    assert phases["receive"]["seen_in_execution_trade_log"] is True
    assert phases["effective_prevention"]["new_risk_submit_count"] == 0
    assert phases["effective_prevention"]["market_liquidation_submit_count"] == 0
    assert assertions["paused_buy_submit_count"] == 0
    assert assertions["paused_sell_submit_count"] == 0
    assert assertions["unpaused_buy_submit_count"] == 1
    assert assertions["safe_query_after_pause_count"] == 1
    assert assertions["cancel_all_invoked"] is False


def test_real_seams_are_entered(tmp_path, monkeypatch):
    from application import offline_fault_acceptance as mod

    called = set()

    def wrap(name, fn):
        def inner(*args, **kwargs):
            called.add(name)
            return fn(*args, **kwargs)

        return inner

    monkeypatch.setattr(mod, "claim_execution_marker", wrap("claim", mod.claim_execution_marker))
    monkeypatch.setattr(mod, "execute_rebalance_cycle", wrap("execute", mod.execute_rebalance_cycle))
    monkeypatch.setattr(
        mod,
        "collect_read_only_reconciliation_observations",
        wrap("reconcile", mod.collect_read_only_reconciliation_observations),
    )
    monkeypatch.setattr(mod, "build_runtime_broker_adapters", wrap("adapters", mod.build_runtime_broker_adapters))
    monkeypatch.setattr("application.runtime_broker_adapters.time.sleep", lambda _seconds: None)
    mod.run_offline_fault_acceptance(tmp_path)
    assert called == {"claim", "execute", "reconcile", "adapters"}
