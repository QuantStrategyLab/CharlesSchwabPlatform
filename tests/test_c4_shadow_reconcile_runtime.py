from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from application.broker_reconciliation import SchwabReconciliationObservations
from application.c4_shadow_materialization import calculate_final_risk_assessment_sha256
from application.c4_shadow_reconcile_assembly import (
    build_materialized_account_facts_from_reconcile_observations,
)
from application.c4_shadow_reconcile_runtime import (
    c4_shadow_zero_submit_to_safe_diagnostics,
    run_c4_shadow_zero_submit_for_reconcile,
)
from quant_platform_kit.common.broker_reconciliation import (
    build_broker_reconciliation_evidence,
    calculate_broker_observation_sha256,
)


AS_OF = (
    (datetime.now(timezone.utc) - timedelta(minutes=1))
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z")
)
ACCOUNT_ID = "acct-paper-1"
STRATEGY_ID = "us_equity_combo_shadow"


def _observations(*, complete: bool = True) -> SchwabReconciliationObservations:
    return SchwabReconciliationObservations(
        account_scope={"account_hash": "acct-hash"},
        account_identity_match=True,
        positions=({"symbol": "SOXL", "quantity": 2.0, "market_value": 40.0},),
        cash={"cash_balance": 60.0, "buying_power": 60.0, "total_equity": 100.0},
        open_orders=({"order_id": "oid-1", "status": "WORKING"},),
        recent_executions=(),
        open_orders_complete=complete,
        recent_executions_complete=False,
        coverage={"open_orders_complete": complete},
    )


def _evidence(observations: SchwabReconciliationObservations) -> object:
    return build_broker_reconciliation_evidence(
        platform_id="schwab",
        strategy_profile=STRATEGY_ID,
        account_scope_sha256=calculate_broker_observation_sha256(observations.account_scope),
        baseline_id="synthetic-c4-baseline",
        baseline_target_sha256="a" * 64,
        runtime_target_sha256="a" * 64,
        observed_at=AS_OF,
        broker_connected=True,
        account_identity_match=True,
        positions_match=False,
        cash_match=False,
        open_orders_match=observations.open_orders_complete,
        recent_executions_match=False,
        local_execution_ledger_match=False,
        positions_sha256="c" * 64,
        cash_sha256="d" * 64,
        open_orders_sha256=calculate_broker_observation_sha256(observations.open_orders),
        recent_executions_sha256="e" * 64,
        local_execution_ledger_sha256="f" * 64,
    )


def _risk(account_digest: str, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "strategy_id": STRATEGY_ID,
        "as_of": AS_OF,
        "snapshot_version": "risk-engine-final-assessment.v1",
        "freshness": "FRESH",
        "status": "APPROVE",
        "execution_authorized": False,
        "account_digest": account_digest,
        "risk_policy_digest": "c" * 64,
    }
    payload.update(overrides)
    payload["assessment_digest"] = calculate_final_risk_assessment_sha256(payload)
    return payload


def _assert_zero_submit(result: dict[str, object]) -> None:
    assert result["no_order"] is True
    assert result["proposed_orders"] == []
    assert result["submission_attempted"] is False
    assert result["execution_permitted"] is False
    assert result["execution_authorized"] is False


def test_complete_observations_without_assessment_park_with_assessment_missing() -> None:
    observations = _observations(complete=True)

    result = run_c4_shadow_zero_submit_for_reconcile(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
        final_risk_assessment_provider=None,
    )

    assert result["status"] == "PARKED"
    assert "assessment missing" in result["reason_codes"]
    _assert_zero_submit(result)


def test_real_provider_matching_identity_reaches_ready_shadow_zero_submit() -> None:
    observations = _observations(complete=True)
    account = build_materialized_account_facts_from_reconcile_observations(
        observations,
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
    )
    expected = _risk(str(account["account_digest"]))

    def provider(*, account_id: str, strategy_id: str, as_of: str, **_kwargs):
        assert (account_id, strategy_id, as_of) == (ACCOUNT_ID, STRATEGY_ID, AS_OF)
        return expected

    result = run_c4_shadow_zero_submit_for_reconcile(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
        final_risk_assessment_provider=provider,
    )

    assert result["status"] == "READY_SHADOW_ZERO_SUBMIT"
    _assert_zero_submit(result)


def test_provider_assessment_with_incomplete_coverage_parks() -> None:
    observations = _observations(complete=False)

    def provider(**_kwargs):
        return _risk("a" * 64)

    result = run_c4_shadow_zero_submit_for_reconcile(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
        final_risk_assessment_provider=provider,
    )

    assert result["status"] == "PARKED"
    assert result["reason_codes"] == ("C4_BRIDGE_OPEN_ORDERS_COVERAGE_INCOMPLETE",)
    _assert_zero_submit(result)


def test_provider_strategy_mismatch_parks() -> None:
    observations = _observations(complete=True)
    account = build_materialized_account_facts_from_reconcile_observations(
        observations,
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
    )

    def provider(**_kwargs):
        return _risk(str(account["account_digest"]))

    result = run_c4_shadow_zero_submit_for_reconcile(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id="other-strategy",
        as_of=AS_OF,
        final_risk_assessment_provider=provider,
    )

    assert result["status"] == "PARKED"
    assert result["reason_codes"] == ("C4_BRIDGE_ACCOUNT_RISK_STRATEGY_ID_MISMATCH",)
    _assert_zero_submit(result)



def test_provider_as_of_mismatch_parks() -> None:
    observations = _observations(complete=True)
    account = build_materialized_account_facts_from_reconcile_observations(
        observations,
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
    )
    wrong_as_of = (
        (datetime.now(timezone.utc) - timedelta(hours=2))
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )

    def provider(**_kwargs):
        return _risk(str(account["account_digest"]))

    result = run_c4_shadow_zero_submit_for_reconcile(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=wrong_as_of,
        final_risk_assessment_provider=provider,
    )

    assert result["status"] == "PARKED"
    assert result["reason_codes"] == ("C4_BRIDGE_ACCOUNT_RISK_AS_OF_MISMATCH",)
    _assert_zero_submit(result)



def test_empty_provider_result_is_not_treated_as_approve() -> None:
    observations = _observations(complete=True)

    def provider(**_kwargs):
        return {}

    result = run_c4_shadow_zero_submit_for_reconcile(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
        final_risk_assessment_provider=provider,
    )

    assert result["status"] == "PARKED"
    assert "assessment missing" in result["reason_codes"]
    _assert_zero_submit(result)


def test_safe_diagnostics_omit_account_detail_fields() -> None:
    observations = _observations(complete=True)
    result = run_c4_shadow_zero_submit_for_reconcile(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
    )
    safe = c4_shadow_zero_submit_to_safe_diagnostics(result)
    encoded = json.dumps(safe, ensure_ascii=False)

    assert safe["status"] == "PARKED"
    assert safe["no_order"] is True
    assert safe["proposed_orders_count"] == 0
    assert safe["submission_attempted"] is False
    assert safe["execution_permitted"] is False
    assert "acct-hash" not in encoded
    assert "SOXL" not in encoded
    assert "cash_balance" not in encoded
    assert "positions" not in encoded
    assert "open_orders" not in encoded


def test_report_attachment_is_internal_only_and_omits_account_details() -> None:
    from application.c4_shadow_reconcile_runtime import build_reconcile_c4_report_attachment

    observations = _observations(complete=True)
    runtime_target = SimpleNamespace(
        strategy_profile=STRATEGY_ID,
        account_scope=ACCOUNT_ID,
    )
    attachment = build_reconcile_c4_report_attachment(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        runtime_target=runtime_target,
        final_risk_assessment_provider=None,
    )
    public_candidate_keys = {
        "schema_version",
        "permits_active_lkg",
        "expected_digests_configured",
        "execution_ledger_records_count",
        "recovery_blockers",
        "evidence",
        "coverage",
    }
    # Attachment is for finalize_runtime_report only; never merge into public body.
    assert set(attachment["summary"]) <= {
        "c4_shadow_status",
        "c4_shadow_no_order",
        "c4_shadow_submission_attempted",
        "c4_shadow_execution_permitted",
    }
    assert set(attachment["diagnostics"]) == {"c4_shadow_zero_submit"}
    assert attachment["summary"]["c4_shadow_status"] == "PARKED"
    assert "assessment missing" in attachment["diagnostics"]["c4_shadow_zero_submit"]["reason_codes"]
    encoded = json.dumps(attachment, ensure_ascii=False)
    assert "acct-hash" not in encoded
    assert "SOXL" not in encoded
    assert "cash_balance" not in encoded
    assert not (public_candidate_keys & set(attachment["summary"]))


def test_main_reconcile_path_wires_c4_into_report_not_public_body() -> None:
    from pathlib import Path

    source = Path("main.py").read_text(encoding="utf-8")
    assert "build_reconcile_c4_report_attachment" in source
    assert '**c4_attachment["diagnostics"]' in source or "**c4_attachment['diagnostics']" in source
    assert "return json.dumps(payload" in source
    assert "FINAL_RISK_ASSESSMENT_PROVIDER = None" in source
    # Public body remains candidate payload only.
    assert "json.dumps(c4_attachment" not in source
    assert "json.dumps(c4_attachment[" not in source
