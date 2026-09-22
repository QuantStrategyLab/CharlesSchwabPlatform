from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

from application.broker_reconciliation import SchwabReconciliationObservations
from application.c4_shadow_materialization import calculate_final_risk_assessment_sha256
from application.c4_shadow_reconcile_assembly import (
    assemble_c4_shadow_zero_submit_from_reconcile_observations,
)
from quant_platform_kit.common.broker_reconciliation import (
    build_broker_reconciliation_evidence,
    calculate_broker_observation_sha256,
)


AS_OF = (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
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


def test_complete_reconcile_observations_assemble_to_ready_shadow() -> None:
    observations = _observations(complete=True)
    # Final risk is supplied by the caller; assembly binds account digest after materializing.
    # Use a two-pass helper pattern: assemble once to learn digest, then with matching risk.
    from application.c4_shadow_reconcile_assembly import (
        build_materialized_account_facts_from_reconcile_observations,
    )

    account = build_materialized_account_facts_from_reconcile_observations(
        observations,
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
    )
    result = assemble_c4_shadow_zero_submit_from_reconcile_observations(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
        final_risk_assessment=_risk(str(account["account_digest"])),
    )

    assert result["status"] == "READY_SHADOW_ZERO_SUBMIT"
    _assert_zero_submit(result)
    assert result["orders_ref"]["order_count"] == 1


def test_incomplete_open_orders_assembly_parks_without_forging_execution_coverage() -> None:
    observations = _observations(complete=False)
    assert observations.recent_executions_complete is False

    result = assemble_c4_shadow_zero_submit_from_reconcile_observations(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
        final_risk_assessment=_risk("a" * 64),
    )

    assert result["status"] == "PARKED"
    assert result["reason_codes"] == ("C4_BRIDGE_OPEN_ORDERS_COVERAGE_INCOMPLETE",)
    _assert_zero_submit(result)
    assert observations.recent_executions_complete is False


def test_non_approve_or_authorized_risk_assembly_parks() -> None:
    observations = _observations(complete=True)
    from application.c4_shadow_reconcile_assembly import (
        build_materialized_account_facts_from_reconcile_observations,
    )

    account = build_materialized_account_facts_from_reconcile_observations(
        observations,
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
    )
    rejected = dict(_risk(str(account["account_digest"])))
    rejected["status"] = "REJECT"
    rejected_result = assemble_c4_shadow_zero_submit_from_reconcile_observations(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
        final_risk_assessment=rejected,
    )
    assert rejected_result["status"] == "PARKED"
    assert rejected_result["reason_codes"] == ("C4_BRIDGE_RISK_ENGINE_NOT_APPROVE",)
    _assert_zero_submit(rejected_result)

    authorized = dict(_risk(str(account["account_digest"])))
    authorized["execution_authorized"] = True
    authorized_result = assemble_c4_shadow_zero_submit_from_reconcile_observations(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
        final_risk_assessment=authorized,
    )
    assert authorized_result["status"] == "PARKED"
    assert authorized_result["reason_codes"] == (
        "C4_BRIDGE_RISK_ENGINE_EXECUTION_AUTHORIZED_SEMANTICS_UNSATISFIED",
    )
    _assert_zero_submit(authorized_result)


def test_strategy_mismatch_assembly_parks() -> None:
    observations = _observations(complete=True)
    from application.c4_shadow_reconcile_assembly import (
        build_materialized_account_facts_from_reconcile_observations,
    )

    account = build_materialized_account_facts_from_reconcile_observations(
        observations,
        account_id=ACCOUNT_ID,
        strategy_id=STRATEGY_ID,
        as_of=AS_OF,
    )
    result = assemble_c4_shadow_zero_submit_from_reconcile_observations(
        observations=observations,
        reconciliation_evidence=_evidence(observations),
        account_id=ACCOUNT_ID,
        strategy_id="other-strategy",
        as_of=AS_OF,
        final_risk_assessment=_risk(str(account["account_digest"])),
    )

    assert result["status"] == "PARKED"
    assert result["reason_codes"] == ("C4_BRIDGE_ACCOUNT_RISK_STRATEGY_ID_MISMATCH",)
    _assert_zero_submit(result)


def test_assembly_has_no_broker_or_execution_entrypoint() -> None:
    source = inspect.getsource(assemble_c4_shadow_zero_submit_from_reconcile_observations)
    assert "materialize_c4_shadow_zero_submit_cycle" in source
    assert "submit_order" not in source
    assert "execution_port" not in source
    assert "run_strategy_core" not in source
    assert "get_orders_for_account" not in source
    assert "build_client" not in source
