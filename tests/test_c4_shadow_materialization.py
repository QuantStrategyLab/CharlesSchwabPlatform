from __future__ import annotations

import inspect
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from application.broker_reconciliation import SchwabReconciliationObservations
from application.c4_shadow_materialization import (
    calculate_final_risk_assessment_sha256,
    calculate_materialized_account_facts_sha256,
    calculate_materialized_orders_sha256,
    materialize_c4_shadow_zero_submit_cycle,
)
from quant_platform_kit.common.broker_reconciliation import (
    BrokerReconciliationEvidence,
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
        positions=(
            {"symbol": "SOXL", "quantity": 2.0, "market_value": 40.0},
        ),
        cash={"cash_balance": 60.0, "buying_power": 60.0, "total_equity": 100.0},
        open_orders=({"order_id": "oid-1", "status": "WORKING"},),
        recent_executions=(),
        open_orders_complete=complete,
        recent_executions_complete=False,
        coverage={"open_orders_complete": complete},
    )


def _account(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "strategy_id": STRATEGY_ID,
        "as_of": AS_OF,
        "snapshot_version": "schwab-account-facts.v1",
        "freshness": "FRESH",
        "account_scope": {"account_hash": "acct-hash"},
        "account_identity_match": True,
        "positions": [{"symbol": "SOXL", "quantity": 2.0, "market_value": 40.0}],
        "cash": {"cash_balance": 60.0, "buying_power": 60.0, "total_equity": 100.0},
    }
    payload.update(overrides)
    payload["account_digest"] = calculate_materialized_account_facts_sha256(payload)
    return payload


def _orders(
    observations: SchwabReconciliationObservations | None = None, **overrides: object
) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "strategy_id": STRATEGY_ID,
        "as_of": AS_OF,
        "snapshot_version": "schwab-open-orders-observation.v1",
        "freshness": "FRESH",
        "observations": observations or _observations(),
    }
    payload.update(overrides)
    payload["orders_digest"] = calculate_materialized_orders_sha256(payload)
    return payload


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


def _reconciliation_evidence(
    observations: SchwabReconciliationObservations, *, observed_at: str = AS_OF
) -> BrokerReconciliationEvidence:
    return build_broker_reconciliation_evidence(
        platform_id="schwab",
        strategy_profile=STRATEGY_ID,
        account_scope_sha256=calculate_broker_observation_sha256(observations.account_scope),
        baseline_id="synthetic-c4-baseline",
        baseline_target_sha256="a" * 64,
        runtime_target_sha256="a" * 64,
        observed_at=observed_at,
        broker_connected=True,
        account_identity_match=True,
        positions_match=True,
        cash_match=True,
        open_orders_match=observations.open_orders_complete,
        recent_executions_match=True,
        local_execution_ledger_match=True,
        positions_sha256="c" * 64,
        cash_sha256="d" * 64,
        open_orders_sha256=calculate_broker_observation_sha256(observations.open_orders),
        recent_executions_sha256="e" * 64,
        local_execution_ledger_sha256="f" * 64,
    )


def _materialize(
    *,
    account_facts: dict[str, object],
    order_snapshot: dict[str, object],
    final_risk_assessment: dict[str, object],
    reconciliation_evidence: BrokerReconciliationEvidence | None = None,
) -> dict[str, object]:
    observations = order_snapshot["observations"]
    assert isinstance(observations, SchwabReconciliationObservations)
    return materialize_c4_shadow_zero_submit_cycle(
        account_facts=account_facts,
        order_snapshot=order_snapshot,
        final_risk_assessment=final_risk_assessment,
        reconciliation_evidence=reconciliation_evidence or _reconciliation_evidence(observations),
    )


def _assert_zero_submit(result: dict[str, object]) -> None:
    assert result["no_order"] is True
    assert result["proposed_orders"] == []
    assert result["submission_attempted"] is False
    assert result["execution_permitted"] is False
    assert result["execution_authorized"] is False


def test_complete_materialized_facts_reach_c4_zero_submit() -> None:
    account = _account()
    orders = _orders()
    evidence = _reconciliation_evidence(orders["observations"])
    result = _materialize(
        account_facts=account,
        order_snapshot=orders,
        final_risk_assessment=_risk(account["account_digest"]),
        reconciliation_evidence=evidence,
    )

    assert result["status"] == "READY_SHADOW_ZERO_SUBMIT"
    _assert_zero_submit(result)
    assert result["orders_ref"]["order_count"] == 1
    assert result["reconciliation_evidence_ref"]["evidence_sha256"] == evidence.evidence_sha256


def test_existing_partial_schwab_observation_parks() -> None:
    account = _account()
    partial_orders = _orders()
    partial_orders["observations"] = _observations(complete=False)
    result = _materialize(
        account_facts=account,
        order_snapshot=partial_orders,
        final_risk_assessment=_risk(account["account_digest"]),
    )

    assert result["status"] == "PARKED"
    assert result["reason_codes"] == ("C4_BRIDGE_OPEN_ORDERS_COVERAGE_INCOMPLETE",)
    _assert_zero_submit(result)


def test_digest_identity_and_time_tampering_park() -> None:
    account = _account()
    digest_tampered = dict(account)
    digest_tampered["account_digest"] = "0" * 64
    tampered = _materialize(
        account_facts=digest_tampered,
        order_snapshot=_orders(),
        final_risk_assessment=_risk(account["account_digest"]),
    )
    assert tampered["status"] == "PARKED"
    assert tampered["reason_codes"] == ("C4_BRIDGE_ACCOUNT_DIGEST_MISMATCH",)
    _assert_zero_submit(tampered)

    order_digest_tampered = _orders()
    order_digest_tampered["orders_digest"] = "0" * 64
    tampered_orders = _materialize(
        account_facts=account,
        order_snapshot=order_digest_tampered,
        final_risk_assessment=_risk(account["account_digest"]),
    )
    assert tampered_orders["status"] == "PARKED"
    assert tampered_orders["reason_codes"] == ("C4_BRIDGE_ORDERS_DIGEST_MISMATCH",)
    _assert_zero_submit(tampered_orders)

    assessment_digest_tampered = _risk(account["account_digest"])
    assessment_digest_tampered["assessment_digest"] = "0" * 64
    tampered_assessment = _materialize(
        account_facts=account,
        order_snapshot=_orders(),
        final_risk_assessment=assessment_digest_tampered,
    )
    assert tampered_assessment["status"] == "PARKED"
    assert tampered_assessment["reason_codes"] == (
        "C4_BRIDGE_RISK_ASSESSMENT_DIGEST_MISMATCH",
    )
    _assert_zero_submit(tampered_assessment)

    assessment_content_tampered = _risk(account["account_digest"])
    assessment_content_tampered["provenance"] = "changed-after-hash"
    tampered_content = _materialize(
        account_facts=account,
        order_snapshot=_orders(),
        final_risk_assessment=assessment_content_tampered,
    )
    assert tampered_content["status"] == "PARKED"
    assert tampered_content["reason_codes"] == (
        "C4_BRIDGE_RISK_ASSESSMENT_DIGEST_MISMATCH",
    )

    different_strategy_orders = _orders(strategy_id="other-strategy")
    identity_mismatch = _materialize(
        account_facts=account,
        order_snapshot=different_strategy_orders,
        final_risk_assessment=_risk(account["account_digest"]),
    )
    assert identity_mismatch["status"] == "PARKED"
    assert identity_mismatch["reason_codes"] == ("C4_BRIDGE_ACCOUNT_ORDERS_STRATEGY_ID_MISMATCH",)

    different_time_orders = _orders(as_of="2026-09-21T14:05:00Z")
    time_mismatch = _materialize(
        account_facts=account,
        order_snapshot=different_time_orders,
        final_risk_assessment=_risk(account["account_digest"]),
    )
    assert time_mismatch["status"] == "PARKED"
    assert time_mismatch["reason_codes"] == ("C4_BRIDGE_ACCOUNT_ORDERS_AS_OF_MISMATCH",)
    _assert_zero_submit(time_mismatch)


def test_order_content_and_account_scope_are_bound() -> None:
    account = _account()
    content_tampered = _orders()
    content_tampered["observations"].open_orders[0]["quantity"] = 99  # type: ignore[index]
    tampered = _materialize(
        account_facts=account,
        order_snapshot=content_tampered,
        final_risk_assessment=_risk(account["account_digest"]),
    )
    assert tampered["status"] == "PARKED"
    assert tampered["reason_codes"] == ("C4_BRIDGE_ORDERS_DIGEST_MISMATCH",)

    other_scope = _orders()
    other_scope["observations"] = replace(
        _observations(), account_scope={"account_hash": "other-account"}
    )
    other_scope["orders_digest"] = calculate_materialized_orders_sha256(other_scope)
    scope_mismatch = _materialize(
        account_facts=account,
        order_snapshot=other_scope,
        final_risk_assessment=_risk(account["account_digest"]),
    )
    assert scope_mismatch["status"] == "PARKED"
    assert scope_mismatch["reason_codes"] == ("C4_BRIDGE_ACCOUNT_ORDERS_SCOPE_MISMATCH",)

    identity_unverified = _orders()
    identity_unverified["observations"] = replace(_observations(), account_identity_match=False)
    result = _materialize(
        account_facts=account,
        order_snapshot=identity_unverified,
        final_risk_assessment=_risk(account["account_digest"]),
    )
    assert result["status"] == "PARKED"
    assert result["reason_codes"] == ("C4_BRIDGE_ORDERS_ACCOUNT_IDENTITY_UNVERIFIED",)


def test_stale_reconciliation_evidence_parks_even_when_its_snapshot_time_matches() -> None:
    old_as_of = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    account = _account(as_of=old_as_of)
    orders = _orders(as_of=old_as_of)
    result = _materialize(
        account_facts=account,
        order_snapshot=orders,
        final_risk_assessment=_risk(account["account_digest"], as_of=old_as_of),
        reconciliation_evidence=_reconciliation_evidence(
            orders["observations"], observed_at=old_as_of
        ),
    )

    assert result["status"] == "PARKED"
    assert result["reason_codes"] == ("C4_BRIDGE_RECONCILIATION_EVIDENCE_UNTRUSTED_OR_STALE",)
    _assert_zero_submit(result)


def test_reject_or_execution_authorized_assessment_parks() -> None:
    account = _account()
    rejected_assessment = _risk(account["account_digest"])
    rejected_assessment["status"] = "REJECT"
    rejected = _materialize(
        account_facts=account,
        order_snapshot=_orders(),
        final_risk_assessment=rejected_assessment,
    )
    assert rejected["status"] == "PARKED"
    assert rejected["reason_codes"] == ("C4_BRIDGE_RISK_ENGINE_NOT_APPROVE",)
    _assert_zero_submit(rejected)

    authorized_assessment = _risk(account["account_digest"])
    authorized_assessment["execution_authorized"] = True
    authorized = _materialize(
        account_facts=account,
        order_snapshot=_orders(),
        final_risk_assessment=authorized_assessment,
    )
    assert authorized["status"] == "PARKED"
    assert authorized["reason_codes"] == (
        "C4_BRIDGE_RISK_ENGINE_EXECUTION_AUTHORIZED_SEMANTICS_UNSATISFIED",
    )
    _assert_zero_submit(authorized)


def test_non_finite_account_fact_parks() -> None:
    account = _account()
    account["cash"] = {"cash_balance": float("nan"), "buying_power": 60.0, "total_equity": 100.0}
    result = _materialize(
        account_facts=account,
        order_snapshot=_orders(),
        final_risk_assessment=_risk("a" * 64),
    )

    assert result["status"] == "PARKED"
    assert result["reason_codes"] == ("C4_BRIDGE_ACCOUNT_CASH_INVALID",)
    _assert_zero_submit(result)


def test_bridge_has_no_execution_entrypoint_dependency() -> None:
    source = inspect.getsource(materialize_c4_shadow_zero_submit_cycle)
    assert "consume_c4_shadow_zero_submit_cycle" in source
    assert "run_strategy_core" not in source
    assert "execution_port" not in source
    assert "submit_order" not in source
