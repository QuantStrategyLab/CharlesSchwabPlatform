"""Assemble RECONCILE_ONLY observations into UES C4 zero-submit inputs.

This module only binds already-collected Schwab reconciliation observations and
an already-produced final RiskEngine assessment into the three materialized
snapshots expected by ``materialize_c4_shadow_zero_submit_cycle``.

It does not open a broker client, submit or cancel orders, invent RiskEngine
APPROVE results, or set ``recent_executions_complete=true``.
"""

from __future__ import annotations

from collections.abc import Mapping

from quant_platform_kit.common.broker_reconciliation import BrokerReconciliationEvidence

from application.broker_reconciliation import SchwabReconciliationObservations
from application.c4_shadow_materialization import (
    calculate_materialized_account_facts_sha256,
    calculate_materialized_orders_sha256,
    materialize_c4_shadow_zero_submit_cycle,
)


_ACCOUNT_SNAPSHOT_VERSION = "schwab-account-facts.v1"
_ORDERS_SNAPSHOT_VERSION = "schwab-open-orders-observation.v1"
_PLACEHOLDER_DIGEST = "0" * 64


def build_materialized_account_facts_from_reconcile_observations(
    observations: SchwabReconciliationObservations,
    *,
    account_id: str,
    strategy_id: str,
    as_of: str,
) -> dict[str, object]:
    """Build the account-facts snapshot from read-only reconcile observations."""

    payload: dict[str, object] = {
        "account_id": account_id,
        "strategy_id": strategy_id,
        "as_of": as_of,
        "snapshot_version": _ACCOUNT_SNAPSHOT_VERSION,
        "freshness": "FRESH",
        "account_scope": dict(observations.account_scope),
        "account_identity_match": observations.account_identity_match is True,
        "positions": [dict(position) for position in observations.positions],
        "cash": dict(observations.cash),
    }
    try:
        payload["account_digest"] = calculate_materialized_account_facts_sha256(payload)
    except (TypeError, ValueError, ArithmeticError, OverflowError):
        payload["account_digest"] = _PLACEHOLDER_DIGEST
    return payload


def build_materialized_order_snapshot_from_reconcile_observations(
    observations: SchwabReconciliationObservations,
    *,
    account_id: str,
    strategy_id: str,
    as_of: str,
) -> dict[str, object]:
    """Build the open-orders observation snapshot; never forges coverage flags."""

    payload: dict[str, object] = {
        "account_id": account_id,
        "strategy_id": strategy_id,
        "as_of": as_of,
        "snapshot_version": _ORDERS_SNAPSHOT_VERSION,
        "freshness": "FRESH",
        "observations": observations,
    }
    try:
        payload["orders_digest"] = calculate_materialized_orders_sha256(payload)
    except (TypeError, ValueError, ArithmeticError, OverflowError):
        payload["orders_digest"] = _PLACEHOLDER_DIGEST
    return payload


def assemble_c4_shadow_zero_submit_from_reconcile_observations(
    *,
    observations: SchwabReconciliationObservations,
    reconciliation_evidence: BrokerReconciliationEvidence,
    account_id: str,
    strategy_id: str,
    as_of: str,
    final_risk_assessment: Mapping[str, object],
) -> dict[str, object]:
    """Run the auditable RECONCILE_ONLY → C4 zero-submit assembly path.

    Callers must collect observations and produce the final RiskEngine
    assessment elsewhere. Incomplete open-order coverage, identity/time/digest
    disagreement, or a non-zero-submit risk assessment returns C4 ``PARKED`` with
    empty proposed orders and no submission.
    """

    account_facts = build_materialized_account_facts_from_reconcile_observations(
        observations,
        account_id=account_id,
        strategy_id=strategy_id,
        as_of=as_of,
    )
    order_snapshot = build_materialized_order_snapshot_from_reconcile_observations(
        observations,
        account_id=account_id,
        strategy_id=strategy_id,
        as_of=as_of,
    )
    return materialize_c4_shadow_zero_submit_cycle(
        account_facts=account_facts,
        order_snapshot=order_snapshot,
        final_risk_assessment=final_risk_assessment,
        reconciliation_evidence=reconciliation_evidence,
    )


__all__ = [
    "assemble_c4_shadow_zero_submit_from_reconcile_observations",
    "build_materialized_account_facts_from_reconcile_observations",
    "build_materialized_order_snapshot_from_reconcile_observations",
]
