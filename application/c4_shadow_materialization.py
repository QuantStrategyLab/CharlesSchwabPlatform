"""Pure Schwab fact materialization for the UES C4 shadow consumer.

This module validates already-collected inputs only.  It has no broker, file,
environment, network, order, intent, claim, or execution-entrypoint access.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import is_dataclass
from datetime import datetime, timezone
from quant_platform_kit.common.broker_reconciliation import (
    BrokerReconciliationEvidence,
    calculate_broker_observation_sha256,
    evaluate_broker_reconciliation_recovery,
)
from us_equity_strategies.research.c4_shadow_zero_submit_cycle import (
    consume_c4_shadow_zero_submit_cycle,
)

from application.broker_reconciliation import SchwabReconciliationObservations


_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_ORDER_OUTCOMES = {
    "WORKING": "OPEN",
    "QUEUED": "OPEN",
    "ACCEPTED": "ACKNOWLEDGED",
    "PENDING_ACTIVATION": "ACKNOWLEDGED",
    "PARTIAL": "PARTIALLY_FILLED",
    "FILLED": "FILLED",
    "CANCELED": "CANCELED",
    "REJECTED": "REJECTED",
    "EXPIRED": "EXPIRED",
}
class _BridgeInputError(ValueError):
    pass


def _canonical_digest(value: object) -> str:
    """Use the existing QPK canonical JSON hash for materialized facts."""

    return calculate_broker_observation_sha256(value)


def _mapping(value: object, *, reason: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _BridgeInputError(reason)
    return value


def _text(value: object, *, reason: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _BridgeInputError(reason)
    return value.strip()


def _identity(value: object, *, reason: str) -> str:
    normalized = _text(value, reason=reason)
    if not _IDENTITY.fullmatch(normalized):
        raise _BridgeInputError(reason)
    return normalized


def _digest(value: object, *, reason: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise _BridgeInputError(reason)
    return value


def _as_of(value: object, *, reason: str) -> str:
    text = _text(value, reason=reason)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise _BridgeInputError(reason) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _BridgeInputError(reason)
    utc_value = parsed.astimezone(timezone.utc)
    if utc_value.microsecond:
        return utc_value.isoformat().replace("+00:00", "Z")
    return utc_value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _number(value: object, *, reason: str) -> float:
    try:
        normalized = float(value)
    except (TypeError, ValueError) as exc:
        raise _BridgeInputError(reason) from exc
    if not math.isfinite(normalized):
        raise _BridgeInputError(reason)
    return normalized


def _canonical_fact(value: object, *, reason: str = "C4_BRIDGE_FACT_CONTENT_INVALID") -> object:
    """Copy all materialized fact content into a finite JSON-safe structure."""

    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _BridgeInputError(reason)
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise _BridgeInputError(reason)
            normalized[key] = _canonical_fact(item, reason=reason)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_canonical_fact(item, reason=reason) for item in value]
    raise _BridgeInputError(reason)


def _common_snapshot(snapshot: Mapping[str, object], *, prefix: str) -> dict[str, object]:
    return {
        "account_id": _identity(snapshot.get("account_id"), reason=f"C4_BRIDGE_{prefix}_ACCOUNT_ID_INVALID"),
        "strategy_id": _identity(snapshot.get("strategy_id"), reason=f"C4_BRIDGE_{prefix}_STRATEGY_ID_INVALID"),
        "as_of": _as_of(snapshot.get("as_of"), reason=f"C4_BRIDGE_{prefix}_AS_OF_INVALID"),
        "snapshot_version": _identity(
            snapshot.get("snapshot_version"), reason=f"C4_BRIDGE_{prefix}_SNAPSHOT_VERSION_INVALID"
        ),
        "freshness": _freshness(snapshot.get("freshness"), prefix=prefix),
    }


def _freshness(value: object, *, prefix: str) -> str:
    if value != "FRESH":
        raise _BridgeInputError(f"C4_BRIDGE_{prefix}_FRESHNESS_STALE_OR_MISSING")
    return "FRESH"


def _account_content(snapshot: Mapping[str, object]) -> dict[str, object]:
    common = _common_snapshot(snapshot, prefix="ACCOUNT")
    account_scope = _mapping(snapshot.get("account_scope"), reason="C4_BRIDGE_ACCOUNT_SCOPE_INVALID")
    if snapshot.get("account_identity_match") is not True:
        raise _BridgeInputError("C4_BRIDGE_ACCOUNT_IDENTITY_UNVERIFIED")
    positions = snapshot.get("positions")
    if not isinstance(positions, Sequence) or isinstance(positions, (str, bytes)):
        raise _BridgeInputError("C4_BRIDGE_ACCOUNT_POSITIONS_INVALID")
    normalized_positions = []
    for position in positions:
        row = _mapping(position, reason="C4_BRIDGE_ACCOUNT_POSITIONS_INVALID")
        normalized_positions.append(
            {
                "symbol": _identity(row.get("symbol"), reason="C4_BRIDGE_ACCOUNT_POSITIONS_INVALID"),
                "quantity": _number(row.get("quantity"), reason="C4_BRIDGE_ACCOUNT_POSITIONS_INVALID"),
                "market_value": _number(row.get("market_value"), reason="C4_BRIDGE_ACCOUNT_POSITIONS_INVALID"),
            }
        )
    cash = _mapping(snapshot.get("cash"), reason="C4_BRIDGE_ACCOUNT_CASH_INVALID")
    normalized_cash = {
        "cash_balance": _number(cash.get("cash_balance"), reason="C4_BRIDGE_ACCOUNT_CASH_INVALID"),
        "buying_power": _number(cash.get("buying_power"), reason="C4_BRIDGE_ACCOUNT_CASH_INVALID"),
        "total_equity": _number(cash.get("total_equity"), reason="C4_BRIDGE_ACCOUNT_CASH_INVALID"),
    }
    return {
        **common,
        "account_scope": dict(account_scope),
        "account_identity_match": True,
        "positions": normalized_positions,
        "cash": normalized_cash,
    }


def _account_digest_content(snapshot: Mapping[str, object]) -> dict[str, object]:
    _account_content(snapshot)
    content = {key: value for key, value in snapshot.items() if key != "account_digest"}
    return dict(_canonical_fact(content))


def calculate_materialized_account_facts_sha256(account_facts: Mapping[str, object]) -> str:
    """Return the QPK hash of validated materialized account content."""

    snapshot = _mapping(account_facts, reason="C4_BRIDGE_ACCOUNT_FACTS_REQUIRED")
    return _canonical_digest(_account_digest_content(snapshot))


def _observations_content(value: object) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object]]:
    if isinstance(value, SchwabReconciliationObservations):
        source: Mapping[str, object] = {
            "account_scope": value.account_scope,
            "account_identity_match": value.account_identity_match,
            "positions": value.positions,
            "cash": value.cash,
            "open_orders": value.open_orders,
            "recent_executions": value.recent_executions,
            "open_orders_complete": value.open_orders_complete,
            "recent_executions_complete": value.recent_executions_complete,
            "coverage": value.coverage,
        }
    elif is_dataclass(value):
        raise _BridgeInputError("C4_BRIDGE_ORDERS_OBSERVATIONS_INVALID")
    else:
        source = _mapping(value, reason="C4_BRIDGE_ORDERS_OBSERVATIONS_INVALID")
    coverage = _mapping(source.get("coverage"), reason="C4_BRIDGE_ORDERS_COVERAGE_INVALID")
    account_scope = _mapping(source.get("account_scope"), reason="C4_BRIDGE_ORDERS_OBSERVATIONS_INVALID")
    if source.get("account_identity_match") is not True:
        raise _BridgeInputError("C4_BRIDGE_ORDERS_ACCOUNT_IDENTITY_UNVERIFIED")
    if source.get("open_orders_complete") is not True or coverage.get("open_orders_complete") is not True:
        raise _BridgeInputError("C4_BRIDGE_OPEN_ORDERS_COVERAGE_INCOMPLETE")
    orders = source.get("open_orders")
    if not isinstance(orders, Sequence) or isinstance(orders, (str, bytes)):
        raise _BridgeInputError("C4_BRIDGE_OPEN_ORDERS_INVALID")
    normalized_orders = []
    for order in orders:
        row = _mapping(order, reason="C4_BRIDGE_OPEN_ORDERS_INVALID")
        status = _text(row.get("status"), reason="C4_BRIDGE_OPEN_ORDER_STATUS_UNKNOWN").upper()
        outcome = _ORDER_OUTCOMES.get(status)
        if outcome is None:
            raise _BridgeInputError("C4_BRIDGE_OPEN_ORDER_STATUS_UNKNOWN")
        normalized_orders.append(
            {
                "order_id": _identity(row.get("order_id"), reason="C4_BRIDGE_OPEN_ORDERS_INVALID"),
                "outcome": outcome,
            }
        )
    facts = dict(_canonical_fact(dict(source)))
    return facts, normalized_orders, dict(_canonical_fact(account_scope))


def _orders_content(
    snapshot: Mapping[str, object],
) -> tuple[dict[str, object], list[dict[str, object]], dict[str, object]]:
    _common_snapshot(snapshot, prefix="ORDERS")
    observation_facts, c4_orders, account_scope = _observations_content(snapshot.get("observations"))
    content = {key: value for key, value in snapshot.items() if key not in {"orders_digest", "observations"}}
    content["observations"] = observation_facts
    return dict(_canonical_fact(content)), c4_orders, account_scope


def calculate_materialized_orders_sha256(order_snapshot: Mapping[str, object]) -> str:
    """Return the QPK hash of validated complete order-observation content."""

    content, _, _ = _orders_content(_mapping(order_snapshot, reason="C4_BRIDGE_ORDERS_REQUIRED"))
    return _canonical_digest(content)


def _assessment_content(snapshot: Mapping[str, object]) -> dict[str, object]:
    common = _common_snapshot(snapshot, prefix="RISK")
    status = snapshot.get("status")
    if status != "APPROVE":
        raise _BridgeInputError("C4_BRIDGE_RISK_ENGINE_NOT_APPROVE")
    if snapshot.get("execution_authorized") is not False:
        raise _BridgeInputError("C4_BRIDGE_RISK_ENGINE_EXECUTION_AUTHORIZED_SEMANTICS_UNSATISFIED")
    return {
        **common,
        "status": "APPROVE",
        "execution_authorized": False,
        "account_digest": _digest(snapshot.get("account_digest"), reason="C4_BRIDGE_RISK_ACCOUNT_DIGEST_INVALID"),
        "risk_policy_digest": _digest(snapshot.get("risk_policy_digest"), reason="C4_BRIDGE_RISK_POLICY_DIGEST_INVALID"),
    }


def _assessment_digest_content(snapshot: Mapping[str, object]) -> dict[str, object]:
    _assessment_content(snapshot)
    content = {key: value for key, value in snapshot.items() if key != "assessment_digest"}
    return dict(_canonical_fact(content))


def calculate_final_risk_assessment_sha256(final_risk_assessment: Mapping[str, object]) -> str:
    """Return the QPK hash of a final materialized RiskEngine assessment."""

    snapshot = _mapping(final_risk_assessment, reason="C4_BRIDGE_RISK_ASSESSMENT_REQUIRED")
    return _canonical_digest(_assessment_digest_content(snapshot))


def _validated_reconciliation_ref(
    *,
    account: Mapping[str, object],
    orders: Mapping[str, object],
    assessment: Mapping[str, object],
    order_observation_facts: Mapping[str, object],
    reconciliation_evidence: object,
) -> dict[str, object]:
    """Bind C4 inputs to an existing runtime reconciliation receipt."""

    if not isinstance(reconciliation_evidence, BrokerReconciliationEvidence):
        raise _BridgeInputError("C4_BRIDGE_RECONCILIATION_EVIDENCE_REQUIRED")
    evidence = BrokerReconciliationEvidence.from_dict(reconciliation_evidence.to_dict())
    expected_scope = _canonical_digest(account["account_scope"])
    expected_orders = _canonical_digest(order_observation_facts["open_orders"])
    findings = evaluate_broker_reconciliation_recovery(
        evidence,
        expected_platform_id="schwab",
        expected_strategy_profile=str(account["strategy_id"]),
        expected_account_scope_sha256=expected_scope,
        expected_open_orders_sha256=expected_orders,
    )
    if findings:
        raise _BridgeInputError("C4_BRIDGE_RECONCILIATION_EVIDENCE_UNTRUSTED_OR_STALE")
    observed_at = _as_of(evidence.to_dict()["observed_at"], reason="C4_BRIDGE_RECONCILIATION_EVIDENCE_INVALID")
    for snapshot, prefix in ((account, "ACCOUNT"), (orders, "ORDERS"), (assessment, "RISK")):
        if snapshot["as_of"] != observed_at:
            raise _BridgeInputError(f"C4_BRIDGE_{prefix}_RECONCILIATION_AS_OF_MISMATCH")
    return {
        "schema_version": evidence.schema_version,
        "evidence_sha256": evidence.evidence_sha256,
        "observed_at": observed_at,
        "account_scope_sha256": evidence.account_scope_sha256,
        "open_orders_sha256": evidence.open_orders_sha256,
    }


def _refresh_evidence_digest(result: dict[str, object]) -> dict[str, object]:
    result.pop("evidence_digest", None)
    encoded = json.dumps(
        result, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    result["evidence_digest"] = hashlib.sha256(encoded).hexdigest()
    return result


def _c4_parked(reason: str) -> dict[str, object]:
    """Keep every bridge failure in the C4 zero-submit envelope."""

    result = consume_c4_shadow_zero_submit_cycle(
        account_snapshot={},
        open_orders_snapshot={},
        risk_engine_result={},
    )
    result["reason_codes"] = (reason,)
    return _refresh_evidence_digest(result)


def materialize_c4_shadow_zero_submit_cycle(
    *,
    account_facts: Mapping[str, object],
    order_snapshot: Mapping[str, object],
    final_risk_assessment: Mapping[str, object],
    reconciliation_evidence: BrokerReconciliationEvidence,
) -> dict[str, object]:
    """Validate materialized Schwab facts and pass only a zero-submit C4 input.

    The caller is responsible for collecting these facts elsewhere.  Incomplete
    order coverage, digest mismatch, identity/time disagreement, unknown order
    state, or a non-final risk assessment returns a C4-shaped ``PARKED`` result.
    """

    try:
        account = _mapping(account_facts, reason="C4_BRIDGE_ACCOUNT_FACTS_REQUIRED")
        account_content = _account_content(account)
        account_digest = _canonical_digest(_account_digest_content(account))
        if _digest(account.get("account_digest"), reason="C4_BRIDGE_ACCOUNT_DIGEST_INVALID") != account_digest:
            raise _BridgeInputError("C4_BRIDGE_ACCOUNT_DIGEST_MISMATCH")

        orders = _mapping(order_snapshot, reason="C4_BRIDGE_ORDERS_REQUIRED")
        orders_content, c4_orders, orders_account_scope = _orders_content(orders)
        orders_digest = _canonical_digest(orders_content)
        if _digest(orders.get("orders_digest"), reason="C4_BRIDGE_ORDERS_DIGEST_INVALID") != orders_digest:
            raise _BridgeInputError("C4_BRIDGE_ORDERS_DIGEST_MISMATCH")

        assessment = _mapping(final_risk_assessment, reason="C4_BRIDGE_RISK_ASSESSMENT_REQUIRED")
        assessment_content = _assessment_content(assessment)
        assessment_digest = _canonical_digest(_assessment_digest_content(assessment))
        if _digest(assessment.get("assessment_digest"), reason="C4_BRIDGE_RISK_ASSESSMENT_DIGEST_INVALID") != assessment_digest:
            raise _BridgeInputError("C4_BRIDGE_RISK_ASSESSMENT_DIGEST_MISMATCH")

        for field, reason in (
            ("account_id", "C4_BRIDGE_ACCOUNT_ORDERS_ACCOUNT_ID_MISMATCH"),
            ("strategy_id", "C4_BRIDGE_ACCOUNT_ORDERS_STRATEGY_ID_MISMATCH"),
            ("as_of", "C4_BRIDGE_ACCOUNT_ORDERS_AS_OF_MISMATCH"),
        ):
            if account_content[field] != orders_content[field]:
                raise _BridgeInputError(reason)
        for field, reason in (
            ("account_id", "C4_BRIDGE_ACCOUNT_RISK_ACCOUNT_ID_MISMATCH"),
            ("strategy_id", "C4_BRIDGE_ACCOUNT_RISK_STRATEGY_ID_MISMATCH"),
            ("as_of", "C4_BRIDGE_ACCOUNT_RISK_AS_OF_MISMATCH"),
        ):
            if account_content[field] != assessment_content[field]:
                raise _BridgeInputError(reason)
        if dict(_canonical_fact(account_content["account_scope"])) != orders_account_scope:
            raise _BridgeInputError("C4_BRIDGE_ACCOUNT_ORDERS_SCOPE_MISMATCH")
        if assessment_content["account_digest"] != account_digest:
            raise _BridgeInputError("C4_BRIDGE_RISK_ACCOUNT_DIGEST_MISMATCH")
        reconciliation_ref = _validated_reconciliation_ref(
            account=account_content,
            orders=orders_content,
            assessment=assessment_content,
            order_observation_facts=orders_content["observations"],
            reconciliation_evidence=reconciliation_evidence,
        )

        result = consume_c4_shadow_zero_submit_cycle(
            account_snapshot={**account_content, "account_digest": account_digest},
            open_orders_snapshot={
                **orders_content,
                "orders_digest": orders_digest,
                "account_digest": account_digest,
                "orders": c4_orders,
            },
            risk_engine_result={
                **assessment_content,
                "assessment_digest": assessment_digest,
            },
        )
        result["reconciliation_evidence_ref"] = reconciliation_ref
        return _refresh_evidence_digest(result)
    except _BridgeInputError as exc:
        return _c4_parked(str(exc))
    except (TypeError, ValueError, ArithmeticError, OverflowError):
        return _c4_parked("C4_BRIDGE_INPUT_INVALID")


__all__ = [
    "calculate_final_risk_assessment_sha256",
    "calculate_materialized_account_facts_sha256",
    "calculate_materialized_orders_sha256",
    "materialize_c4_shadow_zero_submit_cycle",
]
