"""Read-only Schwab observations for a frozen live-baseline recovery.

This adapter never submits or changes orders.  It normalizes the broker facts
locally and returns only SHA-256 evidence plus stable, redacted findings.
"""

from __future__ import annotations

import json
import math
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from quant_platform_kit.common.broker_reconciliation import (
    BrokerReconciliationEvidence,
    BrokerReconciliationFinding,
    build_broker_reconciliation_evidence,
    calculate_broker_observation_sha256,
    evaluate_broker_reconciliation_recovery,
)
from quant_platform_kit.common.execution_state import build_execution_marker_store_from_env


SCHWAB_RECONCILIATION_EXPECTED_DIGESTS_ENV = "SCHWAB_RECONCILIATION_EXPECTED_DIGESTS_JSON"
SCHWAB_RECONCILIATION_ENABLED_ENV = "SCHWAB_BROKER_RECONCILIATION_ENABLED"
_EXPECTED_DIGEST_KEYS = (
    "account_scope_sha256",
    "positions_sha256",
    "cash_sha256",
    "open_orders_sha256",
    "recent_executions_sha256",
    "local_execution_ledger_sha256",
)
_SAFE_CANDIDATE_KEYS = frozenset(
    {
        "schema_version",
        "permits_active_lkg",
        "expected_digests_configured",
        "execution_ledger_records_count",
        "recovery_blockers",
        "evidence",
    }
)
_TERMINAL_ORDER_STATUSES = frozenset({"CANCELED", "REJECTED", "EXPIRED", "FILLED", "REPLACED"})


class SchwabReconciliationReadError(RuntimeError):
    """A required read-only Schwab surface was unavailable or malformed."""


def validate_reconciliation_preconditions(
    *,
    runtime_target: object,
    collector: object,
    env_reader: Callable[[str, str | None], str | None] = os.getenv,
) -> None:
    """Fail before building any broker context unless reconciliation is safe."""

    enabled = _text(env_reader(SCHWAB_RECONCILIATION_ENABLED_ENV, None)).lower()
    if enabled != "true":
        raise SchwabReconciliationReadError("Schwab broker reconciliation is disabled.")
    if not callable(collector):
        raise SchwabReconciliationReadError("Schwab broker reconciliation collector is unavailable.")
    if runtime_target is None:
        raise SchwabReconciliationReadError(
            "Schwab reconciliation requires an explicit runtime target."
        )
    continuity_state = _text(
        getattr(getattr(runtime_target, "live_continuity", None), "state", "")
    ).upper()
    if continuity_state != "RECONCILE_ONLY":
        raise SchwabReconciliationReadError(
            "Schwab reconciliation is only available for a frozen baseline."
        )


def validate_reconciliation_candidate(candidate: object) -> dict[str, object]:
    """Require the canonical redacted QPK receipt before reporting success."""

    try:
        payload = candidate.to_safe_dict()
        evidence_payload = payload["evidence"]
        evidence = BrokerReconciliationEvidence.from_dict(evidence_payload)
    except Exception as exc:
        raise SchwabReconciliationReadError(
            "Schwab reconciliation receipt is invalid."
        ) from exc
    if set(payload) != _SAFE_CANDIDATE_KEYS:
        raise SchwabReconciliationReadError("Schwab reconciliation receipt is invalid.")
    if payload.get("schema_version") != "schwab_reconciliation_candidate.v1":
        raise SchwabReconciliationReadError("Schwab reconciliation receipt is invalid.")
    if evidence.platform_id != "schwab":
        raise SchwabReconciliationReadError("Schwab reconciliation receipt is invalid.")
    normalized = dict(payload)
    normalized["evidence"] = evidence.to_dict()
    return normalized


def _text(value: object) -> str:
    return str(value or "").strip()


def _number(value: object, *, field_name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SchwabReconciliationReadError(f"Schwab reconciliation is missing {field_name}.") from exc
    if not math.isfinite(number):
        raise SchwabReconciliationReadError(f"Schwab reconciliation has non-finite {field_name}.")
    return number


def _response_json(response: object, *, surface: str) -> object:
    if getattr(response, "status_code", None) not in {200, 201}:
        raise SchwabReconciliationReadError(f"Schwab reconciliation could not read {surface}.")
    try:
        return response.json()
    except Exception as exc:
        raise SchwabReconciliationReadError(f"Schwab reconciliation received invalid {surface}.") from exc


def _canonical_records(records: list[Mapping[str, object]]) -> tuple[dict[str, object], ...]:
    return tuple(sorted((dict(item) for item in records), key=lambda item: json.dumps(item, sort_keys=True)))


def _normalize_position(position: object) -> dict[str, object]:
    symbol = _text(getattr(position, "symbol", "")).upper()
    if not symbol:
        raise SchwabReconciliationReadError("Schwab reconciliation received a position without a symbol.")
    return {
        "symbol": symbol,
        "quantity": _number(getattr(position, "quantity", None), field_name="position quantity"),
        "market_value": _number(getattr(position, "market_value", None), field_name="position market value"),
    }


def _normalize_order_leg(leg: Mapping[str, object]) -> dict[str, object]:
    instrument = leg.get("instrument")
    if not isinstance(instrument, Mapping):
        raise SchwabReconciliationReadError("Schwab reconciliation received an order leg without an instrument.")
    symbol = _text(instrument.get("symbol")).upper()
    if not symbol:
        raise SchwabReconciliationReadError("Schwab reconciliation received an order leg without a symbol.")
    return {
        "instruction": _text(leg.get("instruction")).upper(),
        "quantity": _number(leg.get("quantity"), field_name="order leg quantity"),
        "quantity_type": _text(leg.get("quantityType")).upper(),
        "symbol": symbol,
        "asset_type": _text(instrument.get("assetType")).upper(),
    }


def _normalize_order(order: Mapping[str, object]) -> dict[str, object]:
    legs = order.get("orderLegCollection") or ()
    if not isinstance(legs, list):
        raise SchwabReconciliationReadError("Schwab reconciliation received invalid order legs.")
    order_id = _text(order.get("orderId"))
    status = _text(order.get("status")).upper()
    if not order_id or not status:
        raise SchwabReconciliationReadError("Schwab reconciliation received an order without id or status.")
    return {
        "order_id": order_id,
        "status": status,
        "order_type": _text(order.get("orderType")).upper(),
        "strategy_type": _text(order.get("orderStrategyType")).upper(),
        "entered_time": _text(order.get("enteredTime")),
        "close_time": _text(order.get("closeTime")),
        "filled_quantity": _number(order.get("filledQuantity", 0.0), field_name="filled quantity"),
        "remaining_quantity": _number(order.get("remainingQuantity", 0.0), field_name="remaining quantity"),
        "legs": list(_canonical_records([_normalize_order_leg(leg) for leg in legs if isinstance(leg, Mapping)])),
    }


@dataclass(frozen=True)
class SchwabReconciliationObservations:
    """Sensitive in-memory state.  Never serialize this object to a response."""

    account_scope: Mapping[str, object]
    account_identity_match: bool
    positions: tuple[Mapping[str, object], ...]
    cash: Mapping[str, object]
    open_orders: tuple[Mapping[str, object], ...]
    recent_executions: tuple[Mapping[str, object], ...]


@dataclass(frozen=True)
class SchwabReconciliationCandidate:
    evidence: BrokerReconciliationEvidence
    recovery_blockers: tuple[BrokerReconciliationFinding, ...]
    expected_digests_configured: bool
    execution_ledger_records_count: int

    @property
    def permits_active_lkg(self) -> bool:
        return not self.recovery_blockers

    def to_safe_dict(self) -> dict[str, object]:
        return {
            "schema_version": "schwab_reconciliation_candidate.v1",
            "permits_active_lkg": self.permits_active_lkg,
            "expected_digests_configured": self.expected_digests_configured,
            "execution_ledger_records_count": self.execution_ledger_records_count,
            "recovery_blockers": [finding.value for finding in self.recovery_blockers],
            "evidence": self.evidence.to_dict(),
        }


def collect_read_only_reconciliation_observations(
    client: Any,
    *,
    fetch_account_snapshot: Callable[..., Any],
    now: datetime | None = None,
    lookback: timedelta = timedelta(days=7),
) -> SchwabReconciliationObservations:
    """Read account and order surfaces without submitting or changing anything."""

    get_account_numbers = getattr(client, "get_account_numbers", None)
    get_orders_for_account = getattr(client, "get_orders_for_account", None)
    if not callable(get_account_numbers) or not callable(get_orders_for_account):
        raise SchwabReconciliationReadError("Schwab reconciliation requires read-only account and order-history support.")
    account_numbers = _response_json(get_account_numbers(), surface="account identity")
    if not isinstance(account_numbers, list) or not account_numbers:
        raise SchwabReconciliationReadError("Schwab reconciliation received no account identities.")
    known_hashes = {_text(item.get("hashValue")) for item in account_numbers if isinstance(item, Mapping)} - {""}
    snapshot = fetch_account_snapshot(client, strategy_symbols=())
    metadata = getattr(snapshot, "metadata", {})
    account_hash = _text(metadata.get("account_hash") if isinstance(metadata, Mapping) else "")
    if not account_hash:
        raise SchwabReconciliationReadError("Schwab reconciliation snapshot is missing account identity.")
    reference_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    try:
        orders_payload = _response_json(
            get_orders_for_account(account_hash, from_entered_datetime=reference_now - lookback, to_entered_datetime=reference_now),
            surface="recent orders",
        )
    except TypeError as exc:
        raise SchwabReconciliationReadError("Schwab reconciliation requires bounded read-only order-history support.") from exc
    if not isinstance(orders_payload, list) or any(not isinstance(item, Mapping) for item in orders_payload):
        raise SchwabReconciliationReadError("Schwab reconciliation received invalid recent orders.")
    normalized_orders = [_normalize_order(order) for order in orders_payload]
    open_orders = [order for order in normalized_orders if order["status"] not in _TERMINAL_ORDER_STATUSES]
    recent_executions = [order for order in normalized_orders if order["status"] == "FILLED" or float(order["filled_quantity"]) > 0.0]
    cash = {
        "cash_balance": _number(getattr(snapshot, "cash_balance", None), field_name="cash balance"),
        "buying_power": _number(getattr(snapshot, "buying_power", None), field_name="buying power"),
        "total_equity": _number(getattr(snapshot, "total_equity", None), field_name="total equity"),
    }
    return SchwabReconciliationObservations(
        account_scope={"account_hash": account_hash},
        account_identity_match=account_hash in known_hashes,
        positions=_canonical_records([_normalize_position(position) for position in (getattr(snapshot, "positions", ()) or ())]),
        cash=cash,
        open_orders=_canonical_records(open_orders),
        recent_executions=_canonical_records(recent_executions),
    )


def _expected_digests(*, env_reader: Callable[[str, str | None], str | None] = os.getenv) -> Mapping[str, str] | None:
    raw = _text(env_reader(SCHWAB_RECONCILIATION_EXPECTED_DIGESTS_ENV, None))
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SchwabReconciliationReadError("Schwab reconciliation expected digests are invalid JSON.") from exc
    if not isinstance(value, Mapping) or set(value) != set(_EXPECTED_DIGEST_KEYS):
        raise SchwabReconciliationReadError("Schwab reconciliation expected digests are incomplete.")
    normalized = {key: _text(value[key]).lower().removeprefix("sha256:") for key in _EXPECTED_DIGEST_KEYS}
    if any(len(digest) != 64 for digest in normalized.values()):
        raise SchwabReconciliationReadError("Schwab reconciliation expected digests are invalid.")
    return normalized


def _continuity_fields(runtime_target: Any) -> tuple[str, str, str]:
    continuity = getattr(runtime_target, "live_continuity", None)
    baseline_id = _text(getattr(continuity, "baseline_id", ""))
    baseline_target_sha256 = _text(getattr(continuity, "baseline_target_sha256", "")).lower()
    if continuity is None or not baseline_id or len(baseline_target_sha256) != 64:
        raise SchwabReconciliationReadError("Schwab reconciliation requires a frozen live-continuity baseline.")
    return baseline_id, baseline_target_sha256, baseline_target_sha256


def build_reconciliation_candidate(
    *,
    observations: SchwabReconciliationObservations,
    runtime_target: Any,
    project_id: str | None,
    env_reader: Callable[[str, str | None], str | None] = os.getenv,
    observed_at: datetime | None = None,
) -> SchwabReconciliationCandidate:
    """Return redacted evidence; absence of a trusted baseline remains blocked."""

    expected = _expected_digests(env_reader=env_reader)
    platform_id = _text(getattr(runtime_target, "platform_id", ""))
    strategy_profile = _text(getattr(runtime_target, "strategy_profile", ""))
    account_scope = _text(getattr(runtime_target, "account_scope", ""))
    if not platform_id or not strategy_profile or not account_scope:
        raise SchwabReconciliationReadError("Schwab reconciliation runtime target is incomplete.")
    baseline_id, baseline_target_sha256, runtime_target_sha256 = _continuity_fields(runtime_target)
    digests = {
        "positions_sha256": calculate_broker_observation_sha256(observations.positions),
        "cash_sha256": calculate_broker_observation_sha256(observations.cash),
        "open_orders_sha256": calculate_broker_observation_sha256(observations.open_orders),
        "recent_executions_sha256": calculate_broker_observation_sha256(observations.recent_executions),
    }
    marker_store = build_execution_marker_store_from_env(platform_env_prefix="SCHWAB", env_reader=env_reader, project_id=project_id)
    ledger_digest, records_count = marker_store.calculate_recent_ledger_digest(
        platform=platform_id, strategy_profile=strategy_profile, account_scope=account_scope, execution_mode="live"
    )
    digests["local_execution_ledger_sha256"] = ledger_digest
    timestamp = observed_at or datetime.now(timezone.utc)
    evidence = build_broker_reconciliation_evidence(
        platform_id=platform_id,
        strategy_profile=strategy_profile,
        account_scope_sha256=calculate_broker_observation_sha256(observations.account_scope),
        baseline_id=baseline_id,
        baseline_target_sha256=baseline_target_sha256,
        runtime_target_sha256=runtime_target_sha256,
        observed_at=timestamp,
        broker_connected=True,
        account_identity_match=observations.account_identity_match,
        positions_match=expected is not None and expected["positions_sha256"] == digests["positions_sha256"],
        cash_match=expected is not None and expected["cash_sha256"] == digests["cash_sha256"],
        open_orders_match=expected is not None and expected["open_orders_sha256"] == digests["open_orders_sha256"],
        recent_executions_match=expected is not None and expected["recent_executions_sha256"] == digests["recent_executions_sha256"],
        local_execution_ledger_match=expected is not None and expected["local_execution_ledger_sha256"] == digests["local_execution_ledger_sha256"],
        **digests,
    )
    blockers = evaluate_broker_reconciliation_recovery(
        evidence,
        now=timestamp,
        expected_platform_id=platform_id,
        expected_strategy_profile=strategy_profile,
        expected_account_scope_sha256=(expected or {}).get("account_scope_sha256"),
        expected_baseline_id=baseline_id,
        expected_runtime_target_sha256=runtime_target_sha256,
        baseline_reference_available=expected is not None,
        **{
            f"expected_{key}": (expected or {}).get(key)
            for key in _EXPECTED_DIGEST_KEYS
            if key != "account_scope_sha256"
        },
    )
    return SchwabReconciliationCandidate(
        evidence=evidence,
        recovery_blockers=blockers,
        expected_digests_configured=expected is not None,
        execution_ledger_records_count=records_count,
    )
