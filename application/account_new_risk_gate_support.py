"""Charles Schwab adapter for QPK account-level NEW_RISK gate (fail-closed)."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any

from quant_platform_kit.risk.account_new_risk_gate import (
    AccountNewRiskGateError,
    InjectedReconciliationSnapshot,
    NewRiskAdmissionResult,
    NewRiskDisposition,
    evaluate_new_risk_admission,
)
from quant_platform_kit.risk.cycle_new_risk_health import (
    CycleNewRiskHealthEvidence,
    apply_cycle_new_risk_health_axes,
)

ACCOUNT_NEW_RISK_GATE_ENV = "ACCOUNT_NEW_RISK_GATE"

_cycle_snapshot: InjectedReconciliationSnapshot | None = None


def is_account_new_risk_gate_enabled() -> bool:
    """Production default on; set ACCOUNT_NEW_RISK_GATE=0 only for tests."""
    return str(os.environ.get(ACCOUNT_NEW_RISK_GATE_ENV, "") or "").strip() != "0"


def _coerce_optional_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _resolve_equity_usd(portfolio: Mapping[str, Any], execution: Mapping[str, Any] | None) -> float | None:
    """Map Schwab account equity from broker liquidation value or plan portfolio."""
    for key in ("total_equity", "total_strategy_equity"):
        equity = _coerce_optional_float(portfolio.get(key))
        if equity is not None and equity > 0.0:
            return equity
    metadata = portfolio.get("metadata")
    if isinstance(metadata, Mapping):
        if metadata.get("total_equity_source") == "broker_liquidation_value":
            equity = _coerce_optional_float(portfolio.get("total_equity"))
            if equity is not None and equity > 0.0:
                return equity
    broker_capital = portfolio.get("broker_capital")
    if isinstance(broker_capital, Mapping):
        equity = _coerce_optional_float(broker_capital.get("net_assets"))
        if equity is not None and equity > 0.0:
            return equity
    if execution is not None:
        equity = _coerce_optional_float(execution.get("portfolio_total_equity"))
        if equity is not None and equity > 0.0:
            return equity
    return None


def _is_explicit_open(value: object) -> bool:
    return str(value or "").strip().upper() == "OPEN"


def _mapping_or_empty(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _build_cycle_new_risk_health_evidence(
    portfolio: Mapping[str, Any],
    *,
    execution: Mapping[str, Any] | None,
    projection: Mapping[str, Any],
    equity_usd: float | None,
) -> CycleNewRiskHealthEvidence:
    """Derive cycle-health evidence from portfolio/execution facts available this cycle.

    Only explicit durable/metadata flags count as durable breaker evidence; the
    fail-closed default (missing equity) must never be mistaken for a durable
    circuit-breaker trip.
    """
    metadata = _mapping_or_empty(portfolio.get("metadata"))
    execution_metadata = _mapping_or_empty(_mapping_or_empty(execution).get("metadata"))

    unknown_pending = bool(
        portfolio.get("unknown_pending_orders")
        or metadata.get("unknown_pending_orders")
        or portfolio.get("pending_reconciliation")
        or metadata.get("pending_reconciliation")
        or _mapping_or_empty(execution).get("pending_reconciliation")
        or execution_metadata.get("pending_reconciliation")
    )

    durable_breaker_open = (
        _is_explicit_open(portfolio.get("durable_circuit_breaker_state"))
        or _is_explicit_open(metadata.get("durable_circuit_breaker_state"))
        or _is_explicit_open(projection.get("circuit_breaker_state"))
    )

    digests_configured = bool(projection.get("digests_configured") or metadata.get("digests_configured"))
    digests_verified = bool(
        projection.get("digests_verified")
        or metadata.get("digests_verified")
        or projection.get("permits_active_lkg")
        or metadata.get("permits_active_lkg")
    )

    cycle_trip_open = bool(projection.get("cycle_trip_open") or metadata.get("cycle_trip_open"))

    return CycleNewRiskHealthEvidence(
        observation_ok=equity_usd is not None and equity_usd > 0.0,
        unknown_pending=unknown_pending,
        digests_configured=digests_configured,
        digests_verified=digests_verified,
        durable_breaker_open=durable_breaker_open,
        cycle_trip_open=cycle_trip_open,
    )


def build_account_new_risk_snapshot(
    portfolio: Mapping[str, Any],
    *,
    execution: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an explicit fail-closed projection from evidence available this cycle.

    Resolves equity first, then projects cycle-health axes (observation /
    reconciliation / circuit-breaker) from evidence available this cycle.
    Explicit keys already present on the incoming ``account_new_risk_snapshot``
    always win over the projected axes (tests / HITL overrides). Missing or
    non-positive equity keeps ``observation_ok=False``, which fails closed via
    ``EQUITY_UNKNOWN_FAIL_CLOSED`` in the gate regardless of the projected axes.
    """
    projection = dict(portfolio.get("account_new_risk_snapshot") or {})
    if "equity_usd" in projection:
        equity_usd = _coerce_optional_float(projection.get("equity_usd"))
    else:
        equity_usd = _resolve_equity_usd(portfolio, execution)

    evidence = _build_cycle_new_risk_health_evidence(
        portfolio,
        execution=execution,
        projection=projection,
        equity_usd=equity_usd,
    )
    projection = apply_cycle_new_risk_health_axes(projection, evidence)
    projection["equity_usd"] = equity_usd
    return projection



def _resolve_production_drift_status(portfolio: Mapping[str, Any], projection: Mapping[str, Any]) -> str | None:
    raw = projection.get("production_drift_status")
    if raw is None or raw == "":
        raw = portfolio.get("production_drift_status")
    if raw is None or raw == "":
        return None
    return str(raw).strip()


def maybe_inject_production_drift_status(
    portfolio: Mapping[str, Any],
    *,
    strategy_profile: str | None,
    domain: str | None,
    store=None,
) -> dict[str, Any]:
    """Return a shallow-copied portfolio with production_drift_status filled from store when absent.

    Explicit account_new_risk_snapshot.production_drift_status or portfolio.production_drift_status wins.
    Missing profile/domain, parked store, or any exception → leave unchanged (omit; never invent CRITICAL).
    Never optimizes or grants live.
    """
    out = dict(portfolio)
    projection = dict(out.get("account_new_risk_snapshot") or {})
    if _resolve_production_drift_status(out, projection) is not None:
        return out
    profile = str(strategy_profile or "").strip() or None
    domain_key = str(domain or "").strip() or None
    if not profile or not domain_key:
        return out
    try:
        from quant_platform_kit.risk.production_drift_new_risk import (
            resolve_production_drift_status_from_store,
        )

        status = resolve_production_drift_status_from_store(
            strategy_profile=profile,
            domain=domain_key,
            store=store,
        )
    except Exception:
        return out
    if status:
        projection["production_drift_status"] = status
        out["account_new_risk_snapshot"] = projection
    return out


def build_snapshot_from_portfolio(
    portfolio: Mapping[str, Any],
    *,
    execution: Mapping[str, Any] | None = None,
) -> InjectedReconciliationSnapshot:
    """Project an injected reconciliation snapshot from an existing portfolio read."""
    projection = build_account_new_risk_snapshot(portfolio, execution=execution)
    equity_usd = _coerce_optional_float(projection.get("equity_usd"))
    if equity_usd is None:
        equity_usd = _resolve_equity_usd(portfolio, execution)
    return InjectedReconciliationSnapshot(
        observation_status=str(projection.get("observation_status") or "UNAVAILABLE"),
        reconciliation_status=str(projection.get("reconciliation_status") or "UNVERIFIED"),
        circuit_breaker_state=str(projection.get("circuit_breaker_state") or "OPEN"),
        equity_usd=equity_usd,
        peak_equity_usd=_coerce_optional_float(projection.get("peak_equity_usd"))
        if "peak_equity_usd" in projection
        else _coerce_optional_float(portfolio.get("peak_equity_usd")),
        drawdown_from_peak=_coerce_optional_float(projection.get("drawdown_from_peak"))
        if "drawdown_from_peak" in projection
        else _coerce_optional_float(portfolio.get("drawdown_from_peak")),
        realized_vol=_coerce_optional_float(projection.get("realized_vol"))
        if "realized_vol" in projection
        else _coerce_optional_float(portfolio.get("realized_vol")),
        production_drift_status=_resolve_production_drift_status(portfolio, projection),
    )


def evaluate_portfolio_new_risk_admission(
    portfolio: Mapping[str, Any],
    *,
    execution: Mapping[str, Any] | None = None,
) -> NewRiskAdmissionResult:
    try:
        snapshot = build_snapshot_from_portfolio(portfolio, execution=execution)
        return evaluate_new_risk_admission(snapshot)
    except AccountNewRiskGateError:
        return NewRiskAdmissionResult(
            disposition=NewRiskDisposition.NEW_RISK_PROHIBITED,
            reason_codes=("SNAPSHOT_VALIDATION_FAIL_CLOSED",),
        )


def new_risk_buy_prohibited(result: NewRiskAdmissionResult) -> bool:
    return result.disposition == NewRiskDisposition.NEW_RISK_PROHIBITED


def apply_combined_scale(value: float, scale: float | None) -> float:
    """Apply a valid reducing scale; missing or out-of-range values are a no-op."""
    if scale is None or not math.isfinite(scale) or not 0.0 < scale <= 1.0:
        return value
    return value * scale


def get_cycle_snapshot() -> InjectedReconciliationSnapshot | None:
    return _cycle_snapshot


def set_cycle_snapshot(snapshot: InjectedReconciliationSnapshot | None) -> None:
    global _cycle_snapshot
    _cycle_snapshot = snapshot


@contextmanager
def account_new_risk_gate_cycle(portfolio: Mapping[str, Any], *, execution: Mapping[str, Any] | None = None):
    """Bind one portfolio projection for the current execution cycle."""
    previous = _cycle_snapshot
    set_cycle_snapshot(build_snapshot_from_portfolio(portfolio, execution=execution))
    try:
        yield
    finally:
        set_cycle_snapshot(previous)


def evaluate_cycle_new_risk_admission() -> NewRiskAdmissionResult:
    if _cycle_snapshot is None:
        return NewRiskAdmissionResult(
            disposition=NewRiskDisposition.NEW_RISK_PROHIBITED,
            reason_codes=("EQUITY_UNKNOWN_FAIL_CLOSED",),
        )
    try:
        return evaluate_new_risk_admission(_cycle_snapshot)
    except AccountNewRiskGateError:
        return NewRiskAdmissionResult(
            disposition=NewRiskDisposition.NEW_RISK_PROHIBITED,
            reason_codes=("SNAPSHOT_VALIDATION_FAIL_CLOSED",),
        )
