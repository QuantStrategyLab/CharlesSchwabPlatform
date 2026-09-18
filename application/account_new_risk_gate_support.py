"""Charles Schwab adapter for QPK account-level NEW_RISK gate (fail-closed)."""

from __future__ import annotations

import json
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
from quant_platform_kit.risk.contracts import RuntimeRiskLimits
from quant_platform_kit.risk.cycle_new_risk_health import (
    CycleNewRiskHealthEvidence,
    apply_cycle_new_risk_health_axes,
)
from quant_platform_kit.risk.production_drift_new_risk import (
    resolve_production_drift_status_from_store,
)

ACCOUNT_NEW_RISK_GATE_ENV = "ACCOUNT_NEW_RISK_GATE"
_MAX_DAILY_LOSS_ENV_KEYS = ("SCHWAB_MAX_DAILY_LOSS_USD", "MAX_DAILY_LOSS_USD")

_DEFAULT_STRATEGY_PROFILE = "soxl_soxx_trend_income"
_DEFAULT_DOMAIN = "us_equity"

# Minimal carrier for gate-only daily-loss axis; not a production RRL binding.
_DAILY_LOSS_LIMIT_CARRIER_SYMBOL = "SPY"

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


def _positive_limit_or_none(value: object) -> float | None:
    """Accept only finite positive limits; never invent a production default."""
    number = _coerce_optional_float(value)
    if number is None or number <= 0.0:
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
    """Prefer explicit inject; else read-only PerformanceStore (Policy A). Fail-soft."""
    for source in (projection, portfolio, _mapping_or_empty(portfolio.get("metadata"))):
        raw = source.get("production_drift_status")
        if raw is not None and raw != "":
            return str(raw).strip()
    profile = ""
    domain = ""
    for source in (projection, portfolio, _mapping_or_empty(portfolio.get("metadata"))):
        if not profile:
            value = source.get("strategy_profile")
            if isinstance(value, str) and value.strip():
                profile = value.strip()
        if not domain:
            value = source.get("strategy_domain") or source.get("domain")
            if isinstance(value, str) and value.strip():
                domain = value.strip()
    profile = profile or str(os.environ.get("STRATEGY_PROFILE") or "").strip() or _DEFAULT_STRATEGY_PROFILE
    domain = domain or str(os.environ.get("STRATEGY_DOMAIN") or "").strip() or _DEFAULT_DOMAIN
    return resolve_production_drift_status_from_store(
        strategy_profile=profile,
        domain=domain,
    )


def _resolve_drawdown_from_peak(
    *,
    equity_usd: float | None,
    peak_equity_usd: float | None,
    explicit: float | None,
) -> float | None:
    if explicit is not None:
        return explicit
    if equity_usd is None or peak_equity_usd is None or peak_equity_usd <= 0.0:
        return None
    return max(0.0, 1.0 - (equity_usd / peak_equity_usd))


def _resolve_explicit_daily_loss_usd(
    projection: Mapping[str, Any],
    portfolio: Mapping[str, Any],
    execution: Mapping[str, Any] | None,
) -> float | None:
    """Pass through an explicit daily_loss_usd fact only; never invent one."""
    for source in (projection, portfolio, _mapping_or_empty(execution)):
        if "daily_loss_usd" in source:
            return _coerce_optional_float(source.get("daily_loss_usd"))
    return None


def _max_daily_loss_from_runtime_target_json() -> float | None:
    """Read max_daily_loss_usd from RUNTIME_TARGET_JSON when present; soft-omit on errors."""
    raw_target = os.environ.get("RUNTIME_TARGET_JSON")
    if raw_target is None or not str(raw_target).strip():
        return None
    try:
        payload = json.loads(raw_target)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    policy = payload.get("runtime_risk_limits")
    if not isinstance(policy, dict) or "max_daily_loss_usd" not in policy:
        return None
    return _positive_limit_or_none(policy.get("max_daily_loss_usd"))


def _runtime_risk_limits_policy() -> dict[str, Any] | None:
    raw_target = os.environ.get("RUNTIME_TARGET_JSON")
    if raw_target is None or not str(raw_target).strip():
        return None
    try:
        payload = json.loads(raw_target)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    policy = payload.get("runtime_risk_limits")
    return policy if isinstance(policy, dict) else None


def resolve_max_daily_loss_usd_from_equity_schedule(
    equity_usd: float | None,
    schedule: object,
) -> float | None:
    """Map account equity to an absolute daily-loss limit via a pct schedule.

    Schedule entries are ordered by ascending ``equity_lte_usd`` (optional on the
    final catch-all). Small-equity tiers may use a larger pct; large-equity tiers
    a smaller pct. No approved production default lives in code — omit when the
    schedule is absent/invalid or equity is missing/non-positive.
    """
    equity = _coerce_optional_float(equity_usd)
    if equity is None or equity <= 0.0:
        return None
    if not isinstance(schedule, list) or not schedule:
        return None

    tiers: list[tuple[float, float]] = []
    catch_all_pct: float | None = None
    for raw in schedule:
        if not isinstance(raw, Mapping):
            return None
        pct = _coerce_optional_float(raw.get("max_daily_loss_pct"))
        if pct is None or pct <= 0.0 or pct > 1.0:
            return None
        if "equity_lte_usd" not in raw or raw.get("equity_lte_usd") is None:
            if catch_all_pct is not None:
                return None
            catch_all_pct = pct
            continue
        ceiling = _coerce_optional_float(raw.get("equity_lte_usd"))
        if ceiling is None or ceiling <= 0.0:
            return None
        tiers.append((ceiling, pct))

    tiers.sort(key=lambda item: item[0])
    # Reject overlapping / non-increasing ceilings after sort duplicates.
    last_ceiling = 0.0
    for ceiling, _pct in tiers:
        if ceiling <= last_ceiling:
            return None
        last_ceiling = ceiling

    chosen_pct: float | None = None
    for ceiling, pct in tiers:
        if equity <= ceiling:
            chosen_pct = pct
            break
    if chosen_pct is None:
        chosen_pct = catch_all_pct
    if chosen_pct is None:
        return None
    return equity * chosen_pct


def resolve_max_daily_loss_usd_from_equity_formula(
    equity_usd: float | None,
    formula: object,
) -> float | None:
    """Smooth capital-dependent daily-loss budget.

    ``p(E) = pct_min + (pct_max - pct_min) * equity_scale / (equity_scale + E)``
    ``limit = p(E) * E``

    As equity shrinks, allowed daily-loss fraction approaches ``pct_max``; as equity
    grows it approaches ``pct_min``. ``equity_scale_usd`` is the transition scale
    (near E≈scale, fraction is about midway). No production defaults in code.
    """
    equity = _coerce_optional_float(equity_usd)
    if equity is None or equity <= 0.0:
        return None
    if not isinstance(formula, Mapping):
        return None
    pct_max = _coerce_optional_float(formula.get("pct_max"))
    pct_min = _coerce_optional_float(formula.get("pct_min"))
    scale = _coerce_optional_float(
        formula.get("equity_scale_usd")
        if "equity_scale_usd" in formula
        else formula.get("E0_usd")
    )
    if pct_max is None or pct_min is None or scale is None:
        return None
    if not (0.0 < pct_min <= pct_max <= 1.0):
        return None
    if scale <= 0.0:
        return None
    pct = pct_min + (pct_max - pct_min) * (scale / (scale + equity))
    return equity * pct


def _equity_for_daily_loss_schedule(
    portfolio: Mapping[str, Any] | None,
) -> float | None:
    """Prefer session baseline equity when the fact producer attached it."""
    if portfolio is None:
        return None
    projection = _mapping_or_empty(portfolio.get("account_new_risk_snapshot"))
    for key in ("daily_loss_baseline_equity_usd", "equity_usd"):
        equity = _coerce_optional_float(projection.get(key))
        if equity is not None and equity > 0.0:
            return equity
    return _resolve_equity_usd(portfolio, None)


def resolve_max_daily_loss_usd(
    portfolio: Mapping[str, Any] | None = None,
) -> float | None:
    """Resolve max_daily_loss_usd; omit the axis when unset.

    Priority:
    1. explicit absolute ``max_daily_loss_usd`` on snapshot/portfolio
    2. RUNTIME_TARGET absolute ``runtime_risk_limits.max_daily_loss_usd``
    3. RUNTIME_TARGET ``runtime_risk_limits.max_daily_loss_equity_formula`` (smooth)
    4. RUNTIME_TARGET ``runtime_risk_limits.max_daily_loss_equity_schedule`` (tiers)
    5. SCHWAB_MAX_DAILY_LOSS_USD / MAX_DAILY_LOSS_USD env absolutes

    No approved production default in code.
    """
    if portfolio is not None:
        projection = _mapping_or_empty(portfolio.get("account_new_risk_snapshot"))
        for source in (projection, portfolio):
            if "max_daily_loss_usd" in source:
                return _positive_limit_or_none(source.get("max_daily_loss_usd"))
    policy_limit = _max_daily_loss_from_runtime_target_json()
    if policy_limit is not None:
        return policy_limit
    policy = _runtime_risk_limits_policy()
    equity = _equity_for_daily_loss_schedule(portfolio)
    if policy is not None and "max_daily_loss_equity_formula" in policy:
        from_formula = resolve_max_daily_loss_usd_from_equity_formula(
            equity,
            policy.get("max_daily_loss_equity_formula"),
        )
        if from_formula is not None:
            return from_formula
    if policy is not None and "max_daily_loss_equity_schedule" in policy:
        scheduled = resolve_max_daily_loss_usd_from_equity_schedule(
            equity,
            policy.get("max_daily_loss_equity_schedule"),
        )
        if scheduled is not None:
            return scheduled
    for key in _MAX_DAILY_LOSS_ENV_KEYS:
        raw = os.environ.get(key)
        if raw is None or not str(raw).strip():
            continue
        limit = _positive_limit_or_none(raw)
        if limit is not None:
            return limit
    return None


def runtime_risk_limits_for_daily_loss_axis(
    max_daily_loss_usd: float | None,
) -> RuntimeRiskLimits | None:
    """Build admission-only limits carrying ``max_daily_loss_usd``, or omit.

    SPY/1.0 caps are a minimal legal RuntimeRiskLimits carrier for the gate only —
    not a production RRL binding and not an exposure raise.
    """
    if max_daily_loss_usd is None:
        return None
    symbol = _DAILY_LOSS_LIMIT_CARRIER_SYMBOL
    return RuntimeRiskLimits(
        allowed_symbols=(symbol,),
        product_leverage_factors={symbol: 1},
        nominal_caps={symbol: 1.0},
        total_nominal_exposure_cap=1.0,
        total_effective_exposure_cap=1.0,
        max_positions=1,
        max_daily_loss_usd=max_daily_loss_usd,
    )


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
    peak_equity_usd = (
        _coerce_optional_float(projection.get("peak_equity_usd"))
        if "peak_equity_usd" in projection
        else _coerce_optional_float(portfolio.get("peak_equity_usd"))
    )
    explicit_dd = (
        _coerce_optional_float(projection.get("drawdown_from_peak"))
        if "drawdown_from_peak" in projection
        else _coerce_optional_float(portfolio.get("drawdown_from_peak"))
    )
    return InjectedReconciliationSnapshot(
        observation_status=str(projection.get("observation_status") or "UNAVAILABLE"),
        reconciliation_status=str(projection.get("reconciliation_status") or "UNVERIFIED"),
        circuit_breaker_state=str(projection.get("circuit_breaker_state") or "OPEN"),
        equity_usd=equity_usd,
        peak_equity_usd=peak_equity_usd,
        drawdown_from_peak=_resolve_drawdown_from_peak(
            equity_usd=equity_usd,
            peak_equity_usd=peak_equity_usd,
            explicit=explicit_dd,
        ),
        realized_vol=_coerce_optional_float(projection.get("realized_vol"))
        if "realized_vol" in projection
        else _coerce_optional_float(portfolio.get("realized_vol")),
        production_drift_status=_resolve_production_drift_status(portfolio, projection),
        daily_loss_usd=_resolve_explicit_daily_loss_usd(projection, portfolio, execution),
    )


def evaluate_portfolio_new_risk_admission(
    portfolio: Mapping[str, Any],
    *,
    execution: Mapping[str, Any] | None = None,
) -> NewRiskAdmissionResult:
    try:
        snapshot = build_snapshot_from_portfolio(portfolio, execution=execution)
        limits = runtime_risk_limits_for_daily_loss_axis(resolve_max_daily_loss_usd(portfolio))
        return evaluate_new_risk_admission(snapshot, limits)
    except AccountNewRiskGateError:
        return NewRiskAdmissionResult(
            disposition=NewRiskDisposition.NEW_RISK_PROHIBITED,
            reason_codes=("SNAPSHOT_VALIDATION_FAIL_CLOSED",),
        )


def new_risk_buy_prohibited(result: NewRiskAdmissionResult) -> bool:
    return result.disposition == NewRiskDisposition.NEW_RISK_PROHIBITED


def apply_combined_scale_to_allocation_targets(
    allocation: Mapping[str, Any] | None,
    combined_scale: float | None,
) -> dict[str, Any]:
    """Shrink allocation targets by admission combined_scale; omit when scale missing."""
    from quant_platform_kit.risk.capital_risk_envelope import apply_combined_scale_to_targets

    allocation_out = dict(allocation or {})
    allocation_out["targets"] = apply_combined_scale_to_targets(
        allocation_out.get("targets"),
        combined_scale,
    )
    return allocation_out



_ATTENTION_PLATFORM = "schwab"
_OPERATIONAL_UNCERTAIN_REASONS = frozenset(
    {
        "EQUITY_UNKNOWN_FAIL_CLOSED",
        "SNAPSHOT_VALIDATION_FAIL_CLOSED",
        "RECONCILIATION_NOT_VERIFIED",
        "CIRCUIT_BREAKER_OPEN",
        "UNKNOWN_PENDING_ORDERS",
    }
)
_attention_sent_keys: set[str] = set()


def _resolve_attention_strategy_profile(portfolio: Mapping[str, Any]) -> str:
    projection = _mapping_or_empty(portfolio.get("account_new_risk_snapshot"))
    for source in (projection, portfolio, _mapping_or_empty(portfolio.get("metadata"))):
        value = source.get("strategy_profile")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return str(os.environ.get("STRATEGY_PROFILE") or "").strip() or _DEFAULT_STRATEGY_PROFILE


def _resolve_attention_account_alias(
    portfolio: Mapping[str, Any],
    execution: Mapping[str, Any] | None,
) -> str:
    for source in (
        portfolio,
        _mapping_or_empty(portfolio.get("metadata")),
        _mapping_or_empty(execution),
    ):
        for key in ("account_alias", "account_hash", "account_id", "account"):
            value = source.get(key)
            if value is not None and str(value).strip():
                text = str(value).strip()
                return text[-8:] if len(text) > 8 else text
    for env_key in ("SCHWAB_ACCOUNT_HASH", "ACCOUNT_ALIAS"):
        env_alias = str(os.environ.get(env_key) or "").strip()
        if env_alias:
            return env_alias[-8:] if len(env_alias) > 8 else env_alias
    return "unknown"


def maybe_publish_attention_for_admission(
    admission: NewRiskAdmissionResult,
    *,
    portfolio: Mapping[str, Any],
    execution: Mapping[str, Any] | None = None,
    snapshot: InjectedReconciliationSnapshot | None = None,
    telegram_sender: Any | None = None,
    log_message: Any = print,
) -> Mapping[str, int]:
    """Publish ACTION/HALT attention when NEW_RISK / ops axes require a page.

    Dedupes on transition keys within the process. Never grants live, raises RRL,
    or invents daily-loss facts.
    """

    try:
        from quant_platform_kit.risk.attention import (
            AttentionAxes,
            evaluate_attention,
            resolve_mandate_dd_budget,
        )
        from quant_platform_kit.risk.attention_notify import publish_attention_telegram_transition
    except ImportError:
        try:
            log_message("attention_telegram_skipped reason=attention_api_unavailable")
        except TypeError:
            log_message("attention_telegram_skipped reason=attention_api_unavailable", flush=True)
        return {"sent": 0, "skipped": 1, "failed": 0}

    reasons = tuple(admission.reason_codes or ())
    prohibited = new_risk_buy_prohibited(admission)
    operational_uncertain = any(code in _OPERATIONAL_UNCERTAIN_REASONS for code in reasons)
    profile = _resolve_attention_strategy_profile(portfolio)
    drawdown = None if snapshot is None else snapshot.drawdown_from_peak
    decision = evaluate_attention(
        AttentionAxes(
            new_risk_prohibited=True if prohibited else None,
            operational_uncertain=True if operational_uncertain else None,
            drawdown_from_peak=drawdown,
            mandate_dd_budget=resolve_mandate_dd_budget(profile),
        )
    )
    return publish_attention_telegram_transition(
        decision=decision,
        platform=_ATTENTION_PLATFORM,
        account_alias=_resolve_attention_account_alias(portfolio, execution),
        strategy_profile=profile,
        previous_level=None,
        already_sent_keys=list(_attention_sent_keys),
        record_sent_key=_attention_sent_keys.add,
        telegram_sender=telegram_sender,
        log_message=log_message,
    )


def reset_attention_sent_keys_for_tests() -> None:
    _attention_sent_keys.clear()


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
        limits = runtime_risk_limits_for_daily_loss_axis(resolve_max_daily_loss_usd())
        return evaluate_new_risk_admission(_cycle_snapshot, limits)
    except AccountNewRiskGateError:
        return NewRiskAdmissionResult(
            disposition=NewRiskDisposition.NEW_RISK_PROHIBITED,
            reason_codes=("SNAPSHOT_VALIDATION_FAIL_CLOSED",),
        )
