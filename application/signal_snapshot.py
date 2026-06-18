"""Shared signal snapshot payload helpers for platform reports."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from typing import Any

SIGNAL_SNAPSHOT_SCHEMA_VERSION = "signal_snapshot.v1"

_INDICATOR_FIELDS = (
    "benchmark_symbol",
    "benchmark_price",
    "long_trend_value",
    "exit_line",
    "active_risk_asset",
    "allocation_mode",
    "trend_symbol",
    "trend_price",
    "trend_ma",
    "trend_ma20",
    "trend_ma20_slope",
    "trend_rsi14",
    "trend_rsi14_dynamic_threshold",
    "trend_rsi14_effective_threshold",
    "trend_bb_upper",
    "blend_gate_volatility_delever_symbol",
    "blend_gate_volatility_delever_window",
    "blend_gate_volatility_delever_threshold_mode",
    "blend_gate_volatility_delever_threshold",
    "blend_gate_volatility_delever_dynamic_threshold",
    "blend_gate_volatility_delever_dynamic_sample_count",
    "blend_gate_volatility_delever_dynamic_lookback",
    "blend_gate_volatility_delever_dynamic_percentile",
    "blend_gate_volatility_delever_dynamic_min_periods",
    "blend_gate_volatility_delever_dynamic_floor",
    "blend_gate_volatility_delever_dynamic_cap",
    "blend_gate_volatility_delever_metric",
    "blend_gate_volatility_delever_triggered",
    "blend_gate_volatility_delever_retention_ratio",
    "blend_gate_volatility_delever_retention_mode",
    "blend_gate_volatility_delever_retention_policy",
    "blend_gate_volatility_delever_effective_retention_ratio",
    "blend_gate_volatility_delever_retention_source",
    "blend_gate_volatility_delever_retention_context_found",
    "blend_gate_volatility_delever_retention_reason_codes",
    "blend_gate_volatility_delever_redirect_symbol",
    "blend_gate_volatility_delever_removed_ratio",
    "dual_drive_volatility_delever_enabled",
    "dual_drive_volatility_delever_window",
    "dual_drive_volatility_delever_threshold_mode",
    "dual_drive_volatility_delever_threshold",
    "dual_drive_volatility_delever_exit_threshold",
    "dual_drive_volatility_delever_dynamic_threshold",
    "dual_drive_volatility_delever_dynamic_sample_count",
    "dual_drive_volatility_delever_dynamic_lookback",
    "dual_drive_volatility_delever_dynamic_percentile",
    "dual_drive_volatility_delever_dynamic_min_periods",
    "dual_drive_volatility_delever_dynamic_floor",
    "dual_drive_volatility_delever_dynamic_cap",
    "dual_drive_volatility_delever_metric",
    "dual_drive_volatility_delever_triggered",
    "dual_drive_volatility_delever_entry_triggered",
    "dual_drive_volatility_delever_hysteresis_triggered",
    "dual_drive_volatility_delever_trigger_reason",
    "dual_drive_volatility_delever_applied",
    "dual_drive_volatility_delever_vetoed",
    "dual_drive_volatility_delever_veto_reason",
    "dual_drive_volatility_delever_taco_veto_enabled",
    "dual_drive_volatility_delever_taco_rebound_context_active",
    "dual_drive_volatility_delever_true_crisis_active",
    "dual_drive_volatility_delever_retention_mode",
    "dual_drive_volatility_delever_retention_policy",
    "dual_drive_volatility_delever_retention_ratio",
    "dual_drive_volatility_delever_retention_source",
    "dual_drive_volatility_delever_retention_context_found",
    "dual_drive_volatility_delever_retention_reason_codes",
    "dual_drive_volatility_delever_redirect_symbol",
    "dual_drive_volatility_delever_source_value",
    "dual_drive_volatility_delever_retained_value",
    "dual_drive_volatility_delever_removed_value",
    "dual_drive_volatility_delever_retained_ratio",
    "dual_drive_volatility_delever_redirected_ratio",
    "dual_drive_macro_risk_governor_enabled",
    "dual_drive_macro_risk_governor_found",
    "dual_drive_macro_risk_governor_route",
    "dual_drive_macro_risk_governor_active",
    "dual_drive_macro_risk_governor_applied",
    "dual_drive_macro_risk_governor_leverage_scalar",
    "dual_drive_macro_risk_governor_risk_asset_scalar",
    "dual_drive_macro_risk_governor_removed_value",
    "dual_drive_macro_risk_governor_redirected_to_unlevered",
    "dual_drive_crisis_defense_enabled",
    "dual_drive_crisis_defense_triggered",
    "dual_drive_crisis_defense_applied",
    "dual_drive_crisis_defense_destination",
    "dual_drive_crisis_defense_removed_value",
    "market_regime_control_enabled",
    "market_regime_control_found",
    "market_regime_control_source",
    "market_regime_control_schema_version",
    "market_regime_control_route",
    "market_regime_control_route_source",
    "market_regime_control_active",
    "market_regime_control_applied",
    "market_regime_control_route_allowed",
    "market_regime_control_risk_scalar",
    "market_regime_control_risk_budget_scalar",
    "market_regime_control_leverage_scalar",
    "market_regime_control_risk_asset_scalar",
    "market_regime_control_taco_allowed",
    "market_regime_control_local_delever_veto_allowed",
    "market_regime_control_crisis_defense_required",
    "market_regime_control_blocked_actions",
    "market_regime_control_vetoes",
    "market_regime_control_reason_codes",
    "market_regime_control_removed_weight",
    "market_regime_control_removed_ratio",
    "market_regime_control_redirected_to_unlevered_ratio",
    "market_regime_control_safe_haven",
    "market_regime_control_risk_symbols",
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _first_value(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _merge_signal_sources(*sources: Mapping[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        annotations = source.get("execution_annotations")
        if isinstance(annotations, Mapping):
            merged.update(annotations)
        merged.update(source)
    return merged


def _normalized_numeric_mapping(value: Any) -> dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    normalized: dict[str, float] = {}
    for key, raw_value in value.items():
        symbol = str(key or "").strip().upper()
        if not symbol:
            continue
        try:
            normalized[symbol] = float(raw_value)
        except (TypeError, ValueError):
            continue
    return normalized


def _target_payload(
    *,
    allocation: Mapping[str, Any] | None,
    explicit_target_weights: Mapping[str, Any] | None,
) -> tuple[str | None, dict[str, float], dict[str, float]]:
    allocation = allocation if isinstance(allocation, Mapping) else {}
    target_mode = str(allocation.get("target_mode") or "").strip() or None
    targets = _normalized_numeric_mapping(explicit_target_weights or allocation.get("targets"))
    if target_mode == "value":
        return target_mode, {}, targets
    return target_mode, targets, {}


def build_signal_snapshot(
    *,
    platform: str,
    strategy_profile: str | None = None,
    generated_at: datetime | None = None,
    diagnostics: Mapping[str, Any] | None = None,
    execution: Mapping[str, Any] | None = None,
    allocation: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    target_weights: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = _merge_signal_sources(metadata, diagnostics, execution)
    target_mode, normalized_weights, normalized_values = _target_payload(
        allocation=allocation,
        explicit_target_weights=target_weights,
    )
    indicators = {
        field: _json_safe(source[field])
        for field in _INDICATOR_FIELDS
        if source.get(field) not in (None, "")
    }
    snapshot = {
        "schema_version": SIGNAL_SNAPSHOT_SCHEMA_VERSION,
        "platform": str(platform or "").strip(),
        "strategy_profile": _first_value(strategy_profile, source.get("strategy_profile")),
        "strategy_version": source.get("strategy_version"),
        "generated_at": _json_safe(generated_at or _utcnow()),
        "signal_as_of": _json_safe(
            _first_value(
                source.get("signal_as_of"),
                source.get("signal_date"),
                source.get("snapshot_as_of"),
                source.get("trade_date"),
            )
        ),
        "market_date": _json_safe(
            _first_value(
                source.get("market_date"),
                source.get("signal_date"),
                source.get("snapshot_as_of"),
                source.get("trade_date"),
            )
        ),
        "effective_date": _json_safe(source.get("effective_date")),
        "latest_price_source": _first_value(
            source.get("latest_price_source"),
            source.get("price_source_mode"),
            source.get("market_data_source"),
            source.get("signal_source"),
        ),
        "quote_overlay_used": source.get("quote_overlay_used"),
        "data_freshness_warning": _first_value(
            source.get("data_freshness_warning"),
            source.get("snapshot_price_fallback_used"),
        ),
        "signal": _first_value(
            source.get("signal_display"),
            source.get("signal_description"),
            source.get("signal_message"),
        ),
        "status": _first_value(
            source.get("status_display"),
            source.get("status_description"),
            source.get("market_status"),
            source.get("canary_status"),
        ),
        "target_mode": target_mode,
        "target_weights": normalized_weights,
        "target_values": normalized_values,
        "indicators": indicators,
    }
    return {key: _json_safe(value) for key, value in snapshot.items()}
