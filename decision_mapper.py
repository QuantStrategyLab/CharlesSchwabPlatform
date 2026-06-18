from __future__ import annotations

from typing import Any

from us_equity_strategies.catalog import resolve_canonical_profile
from quant_platform_kit.strategy_contracts import (
    PositionTarget,
    StrategyDecision,
    ValueTargetExecutionAnnotations,
    build_value_target_portfolio_inputs_from_snapshot,
    build_value_target_runtime_plan,
    resolve_decision_target_mode,
    translate_decision_to_target_mode,
)


def _resolve_reserved_cash(
    *,
    snapshot,
    diagnostics: dict[str, Any],
    execution_annotations: dict[str, Any],
    runtime_metadata: dict[str, Any],
    strategy_profile: str,
) -> float:
    base_reserved_cash = float(
        execution_annotations.get("reserved_cash", diagnostics.get("reserved", 0.0)) or 0.0
    )
    if resolve_canonical_profile(strategy_profile) == "soxl_soxx_trend_income":
        return base_reserved_cash
    raw_policy = runtime_metadata.get("schwab_execution_policy")
    if not isinstance(raw_policy, dict):
        return base_reserved_cash
    total_equity = max(0.0, float(getattr(snapshot, "total_equity", 0.0) or 0.0))
    reserved_cash_floor_usd = max(0.0, float(raw_policy.get("reserved_cash_floor_usd", 0.0) or 0.0))
    reserved_cash_ratio = float(raw_policy.get("reserved_cash_ratio", 0.0) or 0.0)
    reserved_cash_ratio = max(0.0, min(1.0, reserved_cash_ratio))
    policy_reserved_cash = max(reserved_cash_floor_usd, total_equity * reserved_cash_ratio)
    return max(base_reserved_cash, policy_reserved_cash)


def _symbol_role(symbol: str) -> str | None:
    normalized = str(symbol or "").strip().upper()
    if normalized in {"BOXX", "BIL"}:
        return "safe_haven"
    if normalized in {"QQQI", "SPYI"}:
        return "income"
    return None


def _build_zero_equity_value_decision(decision: StrategyDecision) -> StrategyDecision:
    positions = []
    for position in decision.positions:
        positions.append(
            PositionTarget(
                symbol=position.symbol,
                target_value=0.0,
                role=position.role or _symbol_role(position.symbol),
                order_preference=position.order_preference,
            )
        )
    return StrategyDecision(
        positions=tuple(positions),
        budgets=decision.budgets,
        risk_flags=tuple(dict.fromkeys((*decision.risk_flags, "no_execute"))),
        diagnostics={
            **dict(decision.diagnostics),
            "execution_blocked_reason": "non_positive_total_equity",
        },
    )


def map_strategy_decision_to_plan(
    decision: StrategyDecision,
    *,
    snapshot,
    strategy_profile: str,
    runtime_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_metadata = dict(runtime_metadata or {})
    target_mode = resolve_decision_target_mode(decision)
    total_equity = float(snapshot.total_equity)
    if target_mode == "weight" and total_equity <= 0.0:
        normalized_decision = _build_zero_equity_value_decision(decision)
    else:
        normalized_decision = translate_decision_to_target_mode(
            decision,
            target_mode="value",
            total_equity=total_equity,
        )
    diagnostics = {**runtime_metadata, **dict(decision.diagnostics)}
    execution_annotations: dict[str, Any] = {}
    raw_runtime_annotations = runtime_metadata.get("execution_annotations")
    if isinstance(raw_runtime_annotations, dict):
        execution_annotations.update(raw_runtime_annotations)
    raw_annotations = diagnostics.get("execution_annotations")
    if isinstance(raw_annotations, dict):
        execution_annotations.update(raw_annotations)
    reserved_cash = _resolve_reserved_cash(
        snapshot=snapshot,
        diagnostics=diagnostics,
        execution_annotations=execution_annotations,
        runtime_metadata=runtime_metadata,
        strategy_profile=strategy_profile,
    )
    portfolio_inputs = build_value_target_portfolio_inputs_from_snapshot(snapshot)
    plan = build_value_target_runtime_plan(
        normalized_decision,
        strategy_profile=strategy_profile,
        portfolio_inputs=portfolio_inputs,
        strategy_symbols_order="risk_safe_income",
        portfolio_rows_layout=("risk_safe", "income"),
        execution_fields=(
            "trade_threshold_value",
            "reserved_cash",
            "signal_display",
            "status_display",
            "dashboard_text",
            "signal_date",
            "effective_date",
            "execution_timing_contract",
            "execution_calendar_source",
            "signal_effective_after_trading_days",
            "separator",
            "benchmark_symbol",
            "benchmark_price",
            "long_trend_value",
            "exit_line",
            "deploy_ratio_text",
            "income_ratio_text",
            "income_locked_ratio_text",
            "active_risk_asset",
            "current_min_trade",
            "investable_cash",
        ),
        execution_defaults={
            "reserved_cash": 0.0,
            "signal_display": "",
            "status_display": "",
            "dashboard_text": "",
            "signal_date": "",
            "effective_date": "",
            "execution_timing_contract": "",
            "execution_calendar_source": "",
            "signal_effective_after_trading_days": None,
            "separator": "━━━━━━━━━━━━━━━━━━",
            "benchmark_symbol": "QQQ",
            "benchmark_price": 0.0,
            "long_trend_value": 0.0,
            "exit_line": 0.0,
            "deploy_ratio_text": "",
            "income_ratio_text": "",
            "income_locked_ratio_text": "",
            "active_risk_asset": "",
            "current_min_trade": 0.0,
            "investable_cash": portfolio_inputs.liquid_cash,
        },
        annotations=ValueTargetExecutionAnnotations(
            trade_threshold_value=float(
                execution_annotations.get("trade_threshold_value", diagnostics.get("threshold", 0.0)) or 0.0
            ),
            reserved_cash=reserved_cash,
            signal_display=str(
                execution_annotations.get("signal_display")
                or diagnostics.get("signal_display")
                or diagnostics.get("signal_description")
                or ""
            ),
            status_display=str(
                execution_annotations.get("status_display")
                or diagnostics.get("status_display")
                or diagnostics.get("status_description")
                or diagnostics.get("canary_status")
                or ""
            ),
            dashboard_text=str(
                execution_annotations.get("dashboard_text")
                or diagnostics.get("dashboard")
                or ""
            ),
            signal_date=str(execution_annotations.get("signal_date") or diagnostics.get("signal_date") or "") or None,
            effective_date=str(
                execution_annotations.get("effective_date") or diagnostics.get("effective_date") or ""
            )
            or None,
            execution_timing_contract=str(
                execution_annotations.get("execution_timing_contract")
                or diagnostics.get("execution_timing_contract")
                or ""
            )
            or None,
            execution_calendar_source=str(
                execution_annotations.get("execution_calendar_source")
                or diagnostics.get("execution_calendar_source")
                or ""
            )
            or None,
            signal_effective_after_trading_days=(
                int(signal_delay)
                if (
                    signal_delay := execution_annotations.get(
                        "signal_effective_after_trading_days",
                        diagnostics.get("signal_effective_after_trading_days"),
                    )
                )
                is not None
                else None
            ),
            separator=str(execution_annotations.get("separator") or "━━━━━━━━━━━━━━━━━━"),
            benchmark_symbol=str(execution_annotations.get("benchmark_symbol") or "QQQ"),
            benchmark_price=float(execution_annotations.get("benchmark_price", diagnostics.get("qqq_price", 0.0)) or 0.0),
            long_trend_value=float(execution_annotations.get("long_trend_value", diagnostics.get("ma200", 0.0)) or 0.0),
            exit_line=float(execution_annotations.get("exit_line", diagnostics.get("exit_line", 0.0)) or 0.0),
            deploy_ratio_text=str(execution_annotations.get("deploy_ratio_text") or ""),
            income_ratio_text=str(execution_annotations.get("income_ratio_text") or ""),
            income_locked_ratio_text=str(execution_annotations.get("income_locked_ratio_text") or ""),
            active_risk_asset=str(execution_annotations.get("active_risk_asset") or ""),
            current_min_trade=float(execution_annotations.get("current_min_trade", 0.0) or 0.0),
            investable_cash=float(execution_annotations.get("investable_cash", portfolio_inputs.liquid_cash) or 0.0),
        ),
    )
    plan["account_hash"] = snapshot.metadata["account_hash"]
    execution = plan.setdefault("execution", {})
    for field_name in (
        "allocation_mode",
        "trend_entry_buffer",
        "trend_mid_buffer",
        "trend_exit_buffer",
        "blend_tier",
        "base_blend_tier",
        "overlay_trigger_count",
        "overlay_trigger_reasons",
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
        "dual_drive_volatility_delever_removed_value",
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
    ):
        if field_name in diagnostics:
            execution[field_name] = diagnostics[field_name]
    return plan
