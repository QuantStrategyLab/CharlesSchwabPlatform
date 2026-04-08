from __future__ import annotations

from typing import Any

from quant_platform_kit.strategy_contracts import (
    StrategyDecision,
    build_value_target_portfolio_inputs_from_snapshot,
    build_value_target_runtime_plan,
)


def map_strategy_decision_to_plan(
    decision: StrategyDecision,
    *,
    snapshot,
    strategy_profile: str,
) -> dict[str, Any]:
    portfolio_inputs = build_value_target_portfolio_inputs_from_snapshot(snapshot)
    plan = build_value_target_runtime_plan(
        decision,
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
    )
    plan["account_hash"] = snapshot.metadata["account_hash"]
    return plan
