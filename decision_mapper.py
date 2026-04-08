from __future__ import annotations

from typing import Any

from quant_platform_kit.strategy_contracts import (
    StrategyDecision,
    build_value_target_execution_annotations,
    build_value_target_execution_plan,
    build_value_target_plan_payload,
    build_value_target_portfolio_plan,
)


def _extract_snapshot_positions(snapshot) -> tuple[dict[str, float], dict[str, int]]:
    market_values: dict[str, float] = {}
    quantities: dict[str, int] = {}
    for position in snapshot.positions:
        market_values[position.symbol] = float(position.market_value)
        quantities[position.symbol] = int(position.quantity)
    return market_values, quantities


def map_strategy_decision_to_plan(
    decision: StrategyDecision,
    *,
    snapshot,
    strategy_profile: str,
) -> dict[str, Any]:
    execution_plan = build_value_target_execution_plan(
        decision,
        strategy_profile=strategy_profile,
    )
    annotations = build_value_target_execution_annotations(decision)
    market_values, quantities = _extract_snapshot_positions(snapshot)
    portfolio_plan = build_value_target_portfolio_plan(
        execution_plan,
        market_values=market_values,
        quantities=quantities,
        total_equity=float(snapshot.total_equity),
        liquid_cash=float(snapshot.buying_power or 0.0),
        strategy_symbols_order="risk_safe_income",
        portfolio_rows_layout=("risk_safe", "income"),
    )
    plan = build_value_target_plan_payload(
        strategy_profile=strategy_profile,
        portfolio_plan=portfolio_plan,
        annotations=annotations,
        execution_fields=(
            "trade_threshold_value",
            "reserved_cash",
            "signal_display",
            "dashboard_text",
            "separator",
            "benchmark_symbol",
            "benchmark_price",
            "long_trend_value",
            "exit_line",
        ),
        execution_defaults={
            "reserved_cash": 0.0,
            "signal_display": "",
            "dashboard_text": "",
            "separator": "━━━━━━━━━━━━━━━━━━",
            "benchmark_symbol": "QQQ",
            "benchmark_price": 0.0,
            "long_trend_value": 0.0,
            "exit_line": 0.0,
        },
    )
    plan["account_hash"] = snapshot.metadata["account_hash"]
    return plan
