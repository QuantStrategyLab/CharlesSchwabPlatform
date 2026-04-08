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
    risk_symbols = list(portfolio_plan.risk_symbols)
    income_symbols = list(portfolio_plan.income_symbols)
    safe_haven_symbols = list(portfolio_plan.safe_haven_symbols)
    plan.update({
        "strategy_symbols": portfolio_plan.strategy_symbols,
        "sell_order_symbols": tuple(risk_symbols + income_symbols + safe_haven_symbols),
        "buy_order_symbols": tuple(income_symbols + risk_symbols),
        "cash_sweep_symbol": portfolio_plan.cash_sweep_symbol,
        "portfolio_rows": portfolio_plan.portfolio_rows,
        "account_hash": snapshot.metadata["account_hash"],
        "market_values": dict(portfolio_plan.market_values),
        "quantities": dict(portfolio_plan.quantities),
        "total_equity": portfolio_plan.total_equity,
        "real_buying_power": portfolio_plan.liquid_cash,
        "reserved": float(annotations.reserved_cash),
        "threshold": float(annotations.trade_threshold_value),
        "target_values": dict(portfolio_plan.target_values),
        "sig_display": annotations.signal_display or "",
        "dashboard": annotations.dashboard_text or "",
        "qqq_p": float(annotations.benchmark_price or 0.0),
        "ma200": float(annotations.long_trend_value or 0.0),
        "exit_line": float(annotations.exit_line or 0.0),
        "separator": annotations.separator or "━━━━━━━━━━━━━━━━━━",
    })
    return plan
