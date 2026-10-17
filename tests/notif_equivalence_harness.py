"""Synthetic call recorder for the real rebalance entrypoint; no external I/O."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from unittest.mock import patch

from application import rebalance_service

from application.rebalance_service import run_strategy_core
from application.runtime_dependencies import SchwabRebalanceConfig, SchwabRebalanceRuntime
from notifications.telegram import build_translator
from quant_platform_kit.common.models import ExecutionReport, PortfolioSnapshot, Position, QuoteSnapshot
from quant_platform_kit.common.port_adapters import CallableExecutionPort, CallableMarketDataPort, CallableNotificationPort, CallablePortfolioPort


def record_cycle(scenario, locale="en"):
    calls = []
    snapshot = PortfolioSnapshot(as_of=datetime(2026, 10, 16, 19, 45, tzinfo=timezone.utc),
        total_equity=1000.0, buying_power=1000.0, positions=(Position(symbol="SPY", quantity=0, market_value=0.0),),
        metadata={"account_hash": "synthetic"})
    plan = {
        "strategy_profile": "synthetic_profile", "account_hash": "synthetic",
        "allocation": {"target_mode": "value", "strategy_symbols": ("SPY",), "risk_symbols": ("SPY",),
            "income_symbols": (), "safe_haven_symbols": (), "targets": {"SPY": 500.0 if scenario in {"order", "dry_run"} else 0.0}},
        "portfolio": {"strategy_symbols": ("SPY",), "portfolio_rows": (("SPY",),), "market_values": {"SPY": 0.0},
            "quantities": {"SPY": 0}, "total_equity": 1000.0, "liquid_cash": 1000.0, "cash_sweep_symbol": "",
            "account_new_risk_snapshot": {"observation_status": "COMPLETE", "reconciliation_status": "VERIFIED", "circuit_breaker_state": "CLOSED"}},
        "execution": {"trade_threshold_value": 10.0, "reserved_cash": 0.0, "signal_display": "",
            "dashboard_text": "", "separator": "---", "benchmark_symbol": "SPY", "benchmark_price": 100.0,
            "long_trend_value": 90.0, "exit_line": 80.0, "signal_date": "2026-10-16", "effective_date": "2026-10-17"},
    }
    if scenario == "blocked":
        plan["execution"].update(execution_status="blocked", no_op_reason="strategy_risk_rejected")
    elif scenario == "healthy":
        plan["execution"]["execution_status"] = "no_action"

    def record(name, value, result):
        calls.append([name, value])
        return result

    def submit(intent):
        calls.append(["broker", asdict(intent)])
        return ExecutionReport(symbol=intent.symbol, side=intent.side, quantity=intent.quantity,
            status="filled", broker_order_id="synthetic-order")

    class Store:
        def claim_marker(self, key, **kwargs):
            return record("claim", [key, kwargs], True)
        def has_marker(self, key):
            return record("marker_read", key, False)
        def record_outcome(self, key, **kwargs):
            record("outcome", [key, kwargs], None)

    original_snapshot = rebalance_service.build_signal_snapshot

    def frozen_signal_snapshot(**kwargs):
        value = original_snapshot(**kwargs)
        value["generated_at"] = "2026-10-16T19:45:00+00:00"
        return value

    with patch.object(rebalance_service, "build_signal_snapshot", frozen_signal_snapshot):
        result = run_strategy_core(object(), None,
        runtime=SchwabRebalanceRuntime(
            fetch_reference_history=lambda: record("history", None, [{"close": 100.0, "high": 100.0, "low": 100.0}]),
            portfolio_port=CallablePortfolioPort(lambda: record("portfolio", None, snapshot)),
            market_data_port=CallableMarketDataPort(quote_loader=lambda symbol: record("quote", symbol,
                QuoteSnapshot(symbol=symbol, as_of=snapshot.as_of, last_price=100.0, ask_price=100.0))),
            resolve_rebalance_plan=lambda **kwargs: record("plan", None, plan),
            notifications=CallableNotificationPort(lambda text: record("send", text, True)),
            execution_port_factory=lambda account: record("execution_port", account, CallableExecutionPort(submit)),
        ),
        config=SchwabRebalanceConfig(translator=build_translator(locale), strategy_display_name="Synthetic strategy",
            strategy_profile="synthetic_profile", limit_buy_premium=1.0, sell_settle_delay_sec=0,
            dry_run_only=scenario == "dry_run", execution_state_store=Store(), execution_dedup_enabled=True,
            execution_state_account_scope="synthetic", notify_no_trade_cycles=True),
        )
    recorded_result = asdict(result)
    # The merge test can include a newer main adding optional account facts.
    # Absent and explicit unknown average cost are equivalent for this fixture;
    # known values, every trading field, call argument and send byte stay exact.
    for position in (recorded_result.get("account_observation") or {}).get("broker_reported_positions") or ():
        if "avg_cost" in position and position["avg_cost"] is None:
            del position["avg_cost"]
    return {"calls": calls, "result": recorded_result}
