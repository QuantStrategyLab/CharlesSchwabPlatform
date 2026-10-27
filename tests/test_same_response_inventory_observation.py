"""Native reporting inventory must not replace the strategy's filtered positions."""
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from application.account_observation import build_account_observation


def row(symbol="OTHER", **changes):
    return {"instrument": {"assetType": "EQUITY", "symbol": symbol},
            "longQuantity": "1.12345678", "shortQuantity": "0", "marketValue": "10.001", **changes}


def snapshot(native):
    return SimpleNamespace(as_of=datetime(2026, 10, 28, tzinfo=timezone.utc), total_equity=1210,
                          cash_balance=1000, positions=(), metadata={"account_hash": "synthetic",
                          "total_equity_source": "broker_liquidation_value", "broker_stock_inventory": native})


def test_same_response_unfiltered_inventory_is_reported_at_snapshot_time():
    source = snapshot({"positions_field_present": True, "native_positions_count": 2,
                       "positions": [row(), row("SHORT", longQuantity="0", shortQuantity="3", marketValue=None)]})
    original = deepcopy(source)
    observation = build_account_observation(source, net_assets_currency="USD")
    assert observation["broker_reported_positions_scope"] == "stocks_only"
    assert observation["broker_reported_positions_complete"] is True
    assert observation["broker_reported_positions_observed_at"] == observation["observed_at"] == "2026-10-28T00:00:00Z"
    assert observation["broker_reported_positions"][0]["quantity"] == "1.12345678"
    assert observation["broker_reported_positions"][1]["quantity"] == "-3"
    assert observation["broker_reported_positions"][1]["market_value"] is None
    assert observation["broker_stock_inventory_status"] == "available"
    assert source.__dict__ == original.__dict__
    assert "broker_stock_inventory" not in observation


@pytest.mark.parametrize("native,expected,present", [
    ({"positions_field_present": False, "native_positions_count": None}, "unknown", False),
    ({"positions_field_present": True, "native_positions_count": 0, "positions": []}, "available", True),
    ({"positions_field_present": True, "native_positions_count": 1, "positions": [row(shortQuantity=None)]}, "unknown", True),
    ({"positions_field_present": True, "native_positions_count": 513, "positions": None}, "unknown", True),
])
def test_missing_invalid_and_true_empty_are_distinct(native, expected, present):
    observation = build_account_observation(snapshot(native), net_assets_currency="USD")
    assert observation["broker_stock_inventory_status"] == expected
    assert observation["broker_stock_inventory_diagnostics"]["positions_field_present"] is present
    if expected == "unknown":
        assert "broker_reported_positions" not in observation
        assert "broker_reported_positions_complete" not in observation
    else:
        assert observation["broker_reported_positions"] == []
        assert observation["broker_reported_positions_complete"] is True


def test_unknown_currency_is_not_substituted_with_usd():
    observation = build_account_observation(snapshot({"positions_field_present": True, "native_positions_count": 1, "positions": [row()]}))
    assert observation["net_assets_currency"] is None
    assert "broker_reported_positions_complete" not in observation
    assert "broker_reported_positions" not in observation


def test_inventory_metadata_survives_existing_nlv_alignment_and_reaches_observation():
    from application.portfolio_nlv_align import align_cash_only_sleeve_to_broker_liquidation

    native = {"positions_field_present": True, "native_positions_count": 1,
              "positions": [row()]}
    source = snapshot(native)
    source.cash_balance = 2000
    source.metadata["cash_available_for_trading"] = 2000
    aligned = align_cash_only_sleeve_to_broker_liquidation(source, cash_only_execution=True)
    assert aligned.cash_balance == 1210 and aligned.positions == source.positions
    assert aligned.as_of == source.as_of
    assert aligned.metadata["broker_stock_inventory"] == native
    observation = build_account_observation(aligned, net_assets_currency="USD")
    assert observation["broker_reported_positions"][0]["symbol"] == "OTHER"
    assert observation["broker_stock_inventory_status"] == "available"


def test_reporting_metadata_does_not_change_actual_risk_plan_or_execution_identity_consumers(monkeypatch):
    from dataclasses import replace
    from hashlib import sha256
    import json
    from application.rebalance_service import _build_execution_marker_key
    from application.runtime_dependencies import SchwabRebalanceConfig
    from application import execution_service
    from application.execution_service import execute_rebalance_cycle
    from notifications.telegram import build_translator
    from decision_mapper import map_strategy_decision_to_plan
    from quant_platform_kit.common.capital_base import build_capital_base_snapshot, CapitalScope, CapitalValuationBasis
    from quant_platform_kit.common.models import PortfolioSnapshot, Position, ExecutionReport, QuoteSnapshot
    from quant_platform_kit.common.port_adapters import CallableExecutionPort, CallableMarketDataPort
    from quant_platform_kit.common.strategy_contracts import StrategyDecision, PositionTarget
    from quant_platform_kit.risk.gate import _snapshot_metrics, apply_risk_gate
    from quant_platform_kit.risk.contracts import RuntimeRiskLimits
    from us_equity_strategies.cash_only_equity import build_portfolio_inputs_from_snapshot

    # Keep real risk/quantity/intent consumers while isolating I/O adapters.
    import socket
    network_attempts = []
    def no_network(*args, **kwargs):
        network_attempts.append(True)
        raise AssertionError("synthetic consumer comparison must not use the network")
    monkeypatch.setattr(socket.socket, "connect", no_network)
    monkeypatch.setattr(execution_service, "attach_daily_loss_fact_to_portfolio",
                        lambda portfolio, **kwargs: dict(portfolio))
    monkeypatch.setattr(execution_service, "maybe_publish_attention_for_admission",
                        lambda *args, **kwargs: {"sent": 0, "skipped": 1, "failed": 0})
    # Main requires continuity evidence for live sells. Exercise a funded buy
    # instead: $40 holding + $1170 cash = $1210 NLV; an 8% target buys one share.
    baseline = PortfolioSnapshot(as_of=datetime(2026, 10, 28, tzinfo=timezone.utc),
        total_equity=1210, buying_power=1170, cash_balance=1170,
        positions=(Position(symbol="TQQQ", quantity=1, market_value=40),),
        metadata={"account_hash": "synthetic", "total_equity_source": "broker_liquidation_value",
                  "source_digest_sha256": "a" * 64, "observed_effective_exposure": 120 / 1210})
    changed = replace(baseline, metadata={**baseline.metadata, "broker_stock_inventory": {
        "positions_field_present": True, "native_positions_count": 1, "positions": [row()]}})
    decision = StrategyDecision(positions=(PositionTarget(symbol="TQQQ", target_weight=0.08),))
    results = []
    for source in (baseline, changed):
        inputs = build_portfolio_inputs_from_snapshot(source)
        risk_payload = _snapshot_metrics(source, now=source.as_of, max_snapshot_age_seconds=None)
        capital = build_capital_base_snapshot(source, account_scope="synthetic", runtime_scope="synthetic",
            strategy_scope="tqqq_growth_income", reported_currency="USD", target_currency="USD",
            fx_rate_to_target=1, source_digest_sha256=source.metadata["source_digest_sha256"],
            capital_scope=CapitalScope.ACCOUNT, valuation_basis=CapitalValuationBasis.BROKER_ACCOUNT_NET_LIQUIDATION)
        limits = RuntimeRiskLimits(allowed_symbols=("TQQQ",), product_leverage_factors={"TQQQ": 3},
            nominal_caps={"TQQQ": 0.1}, total_nominal_exposure_cap=0.1, total_effective_exposure_cap=0.3, max_positions=1)
        approved = apply_risk_gate(decision, portfolio_snapshot=source, max_single_weight=1, max_total_exposure=1,
                                   runtime_risk_limits=limits)
        plan = map_strategy_decision_to_plan(approved, snapshot=source, strategy_profile="tqqq_growth_income",
            runtime_metadata={"signal_date": "2026-10-28", "effective_date": "2026-10-29", "execution_timing_contract": "next_trading_day"})
        assert plan["allocation"]["targets"]
        # A stable signal marker is the live one-time claim identity, not a
        # hash of PortfolioSnapshot.metadata. Compare its actual caller too.
        config = SchwabRebalanceConfig(translator=lambda x: x, strategy_display_name="synthetic",
            limit_buy_premium=1, sell_settle_delay_sec=0, strategy_profile="tqqq_growth_income", execution_dedup_enabled=True)
        marker = _build_execution_marker_key(config=config, execution=plan["execution"], plan=plan)
        assert marker
        canonical_plan = json.dumps(plan, sort_keys=True, default=str)
        intents = []
        def submit(intent):
            intents.append(intent)
            return ExecutionReport(symbol=intent.symbol, side=intent.side, quantity=intent.quantity,
                                   status="accepted", broker_order_id="synthetic")
        execute_rebalance_cycle(client=object(), plan=plan, portfolio=plan["portfolio"],
            execution=plan["execution"], allocation=plan["allocation"],
            fetch_managed_snapshot=lambda client: source,
            market_data_port=CallableMarketDataPort(quote_loader=lambda symbol: QuoteSnapshot(symbol=symbol,
                as_of=source.as_of, last_price=40, ask_price=40)),
            load_plan=lambda current: (plan, plan["portfolio"], plan["execution"], plan["allocation"]),
            execution_port=CallableExecutionPort(submit), translator=build_translator("en"),
            limit_buy_premium=1, sell_settle_delay_sec=0, publish_order_issue=lambda message: None)
        assert intents
        assert [(intent.symbol, intent.side, intent.quantity) for intent in intents] == [("TQQQ", "buy", 1)]
        results.append((inputs, risk_payload, capital.to_safe_dict(), approved, marker,
                        sha256(canonical_plan.encode()).hexdigest(), tuple(intents)))
    assert results[0] == results[1]
    assert not network_attempts
    assert "OTHER" not in canonical_plan and "broker_stock_inventory" not in canonical_plan


@pytest.mark.parametrize("declared_currency,expected", [(None, "unknown"), ("USD", "available")])
def test_natural_cycle_passes_explicit_owner_currency_without_more_snapshot_reads(monkeypatch, capsys, declared_currency, expected):
    from unittest.mock import Mock
    from application import rebalance_service
    from application.rebalance_service import run_strategy_core
    from application.runtime_dependencies import SchwabRebalanceConfig, SchwabRebalanceRuntime
    from notifications.telegram import build_translator
    from quant_platform_kit.common.port_adapters import CallableMarketDataPort, CallableNotificationPort, CallablePortfolioPort

    monkeypatch.setattr(rebalance_service, "_should_publish_cycle_notification", lambda *args, **kwargs: False)

    if declared_currency:
        monkeypatch.setenv("SCHWAB_NET_ASSETS_CURRENCY", declared_currency)
    else:
        monkeypatch.delenv("SCHWAB_NET_ASSETS_CURRENCY", raising=False)
    source = snapshot({"positions_field_present": True, "native_positions_count": 1, "positions": [row()]})
    read_snapshot = Mock(return_value=source)
    plan = {"account_hash": "synthetic", "allocation": {"target_mode": "value", "targets": {},
            "strategy_symbols": (), "risk_symbols": (), "income_symbols": (), "safe_haven_symbols": ()},
            "portfolio": {"market_values": {}, "quantities": {}, "liquid_cash": 1000, "total_equity": 1210, "cash_sweep_symbol": ""},
            "execution": {"execution_status": "blocked", "no_op_reason": "synthetic_no_funding",
                          "trade_threshold_value": 10, "reserved_cash": 0}}
    result = run_strategy_core(
        object(), None,
        runtime=SchwabRebalanceRuntime(
            fetch_reference_history=lambda: [], portfolio_port=CallablePortfolioPort(read_snapshot),
            market_data_port=CallableMarketDataPort(quote_loader=lambda symbol: None),
            resolve_rebalance_plan=lambda **kwargs: plan,
            notifications=CallableNotificationPort(lambda message: None),
        ),
        config=SchwabRebalanceConfig(translator=build_translator("en"), strategy_display_name="synthetic",
                                    limit_buy_premium=1, sell_settle_delay_sec=0,
                                    dry_run_only=True, notify_no_trade_cycles=False),
    )
    assert read_snapshot.call_count == 1
    assert result.submitted_orders == ()
    assert result.account_observation["broker_stock_inventory_status"] == expected
    assert result.account_observation["net_assets_currency"] == declared_currency
    assert source.positions == () and source.total_equity == 1210
    assert "OTHER" not in capsys.readouterr().out
