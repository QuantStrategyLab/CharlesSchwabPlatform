import os
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
QPK_SRC = ROOT.parent / "QuantPlatformKit" / "src"
if (QPK_SRC / "quant_platform_kit").exists() and str(QPK_SRC) not in sys.path:
    sys.path.insert(0, str(QPK_SRC))

from application.account_new_risk_gate_support import (
    ACCOUNT_NEW_RISK_GATE_ENV,
    build_snapshot_from_portfolio,
    evaluate_portfolio_new_risk_admission,
    new_risk_buy_prohibited,
    set_cycle_snapshot,
)
from application.execution_service import execute_rebalance_cycle
from notifications.telegram import build_translator
from quant_platform_kit.common.models import QuoteSnapshot
from quant_platform_kit.common.port_adapters import CallableExecutionPort, CallableMarketDataPort
from quant_platform_kit.risk.account_new_risk_gate import NewRiskDisposition


class AccountNewRiskGateSupportTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_cycle_snapshot(None)
        os.environ.pop(ACCOUNT_NEW_RISK_GATE_ENV, None)

    def test_missing_equity_prohibits_fail_closed(self) -> None:
        portfolio = {"market_values": {"SOXL": 0.0}, "liquid_cash": 100.0}
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertTrue(new_risk_buy_prohibited(result))
        self.assertIn("EQUITY_UNKNOWN_FAIL_CLOSED", result.reason_codes)

    def test_drawdown_brake_prohibits_new_risk(self) -> None:
        portfolio = {
            "total_equity": 85_000.0,
            "account_new_risk_snapshot": {
                "peak_equity_usd": 100_000.0,
            },
        }
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertTrue(new_risk_buy_prohibited(result))
        self.assertIn("DRAWDOWN_BRAKE_TRIPPED", result.reason_codes)

    def test_healthy_broker_liquidation_equity_allows_new_risk(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
        }
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.ALLOW_NEW_RISK)
        self.assertFalse(result.live_authority_granted)

    def test_snapshot_maps_total_equity_from_portfolio(self) -> None:
        snapshot = build_snapshot_from_portfolio({"total_equity": 12_345.0})
        self.assertEqual(snapshot.equity_usd, 12_345.0)


class AccountNewRiskGateExecutionCycleTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_cycle_snapshot(None)
        os.environ.pop(ACCOUNT_NEW_RISK_GATE_ENV, None)

    def _run_buy_cycle(self, *, portfolio_overrides=None):
        submitted_orders = []
        plan = {
            "account_hash": "demo",
            "allocation": {
                "target_mode": "value",
                "strategy_symbols": ("SOXL",),
                "risk_symbols": ("SOXL",),
                "income_symbols": (),
                "safe_haven_symbols": (),
                "targets": {"SOXL": 400.0},
            },
            "portfolio": {
                "market_values": {"SOXL": 0.0},
                "quantities": {"SOXL": 0},
                "total_equity": 50_000.0,
                "liquid_cash": 500.0,
                "cash_sweep_symbol": None,
            },
            "execution": {
                "trade_threshold_value": 10.0,
                "reserved_cash": 0.0,
            },
        }
        if portfolio_overrides:
            plan["portfolio"].update(portfolio_overrides)

        return execute_rebalance_cycle(
            client=object(),
            plan=plan,
            portfolio=plan["portfolio"],
            execution=plan["execution"],
            allocation=plan["allocation"],
            fetch_managed_snapshot=lambda _client: None,
            market_data_port=CallableMarketDataPort(
                quote_loader=lambda symbol: QuoteSnapshot(
                    symbol=symbol,
                    as_of="2026-08-24",
                    last_price=100.0,
                    ask_price=100.0,
                )
            ),
            load_plan=lambda _snapshot: (
                plan,
                plan["portfolio"],
                plan["execution"],
                plan["allocation"],
            ),
            execution_port=CallableExecutionPort(submitted_orders.append),
            translator=build_translator("en"),
            limit_buy_premium=1.0,
            sell_settle_delay_sec=0,
            publish_order_issue=lambda _message: None,
        ), submitted_orders

    def test_execution_cycle_blocks_buys_when_equity_missing(self) -> None:
        result, submitted_orders = self._run_buy_cycle(
            portfolio_overrides={
                "total_equity": None,
            }
        )
        self.assertEqual(submitted_orders, [])
        self.assertTrue(any("Account new-risk gate" in log for log in result.trade_logs))

    def test_execution_cycle_allows_buys_when_healthy(self) -> None:
        result, submitted_orders = self._run_buy_cycle()
        self.assertEqual(len(submitted_orders), 1)
        self.assertEqual(str(getattr(submitted_orders[0], "side", "")).lower(), "buy")
        self.assertFalse(any("Account new-risk gate" in log for log in result.trade_logs))

    def test_execution_cycle_allows_sell_when_buy_prohibited(self) -> None:
        submitted_orders = []
        plan = {
            "account_hash": "demo",
            "allocation": {
                "target_mode": "value",
                "strategy_symbols": ("SOXL",),
                "risk_symbols": ("SOXL",),
                "income_symbols": (),
                "safe_haven_symbols": (),
                "targets": {"SOXL": 0.0},
            },
            "portfolio": {
                "market_values": {"SOXL": 400.0},
                "quantities": {"SOXL": 4},
                "total_equity": None,
                "liquid_cash": 100.0,
                "cash_sweep_symbol": None,
            },
            "execution": {
                "trade_threshold_value": 10.0,
                "reserved_cash": 0.0,
            },
        }
        result = execute_rebalance_cycle(
            client=object(),
            plan=plan,
            portfolio=plan["portfolio"],
            execution=plan["execution"],
            allocation=plan["allocation"],
            fetch_managed_snapshot=lambda _client: None,
            market_data_port=CallableMarketDataPort(
                quote_loader=lambda symbol: QuoteSnapshot(
                    symbol=symbol,
                    as_of="2026-08-24",
                    last_price=100.0,
                    ask_price=100.0,
                )
            ),
            load_plan=lambda _snapshot: (
                plan,
                plan["portfolio"],
                plan["execution"],
                plan["allocation"],
            ),
            execution_port=CallableExecutionPort(submitted_orders.append),
            translator=build_translator("en"),
            limit_buy_premium=1.0,
            sell_settle_delay_sec=0,
            publish_order_issue=lambda _message: None,
        )
        self.assertEqual(len(submitted_orders), 1)
        self.assertEqual(str(getattr(submitted_orders[0], "side", "")).lower(), "sell")

    def test_gate_disabled_via_env_skips_buy_block(self) -> None:
        os.environ[ACCOUNT_NEW_RISK_GATE_ENV] = "0"
        result, submitted_orders = self._run_buy_cycle(
            portfolio_overrides={
                "total_equity": None,
            }
        )
        self.assertEqual(len(submitted_orders), 1)
        self.assertEqual(str(getattr(submitted_orders[0], "side", "")).lower(), "buy")
        self.assertFalse(any("Account new-risk gate" in log for log in result.trade_logs))


if __name__ == "__main__":
    unittest.main()
