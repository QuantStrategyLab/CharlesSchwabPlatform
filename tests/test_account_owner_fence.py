from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from application.rebalance_service import run_strategy_core
from application.runtime_dependencies import SchwabRebalanceConfig, SchwabRebalanceRuntime
from notifications.telegram import build_translator
from quant_platform_kit.common.execution_state import ExecutionMarkerStore, claim_account_owner
from quant_platform_kit.common.models import ExecutionReport, PortfolioSnapshot, Position, QuoteSnapshot
from quant_platform_kit.common.port_adapters import (
    CallableExecutionPort,
    CallableMarketDataPort,
    CallableNotificationPort,
    CallablePortfolioPort,
)


class AccountOwnerFenceTests(unittest.TestCase):
    def test_contested_account_owner_blocks_broker_submission(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ExecutionMarkerStore(local_dir=tmpdir, cloud_prefix_uri=None)
            self.assertTrue(
                claim_account_owner(
                    store,
                    broker="schwab",
                    account_id="demo",
                    owner_id="soxl_soxx_trend_income",
                ).allowed
            )

            sent_messages: list[str] = []
            observed_orders: list = []
            plan = {
                "account_hash": "demo",
                "allocation": {
                    "target_mode": "value",
                    "strategy_symbols": ("SOXX",),
                    "risk_symbols": ("SOXX",),
                    "income_symbols": (),
                    "safe_haven_symbols": (),
                    "targets": {"SOXX": 500.0},
                },
                "portfolio": {
                    "strategy_symbols": ("SOXX",),
                    "portfolio_rows": (("SOXX",),),
                    "market_values": {"SOXX": 0.0},
                    "quantities": {"SOXX": 0},
                    "liquid_cash": 600.0,
                    "total_equity": 600.0,
                    "cash_sweep_symbol": "BOXX",
                },
                "execution": {
                    "trade_threshold_value": 10.0,
                    "reserved_cash": 0.0,
                    "signal_display": "Risk on",
                    "dashboard_text": "dashboard",
                    "signal_date": "2026-06-01",
                    "effective_date": "2026-06-02",
                    "execution_timing_contract": "next_trading_day",
                    "separator": "━━━━━━━━━━━━━━━━━━",
                },
            }
            snapshot = PortfolioSnapshot(
                as_of="2026-06-02",
                total_equity=600.0,
                buying_power=600.0,
                positions=(Position(symbol="SOXX", quantity=0, market_value=0.0),),
                metadata={"account_hash": "demo"},
            )
            quote_snapshots = {
                "SOXX": QuoteSnapshot(
                    symbol="SOXX",
                    as_of="2026-06-02",
                    last_price=524.0,
                    ask_price=524.0,
                ),
            }

            run_strategy_core(
                runtime=SchwabRebalanceRuntime(
                    fetch_reference_history=lambda: [{"close": 1.0, "high": 1.0, "low": 1.0}],
                    portfolio_port=CallablePortfolioPort(lambda: snapshot),
                    market_data_port=CallableMarketDataPort(
                        quote_loader=lambda symbol: quote_snapshots[symbol]
                    ),
                    resolve_rebalance_plan=lambda *, qqq_history, snapshot: plan,
                    notifications=CallableNotificationPort(sent_messages.append),
                    execution_port_factory=lambda _account_hash: CallableExecutionPort(
                        lambda order_intent: (
                            observed_orders.append(order_intent),
                            ExecutionReport(
                                symbol=order_intent.symbol,
                                side=order_intent.side,
                                quantity=order_intent.quantity,
                                status="accepted",
                                broker_order_id="schwab-order-1",
                            ),
                        )[-1]
                    ),
                ),
                config=SchwabRebalanceConfig(
                    translator=build_translator("en"),
                    strategy_display_name="Other Profile",
                    limit_buy_premium=1.005,
                    sell_settle_delay_sec=0,
                    strategy_profile="other_profile",
                    dry_run_only=False,
                    execution_dedup_enabled=True,
                    execution_state_store=store,
                    execution_state_account_scope="LIVE",
                ),
            )

            self.assertEqual(observed_orders, [])

    def test_same_owner_replay_is_allowed_by_primitive(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ExecutionMarkerStore(local_dir=tmpdir, cloud_prefix_uri=None)
            first = claim_account_owner(
                store,
                broker="schwab",
                account_id="demo",
                owner_id="soxl_soxx_trend_income",
            )
            again = claim_account_owner(
                store,
                broker="schwab",
                account_id="demo",
                owner_id="soxl_soxx_trend_income",
            )
            self.assertTrue(first.allowed)
            self.assertTrue(again.allowed)
            self.assertFalse(again.contested)


if __name__ == "__main__":
    unittest.main()
