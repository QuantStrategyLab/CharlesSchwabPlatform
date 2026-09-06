from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from application.rebalance_service import run_strategy_core
from application.runtime_dependencies import SchwabRebalanceConfig, SchwabRebalanceRuntime
from notifications.telegram import build_translator
from quant_platform_kit.common.execution_state import (
    ExecutionMarkerStore,
    build_account_owner_marker_key,
    claim_account_owner,
)
from quant_platform_kit.common.models import ExecutionReport, PortfolioSnapshot, Position, QuoteSnapshot
from quant_platform_kit.common.port_adapters import (
    CallableExecutionPort,
    CallableMarketDataPort,
    CallableNotificationPort,
    CallablePortfolioPort,
)


def _buy_plan(*, account_hash: str = "demo") -> dict:
    return {
        "account_hash": account_hash,
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
            "cash_sweep_symbol": "",
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


def _snapshot() -> PortfolioSnapshot:
    return PortfolioSnapshot(
        as_of="2026-06-02",
        total_equity=600.0,
        buying_power=600.0,
        positions=(Position(symbol="SOXX", quantity=0, market_value=0.0),),
        metadata={"account_hash": "demo"},
    )


def _quotes() -> dict[str, QuoteSnapshot]:
    return {
        "SOXX": QuoteSnapshot(
            symbol="SOXX",
            as_of="2026-06-02",
            last_price=524.0,
            ask_price=524.0,
        ),
    }


def _run_core(
    *,
    store,
    plan: dict,
    strategy_profile: str,
    dry_run_only: bool,
    observed_orders: list,
    sent_messages: list | None = None,
    account_scope: str | None = None,
) -> None:
    messages = sent_messages if sent_messages is not None else []
    quotes = _quotes()
    run_strategy_core(
        runtime=SchwabRebalanceRuntime(
            fetch_reference_history=lambda: [{"close": 1.0, "high": 1.0, "low": 1.0}],
            portfolio_port=CallablePortfolioPort(lambda: _snapshot()),
            market_data_port=CallableMarketDataPort(quote_loader=lambda symbol: quotes[symbol]),
            resolve_rebalance_plan=lambda *, qqq_history, snapshot: plan,
            notifications=CallableNotificationPort(messages.append),
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
            strategy_display_name=strategy_profile,
            limit_buy_premium=1.005,
            sell_settle_delay_sec=0,
            strategy_profile=strategy_profile,
            dry_run_only=dry_run_only,
            execution_dedup_enabled=True,
            execution_state_store=store,
            execution_state_account_scope=account_scope
            or ("PAPER" if dry_run_only else "LIVE"),
        ),
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

            observed_orders: list = []
            _run_core(
                store=store,
                plan=_buy_plan(),
                strategy_profile="other_profile",
                dry_run_only=False,
                observed_orders=observed_orders,
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

    def test_dry_run_only_does_not_claim_account_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ExecutionMarkerStore(local_dir=tmpdir, cloud_prefix_uri=None)
            owner_key = build_account_owner_marker_key(broker="schwab", account_id="demo")
            observed_orders: list = []

            with patch(
                "application.rebalance_service.claim_account_owner",
                wraps=claim_account_owner,
            ) as claim_spy:
                _run_core(
                    store=store,
                    plan=_buy_plan(),
                    strategy_profile="dry_run_profile_a",
                    dry_run_only=True,
                    observed_orders=observed_orders,
                )

            self.assertEqual(claim_spy.call_count, 0)
            self.assertFalse(store.has_marker(owner_key))
            self.assertIsNone(store.read_marker(owner_key))
            # dry-run still simulates trades locally; it must not hit the broker port.
            self.assertEqual(observed_orders, [])

    def test_live_claims_idempotent_same_owner_and_rejects_other(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ExecutionMarkerStore(local_dir=tmpdir, cloud_prefix_uri=None)
            owner_key = build_account_owner_marker_key(broker="schwab", account_id="demo")
            plan = _buy_plan()

            first_orders: list = []
            with patch(
                "application.rebalance_service.claim_account_owner",
                wraps=claim_account_owner,
            ) as claim_spy:
                _run_core(
                    store=store,
                    plan=plan,
                    strategy_profile="live_profile_b",
                    dry_run_only=False,
                    observed_orders=first_orders,
                )
            self.assertEqual(claim_spy.call_count, 1)
            self.assertTrue(store.has_marker(owner_key))
            self.assertEqual(store.read_marker(owner_key)["metadata"]["owner_id"], "live_profile_b")
            self.assertEqual(len(first_orders), 1)

            # Same owner, different signal day: claim allowed again (idempotent), submission proceeds.
            plan_replay = dict(plan)
            plan_replay["execution"] = dict(plan["execution"])
            plan_replay["execution"]["signal_date"] = "2026-06-03"
            plan_replay["execution"]["effective_date"] = "2026-06-04"
            replay_orders: list = []
            with patch(
                "application.rebalance_service.claim_account_owner",
                wraps=claim_account_owner,
            ) as claim_spy:
                _run_core(
                    store=store,
                    plan=plan_replay,
                    strategy_profile="live_profile_b",
                    dry_run_only=False,
                    observed_orders=replay_orders,
                )
            self.assertEqual(claim_spy.call_count, 1)
            self.assertEqual(len(replay_orders), 1)
            self.assertEqual(store.read_marker(owner_key)["metadata"]["owner_id"], "live_profile_b")

            contested_orders: list = []
            _run_core(
                store=store,
                plan=_buy_plan(),
                strategy_profile="other_live_profile",
                dry_run_only=False,
                observed_orders=contested_orders,
            )
            self.assertEqual(contested_orders, [])
            self.assertEqual(store.read_marker(owner_key)["metadata"]["owner_id"], "live_profile_b")

    def test_live_owner_claim_store_failure_is_fail_closed(self) -> None:
        class FailingStore:
            def claim_marker(self, marker_key, *, metadata=None):
                raise RuntimeError("store unavailable")

            def has_marker(self, marker_key):
                raise AssertionError("fail-closed must stop before marker reads")

            def read_marker(self, marker_key):
                raise AssertionError("fail-closed must stop before marker reads")

            def record_marker(self, *_args, **_kwargs):
                raise AssertionError("fail-closed must not record markers")

        observed_orders: list = []
        with self.assertRaisesRegex(RuntimeError, "Account owner fence failed"):
            _run_core(
                store=FailingStore(),
                plan=_buy_plan(),
                strategy_profile="live_profile_b",
                dry_run_only=False,
                observed_orders=observed_orders,
            )
        self.assertEqual(observed_orders, [])

    def test_shared_store_dry_run_then_live_only_live_claims(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            store = ExecutionMarkerStore(local_dir=tmpdir, cloud_prefix_uri=None)
            owner_key = build_account_owner_marker_key(broker="schwab", account_id="demo")
            plan = _buy_plan()

            dry_orders: list = []
            with patch(
                "application.rebalance_service.claim_account_owner",
                wraps=claim_account_owner,
            ) as claim_spy:
                _run_core(
                    store=store,
                    plan=plan,
                    strategy_profile="dry_run_profile_a",
                    dry_run_only=True,
                    observed_orders=dry_orders,
                )
            self.assertEqual(claim_spy.call_count, 0)
            self.assertFalse(store.has_marker(owner_key))
            self.assertEqual(dry_orders, [])

            live_orders: list = []
            with patch(
                "application.rebalance_service.claim_account_owner",
                wraps=claim_account_owner,
            ) as claim_spy:
                _run_core(
                    store=store,
                    plan=plan,
                    strategy_profile="live_profile_b",
                    dry_run_only=False,
                    observed_orders=live_orders,
                )
            self.assertEqual(claim_spy.call_count, 1)
            self.assertTrue(store.has_marker(owner_key))
            self.assertEqual(store.read_marker(owner_key)["metadata"]["owner_id"], "live_profile_b")
            self.assertEqual(len(live_orders), 1)


if __name__ == "__main__":
    unittest.main()
