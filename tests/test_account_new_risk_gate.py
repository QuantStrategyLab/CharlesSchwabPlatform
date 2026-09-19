import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# Prefer installed QPK (site-packages / uv env). Sibling checkout is append-only
# fallback only — not pin proof. Authoritative pin checks use pytest -o pythonpath
# to an unmodified c7646a7168b3 export matching pyproject/uv.lock/qsl.toml; do not
# hardcode worktrees.
REPO_ROOT = ROOT.parent.parent if ROOT.parent.name == ".worktrees" else ROOT
for _qpk_src in (
    REPO_ROOT / "QuantPlatformKit" / "src",
    REPO_ROOT.parent / "QuantPlatformKit" / "src",
):
    if (_qpk_src / "quant_platform_kit").is_dir() and str(_qpk_src) not in sys.path:
        sys.path.append(str(_qpk_src))
        break

from application.account_new_risk_gate_support import (
    ACCOUNT_NEW_RISK_GATE_ENV,
    apply_combined_scale_to_allocation_targets,
    build_account_new_risk_snapshot,
    build_snapshot_from_portfolio,
    evaluate_portfolio_new_risk_admission,
    maybe_publish_attention_for_admission,
    new_risk_buy_prohibited,
    reset_attention_sent_keys_for_tests,
    set_cycle_snapshot,
)
from application.execution_service import execute_rebalance_cycle
from application.rebalance_service import run_strategy_core
from application.runtime_dependencies import SchwabRebalanceConfig, SchwabRebalanceRuntime
from notifications.telegram import build_translator
from quant_platform_kit.common.models import PortfolioSnapshot, QuoteSnapshot
from quant_platform_kit.common.port_adapters import (
    CallableExecutionPort,
    CallableMarketDataPort,
    CallableNotificationPort,
    CallablePortfolioPort,
)
from quant_platform_kit.risk.account_new_risk_gate import NewRiskDisposition


class AccountNewRiskGateSupportTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_cycle_snapshot(None)
        reset_attention_sent_keys_for_tests()
        os.environ.pop(ACCOUNT_NEW_RISK_GATE_ENV, None)
        os.environ.pop("SCHWAB_MAX_DAILY_LOSS_USD", None)
        os.environ.pop("MAX_DAILY_LOSS_USD", None)
        os.environ.pop("RUNTIME_TARGET_JSON", None)

    def test_missing_equity_prohibits_fail_closed(self) -> None:
        portfolio = {"market_values": {"SOXL": 0.0}, "liquid_cash": 100.0}
        snapshot = build_account_new_risk_snapshot(portfolio)
        # Missing equity keeps observation_ok=False under cycle-health, which
        # still fails closed via EQUITY_UNKNOWN_FAIL_CLOSED below -- the
        # circuit-breaker axis is not durably OPEN here since there is no
        # explicit unknown-pending / durable-breaker evidence this cycle.
        self.assertEqual(snapshot["observation_status"], "UNAVAILABLE")
        self.assertIsNone(snapshot["equity_usd"])
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertTrue(new_risk_buy_prohibited(result))
        self.assertIn("EQUITY_UNKNOWN_FAIL_CLOSED", result.reason_codes)

    def test_unknown_pending_orders_prohibits_and_opens_breaker(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "unknown_pending_orders": True,
        }
        snapshot = build_account_new_risk_snapshot(portfolio)
        self.assertEqual(snapshot["circuit_breaker_state"], "OPEN")
        self.assertEqual(snapshot["reconciliation_status"], "UNVERIFIED")
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertTrue(new_risk_buy_prohibited(result))
        self.assertIn("CIRCUIT_BREAKER_OPEN", result.reason_codes)
        self.assertIn("RECONCILIATION_NOT_VERIFIED", result.reason_codes)

    def test_default_projection_without_equity_still_prohibits(self) -> None:
        """A bare portfolio with no equity and no explicit snapshot still fails closed."""
        result = evaluate_portfolio_new_risk_admission({})
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

    def test_healthy_equity_only_portfolio_allows_new_risk(self) -> None:
        """A plain healthy-equity portfolio (no explicit snapshot) now derives
        COMPLETE/VERIFIED/CLOSED via cycle-health instead of failing closed on
        soft UNAVAILABLE/UNVERIFIED/OPEN defaults."""
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
        }
        with patch(
            "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
            return_value=None,
        ):
            snapshot = build_account_new_risk_snapshot(portfolio)
            self.assertEqual(snapshot["observation_status"], "COMPLETE")
            self.assertEqual(snapshot["reconciliation_status"], "VERIFIED")
            self.assertEqual(snapshot["circuit_breaker_state"], "CLOSED")
            result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.ALLOW_NEW_RISK)

    def test_explicit_healthy_snapshot_allows_new_risk(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "account_new_risk_snapshot": {
                "observation_status": "COMPLETE",
                "reconciliation_status": "VERIFIED",
                "circuit_breaker_state": "CLOSED",
            },
        }
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.ALLOW_NEW_RISK)
        self.assertFalse(result.live_authority_granted)

    def test_snapshot_maps_total_equity_from_portfolio(self) -> None:
        snapshot = build_snapshot_from_portfolio({"total_equity": 12_345.0})
        self.assertEqual(snapshot.equity_usd, 12_345.0)

    def test_explicit_daily_loss_at_limit_prohibits_buy(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "account_new_risk_snapshot": {
                "daily_loss_usd": 100.0,
                "max_daily_loss_usd": 100.0,
            },
        }
        with patch(
            "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
            return_value=None,
        ):
            snapshot = build_snapshot_from_portfolio(portfolio)
            result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(snapshot.daily_loss_usd, 100.0)
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("DAILY_LOSS_LIMIT_EXCEEDED", result.reason_codes)
        self.assertTrue(new_risk_buy_prohibited(result))

    def test_unconfigured_daily_loss_limit_omits_axis(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            # daily_loss fact absent / invalid must not invent a prohibit when
            # no max_daily_loss_usd is configured.
            "account_new_risk_snapshot": {"daily_loss_usd": float("nan")},
        }
        with patch(
            "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
            return_value=None,
        ):
            result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.ALLOW_NEW_RISK)
        self.assertNotIn("DAILY_LOSS_UNKNOWN_FAIL_CLOSED", result.reason_codes)
        self.assertNotIn("DAILY_LOSS_LIMIT_EXCEEDED", result.reason_codes)

    def test_configured_limit_without_daily_loss_fact_fails_closed(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "account_new_risk_snapshot": {"max_daily_loss_usd": 100.0},
        }
        with patch(
            "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
            return_value=None,
        ):
            result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("DAILY_LOSS_UNKNOWN_FAIL_CLOSED", result.reason_codes)

    def test_equity_formula_smooths_from_aggressive_to_conservative(self) -> None:
        from application.account_new_risk_gate_support import (
            resolve_max_daily_loss_usd,
            resolve_max_daily_loss_usd_from_equity_formula,
        )

        formula = {"pct_max": 0.05, "pct_min": 0.01, "equity_scale_usd": 2000.0}
        # E=400 → p = 0.01 + 0.04 * 2000/2400 = 0.04333... → limit ≈ 17.333
        small = resolve_max_daily_loss_usd_from_equity_formula(400.0, formula)
        mid = resolve_max_daily_loss_usd_from_equity_formula(2000.0, formula)
        large = resolve_max_daily_loss_usd_from_equity_formula(20_000.0, formula)
        self.assertIsNotNone(small)
        self.assertIsNotNone(mid)
        self.assertIsNotNone(large)
        assert small is not None and mid is not None and large is not None
        self.assertAlmostEqual(small, 400.0 * (0.01 + 0.04 * 2000.0 / 2400.0))
        self.assertAlmostEqual(mid, 2000.0 * 0.03)
        self.assertAlmostEqual(large, 20_000.0 * (0.01 + 0.04 * 2000.0 / 22_000.0))
        # Fraction falls as equity rises.
        self.assertGreater(small / 400.0, mid / 2000.0)
        self.assertGreater(mid / 2000.0, large / 20_000.0)
        # Approved mid-equity point: E=2000 → limit exactly 60.
        self.assertAlmostEqual(mid, 60.0)

        portfolio = {
            "total_equity": 400.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "account_new_risk_snapshot": {
                "daily_loss_usd": 18.0,
                "daily_loss_baseline_equity_usd": 400.0,
            },
        }
        target = json.dumps(
            {"runtime_risk_limits": {"max_daily_loss_equity_formula": formula}}
        )
        with patch.dict(os.environ, {"RUNTIME_TARGET_JSON": target}, clear=False):
            with patch(
                "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
                return_value=None,
            ):
                self.assertAlmostEqual(
                    resolve_max_daily_loss_usd(portfolio) or 0.0,
                    400.0 * (0.01 + 0.04 * 2000.0 / 2400.0),
                )
                result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("DAILY_LOSS_LIMIT_EXCEEDED", result.reason_codes)

    def test_equity_formula_invalid_or_missing_equity_fails_closed_not_absent(self) -> None:
        from application.account_new_risk_gate_support import (
            resolve_max_daily_loss_resolution,
            resolve_max_daily_loss_usd_from_equity_formula,
        )

        healthy = {
            "total_equity": 2000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "account_new_risk_snapshot": {
                "observation_status": "COMPLETE",
                "reconciliation_status": "VERIFIED",
                "circuit_breaker_state": "CLOSED",
                "daily_loss_usd": 0.0,
                "daily_loss_baseline_equity_usd": 2000.0,
            },
        }
        cases = [
            {"pct_max": 0.05, "pct_min": 0.01, "equity_scale_usd": 0},
            {"pct_max": 0.05, "pct_min": 0.01, "equity_scale_usd": float("nan")},
            {"pct_max": 0.05, "pct_min": 0.01, "equity_scale_usd": True},
            {"pct_max": 0.01, "pct_min": 0.05, "equity_scale_usd": 2000.0},
            {"pct_max": 0.05, "pct_min": 0.01},  # missing scale
        ]
        for formula in cases:
            with self.subTest(formula=formula):
                self.assertIsNone(
                    resolve_max_daily_loss_usd_from_equity_formula(2000.0, formula)
                )
                target = json.dumps(
                    {
                        "runtime_risk_limits": {
                            "max_daily_loss_equity_formula": formula,
                            # Must not fall through to schedule when formula is explicit+illegal.
                            "max_daily_loss_equity_schedule": [
                                {"max_daily_loss_pct": 0.01},
                            ],
                        }
                    }
                )
                with patch.dict(os.environ, {"RUNTIME_TARGET_JSON": target}, clear=False):
                    with patch(
                        "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
                        return_value=None,
                    ):
                        resolution = resolve_max_daily_loss_resolution(healthy)
                        result = evaluate_portfolio_new_risk_admission(healthy)
                self.assertEqual(resolution.status, "invalid")
                self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
                self.assertTrue(new_risk_buy_prohibited(result))
                self.assertTrue(
                    set(result.reason_codes)
                    & {
                        "DAILY_LOSS_UNKNOWN_FAIL_CLOSED",
                        "SNAPSHOT_VALIDATION_FAIL_CLOSED",
                    }
                )

        # Legitimate formula present but equity missing → invalid, not omitted axis.
        missing_equity = {
            "account_new_risk_snapshot": {
                "observation_status": "COMPLETE",
                "reconciliation_status": "VERIFIED",
                "circuit_breaker_state": "CLOSED",
                "daily_loss_usd": 0.0,
            },
        }
        legal = {"pct_max": 0.05, "pct_min": 0.01, "equity_scale_usd": 2000.0}
        target = json.dumps(
            {"runtime_risk_limits": {"max_daily_loss_equity_formula": legal}}
        )
        with patch.dict(os.environ, {"RUNTIME_TARGET_JSON": target}, clear=False):
            with patch(
                "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
                return_value=None,
            ):
                resolution = resolve_max_daily_loss_resolution(missing_equity)
                result = evaluate_portfolio_new_risk_admission(missing_equity)
        self.assertEqual(resolution.status, "invalid")
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertTrue(new_risk_buy_prohibited(result))

        # Truly absent daily-loss config remains optional (no invented axis).
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RUNTIME_TARGET_JSON", None)
            with patch(
                "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
                return_value=None,
            ):
                resolution = resolve_max_daily_loss_resolution(healthy)
                result = evaluate_portfolio_new_risk_admission(healthy)
        self.assertEqual(resolution.status, "absent")
        self.assertEqual(result.disposition, NewRiskDisposition.ALLOW_NEW_RISK)

    def test_equity_schedule_uses_larger_pct_for_small_account(self) -> None:
        from application.account_new_risk_gate_support import (
            resolve_max_daily_loss_usd,
            resolve_max_daily_loss_usd_from_equity_schedule,
        )

        schedule = [
            {"equity_lte_usd": 500, "max_daily_loss_pct": 0.05},
            {"equity_lte_usd": 5000, "max_daily_loss_pct": 0.02},
            {"max_daily_loss_pct": 0.01},
        ]
        self.assertAlmostEqual(
            resolve_max_daily_loss_usd_from_equity_schedule(400.0, schedule) or 0.0,
            20.0,
        )
        self.assertAlmostEqual(
            resolve_max_daily_loss_usd_from_equity_schedule(2000.0, schedule) or 0.0,
            40.0,
        )
        self.assertAlmostEqual(
            resolve_max_daily_loss_usd_from_equity_schedule(20_000.0, schedule) or 0.0,
            200.0,
        )

        portfolio = {
            "total_equity": 400.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "account_new_risk_snapshot": {
                "daily_loss_usd": 25.0,
                "daily_loss_baseline_equity_usd": 400.0,
            },
        }
        target = json.dumps(
            {
                "runtime_risk_limits": {
                    "max_daily_loss_equity_schedule": schedule,
                }
            }
        )
        with patch.dict(os.environ, {"RUNTIME_TARGET_JSON": target}, clear=False):
            with patch(
                "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
                return_value=None,
            ):
                self.assertAlmostEqual(resolve_max_daily_loss_usd(portfolio) or 0.0, 20.0)
                result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("DAILY_LOSS_LIMIT_EXCEEDED", result.reason_codes)

    def test_snapshot_maps_production_drift_status_from_account_new_risk_snapshot(self) -> None:
        snapshot = build_snapshot_from_portfolio(
            {
                "total_equity": 10_000.0,
                "account_new_risk_snapshot": {"production_drift_status": "review"},
            }
        )
        self.assertEqual(snapshot.production_drift_status, "review")

    def test_production_drift_review_prohibits_new_risk(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "account_new_risk_snapshot": {"production_drift_status": "review"},
        }
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("PRODUCTION_DRIFT_REVIEW", result.reason_codes)

    def test_production_drift_critical_prohibits_new_risk(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "account_new_risk_snapshot": {"production_drift_status": "critical"},
        }
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("PRODUCTION_DRIFT_CRITICAL", result.reason_codes)

    def test_production_drift_invalid_status_prohibits_new_risk(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "account_new_risk_snapshot": {"production_drift_status": "invalid"},
        }
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("PRODUCTION_DRIFT_STATUS_INVALID_FAIL_CLOSED", result.reason_codes)

    def test_production_drift_status_prefers_account_snapshot_over_portfolio(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "production_drift_status": "healthy",
            "account_new_risk_snapshot": {"production_drift_status": "critical"},
        }
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("PRODUCTION_DRIFT_CRITICAL", result.reason_codes)

    def test_production_drift_status_falls_back_to_portfolio(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "production_drift_status": "review",
        }
        result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("PRODUCTION_DRIFT_REVIEW", result.reason_codes)

    def test_absent_production_drift_status_still_allows_when_healthy(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
        }
        with patch(
            "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
            return_value=None,
        ):
            result = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertEqual(result.disposition, NewRiskDisposition.ALLOW_NEW_RISK)

    def test_store_critical_production_drift_prohibits_when_status_absent(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
        }
        with patch(
            "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
            return_value="critical",
        ) as store_resolver:
            result = evaluate_portfolio_new_risk_admission(portfolio)
        store_resolver.assert_called_once()
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("PRODUCTION_DRIFT_CRITICAL", result.reason_codes)

    def test_explicit_production_drift_status_skips_store_lookup(self) -> None:
        portfolio = {
            "total_equity": 50_000.0,
            "metadata": {"total_equity_source": "broker_liquidation_value"},
            "account_new_risk_snapshot": {"production_drift_status": "critical"},
        }
        with patch(
            "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
            return_value="review",
        ) as store_resolver:
            result = evaluate_portfolio_new_risk_admission(portfolio)
        store_resolver.assert_not_called()
        self.assertEqual(result.disposition, NewRiskDisposition.NEW_RISK_PROHIBITED)
        self.assertIn("PRODUCTION_DRIFT_CRITICAL", result.reason_codes)

    def test_rebalance_policy_a_store_read_is_fail_soft_when_unbound(self) -> None:
        """Gate may read PerformanceStore; unbound/empty probe must not invent bans."""
        plan = {
            "account_hash": "demo",
            "allocation": {
                "target_mode": "value",
                "strategy_symbols": (),
                "risk_symbols": (),
                "income_symbols": (),
                "safe_haven_symbols": (),
                "targets": {},
            },
            "portfolio": {
                "metadata": {
                    "strategy_domain": "us_equity",
                    "total_equity_source": "broker_liquidation_value",
                },
                "market_values": {},
                "quantities": {},
                "portfolio_rows": (),
                "total_equity": 50_000.0,
                "liquid_cash": 50_000.0,
                "cash_sweep_symbol": None,
            },
            "execution": {
                "trade_threshold_value": 10.0,
                "reserved_cash": 0.0,
                "signal_display": "No trade",
                "dashboard_text": "dashboard",
                "separator": "---",
                "signal_date": "2026-09-17",
                "effective_date": "2026-09-18",
            },
        }
        snapshot = PortfolioSnapshot(
            as_of="2026-09-17",
            total_equity=50_000.0,
            buying_power=50_000.0,
            positions=(),
            metadata={},
        )
        resolver = patch(
            "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
            return_value=None,
        )
        with resolver as store_resolver:
            result = run_strategy_core(
                runtime=SchwabRebalanceRuntime(
                    fetch_reference_history=lambda: [],
                    portfolio_port=CallablePortfolioPort(lambda: snapshot),
                    market_data_port=CallableMarketDataPort(quote_loader=lambda _symbol: None),
                    resolve_rebalance_plan=lambda **_kwargs: plan,
                    notifications=CallableNotificationPort(lambda _message: None),
                    execution_port_factory=lambda _account_hash: CallableExecutionPort(
                        lambda _order_intent: None
                    ),
                ),
                config=SchwabRebalanceConfig(
                    translator=build_translator("en"),
                    strategy_display_name="Demo",
                    limit_buy_premium=1.0,
                    sell_settle_delay_sec=0.0,
                    strategy_profile="demo_profile",
                ),
            )

        store_resolver.assert_called()
        self.assertNotIn(
            "production_drift_status",
            result.portfolio["account_new_risk_snapshot"],
        )

    def test_combined_scale_halves_allocation_targets(self) -> None:
        scaled = apply_combined_scale_to_allocation_targets(
            {"targets": {"SOXL": 0.6, "SOXX": 0.4}},
            0.5,
        )
        self.assertEqual(scaled["targets"], {"SOXL": 0.3, "SOXX": 0.2})

    def test_missing_combined_scale_leaves_targets(self) -> None:
        allocation = {"targets": {"SOXL": 0.6}}
        self.assertEqual(
            apply_combined_scale_to_allocation_targets(allocation, None)["targets"],
            {"SOXL": 0.6},
        )

    def test_attention_notify_on_new_risk_prohibit_dedupes(self) -> None:
        reset_attention_sent_keys_for_tests()
        portfolio = {
            "total_equity": 50_000.0,
            "strategy_profile": "soxl_soxx_trend_income",
            "account_new_risk_snapshot": {
                "production_drift_status": "critical",
            },
        }
        plan = {"account_hash": "00682abc"}
        admission = evaluate_portfolio_new_risk_admission(portfolio)
        self.assertTrue(new_risk_buy_prohibited(admission))
        self.assertIn("PRODUCTION_DRIFT_CRITICAL", admission.reason_codes)
        snapshot = build_snapshot_from_portfolio(portfolio)
        self.assertEqual(snapshot.production_drift_status, "critical")
        payloads: list[str] = []

        def _sender(*, text: str, alert_key: str | None = None, **_kwargs) -> bool:
            payloads.append(text)
            return True

        with unittest.mock.patch.dict(os.environ, {"NOTIFY_LANG": "zh"}, clear=False):
            counts = maybe_publish_attention_for_admission(
                admission,
                portfolio=portfolio,
                plan=plan,
                snapshot=snapshot,
                telegram_sender=_sender,
                log_message=lambda *_a, **_k: None,
            )
        self.assertEqual(counts.get("sent"), 1)
        self.assertEqual(len(payloads), 1)
        text = payloads[0]
        self.assertIn("00682abc", text)
        self.assertIn("生产偏离严重", text)
        self.assertNotIn("原因：new_risk_prohibited", text)
        self.assertIn("打开管理站处理恢复", text)
        self.assertNotIn("accept ≠ live", text)
        counts2 = maybe_publish_attention_for_admission(
            admission,
            portfolio=portfolio,
            plan=plan,
            snapshot=snapshot,
            telegram_sender=_sender,
            log_message=lambda *_a, **_k: None,
        )
        self.assertEqual(counts2.get("sent"), 0)
        self.assertEqual(counts2.get("skipped"), 1)
        self.assertEqual(len(payloads), 1)



class AccountNewRiskGateExecutionCycleTests(unittest.TestCase):
    def tearDown(self) -> None:
        set_cycle_snapshot(None)
        reset_attention_sent_keys_for_tests()
        os.environ.pop(ACCOUNT_NEW_RISK_GATE_ENV, None)
        os.environ.pop("RUNTIME_TARGET_JSON", None)
        os.environ.pop("SCHWAB_MAX_DAILY_LOSS_USD", None)
        os.environ.pop("MAX_DAILY_LOSS_USD", None)

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
        self.assertEqual(
            result.portfolio["account_new_risk_snapshot"]["observation_status"],
            "UNAVAILABLE",
        )
        self.assertTrue(any("Account new-risk gate" in log for log in result.trade_logs))


    def test_execution_cycle_blocks_buys_when_production_drift_review(self) -> None:
        result, submitted_orders = self._run_buy_cycle(
            portfolio_overrides={
                "account_new_risk_snapshot": {
                    "observation_status": "COMPLETE",
                    "reconciliation_status": "VERIFIED",
                    "circuit_breaker_state": "CLOSED",
                    "production_drift_status": "review",
                },
            }
        )
        self.assertEqual(submitted_orders, [])
        self.assertTrue(any("Account new-risk gate" in log for log in result.trade_logs))
        self.assertTrue(any("NEW_RISK_PROHIBITED" in log for log in result.trade_logs))

    def test_execution_cycle_zero_buy_submit_when_equity_formula_invalid(self) -> None:
        """Explicit illegal formula must not omit the daily-loss axis and allow buys."""
        formula = {"pct_max": 0.05, "pct_min": 0.01, "equity_scale_usd": 0}
        target = json.dumps(
            {"runtime_risk_limits": {"max_daily_loss_equity_formula": formula}}
        )
        with patch.dict(os.environ, {"RUNTIME_TARGET_JSON": target}, clear=False):
            with patch(
                "application.execution_service.attach_daily_loss_fact_to_portfolio",
                side_effect=lambda portfolio, **_kwargs: dict(portfolio),
            ):
                with patch(
                    "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
                    return_value=None,
                ):
                    _result, submitted_orders = self._run_buy_cycle(
                        portfolio_overrides={
                            "total_equity": 2000.0,
                            "account_new_risk_snapshot": {
                                "observation_status": "COMPLETE",
                                "reconciliation_status": "VERIFIED",
                                "circuit_breaker_state": "CLOSED",
                                "daily_loss_usd": 0.0,
                                "daily_loss_baseline_equity_usd": 2000.0,
                            },
                        }
                    )
        self.assertEqual(submitted_orders, [])
        self.assertTrue(any("NEW_RISK_PROHIBITED" in log for log in _result.trade_logs))

    def test_execution_cycle_zero_buy_submit_when_daily_loss_fact_omitted(self) -> None:
        """F2: configured limit + omitted/unverified daily-loss fact → zero buy submit."""

        def _omit_unverified_fact(portfolio, **_kwargs):
            out = dict(portfolio)
            snap = dict(out.get("account_new_risk_snapshot") or {})
            snap["max_daily_loss_usd"] = 100.0
            snap.pop("daily_loss_usd", None)
            out["account_new_risk_snapshot"] = snap
            return out

        with patch(
            "application.execution_service.attach_daily_loss_fact_to_portfolio",
            side_effect=_omit_unverified_fact,
        ):
            with patch(
                "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
                return_value=None,
            ):
                result, submitted_orders = self._run_buy_cycle(
                    portfolio_overrides={
                        "total_equity": 50_000.0,
                        "account_new_risk_snapshot": {
                            "observation_status": "COMPLETE",
                            "reconciliation_status": "VERIFIED",
                            "circuit_breaker_state": "CLOSED",
                        },
                    }
                )
        self.assertEqual(submitted_orders, [])
        self.assertTrue(any("NEW_RISK_PROHIBITED" in log for log in result.trade_logs))
        self.assertTrue(
            any("DAILY_LOSS_UNKNOWN_FAIL_CLOSED" in log for log in result.trade_logs)
        )

    def test_execution_cycle_passes_plan_account_hash_to_daily_loss_attach(self) -> None:
        """C1: attach must receive plan account_hash (same identity as order submit)."""
        captured: dict[str, object] = {}

        def _capture_attach(portfolio, **kwargs):
            captured["expected_account_hash"] = kwargs.get("expected_account_hash")
            return dict(portfolio)

        with patch(
            "application.execution_service.attach_daily_loss_fact_to_portfolio",
            side_effect=_capture_attach,
        ):
            with patch(
                "application.account_new_risk_gate_support.resolve_production_drift_status_from_store",
                return_value=None,
            ):
                _result, _submitted = self._run_buy_cycle(
                    portfolio_overrides={
                        "account_new_risk_snapshot": {
                            "observation_status": "COMPLETE",
                            "reconciliation_status": "VERIFIED",
                            "circuit_breaker_state": "CLOSED",
                        },
                    }
                )
        self.assertEqual(captured.get("expected_account_hash"), "demo")

    def test_execution_cycle_allows_buys_when_healthy(self) -> None:
        result, submitted_orders = self._run_buy_cycle(
            portfolio_overrides={
                "account_new_risk_snapshot": {
                    "observation_status": "COMPLETE",
                    "reconciliation_status": "VERIFIED",
                    "circuit_breaker_state": "CLOSED",
                },
            }
        )
        self.assertEqual(len(submitted_orders), 1)
        self.assertEqual(str(getattr(submitted_orders[0], "side", "")).lower(), "buy")
        self.assertTrue(any("disposition=ALLOW_NEW_RISK" in log for log in result.trade_logs))
        self.assertFalse(any("NEW_RISK_PROHIBITED" in log for log in result.trade_logs))

    def test_execution_cycle_prints_gate_axes_to_stdout(self) -> None:
        from io import StringIO
        from contextlib import redirect_stdout

        buf = StringIO()
        with redirect_stdout(buf):
            result, _submitted = self._run_buy_cycle(
                portfolio_overrides={
                    "account_new_risk_snapshot": {
                        "observation_status": "COMPLETE",
                        "reconciliation_status": "VERIFIED",
                        "circuit_breaker_state": "CLOSED",
                    },
                }
            )
        printed = buf.getvalue()
        self.assertIn("[Account new-risk gate]", printed)
        self.assertIn("observation=COMPLETE", printed)
        self.assertIn("reconciliation=VERIFIED", printed)
        self.assertIn("breaker=CLOSED", printed)
        self.assertTrue(any("disposition=ALLOW_NEW_RISK" in log for log in result.trade_logs))

    def test_execution_cycle_scales_targets_for_half_combined_scale(self) -> None:
        result, submitted_orders = self._run_buy_cycle(
            portfolio_overrides={
                "total_equity": 40_000.0,
                "account_new_risk_snapshot": {
                    "observation_status": "COMPLETE",
                    "reconciliation_status": "VERIFIED",
                    "circuit_breaker_state": "CLOSED",
                    "drawdown_from_peak": 0.075,
                },
            }
        )
        self.assertTrue(
            any("applied_to_allocation_targets" in log for log in result.trade_logs)
        )
        self.assertEqual(len(submitted_orders), 1)
        # Target value path: half envelope scale → half buy size from full target.
        self.assertEqual(submitted_orders[0].quantity, 2)

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
