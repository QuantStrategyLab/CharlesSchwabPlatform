import unittest
from datetime import datetime, timezone
from types import SimpleNamespace

from quant_platform_kit.common.models import PortfolioSnapshot
from quant_platform_kit.common.strategy_contracts import (
    PositionTarget,
    StrategyDecision,
    StrategyContext,
)
from quant_platform_kit.risk.contracts import RuntimeRiskLimits
from quant_platform_kit.risk.gate import apply_risk_gate

from decision_mapper import map_strategy_decision_to_plan


class DecisionMapperTests(unittest.TestCase):
    @staticmethod
    def _runtime_limits() -> RuntimeRiskLimits:
        symbols = ("SOXL", "SOXX", "BOXX")
        return RuntimeRiskLimits(
            allowed_symbols=symbols,
            product_leverage_factors={"SOXL": 3, "SOXX": 1, "BOXX": 1},
            nominal_caps={"SOXL": 0.679, "SOXX": 0.873, "BOXX": 0.97},
            total_nominal_exposure_cap=0.97,
            total_effective_exposure_cap=2.328,
            max_positions=8,
        )

    @staticmethod
    def _runtime_snapshot() -> PortfolioSnapshot:
        return PortfolioSnapshot(
            as_of=datetime(2026, 9, 17, tzinfo=timezone.utc),
            total_equity=100_000.0,
            buying_power=100_000.0,
            cash_balance=100_000.0,
            positions=(),
            metadata={"account_hash": "demo"},
        )

    def test_runtime_limits_keep_rounded_value_plan_within_caps(self):
        snapshot = self._runtime_snapshot()
        decision = StrategyDecision(
            positions=(
                PositionTarget(symbol="SOXL", target_weight=0.679),
                PositionTarget(symbol="SOXX", target_weight=0.194),
                PositionTarget(symbol="BOXX", target_weight=0.097),
            )
        )
        approved = apply_risk_gate(
            decision,
            portfolio_snapshot=snapshot,
            max_single_weight=1.0,
            max_total_exposure=1.0,
            runtime_risk_limits=self._runtime_limits(),
        )
        plan = map_strategy_decision_to_plan(
            approved,
            snapshot=snapshot,
            strategy_profile="soxl_soxx_trend_income",
        )

        targets = plan["allocation"]["targets"]
        self.assertLessEqual(sum(targets.values()), 97_000.0 + 1e-6)
        self.assertLessEqual(targets["SOXL"], 67_900.0 + 1e-6)
        self.assertLessEqual(targets["SOXX"], 19_400.0 + 1e-6)
        self.assertLessEqual(targets["BOXX"], 9_700.0 + 1e-6)

    def test_runtime_limit_rejection_maps_to_zero_order_plan(self):
        snapshot = self._runtime_snapshot()
        rejected = apply_risk_gate(
            StrategyDecision(
                positions=(PositionTarget(symbol="SOXL", target_weight=0.70),)
            ),
            portfolio_snapshot=snapshot,
            max_single_weight=1.0,
            max_total_exposure=1.0,
            runtime_risk_limits=self._runtime_limits(),
        )
        plan = map_strategy_decision_to_plan(
            rejected,
            snapshot=snapshot,
            strategy_profile="soxl_soxx_trend_income",
        )

        self.assertEqual(plan["allocation"]["targets"], {})
        self.assertEqual(plan["execution"]["execution_status"], "blocked")
        self.assertEqual(plan["execution"]["no_op_reason"], "rejected:runtime_risk_limits")

    def test_synthetic_bound_limits_gate_mapper_cycle(self):
        """Binding-success limits → QPK gate → mapper plan (E denominator, reserve, budgets)."""
        from quant_platform_kit.common.strategy_contracts import BudgetIntent
        import strategy_runtime as strategy_runtime_module
        from dataclasses import replace
        from unittest.mock import patch
        from quant_platform_kit.common.runtime_target import build_runtime_target
        from runtime_config_support import PlatformRuntimeSettings
        from quant_platform_kit.common.strategy_contracts import (
            StrategyManifest,
            StrategyRuntimeAdapter,
        )

        symbols = ("SOXL", "SOXX", "BOXX", "SCHD", "DGRO", "SGOV", "SPYI", "QQQI")
        policy = {
            "binding": {
                "account_scope": "live-account-scope",
                "runtime_scope": "schwab-live-service",
                "account_hash": "account-hash",
                "strategy_profile": "soxl_soxx_trend_income",
                "ues_revision": "ues-revision",
                "execution_mode": "live",
                "cash_only_execution": True,
                "reserved_cash_ratio": 0.03,
                "options_enabled": False,
            },
            "allowed_symbols": list(symbols),
            "product_leverage_factors": {"SOXL": 3, **{symbol: 1 for symbol in symbols[1:]}},
            "nominal_caps": {
                "SOXL": 0.679,
                "SOXX": 0.873,
                **{symbol: 0.97 for symbol in symbols[2:]},
            },
            "total_nominal_exposure_cap": 0.97,
            "total_effective_exposure_cap": 2.328,
            "max_positions": 8,
            "exit_parameters": {"trend_exit_buffer": 0.02},
        }

        class _SoxlEntrypoint:
            manifest = StrategyManifest(
                profile="soxl_soxx_trend_income",
                domain="us_equity",
                display_name="SOXL/SOXX Trend Income",
                description="synthetic",
                required_inputs=frozenset({"benchmark_history", "portfolio_snapshot"}),
                default_config={
                    "benchmark_symbol": "SOXX",
                    "managed_symbols": symbols,
                    "trend_exit_buffer": 0.02,
                    "cash_reserve_ratio": 0.03,
                    "option_overlay_enabled": False,
                    "option_growth_overlay_enabled": False,
                    "option_income_overlay_enabled": False,
                },
            )

            def evaluate(self, ctx):
                self.ctx = ctx
                return StrategyDecision()

        entrypoint = _SoxlEntrypoint()
        target = build_runtime_target(
            platform_id="schwab",
            strategy_profile="soxl_soxx_trend_income",
            dry_run_only=False,
            account_scope="live-account-scope",
            service_name="schwab-live-service",
            strategy_release={
                "release_id": "soxl-release",
                "manifest_sha256": "a" * 64,
                "strategy_revision": "ues-revision",
                "config_sha256": "b" * 64,
                "risk_policy_sha256": "c" * 64,
                "evidence_sha256": "d" * 64,
                "plugin_bundle_sha256": "e" * 64,
                "effective_session": "2026-09-17",
            },
        )
        settings = replace(
            PlatformRuntimeSettings(
                strategy_profile="soxl_soxx_trend_income",
                strategy_display_name="SOXL/SOXX Trend Income",
                strategy_domain="us_equity",
                notify_lang="en",
                dry_run_only=False,
                reserved_cash_ratio=0.03,
                cash_only_execution=True,
            ),
            runtime_target=target,
            trusted_runtime_risk_policy=policy,
        )
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(portfolio_input_name="portfolio_snapshot"),
            runtime_settings=settings,
            merged_runtime_config=dict(entrypoint.manifest.default_config),
        )
        # E=100_000 NAV; S=buying_power deliberately larger so weight math must use E.
        equity_e = 100_000.0
        buying_power_s = 500_000.0
        snapshot = PortfolioSnapshot(
            as_of=datetime(2026, 9, 17, tzinfo=timezone.utc),
            total_equity=equity_e,
            buying_power=buying_power_s,
            cash_balance=equity_e,
            positions=(),
            metadata={
                "account_hash": "account-hash",
                "total_equity_source": "broker_liquidation_value",
                "source_digest_sha256": "a" * 64,
            },
        )
        with patch.object(strategy_runtime_module, "_installed_ues_revision", return_value="ues-revision"):
            bound = runtime.evaluate(
                benchmark_history=[{"close": 1.0}],
                portfolio_snapshot=snapshot,
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )
        limits = entrypoint.ctx.capabilities["runtime_risk_limits"]
        self.assertEqual(bound.metadata["runtime_risk_status"], "verified:runtime_risk_limits")
        self.assertIsInstance(limits, RuntimeRiskLimits)

        approved_decision = StrategyDecision(
            positions=(
                PositionTarget(symbol="SOXL", target_weight=0.679),
                PositionTarget(symbol="SOXX", target_weight=0.194),
                PositionTarget(symbol="BOXX", target_weight=0.097),
            )
        )
        approved = apply_risk_gate(
            approved_decision,
            portfolio_snapshot=snapshot,
            max_single_weight=1.0,
            max_total_exposure=1.0,
            runtime_risk_limits=limits,
        )
        self.assertIn("risk_gate:passed", approved.risk_flags)
        plan = map_strategy_decision_to_plan(
            approved,
            snapshot=snapshot,
            strategy_profile="soxl_soxx_trend_income",
            runtime_metadata={
                "schwab_execution_policy": {
                    "reserved_cash_ratio": 0.03,
                    "cash_only_execution": True,
                },
                "execution_annotations": {"reserved_cash": equity_e * 0.03},
            },
        )
        targets = plan["allocation"]["targets"]
        # Cash reserve 0.03 must remain: total deployable ≤ 0.97 * E (not S).
        self.assertLessEqual(sum(targets.values()), equity_e * 0.97 + 1e-6)
        self.assertAlmostEqual(targets["SOXL"], equity_e * 0.679, places=4)
        self.assertNotAlmostEqual(targets["SOXL"], buying_power_s * 0.679, places=0)
        self.assertEqual(plan["execution"]["reserved_cash"], equity_e * 0.03)

        # Non-empty budgets are unsupported under explicit runtime limits → zero submit.
        budget_rejected = apply_risk_gate(
            StrategyDecision(
                positions=(PositionTarget(symbol="SOXL", target_weight=0.20),),
                budgets=(BudgetIntent(name="reserve", symbol="SOXL", amount=100.0),),
            ),
            portfolio_snapshot=snapshot,
            max_single_weight=1.0,
            max_total_exposure=1.0,
            runtime_risk_limits=limits,
        )
        budget_plan = map_strategy_decision_to_plan(
            budget_rejected,
            snapshot=snapshot,
            strategy_profile="soxl_soxx_trend_income",
        )
        self.assertEqual(budget_rejected.positions, ())
        self.assertEqual(budget_rejected.risk_flags, ("rejected:runtime_risk_limits",))
        self.assertEqual(budget_plan["allocation"]["targets"], {})
        self.assertEqual(budget_plan["execution"]["execution_status"], "blocked")

        # Over-limit reject must clear positions (no silent scale-down).
        overrun = apply_risk_gate(
            StrategyDecision(positions=(PositionTarget(symbol="SOXL", target_weight=0.70),)),
            portfolio_snapshot=snapshot,
            max_single_weight=1.0,
            max_total_exposure=1.0,
            runtime_risk_limits=limits,
        )
        self.assertEqual(overrun.positions, ())
        self.assertEqual(overrun.diagnostics.get("risk_gate"), "REJECT")
        self.assertNotIn("risk_gate:passed", overrun.risk_flags)

        # Income-role sleeve over the SOXL nominal cap also rejects without scaling.
        income_overrun = apply_risk_gate(
            StrategyDecision(
                positions=(
                    PositionTarget(symbol="SOXL", target_weight=0.70, role="income"),
                )
            ),
            portfolio_snapshot=snapshot,
            max_single_weight=1.0,
            max_total_exposure=1.0,
            runtime_risk_limits=limits,
        )
        self.assertEqual(income_overrun.positions, ())
        self.assertEqual(income_overrun.risk_flags, ("rejected:runtime_risk_limits",))
        income_plan = map_strategy_decision_to_plan(
            income_overrun,
            snapshot=snapshot,
            strategy_profile="soxl_soxx_trend_income",
        )
        self.assertEqual(income_plan["allocation"]["targets"], {})
        self.assertEqual(income_plan["execution"]["execution_status"], "blocked")

    def test_preserves_strategy_risk_rejection_for_zero_order_report(self):
        snapshot = SimpleNamespace(
            total_equity=120000.0,
            buying_power=20000.0,
            positions=(SimpleNamespace(symbol="TQQQ", quantity=10, market_value=8000.0),),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(),
            risk_flags=("rejected:too_many_positions",),
            diagnostics={
                "risk_gate": "REJECT",
                "reason": "raw upstream diagnostic must not be published",
            },
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="tqqq_growth_income",
        )

        execution = plan["execution"]
        self.assertEqual(execution["execution_status"], "blocked")
        self.assertEqual(execution["no_op_reason"], "rejected:too_many_positions")
        self.assertNotIn("raw upstream diagnostic", str(execution))

    def test_does_not_infer_risk_rejection_from_flag_without_gate(self):
        snapshot = SimpleNamespace(
            total_equity=120000.0,
            buying_power=20000.0,
            positions=(),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(),
            risk_flags=("rejected:too_many_positions",),
            diagnostics={},
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="tqqq_growth_income",
        )

        self.assertNotIn("execution_status", plan["execution"])
        self.assertNotIn("no_op_reason", plan["execution"])

    def test_unknown_risk_rejection_uses_fixed_fallback_reason(self):
        snapshot = SimpleNamespace(
            total_equity=120000.0,
            buying_power=20000.0,
            positions=(),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(),
            risk_flags=("rejected:future_reason",),
            diagnostics={
                "risk_gate": "REJECT",
                "reason": "raw upstream diagnostic must not be published",
            },
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="tqqq_growth_income",
        )

        self.assertEqual(plan["execution"]["execution_status"], "blocked")
        self.assertEqual(plan["execution"]["no_op_reason"], "strategy_risk_rejected")
        self.assertNotIn("future_reason", str(plan["execution"]))

    def test_maps_hybrid_growth_decision_to_execution_plan(self):
        snapshot = SimpleNamespace(
            total_equity=120000.0,
            buying_power=20000.0,
            positions=(
                SimpleNamespace(symbol="TQQQ", quantity=10, market_value=8000.0),
                SimpleNamespace(symbol="BOXX", quantity=20, market_value=4000.0),
                SimpleNamespace(symbol="SPYI", quantity=30, market_value=1500.0),
                SimpleNamespace(symbol="QQQI", quantity=30, market_value=1700.0),
            ),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(
                PositionTarget(symbol="TQQQ", target_value=30000.0),
                PositionTarget(symbol="BOXX", target_value=35000.0, role="safe_haven"),
                PositionTarget(symbol="SPYI", target_value=12000.0, role="income"),
                PositionTarget(symbol="QQQI", target_value=18000.0, role="income"),
            ),
            diagnostics={
                "signal_display": "💎 Trend Hold",
                "dashboard": "dashboard",
                "threshold": 1200.0,
                "reserved": 2500.0,
                "qqq_price": 400.0,
                "ma200": 380.0,
                "exit_line": 360.0,
                "real_buying_power": 20000.0,
                "total_equity": 120000.0,
                "dual_drive_volatility_delever_threshold_mode": "rolling_percentile",
                "dual_drive_volatility_delever_dynamic_threshold": 0.30,
                "dual_drive_volatility_delever_dynamic_sample_count": 252,
                "dual_drive_volatility_delever_metric": 0.312,
                "dual_drive_volatility_delever_applied": True,
                "dual_drive_volatility_delever_veto_reason": "taco_rebound_context",
                "dual_drive_volatility_delever_taco_veto_enabled": True,
                "dual_drive_volatility_delever_removed_value": 4500.0,
                "dual_drive_volatility_delever_redirect_symbol": "QQQM",
                "dual_drive_macro_risk_governor_applied": True,
                "dual_drive_macro_risk_governor_route": "risk_reduced",
                "dual_drive_crisis_defense_destination": "BOXX",
                "market_regime_control_route": "risk_reduced",
                "market_regime_control_reason_codes": ("macro:vix_crisis_level",),
            },
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="tqqq_growth_income",
        )

        self.assertEqual(plan["account_hash"], "demo")
        self.assertEqual(plan["allocation"]["target_mode"], "value")
        self.assertEqual(plan["allocation"]["strategy_symbols"], ("TQQQ", "BOXX", "QQQI", "SPYI"))
        self.assertEqual(plan["allocation"]["targets"]["BOXX"], 35000.0)
        self.assertEqual(plan["portfolio"]["cash_sweep_symbol"], "BOXX")
        self.assertEqual(plan["portfolio"]["portfolio_rows"], (("TQQQ", "BOXX"), ("QQQI", "SPYI")))
        self.assertEqual(plan["execution"]["trade_threshold_value"], 1200.0)
        self.assertEqual(plan["execution"]["dual_drive_volatility_delever_threshold_mode"], "rolling_percentile")
        self.assertEqual(plan["execution"]["dual_drive_volatility_delever_dynamic_threshold"], 0.30)
        self.assertEqual(plan["execution"]["dual_drive_volatility_delever_dynamic_sample_count"], 252)
        self.assertEqual(plan["execution"]["dual_drive_volatility_delever_metric"], 0.312)
        self.assertIs(plan["execution"]["dual_drive_volatility_delever_applied"], True)
        self.assertEqual(plan["execution"]["dual_drive_volatility_delever_veto_reason"], "taco_rebound_context")
        self.assertIs(plan["execution"]["dual_drive_volatility_delever_taco_veto_enabled"], True)
        self.assertEqual(plan["execution"]["dual_drive_volatility_delever_removed_value"], 4500.0)
        self.assertEqual(plan["execution"]["dual_drive_volatility_delever_redirect_symbol"], "QQQM")
        self.assertIs(plan["execution"]["dual_drive_macro_risk_governor_applied"], True)
        self.assertEqual(plan["execution"]["dual_drive_macro_risk_governor_route"], "risk_reduced")
        self.assertEqual(plan["execution"]["dual_drive_crisis_defense_destination"], "BOXX")
        self.assertEqual(plan["execution"]["market_regime_control_route"], "risk_reduced")
        self.assertEqual(plan["execution"]["market_regime_control_reason_codes"], ("macro:vix_crisis_level",))
        self.assertNotIn("strategy_symbols", plan)
        self.assertNotIn("sell_order_symbols", plan)
        self.assertNotIn("buy_order_symbols", plan)
        self.assertNotIn("target_values", plan)

    def test_prefers_normalized_execution_annotations_when_present(self):
        snapshot = SimpleNamespace(
            total_equity=120000.0,
            buying_power=20000.0,
            positions=(SimpleNamespace(symbol="TQQQ", quantity=10, market_value=8000.0),),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(PositionTarget(symbol="TQQQ", target_value=30000.0),),
            diagnostics={
                "execution_annotations": {
                    "trade_threshold_value": 500.0,
                    "reserved_cash": 1200.0,
                    "signal_display": "hold",
                    "dashboard_text": "dashboard",
                    "benchmark_symbol": "QQQ",
                    "benchmark_price": 400.0,
                    "long_trend_value": 380.0,
                    "exit_line": 360.0,
                }
            },
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="tqqq_growth_income",
        )

        self.assertEqual(plan["execution"]["trade_threshold_value"], 500.0)
        self.assertEqual(plan["execution"]["reserved_cash"], 1200.0)
        self.assertEqual(plan["execution"]["signal_display"], "hold")
        self.assertEqual(plan["execution"]["dashboard_text"], "dashboard")

    def test_applies_platform_reserved_cash_floor_for_weight_targets(self):
        snapshot = SimpleNamespace(
            total_equity=1000.0,
            buying_power=400.0,
            positions=(SimpleNamespace(symbol="AAPL", quantity=1, market_value=400.0),),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(PositionTarget(symbol="AAPL", target_weight=0.40),),
            diagnostics={"signal_display": "hold"},
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="russell_top50_leader_rotation",
            runtime_metadata={
                "schwab_execution_policy": {
                    "reserved_cash_floor_usd": 150.0,
                    "reserved_cash_ratio": 0.03,
                }
            },
        )

        self.assertEqual(plan["execution"]["reserved_cash"], 150.0)

    def test_zero_equity_weight_targets_no_execute_instead_of_translation_error(self):
        snapshot = SimpleNamespace(
            total_equity=0.0,
            buying_power=0.0,
            positions=(),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(PositionTarget(symbol="AAPL", target_weight=0.40),),
            diagnostics={"signal_display": "hold"},
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="russell_top50_leader_rotation",
        )

        self.assertEqual(plan["allocation"]["target_mode"], "value")
        self.assertEqual(plan["allocation"]["targets"], {"AAPL": 0.0})
        self.assertEqual(plan["portfolio"]["total_equity"], 0.0)
        self.assertEqual(plan["execution"]["trade_threshold_value"], 0.0)

    def test_platform_reserved_cash_floor_can_raise_strategy_reserve(self):
        snapshot = SimpleNamespace(
            total_equity=120000.0,
            buying_power=20000.0,
            positions=(SimpleNamespace(symbol="TQQQ", quantity=10, market_value=8000.0),),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(PositionTarget(symbol="TQQQ", target_value=30000.0),),
            diagnostics={
                "execution_annotations": {
                    "trade_threshold_value": 500.0,
                    "reserved_cash": 1200.0,
                }
            },
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="tqqq_growth_income",
            runtime_metadata={
                "schwab_execution_policy": {
                    "reserved_cash_floor_usd": 150.0,
                    "reserved_cash_ratio": 0.03,
                }
            },
        )

        self.assertEqual(plan["execution"]["reserved_cash"], 3600.0)

    def test_soxl_profile_keeps_strategy_reserve_without_platform_floor(self):
        snapshot = SimpleNamespace(
            total_equity=1175.76,
            buying_power=201.75,
            positions=(SimpleNamespace(symbol="SOXL", quantity=0, market_value=0.0),),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(PositionTarget(symbol="SOXL", target_value=1000.0),),
            diagnostics={
                "execution_annotations": {
                    "reserved_cash": 35.27,
                }
            },
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="soxl_soxx_trend_income",
            runtime_metadata={
                "schwab_execution_policy": {
                    "reserved_cash_floor_usd": 150.0,
                    "reserved_cash_ratio": 0.03,
                }
            },
        )

        self.assertEqual(plan["execution"]["reserved_cash"], 35.27)

    def test_translates_weight_targets_for_russell_top50_leader_rotation(self):
        snapshot = SimpleNamespace(
            total_equity=100000.0,
            buying_power=20000.0,
            positions=(
                SimpleNamespace(symbol="AAPL", quantity=10, market_value=10000.0),
                SimpleNamespace(symbol="BOXX", quantity=20, market_value=4000.0),
            ),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(
                PositionTarget(symbol="AAPL", target_weight=0.35),
                PositionTarget(symbol="MSFT", target_weight=0.25),
                PositionTarget(symbol="BOXX", target_weight=0.40, role="safe_haven"),
            ),
            diagnostics={
                "signal_display": "🧲 Risk On",
                "dashboard": "dashboard",
            },
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="russell_top50_leader_rotation",
            runtime_metadata={"schwab_execution_policy": {"cash_only_execution": False}},
        )
        self.assertEqual(plan["allocation"]["targets"]["MSFT"], 25000.0)
        self.assertEqual(plan["allocation"]["targets"]["BOXX"], 40000.0)

    def test_translates_weight_targets_for_global_etf_rotation(self):
        snapshot = SimpleNamespace(
            total_equity=100000.0,
            buying_power=15000.0,
            positions=(
                SimpleNamespace(symbol="VOO", quantity=10, market_value=10000.0),
                SimpleNamespace(symbol="BIL", quantity=20, market_value=2000.0),
            ),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(
                PositionTarget(symbol="VGK", target_weight=0.5),
                PositionTarget(symbol="EWJ", target_weight=0.3),
                PositionTarget(symbol="BIL", target_weight=0.2, role="safe_haven"),
            ),
            diagnostics={
                "signal_description": "quarterly",
                "canary_status": "SPY:✅, EFA:✅",
            },
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="global_etf_rotation",
            runtime_metadata={"schwab_execution_policy": {"cash_only_execution": False}},
        )

        self.assertEqual(plan["allocation"]["target_mode"], "value")
        self.assertEqual(plan["allocation"]["targets"]["VGK"], 50000.0)
        self.assertEqual(plan["allocation"]["targets"]["EWJ"], 30000.0)
        self.assertEqual(plan["allocation"]["targets"]["BIL"], 20000.0)
        self.assertEqual(plan["execution"]["signal_display"], "quarterly")
        self.assertEqual(plan["execution"]["status_display"], "SPY:✅, EFA:✅")

    def test_translates_weight_targets_for_russell_strategy(self):
        snapshot = SimpleNamespace(
            total_equity=100000.0,
            buying_power=15000.0,
            positions=(
                SimpleNamespace(symbol="AAPL", quantity=10, market_value=10000.0),
                SimpleNamespace(symbol="BOXX", quantity=20, market_value=3000.0),
            ),
            metadata={"account_hash": "demo"},
        )
        decision = StrategyDecision(
            positions=(
                PositionTarget(symbol="AAPL", target_weight=0.30),
                PositionTarget(symbol="MSFT", target_weight=0.30),
                PositionTarget(symbol="NVDA", target_weight=0.20),
                PositionTarget(symbol="BOXX", target_weight=0.20, role="safe_haven"),
            ),
            diagnostics={
                "signal_description": "risk on",
                "status_description": "breadth=62.0% | regime=risk_on | benchmark=up",
                "benchmark_symbol": "SPY",
            },
        )

        plan = map_strategy_decision_to_plan(
            decision,
            snapshot=snapshot,
            strategy_profile="russell_top50_leader_rotation",
            runtime_metadata={"schwab_execution_policy": {"cash_only_execution": False}},
        )

        self.assertEqual(plan["allocation"]["target_mode"], "value")
        self.assertEqual(plan["allocation"]["targets"]["AAPL"], 30000.0)
        self.assertEqual(plan["allocation"]["targets"]["BOXX"], 20000.0)
        self.assertEqual(plan["execution"]["signal_display"], "risk on")
        self.assertEqual(
            plan["execution"]["status_display"],
            "breadth=62.0% | regime=risk_on | benchmark=up",
        )


if __name__ == "__main__":
    unittest.main()
