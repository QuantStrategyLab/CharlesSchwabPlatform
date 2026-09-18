import unittest
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import strategy_runtime as strategy_runtime_module
from quant_platform_kit.common.models import PortfolioSnapshot, Position
from quant_platform_kit.common.runtime_target import build_runtime_target
from quant_platform_kit.common.strategy_contracts import (
    StrategyDecision,
    StrategyManifest,
    StrategyRuntimeAdapter,
    StrategyRuntimePolicy,
)
from quant_platform_kit.risk.contracts import RuntimeRiskLimits
from runtime_config_support import PlatformRuntimeSettings


class _FakeEntrypoint:
    def __init__(self):
        self.manifest = StrategyManifest(
            profile="tqqq_growth_income",
            domain="us_equity",
            display_name="Hybrid Growth Income",
            description="test entrypoint",
            required_inputs=frozenset({"benchmark_history", "portfolio_snapshot"}),
            default_config={
                "benchmark_symbol": "QQQ",
                "managed_symbols": ("TQQQ", "QQQ", "BOXX", "SPYI", "QQQI"),
            },
        )

    def evaluate(self, ctx):
        self.ctx = ctx
        return StrategyDecision(diagnostics={"signal_display": "hold"})


class _SoxlEntrypoint:
    manifest = StrategyManifest(
        profile="soxl_soxx_trend_income",
        domain="us_equity",
        display_name="SOXL/SOXX Trend Income",
        description="test entrypoint",
        required_inputs=frozenset({"benchmark_history", "portfolio_snapshot"}),
        default_config={
            "benchmark_symbol": "SOXX",
            "managed_symbols": ("SOXL", "SOXX", "BOXX", "SCHD", "DGRO", "SGOV", "SPYI", "QQQI"),
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


class _TechEntrypoint:
    manifest = StrategyManifest(
        profile="tech_communication_pullback_enhancement",
        domain="us_equity",
        display_name="Tech/Communication Pullback Enhancement",
        description="test entrypoint",
        required_inputs=frozenset({"feature_snapshot"}),
        default_config={"safe_haven": "BOXX", "benchmark_symbol": "QQQ"},
    )

    def evaluate(self, ctx):
        self.ctx = ctx
        return StrategyDecision(diagnostics={"signal_description": "risk on"})


class _RussellEntrypoint:
    manifest = StrategyManifest(
        profile="russell_top50_leader_rotation",
        domain="us_equity",
        display_name="Russell Top50 Leader Rotation",
        description="test entrypoint",
        required_inputs=frozenset({"feature_snapshot"}),
        default_config={"safe_haven": "BOXX", "benchmark_symbol": "SPY"},
    )

    def evaluate(self, ctx):
        self.ctx = ctx
        return StrategyDecision(diagnostics={"signal_description": "broad risk on"})


class _MegaCapTop50Entrypoint:
    manifest = StrategyManifest(
        profile="russell_top50_leader_rotation",
        domain="us_equity",
        display_name="Russell Top50 Leader Rotation",
        description="test entrypoint",
        required_inputs=frozenset({"feature_snapshot"}),
        default_config={"safe_haven": "BOXX", "benchmark_symbol": "QQQ"},
    )

    def evaluate(self, ctx):
        self.ctx = ctx
        return StrategyDecision(diagnostics={"signal_description": "top50 balanced"})


def _build_runtime_settings(
    profile: str,
    *,
    feature_snapshot_path: str | None = None,
    reserved_cash_floor_usd: float = 0.0,
    reserved_cash_ratio: float = 0.0,
    cash_only_execution: bool = False,
) -> PlatformRuntimeSettings:
    return PlatformRuntimeSettings(
        strategy_profile=profile,
        strategy_display_name=(
            "Tech/Communication Pullback Enhancement" if profile == "tech_communication_pullback_enhancement" else "TQQQ Growth Income"
        ),
        strategy_domain="us_equity",
        notify_lang="en",
        dry_run_only=False,
        reserved_cash_floor_usd=reserved_cash_floor_usd,
        reserved_cash_ratio=reserved_cash_ratio,
        cash_only_execution=cash_only_execution,
        feature_snapshot_path=feature_snapshot_path,
        feature_snapshot_manifest_path=None,
        strategy_config_path=None,
        strategy_config_source=None,
    )


def _soxl_runtime_policy(*, account_hash: str = "account-hash") -> dict[str, object]:
    symbols = ("SOXL", "SOXX", "BOXX", "SCHD", "DGRO", "SGOV", "SPYI", "QQQI")
    return {
        "binding": {
            "account_scope": "live-account-scope",
            "runtime_scope": "schwab-live-service",
            "account_hash": account_hash,
            "strategy_profile": "soxl_soxx_trend_income",
            "ues_revision": "ues-revision",
            "execution_mode": "live",
            "cash_only_execution": True,
            "reserved_cash_ratio": 0.03,
            "options_enabled": False,
        },
        "allowed_symbols": list(symbols),
        "product_leverage_factors": {"SOXL": 3, **{symbol: 1 for symbol in symbols[1:]}},
        "nominal_caps": {"SOXL": 0.679, "SOXX": 0.873, **{symbol: 0.97 for symbol in symbols[2:]}},
        "total_nominal_exposure_cap": 0.97,
        "total_effective_exposure_cap": 2.328,
        "max_positions": 8,
        "exit_parameters": {"trend_exit_buffer": 0.02},
    }


class StrategyRuntimeTests(unittest.TestCase):
    def _soxl_runtime(self, *, policy: dict[str, object] | None = None):
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
            _build_runtime_settings("soxl_soxx_trend_income", reserved_cash_ratio=0.03),
            runtime_target=target,
            trusted_runtime_risk_policy=policy,
            cash_only_execution=True,
        )
        return entrypoint, strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(portfolio_input_name="portfolio_snapshot"),
            runtime_settings=settings,
            merged_runtime_config=dict(entrypoint.manifest.default_config),
        )

    @staticmethod
    def _soxl_snapshot(account_hash: str = "account-hash") -> PortfolioSnapshot:
        return PortfolioSnapshot(
            as_of=datetime(2026, 8, 27, tzinfo=timezone.utc),
            total_equity=1_000.0,
            buying_power=100.0,
            cash_balance=100.0,
            metadata={
                "account_hash": account_hash,
                "total_equity_source": "broker_liquidation_value",
                "source_digest_sha256": "a" * 64,
            },
        )

    def test_soxl_runtime_binds_explicit_limits_to_broker_and_installed_ues(self):
        entrypoint, runtime = self._soxl_runtime(policy=_soxl_runtime_policy())
        with patch.object(strategy_runtime_module, "_installed_ues_revision", return_value="ues-revision"):
            result = runtime.evaluate(
                benchmark_history=[{"close": 1.0}],
                portfolio_snapshot=self._soxl_snapshot(),
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        self.assertEqual(result.metadata["runtime_risk_status"], "verified:runtime_risk_limits")
        self.assertEqual(entrypoint.ctx.capabilities["runtime_risk_limits"].max_positions, 8)

    def test_soxl_runtime_accepts_account_hash_casefold_match(self):
        """Broker metadata may upper-case the same hex account hash stored in RRL binding."""
        binding_hash = "bf2e669106f029a41e6f36dc18f704906ad6078d1f16a171ec0001e992d39b7d"
        entrypoint, runtime = self._soxl_runtime(
            policy=_soxl_runtime_policy(account_hash=binding_hash)
        )
        with patch.object(strategy_runtime_module, "_installed_ues_revision", return_value="ues-revision"):
            result = runtime.evaluate(
                benchmark_history=[{"close": 1.0}],
                portfolio_snapshot=self._soxl_snapshot(account_hash=binding_hash.upper()),
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        self.assertEqual(result.metadata["runtime_risk_status"], "verified:runtime_risk_limits")
        self.assertIsInstance(entrypoint.ctx.capabilities["runtime_risk_limits"], RuntimeRiskLimits)

    def test_soxl_runtime_attaches_small_account_hold_policy(self):
        from quant_platform_kit.risk.contracts import SmallAccountRiskHoldPolicy

        policy = _soxl_runtime_policy()
        policy["small_account_hold"] = {
            "enabled": True,
            "hold_below_nav": 1000.0,
            "require_cash_only": True,
        }
        entrypoint, runtime = self._soxl_runtime(policy=policy)
        with patch.object(strategy_runtime_module, "_installed_ues_revision", return_value="ues-revision"):
            result = runtime.evaluate(
                benchmark_history=[{"close": 1.0}],
                portfolio_snapshot=self._soxl_snapshot(),
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        self.assertEqual(result.metadata["runtime_risk_status"], "verified:runtime_risk_limits")
        hold = entrypoint.ctx.capabilities["small_account_hold_policy"]
        self.assertIsInstance(hold, SmallAccountRiskHoldPolicy)
        self.assertEqual(hold.hold_below_nav, 1000.0)
        self.assertTrue(entrypoint.ctx.capabilities["cash_only_execution"])

    def test_soxl_runtime_allows_equity_formula_and_schedule_policy_keys(self):
        """Daily-loss formula/schedule live on RUNTIME_TARGET; RRL binding must not reject them."""
        policy = _soxl_runtime_policy()
        policy["max_daily_loss_equity_formula"] = {
            "pct_max": 0.05,
            "pct_min": 0.01,
            "equity_scale_usd": 2000,
        }
        policy["max_daily_loss_equity_schedule"] = [
            {"equity_lte_usd": 500, "max_daily_loss_pct": 0.05},
            {"max_daily_loss_pct": 0.01},
        ]
        entrypoint, runtime = self._soxl_runtime(policy=policy)
        with patch.object(strategy_runtime_module, "_installed_ues_revision", return_value="ues-revision"):
            result = runtime.evaluate(
                benchmark_history=[{"close": 1.0}],
                portfolio_snapshot=self._soxl_snapshot(),
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        self.assertEqual(result.metadata["runtime_risk_status"], "verified:runtime_risk_limits")
        self.assertIsInstance(entrypoint.ctx.capabilities["runtime_risk_limits"], RuntimeRiskLimits)

    def test_soxl_runtime_exposes_current_portfolio_weights_for_hold(self):
        entrypoint, runtime = self._soxl_runtime(policy=_soxl_runtime_policy())
        snapshot = PortfolioSnapshot(
            as_of=datetime(2026, 8, 27, tzinfo=timezone.utc),
            total_equity=587.0,
            buying_power=50.0,
            cash_balance=50.0,
            positions=(
                Position(symbol="SOXL", quantity=3.0, market_value=350.0, average_cost=100.0),
                Position(symbol="SOXX", quantity=1.0, market_value=187.0, average_cost=180.0),
            ),
            metadata={
                "account_hash": "account-hash",
                "total_equity_source": "broker_liquidation_value",
                "source_digest_sha256": "a" * 64,
            },
        )
        with patch.object(strategy_runtime_module, "_installed_ues_revision", return_value="ues-revision"):
            result = runtime.evaluate(
                benchmark_history=[{"close": 1.0}],
                portfolio_snapshot=snapshot,
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        self.assertEqual(result.metadata["runtime_risk_status"], "verified:runtime_risk_limits")
        weights = entrypoint.ctx.capabilities["current_portfolio_weights"]
        self.assertAlmostEqual(weights["SOXL"], 350.0 / 587.0)
        self.assertAlmostEqual(weights["SOXX"], 187.0 / 587.0)
        self.assertTrue(entrypoint.ctx.capabilities["cash_only_execution"])

    def test_soxl_runtime_rejects_invalid_small_account_hold(self):
        policy = _soxl_runtime_policy()
        policy["small_account_hold"] = {"enabled": True, "hold_below_nav": -1}
        entrypoint, runtime = self._soxl_runtime(policy=policy)
        with patch.object(strategy_runtime_module, "_installed_ues_revision", return_value="ues-revision"):
            result = runtime.evaluate(
                benchmark_history=[{"close": 1.0}],
                portfolio_snapshot=self._soxl_snapshot(),
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        self.assertEqual(result.metadata["runtime_risk_status"], "unavailable:invalid_small_account_hold")
        self.assertNotIn("small_account_hold_policy", entrypoint.ctx.capabilities)

    def test_soxl_runtime_rejects_policy_bound_to_wrong_account(self):
        entrypoint, runtime = self._soxl_runtime(policy=_soxl_runtime_policy(account_hash="other-account"))
        with patch.object(strategy_runtime_module, "_installed_ues_revision", return_value="ues-revision"):
            result = runtime.evaluate(
                benchmark_history=[{"close": 1.0}],
                portfolio_snapshot=self._soxl_snapshot(),
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        self.assertEqual(result.metadata["runtime_risk_status"], "unavailable:runtime_binding_mismatch")
        self.assertNotIsInstance(entrypoint.ctx.capabilities["runtime_risk_limits"], RuntimeRiskLimits)

    def _assert_binding_mismatch_zero_submit(
        self,
        *,
        entrypoint,
        runtime,
        snapshot=None,
        installed_ues_revision: str = "ues-revision",
    ):
        from quant_platform_kit.common.strategy_contracts import PositionTarget
        from quant_platform_kit.risk.gate import apply_risk_gate
        from decision_mapper import map_strategy_decision_to_plan

        snapshot = snapshot or self._soxl_snapshot()
        with patch.object(
            strategy_runtime_module,
            "_installed_ues_revision",
            return_value=installed_ues_revision,
        ):
            result = runtime.evaluate(
                benchmark_history=[{"close": 1.0}],
                portfolio_snapshot=snapshot,
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        limits = entrypoint.ctx.capabilities["runtime_risk_limits"]
        self.assertEqual(result.metadata["runtime_risk_status"], "unavailable:runtime_binding_mismatch")
        self.assertNotIsInstance(limits, RuntimeRiskLimits)

        rejected = apply_risk_gate(
            StrategyDecision(positions=(PositionTarget(symbol="SOXL", target_weight=0.20),)),
            portfolio_snapshot=snapshot,
            max_single_weight=1.0,
            max_total_exposure=1.0,
            runtime_risk_limits=limits,
        )
        plan = map_strategy_decision_to_plan(
            rejected,
            snapshot=snapshot,
            strategy_profile="soxl_soxx_trend_income",
        )
        self.assertEqual(rejected.positions, ())
        self.assertEqual(rejected.diagnostics.get("risk_gate"), "REJECT")
        self.assertEqual(plan["allocation"]["targets"], {})
        self.assertEqual(plan["execution"]["execution_status"], "blocked")

    def test_soxl_runtime_binding_mismatch_zero_submit_matrix(self):
        cases = []

        policy = _soxl_runtime_policy(account_hash="other-account")
        cases.append(("account_hash", self._soxl_runtime(policy=policy), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["binding"] = {**policy["binding"], "runtime_scope": "other-service"}
        cases.append(("service", self._soxl_runtime(policy=policy), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["binding"] = {**policy["binding"], "strategy_profile": "tqqq_growth_income"}
        cases.append(("profile", self._soxl_runtime(policy=policy), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["binding"] = {**policy["binding"], "ues_revision": "other-revision"}
        cases.append(("ues_revision", self._soxl_runtime(policy=policy), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["binding"] = {**policy["binding"], "execution_mode": "paper"}
        cases.append(("execution_mode", self._soxl_runtime(policy=policy), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["binding"] = {**policy["binding"], "cash_only_execution": False}
        cases.append(("cash_only_policy", self._soxl_runtime(policy=policy), "ues-revision", None, None))

        entrypoint, runtime = self._soxl_runtime(policy=_soxl_runtime_policy())
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=runtime.runtime_adapter,
            runtime_settings=replace(runtime.runtime_settings, cash_only_execution=False),
            merged_runtime_config=dict(runtime.merged_runtime_config),
        )
        cases.append(("cash_only_settings", (entrypoint, runtime), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["binding"] = {**policy["binding"], "reserved_cash_ratio": 0.05}
        entrypoint, runtime = self._soxl_runtime(policy=policy)
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=runtime.runtime_adapter,
            runtime_settings=replace(runtime.runtime_settings, reserved_cash_ratio=0.05),
            merged_runtime_config={**runtime.merged_runtime_config, "cash_reserve_ratio": 0.05},
        )
        cases.append(("reserve_equal_wrong", (entrypoint, runtime), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["binding"] = {**policy["binding"], "options_enabled": True}
        cases.append(("options_policy", self._soxl_runtime(policy=policy), "ues-revision", None, None))

        entrypoint, runtime = self._soxl_runtime(policy=_soxl_runtime_policy())
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=runtime.runtime_adapter,
            runtime_settings=runtime.runtime_settings,
            merged_runtime_config={
                **runtime.merged_runtime_config,
                "option_overlay_enabled": True,
            },
        )
        cases.append(("options_config", (entrypoint, runtime), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["exit_parameters"] = {"exit_buffer": 0.02}
        cases.append(("exit_key", self._soxl_runtime(policy=policy), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["exit_parameters"] = {"trend_exit_buffer": 0.05}
        cases.append(("exit_value", self._soxl_runtime(policy=policy), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["exit_parameters"] = {"trend_exit_buffer": 0.05}
        entrypoint, runtime = self._soxl_runtime(policy=policy)
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=runtime.runtime_adapter,
            runtime_settings=runtime.runtime_settings,
            merged_runtime_config={**runtime.merged_runtime_config, "trend_exit_buffer": 0.05},
        )
        cases.append(("exit_equal_wrong", (entrypoint, runtime), "ues-revision", None, None))

        policy = _soxl_runtime_policy()
        policy["binding"] = {**policy["binding"], "ues_revision": "wrong-revision"}
        cases.append(
            ("ues_revision_equal_wrong", self._soxl_runtime(policy=policy), "wrong-revision", None, None)
        )

        for label, (entrypoint, runtime), installed_rev, _snapshot, _extra in cases:
            with self.subTest(label=label):
                self._assert_binding_mismatch_zero_submit(
                    entrypoint=entrypoint,
                    runtime=runtime,
                    installed_ues_revision=installed_rev,
                )

    def test_soxl_runtime_missing_policy_is_fail_closed(self):
        entrypoint, runtime = self._soxl_runtime(policy=None)
        result = runtime.evaluate(
            benchmark_history=[{"close": 1.0}],
            portfolio_snapshot=self._soxl_snapshot(),
            signal_text_fn=str,
            translator=lambda key, **_kwargs: key,
        )

        self.assertEqual(result.metadata["runtime_risk_status"], "unavailable:runtime_risk_policy")
        self.assertIsNotNone(entrypoint.ctx.capabilities["runtime_risk_limits"])

    def test_runtime_attaches_v2_capital_evidence_only_for_broker_liquidation_value(self):
        entrypoint = _FakeEntrypoint()
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(portfolio_input_name="portfolio_snapshot"),
            runtime_settings=replace(
                _build_runtime_settings("tqqq_growth_income"),
                runtime_target=build_runtime_target(
                    platform_id="schwab",
                    strategy_profile="tqqq_growth_income",
                    dry_run_only=False,
                    account_scope="live-account-scope",
                    service_name="schwab-live-service",
                ),
            ),
        )
        snapshot = PortfolioSnapshot(
            as_of=datetime(2026, 8, 27, tzinfo=timezone.utc),
            total_equity=1_000.0,
            buying_power=100.0,
            cash_balance=100.0,
            metadata={
                "total_equity_source": "broker_liquidation_value",
                "source_digest_sha256": "a" * 64,
            },
        )

        result = runtime.evaluate(
            benchmark_history=[{"close": 1.0, "high": 1.0, "low": 1.0}],
            portfolio_snapshot=snapshot,
            signal_text_fn=str,
            translator=lambda key, **_kwargs: key,
        )

        self.assertEqual(result.metadata["capital_base_status"], "verified:broker_account_net_liquidation")
        self.assertEqual(
            entrypoint.ctx.capabilities["capital_base"].to_safe_dict()["valuation_basis"],
            "broker_account_net_liquidation",
        )
        self.assertEqual(
            entrypoint.ctx.capabilities["capital_base_binding"].capital_scope.value,
            "account",
        )

    def test_runtime_withholds_v2_capital_evidence_for_reconstructed_equity(self):
        entrypoint = _FakeEntrypoint()
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(portfolio_input_name="portfolio_snapshot"),
            runtime_settings=replace(
                _build_runtime_settings("tqqq_growth_income"),
                runtime_target=build_runtime_target(
                    platform_id="schwab",
                    strategy_profile="tqqq_growth_income",
                    dry_run_only=False,
                    account_scope="live-account-scope",
                    service_name="schwab-live-service",
                ),
            ),
        )
        snapshot = PortfolioSnapshot(
            as_of=datetime(2026, 8, 27, tzinfo=timezone.utc),
            total_equity=1_000.0,
            buying_power=100.0,
            cash_balance=100.0,
            metadata={
                "total_equity_source": "cash_available_plus_all_position_market_values",
                "source_digest_sha256": "a" * 64,
            },
        )

        result = runtime.evaluate(
            benchmark_history=[{"close": 1.0, "high": 1.0, "low": 1.0}],
            portfolio_snapshot=snapshot,
            signal_text_fn=str,
            translator=lambda key, **_kwargs: key,
        )

        self.assertEqual(
            result.metadata["capital_base_status"],
            "unavailable:non_broker_liquidation_value",
        )
        self.assertEqual(entrypoint.ctx.capabilities, {})

    def test_runtime_exposes_managed_symbols_and_benchmark(self):
        class _FixedDatetime:
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 4, 1, tzinfo=tz or timezone.utc)

        entrypoint = _FakeEntrypoint()
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(
                portfolio_input_name="portfolio_snapshot",
                runtime_policy=StrategyRuntimePolicy(signal_effective_after_trading_days=1),
            ),
            runtime_settings=_build_runtime_settings("tqqq_growth_income"),
            merged_runtime_config={
                "benchmark_symbol": "QQQ",
                "managed_symbols": ("TQQQ", "QQQ", "BOXX", "SPYI", "QQQI"),
            },
        )

        with patch.object(strategy_runtime_module, "datetime", _FixedDatetime):
            result = runtime.evaluate(
                benchmark_history=[{"close": 1.0, "high": 1.0, "low": 1.0}],
                portfolio_snapshot=object(),
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        self.assertEqual(runtime.managed_symbols, ("TQQQ", "QQQ", "BOXX", "SPYI", "QQQI"))
        self.assertEqual(runtime.benchmark_symbol, "QQQ")
        self.assertIn("signal_text_fn", entrypoint.ctx.runtime_config)
        self.assertEqual(entrypoint.ctx.runtime_config["signal_effective_after_trading_days"], 1)
        self.assertEqual(result.metadata["strategy_profile"], "tqqq_growth_income")
        self.assertEqual(result.metadata["signal_date"], "2026-04-01")
        self.assertEqual(result.metadata["effective_date"], "2026-04-02")
        self.assertEqual(result.metadata["execution_timing_contract"], "next_trading_day")

    def test_market_history_runtime_loads_loader_into_context(self):
        class _FixedDatetime:
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 4, 1, tzinfo=tz or timezone.utc)

        class _GlobalEntrypoint:
            manifest = StrategyManifest(
                profile="global_etf_rotation",
                domain="us_equity",
                display_name="Global ETF Rotation",
                description="test entrypoint",
                required_inputs=frozenset({"market_history"}),
                default_config={"safe_haven": "BIL", "ranking_pool": ("VOO", "VGK")},
            )

            def evaluate(self, ctx):
                self.ctx = ctx
                return StrategyDecision(diagnostics={"signal_description": "quarterly"})

        entrypoint = _GlobalEntrypoint()
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(
                portfolio_input_name="portfolio_snapshot",
                runtime_policy=StrategyRuntimePolicy(signal_effective_after_trading_days=1),
            ),
            runtime_settings=_build_runtime_settings("global_etf_rotation"),
            merged_runtime_config={"safe_haven": "BIL", "ranking_pool": ("VOO", "VGK")},
        )

        def market_history_loader(*_args, **_kwargs):
            return [1.0, 2.0, 3.0]

        snapshot = object()
        with patch.object(strategy_runtime_module, "datetime", _FixedDatetime):
            result = runtime.evaluate(
                market_history=market_history_loader,
                portfolio_snapshot=snapshot,
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        self.assertIs(entrypoint.ctx.market_data["market_history"], market_history_loader)
        self.assertIs(entrypoint.ctx.portfolio, snapshot)
        self.assertEqual(entrypoint.ctx.runtime_config["signal_effective_after_trading_days"], 1)
        self.assertEqual(result.metadata["strategy_profile"], "global_etf_rotation")
        self.assertEqual(result.metadata["signal_date"], "2026-04-01")
        self.assertEqual(result.metadata["effective_date"], "2026-04-02")
        self.assertEqual(result.metadata["execution_timing_contract"], "next_trading_day")

    def test_load_strategy_runtime_merges_overrides_on_top_of_entrypoint_defaults(self):
        entrypoint = _FakeEntrypoint()

        with patch.object(strategy_runtime_module, "load_strategy_entrypoint_for_profile", return_value=entrypoint) as mock_loader:
            with patch.object(
                strategy_runtime_module,
                "load_strategy_runtime_adapter_for_profile",
                return_value=StrategyRuntimeAdapter(portfolio_input_name="portfolio_snapshot"),
            ):
                runtime = strategy_runtime_module.load_strategy_runtime(
                    "tqqq_growth_income",
                    runtime_settings=_build_runtime_settings("tqqq_growth_income"),
                    runtime_overrides={"benchmark_symbol": "VGT"},
                )

        mock_loader.assert_called_once_with("tqqq_growth_income")
        self.assertIs(runtime.entrypoint, entrypoint)
        self.assertEqual(runtime.benchmark_symbol, "VGT")
        self.assertEqual(runtime.managed_symbols, ("TQQQ", "QQQ", "BOXX", "SPYI", "QQQI"))

    def test_load_strategy_runtime_applies_reserved_cash_policy_from_settings(self):
        entrypoint = _FakeEntrypoint()

        with patch.object(strategy_runtime_module, "load_strategy_entrypoint_for_profile", return_value=entrypoint):
            with patch.object(
                strategy_runtime_module,
                "load_strategy_runtime_adapter_for_profile",
                return_value=StrategyRuntimeAdapter(portfolio_input_name="portfolio_snapshot"),
            ):
                runtime = strategy_runtime_module.load_strategy_runtime(
                    "tqqq_growth_income",
                    runtime_settings=_build_runtime_settings(
                        "tqqq_growth_income",
                        reserved_cash_floor_usd=150.0,
                        reserved_cash_ratio=0.03,
                    ),
                )

        self.assertEqual(runtime.runtime_overrides["reserved_cash_floor_usd"], 150.0)
        self.assertEqual(runtime.runtime_overrides["reserved_cash_ratio"], 0.03)
        self.assertEqual(runtime.runtime_overrides["cash_reserve_ratio"], 0.03)
        self.assertEqual(runtime.merged_runtime_config["reserved_cash_floor_usd"], 150.0)
        self.assertEqual(runtime.merged_runtime_config["reserved_cash_ratio"], 0.03)
        self.assertEqual(runtime.merged_runtime_config["cash_reserve_ratio"], 0.03)

    def test_load_strategy_runtime_forces_option_overlays_off_when_cash_only(self):
        class _SoxlWithOverlayDefaults(_SoxlEntrypoint):
            manifest = StrategyManifest(
                profile="soxl_soxx_trend_income",
                domain="us_equity",
                display_name="SOXL/SOXX Trend Income",
                description="test entrypoint",
                required_inputs=frozenset({"benchmark_history", "portfolio_snapshot"}),
                default_config={
                    **dict(_SoxlEntrypoint.manifest.default_config),
                    "option_overlay_enabled": True,
                    "option_growth_overlay_enabled": False,
                    "option_income_overlay_enabled": True,
                },
            )

        entrypoint = _SoxlWithOverlayDefaults()

        with patch.object(strategy_runtime_module, "load_strategy_entrypoint_for_profile", return_value=entrypoint):
            with patch.object(
                strategy_runtime_module,
                "load_strategy_runtime_adapter_for_profile",
                return_value=StrategyRuntimeAdapter(portfolio_input_name="portfolio_snapshot"),
            ):
                runtime = strategy_runtime_module.load_strategy_runtime(
                    "soxl_soxx_trend_income",
                    runtime_settings=_build_runtime_settings(
                        "soxl_soxx_trend_income",
                        reserved_cash_ratio=0.03,
                        cash_only_execution=True,
                    ),
                    runtime_overrides={
                        "option_overlay_enabled": True,
                        "option_income_overlay_enabled": True,
                    },
                )

        self.assertFalse(runtime.merged_runtime_config["option_overlay_enabled"])
        self.assertFalse(runtime.merged_runtime_config["option_growth_overlay_enabled"])
        self.assertFalse(runtime.merged_runtime_config["option_income_overlay_enabled"])
        self.assertEqual(runtime.merged_runtime_config["cash_reserve_ratio"], 0.03)

    def test_feature_snapshot_runtime_loads_snapshot_into_context(self):
        entrypoint = _TechEntrypoint()
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(
                status_icon="🧲",
                required_feature_columns=frozenset({"symbol", "close", "as_of"}),
                snapshot_date_columns=("as_of",),
                require_snapshot_manifest=False,
                managed_symbols_extractor=lambda *_args, **_kwargs: ("AAPL", "MSFT", "BOXX"),
                portfolio_input_name="portfolio_snapshot",
            ),
            runtime_settings=_build_runtime_settings(
                "tech_communication_pullback_enhancement",
                feature_snapshot_path="gs://bucket/tech.csv",
            ),
            merged_runtime_config={"safe_haven": "BOXX", "benchmark_symbol": "QQQ"},
            logger=lambda _message: None,
        )

        with patch.object(
            strategy_runtime_module,
            "load_feature_snapshot_guarded",
            return_value=SimpleNamespace(
                frame=[
                    {"as_of": "2026-04-08", "symbol": "AAPL", "close": 100.0},
                    {"as_of": "2026-04-08", "symbol": "MSFT", "close": 200.0},
                ],
                metadata={"snapshot_guard_decision": "proceed", "snapshot_as_of": "2026-04-08"},
            ),
        ):
            result = runtime.evaluate(
                portfolio_snapshot=object(),
                translator=lambda key, **_kwargs: key,
                signal_text_fn=str,
            )

        self.assertEqual(entrypoint.ctx.market_data["feature_snapshot"][0]["symbol"], "AAPL")
        self.assertEqual(result.metadata["managed_symbols"], ("AAPL", "MSFT", "BOXX"))
        self.assertEqual(result.metadata["status_icon"], "🧲")

    def test_feature_snapshot_runtime_loads_russell_snapshot_into_context(self):
        entrypoint = _RussellEntrypoint()
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(
                status_icon="👑",
                required_feature_columns=frozenset({"symbol", "sector", "mom_6_1", "mom_12_1", "sma200_gap", "vol_63", "maxdd_126"}),
                managed_symbols_extractor=lambda *_args, **_kwargs: ("AAPL", "MSFT", "BOXX"),
                portfolio_input_name="portfolio_snapshot",
            ),
            runtime_settings=_build_runtime_settings(
                "russell_top50_leader_rotation",
                feature_snapshot_path="gs://bucket/russell.csv",
            ),
            merged_runtime_config={"safe_haven": "BOXX", "benchmark_symbol": "SPY"},
            logger=lambda _message: None,
        )

        with patch.object(
            strategy_runtime_module,
            "load_feature_snapshot_guarded",
            return_value=SimpleNamespace(
                frame=[
                    {"symbol": "SPY", "sector": "Benchmark", "mom_6_1": 0.1, "mom_12_1": 0.2, "sma200_gap": 0.03, "vol_63": 0.15, "maxdd_126": -0.12},
                    {"symbol": "AAPL", "sector": "Technology", "mom_6_1": 0.3, "mom_12_1": 0.4, "sma200_gap": 0.08, "vol_63": 0.20, "maxdd_126": -0.10},
                ],
                metadata={"snapshot_guard_decision": "proceed", "snapshot_as_of": "2026-04-08"},
            ),
        ):
            result = runtime.evaluate(
                portfolio_snapshot=object(),
                translator=lambda key, **_kwargs: key,
                signal_text_fn=str,
            )

        self.assertEqual(entrypoint.ctx.market_data["feature_snapshot"][1]["symbol"], "AAPL")
        self.assertEqual(result.metadata["managed_symbols"], ("AAPL", "MSFT", "BOXX"))
        self.assertEqual(result.metadata["status_icon"], "👑")

    def test_feature_snapshot_runtime_loads_mega_cap_top50_snapshot_into_context(self):
        entrypoint = _MegaCapTop50Entrypoint()
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(
                status_icon="👑",
                required_feature_columns=frozenset({"symbol", "sector", "close"}),
                managed_symbols_extractor=lambda *_args, **_kwargs: ("NVDA", "META", "BOXX"),
                portfolio_input_name="portfolio_snapshot",
            ),
            runtime_settings=_build_runtime_settings(
                "russell_top50_leader_rotation",
                feature_snapshot_path="gs://bucket/top50.csv",
            ),
            merged_runtime_config={"safe_haven": "BOXX", "benchmark_symbol": "QQQ"},
            logger=lambda _message: None,
        )

        portfolio = object()

        with patch.object(
            strategy_runtime_module,
            "load_feature_snapshot_guarded",
            return_value=SimpleNamespace(
                frame=[
                    {
                        "symbol": "NVDA",
                        "sector": "Technology",
                        "close": 880.0,
                    }
                ],
                metadata={"snapshot_guard_decision": "proceed", "snapshot_as_of": "2026-04-08"},
            ),
        ):
            result = runtime.evaluate(
                portfolio_snapshot=portfolio,
                translator=lambda key, **_kwargs: key,
                signal_text_fn=str,
            )

        self.assertEqual(entrypoint.ctx.market_data["feature_snapshot"][0]["symbol"], "NVDA")
        self.assertIs(entrypoint.ctx.portfolio, portfolio)
        self.assertEqual(result.metadata["managed_symbols"], ("NVDA", "META", "BOXX"))
        self.assertEqual(result.metadata["status_icon"], "👑")

    def test_evaluate_stamps_consecutive_losses_on_portfolio_snapshot(self):
        from quant_platform_kit.common.models import PortfolioSnapshot

        class _GlobalEntrypoint:
            def __init__(self):
                self.manifest = StrategyManifest(
                    profile="global_etf_rotation",
                    domain="us_equity",
                    display_name="Global ETF Rotation",
                    description="test",
                    required_inputs=frozenset({"market_history", "portfolio_snapshot"}),
                )
                self.ctx = None

            def evaluate(self, ctx):
                self.ctx = ctx
                return StrategyDecision()

        entrypoint = _GlobalEntrypoint()
        runtime = strategy_runtime_module.LoadedStrategyRuntime(
            entrypoint=entrypoint,
            runtime_adapter=StrategyRuntimeAdapter(
                portfolio_input_name="portfolio_snapshot",
                runtime_policy=StrategyRuntimePolicy(signal_effective_after_trading_days=0),
            ),
            runtime_settings=_build_runtime_settings("global_etf_rotation"),
            logger=lambda _message: None,
        )
        snapshot = PortfolioSnapshot(
            as_of=datetime.now(timezone.utc),
            total_equity=10_000.0,
            positions=(),
            metadata={},
        )
        stamped = PortfolioSnapshot(
            as_of=snapshot.as_of,
            total_equity=snapshot.total_equity,
            positions=(),
            metadata={"consecutive_losses": 3},
        )

        with patch(
            "quant_platform_kit.strategy_lifecycle.live_equity.stamp_consecutive_losses_on_snapshot",
            return_value=stamped,
        ) as stamp:
            result = runtime.evaluate(
                market_history=lambda *_args, **_kwargs: [1.0, 2.0],
                portfolio_snapshot=snapshot,
                signal_text_fn=str,
                translator=lambda key, **_kwargs: key,
            )

        stamp.assert_called_once()
        self.assertIs(entrypoint.ctx.portfolio, stamped)
        self.assertEqual(entrypoint.ctx.portfolio.metadata["consecutive_losses"], 3)
        self.assertEqual(result.metadata["strategy_profile"], "global_etf_rotation")


if __name__ == "__main__":
    unittest.main()
