from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from importlib import metadata as importlib_metadata
from typing import Any, Callable, Mapping

from quant_platform_kit.common.feature_snapshot import load_feature_snapshot_guarded
from quant_platform_kit.common.feature_snapshot_runtime import (
    FeatureSnapshotRuntimeSettings,
    evaluate_feature_snapshot_strategy,
)
from quant_platform_kit.common.capital_base import (
    CapitalBaseBinding,
    CapitalScope,
    CapitalValuationBasis,
    build_capital_base_snapshot,
)
from quant_platform_kit.common.strategy_contracts import (
    StrategyDecision,
    StrategyEntrypoint,
    StrategyRuntimeAdapter,
    apply_runtime_policy_to_runtime_config,
    build_execution_timing_metadata,
    build_strategy_context_from_available_inputs,
)
from quant_platform_kit.risk.contracts import RuntimeRiskLimits, SmallAccountRiskHoldPolicy
from runtime_config_support import PlatformRuntimeSettings
from us_equity_strategies.signals import resolve_external_market_signal_inputs

from strategy_loader import (
    load_strategy_entrypoint_for_profile,
    load_strategy_runtime_adapter_for_profile,
)

_FEATURE_SNAPSHOT_INPUT = "feature_snapshot"
_SOXL_PROFILE = "soxl_soxx_trend_income"


def _parse_small_account_hold_policy(raw: Any) -> SmallAccountRiskHoldPolicy | None:
    """Parse optional deployment hold policy; invalid shapes return None."""
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        return None
    try:
        return SmallAccountRiskHoldPolicy(
            enabled=raw["enabled"],
            hold_below_nav=raw["hold_below_nav"],
            require_cash_only=raw.get("require_cash_only", True),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _verified_nav_from_capabilities(capabilities: Mapping[str, Any]) -> float | None:
    capital_base = capabilities.get("capital_base")
    if capital_base is None:
        return None
    try:
        nav = float(getattr(capital_base, "target_equity"))
    except (TypeError, ValueError, AttributeError):
        return None
    if nav <= 0.0:
        return None
    return nav


def _portfolio_weight_map_from_snapshot(
    portfolio_snapshot: Any,
    *,
    verified_nav: float | None,
) -> dict[str, float] | None:
    """Book weights vs verified NLV for small-account hold non-worsening checks."""
    if portfolio_snapshot is None or verified_nav is None or verified_nav <= 0.0:
        return None
    positions = getattr(portfolio_snapshot, "positions", None)
    if positions is None:
        return None
    weights: dict[str, float] = {}
    try:
        for position in positions:
            symbol = str(getattr(position, "symbol", "") or "").strip().upper()
            try:
                market_value = float(getattr(position, "market_value"))
            except (TypeError, ValueError, AttributeError):
                continue
            if not symbol or market_value <= 0.0:
                continue
            weights[symbol] = weights.get(symbol, 0.0) + market_value / verified_nav
    except TypeError:
        return None
    return weights or None


def _installed_ues_revision() -> str | None:
    """Read the VCS revision of the installed UES distribution."""
    try:
        distribution = importlib_metadata.distribution("us-equity-strategies")
        raw_direct_url = distribution.read_text("direct_url.json")
        if not raw_direct_url:
            return None
        payload = json.loads(raw_direct_url)
        revision = payload.get("vcs_info", {}).get("commit_id")
    except (ImportError, OSError, TypeError, ValueError, AttributeError):
        return None
    if not isinstance(revision, str) or not revision.strip():
        return None
    return revision.strip()


@dataclass(frozen=True)
class StrategyEvaluationResult:
    decision: StrategyDecision
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LoadedStrategyRuntime:
    entrypoint: StrategyEntrypoint
    runtime_adapter: StrategyRuntimeAdapter
    runtime_settings: PlatformRuntimeSettings
    runtime_overrides: Mapping[str, Any] = field(default_factory=dict)
    runtime_config: Mapping[str, Any] = field(default_factory=dict)
    merged_runtime_config: Mapping[str, Any] = field(default_factory=dict)
    logger: Callable[[str], None] = print

    @property
    def profile(self) -> str:
        return self.entrypoint.manifest.profile

    @property
    def managed_symbols(self) -> tuple[str, ...]:
        configured = self.merged_runtime_config.get("managed_symbols", ())
        return tuple(str(symbol) for symbol in configured)

    @property
    def benchmark_symbol(self) -> str:
        return str(self.merged_runtime_config.get("benchmark_symbol", "QQQ"))

    def _stamp_portfolio_risk_metadata(self, available_inputs: Mapping[str, Any]) -> dict[str, Any]:
        resolved = dict(available_inputs)
        snapshot = resolved.get("portfolio_snapshot")
        if snapshot is None:
            return resolved
        from quant_platform_kit.strategy_lifecycle.live_equity import stamp_consecutive_losses_on_snapshot

        resolved["portfolio_snapshot"] = stamp_consecutive_losses_on_snapshot(
            snapshot,
            strategy_profile=self.profile,
            logger=self.logger,
        )
        return resolved

    def _build_capital_base_capabilities(
        self,
        available_inputs: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str]:
        """Adapt a verified Schwab account snapshot into v2 capital evidence.

        A managed-symbol portfolio is intentionally insufficient here.  The
        common Schwab adapter marks account snapshots that came directly from
        the broker's liquidation value; only those snapshots can become an
        account-level value-target denominator.  Missing or ambiguous facts
        leave capabilities empty, which keeps strict strategies fail-closed.
        """

        snapshot = available_inputs.get("portfolio_snapshot")
        runtime_target = self.runtime_settings.runtime_target
        if snapshot is None:
            return {}, "unavailable:portfolio_snapshot"
        if runtime_target is None:
            return {}, "unavailable:runtime_target"

        account_scope = str(runtime_target.account_scope or "").strip()
        runtime_scope = str(
            runtime_target.service_name or runtime_target.deployment_selector or ""
        ).strip()
        if not account_scope or not runtime_scope:
            return {}, "unavailable:runtime_scope"

        metadata = getattr(snapshot, "metadata", {})
        if not isinstance(metadata, Mapping):
            return {}, "unavailable:portfolio_metadata"
        if metadata.get("total_equity_source") != "broker_liquidation_value":
            return {}, "unavailable:non_broker_liquidation_value"
        source_digest = str(metadata.get("source_digest_sha256") or "").strip()
        if not source_digest:
            return {}, "unavailable:source_digest"

        try:
            capital_base = build_capital_base_snapshot(
                snapshot,
                account_scope=account_scope,
                runtime_scope=runtime_scope,
                strategy_scope=self.profile,
                reported_currency="USD",
                target_currency="USD",
                fx_rate_to_target=1.0,
                source_digest_sha256=source_digest,
                capital_scope=CapitalScope.ACCOUNT,
                valuation_basis=CapitalValuationBasis.BROKER_ACCOUNT_NET_LIQUIDATION,
            )
            binding = CapitalBaseBinding(
                account_scope=account_scope,
                runtime_scope=runtime_scope,
                strategy_scope=self.profile,
                target_currency="USD",
                capital_scope=CapitalScope.ACCOUNT,
                valuation_basis=CapitalValuationBasis.BROKER_ACCOUNT_NET_LIQUIDATION,
            )
        except (TypeError, ValueError):
            return {}, "unavailable:invalid_capital_evidence"
        return {
            "capital_base": capital_base,
            "capital_base_binding": binding,
        }, "verified:broker_account_net_liquidation"

    def _build_runtime_risk_capabilities(
        self,
        available_inputs: Mapping[str, Any],
        capabilities: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str]:
        """Bind explicit limits to the deployed account and installed UES."""
        if self.profile != _SOXL_PROFILE:
            return dict(capabilities), "unavailable:profile_not_supported"
        policy = self.runtime_settings.trusted_runtime_risk_policy
        runtime_target = self.runtime_settings.runtime_target
        snapshot = available_inputs.get("portfolio_snapshot")
        binding = capabilities.get("capital_base_binding")
        if not isinstance(policy, Mapping):
            return {**capabilities, "runtime_risk_limits": object()}, "unavailable:runtime_risk_policy"
        if runtime_target is None or snapshot is None or binding is None:
            return {**capabilities, "runtime_risk_limits": object()}, "unavailable:runtime_binding"
        expected_policy_keys = {
            "binding",
            "allowed_symbols",
            "product_leverage_factors",
            "nominal_caps",
            "total_nominal_exposure_cap",
            "total_effective_exposure_cap",
            "max_positions",
            "exit_parameters",
        }
        optional_policy_keys = {"small_account_hold", "max_daily_loss_usd"}
        policy_keys = set(policy)
        if (
            not expected_policy_keys.issubset(policy_keys)
            or (policy_keys - expected_policy_keys - optional_policy_keys)
            or not isinstance(policy.get("binding"), Mapping)
        ):
            return {**capabilities, "runtime_risk_limits": object()}, "unavailable:invalid_runtime_risk_policy"

        target_release = runtime_target.strategy_release
        policy_binding = policy["binding"]
        expected_binding_keys = {
            "account_scope",
            "runtime_scope",
            "account_hash",
            "strategy_profile",
            "ues_revision",
            "execution_mode",
            "cash_only_execution",
            "reserved_cash_ratio",
            "options_enabled",
        }
        if set(policy_binding) != expected_binding_keys:
            return {**capabilities, "runtime_risk_limits": object()}, "unavailable:invalid_runtime_binding"
        metadata = getattr(snapshot, "metadata", {})
        account_scope = str(runtime_target.account_scope or "").strip()
        runtime_scope = str(runtime_target.service_name or runtime_target.deployment_selector or "").strip()
        actual_account_hash = str(metadata.get("account_hash") or "").strip() if isinstance(metadata, Mapping) else ""
        actual_ues_revision = _installed_ues_revision()
        actual_exit_buffer = self.merged_runtime_config.get("trend_exit_buffer")
        mismatch_reasons: list[str] = []
        if not account_scope:
            mismatch_reasons.append("missing_account_scope")
        if not runtime_scope:
            mismatch_reasons.append("missing_runtime_scope")
        if not actual_account_hash:
            mismatch_reasons.append("missing_account_hash")
        if target_release is None:
            mismatch_reasons.append("missing_strategy_release")
        if str(policy_binding["account_scope"]).strip() != account_scope:
            mismatch_reasons.append("account_scope")
        if str(policy_binding["runtime_scope"]).strip() != runtime_scope:
            mismatch_reasons.append("runtime_scope")
        # Schwab account hashes are hex digests; broker metadata may upper-case them.
        if str(policy_binding["account_hash"]).strip().casefold() != actual_account_hash.casefold():
            mismatch_reasons.append("account_hash")
        if str(policy_binding["strategy_profile"]).strip() != self.profile:
            mismatch_reasons.append("strategy_profile")
        if target_release is not None and str(policy_binding["ues_revision"]).strip() != str(
            target_release.strategy_revision
        ).strip():
            mismatch_reasons.append("ues_revision_target")
        if actual_ues_revision is None:
            mismatch_reasons.append("ues_revision_installed_missing")
        elif actual_ues_revision != str(policy_binding["ues_revision"]).strip():
            mismatch_reasons.append(f"ues_revision_installed:{actual_ues_revision}")
        if str(policy_binding["execution_mode"]).strip().lower() != runtime_target.execution_mode:
            mismatch_reasons.append("execution_mode")
        if policy_binding["cash_only_execution"] is not True:
            mismatch_reasons.append("binding_cash_only")
        if self.runtime_settings.cash_only_execution is not True:
            mismatch_reasons.append("settings_cash_only")
        if policy_binding["reserved_cash_ratio"] != self.merged_runtime_config.get("cash_reserve_ratio"):
            mismatch_reasons.append("cash_reserve_ratio_merged")
        if policy_binding["reserved_cash_ratio"] != self.runtime_settings.reserved_cash_ratio:
            mismatch_reasons.append("cash_reserve_ratio_settings")
        if policy_binding["reserved_cash_ratio"] != 0.03:
            mismatch_reasons.append("cash_reserve_ratio_value")
        if policy_binding["options_enabled"] is not False:
            mismatch_reasons.append("options_enabled")
        for key in (
            "option_overlay_enabled",
            "option_growth_overlay_enabled",
            "option_income_overlay_enabled",
        ):
            if self.merged_runtime_config.get(key) is not False:
                mismatch_reasons.append(key)
        if not isinstance(policy.get("exit_parameters"), Mapping):
            mismatch_reasons.append("exit_parameters_type")
        if actual_exit_buffer is None:
            mismatch_reasons.append("trend_exit_buffer_missing")
        elif actual_exit_buffer != 0.02:
            mismatch_reasons.append(f"trend_exit_buffer:{actual_exit_buffer}")
        if dict(policy.get("exit_parameters") or {}) != {"trend_exit_buffer": 0.02}:
            mismatch_reasons.append("exit_parameters_value")
        if mismatch_reasons:
            self.logger(
                "strategy_runtime_binding_mismatch | "
                f"profile={self.profile} reasons={','.join(mismatch_reasons)} "
                f"actual_account_hash={actual_account_hash[:16]} "
                f"binding_account_hash={str(policy_binding.get('account_hash') or '')[:16]} "
                f"installed_ues={actual_ues_revision} "
                f"target_ues={(target_release.strategy_revision if target_release is not None else None)} "
                f"exit_buffer={actual_exit_buffer}"
            )
            return {**capabilities, "runtime_risk_limits": object()}, "unavailable:runtime_binding_mismatch"
        try:
            daily_loss_kwargs: dict[str, Any] = {}
            if "max_daily_loss_usd" in policy:
                daily_loss_kwargs["max_daily_loss_usd"] = policy.get("max_daily_loss_usd")
            limits = RuntimeRiskLimits(
                allowed_symbols=tuple(policy["allowed_symbols"]),
                product_leverage_factors=policy["product_leverage_factors"],
                nominal_caps=policy["nominal_caps"],
                total_nominal_exposure_cap=policy["total_nominal_exposure_cap"],
                total_effective_exposure_cap=policy["total_effective_exposure_cap"],
                max_positions=policy["max_positions"],
                **daily_loss_kwargs,
            )
        except (TypeError, ValueError):
            return {**capabilities, "runtime_risk_limits": object()}, "unavailable:invalid_runtime_risk_limits"
        capability_payload: dict[str, Any] = {
            **capabilities,
            "runtime_risk_limits": limits,
            "cash_only_execution": bool(self.runtime_settings.cash_only_execution),
        }
        hold_policy = _parse_small_account_hold_policy(policy.get("small_account_hold"))
        if isinstance(policy.get("small_account_hold"), Mapping) and hold_policy is None:
            return {
                **capabilities,
                "runtime_risk_limits": object(),
            }, "unavailable:invalid_small_account_hold"
        if hold_policy is not None:
            if hold_policy.require_cash_only and self.runtime_settings.cash_only_execution is not True:
                return {
                    **capabilities,
                    "runtime_risk_limits": object(),
                }, "unavailable:small_account_hold_cash_only"
            capability_payload["small_account_hold_policy"] = hold_policy
        # Always expose book weights for default/opt-in hold non-worsening checks.
        current_weights = _portfolio_weight_map_from_snapshot(
            available_inputs.get("portfolio_snapshot"),
            verified_nav=_verified_nav_from_capabilities(capabilities),
        )
        if current_weights is not None:
            capability_payload["current_portfolio_weights"] = current_weights
        return capability_payload, "verified:runtime_risk_limits"

    def evaluate(
        self,
        *,
        signal_text_fn: Callable[[str], str],
        translator: Callable[[str], str],
        **available_inputs,
    ) -> StrategyEvaluationResult:
        runtime_config = dict(self.runtime_overrides)
        runtime_config.setdefault("signal_text_fn", signal_text_fn)
        runtime_config.setdefault("translator", translator)
        apply_runtime_policy_to_runtime_config(runtime_config, self.runtime_adapter)
        if _FEATURE_SNAPSHOT_INPUT in frozenset(self.entrypoint.manifest.required_inputs):
            return self._evaluate_feature_snapshot_strategy(
                runtime_config=runtime_config,
                available_inputs=self._stamp_portfolio_risk_metadata(available_inputs),
            )
        as_of = datetime.now(timezone.utc)
        resolved_available_inputs = self._stamp_portfolio_risk_metadata(available_inputs)
        resolved_available_inputs.update(
            resolve_external_market_signal_inputs(
                strategy_profile=self.profile,
                available_inputs=self.runtime_adapter.available_inputs or self.entrypoint.manifest.required_inputs,
                runtime_settings=self.runtime_settings,
                as_of=as_of,
                logger=self.logger,
            )
        )
        capabilities, capital_base_status = self._build_capital_base_capabilities(
            resolved_available_inputs
        )
        capabilities, runtime_risk_status = self._build_runtime_risk_capabilities(
            resolved_available_inputs,
            capabilities,
        )
        ctx = build_strategy_context_from_available_inputs(
            entrypoint=self.entrypoint,
            runtime_adapter=self.runtime_adapter,
            as_of=as_of,
            available_inputs=resolved_available_inputs,
            runtime_config=runtime_config,
            capabilities=capabilities,
        )
        decision = self.entrypoint.evaluate(ctx)
        if any(str(flag).startswith("rejected:runtime_risk") for flag in decision.risk_flags):
            diagnostics = decision.diagnostics if isinstance(decision.diagnostics, Mapping) else {}
            portfolio_snapshot = resolved_available_inputs.get("portfolio_snapshot")
            snapshot_metadata = getattr(portfolio_snapshot, "metadata", None)
            if isinstance(snapshot_metadata, Mapping):
                broker_nlv = snapshot_metadata.get("broker_liquidation_value")
                if broker_nlv is None:
                    broker_nlv = snapshot_metadata.get("broker_net_liquidation")
            else:
                broker_nlv = None
            target_value_sum = sum(
                float(position.target_value)
                for position in decision.positions
                if position.target_value is not None
            )
            self.logger(
                "strategy_runtime_risk_reject | "
                f"profile={self.profile} "
                f"capital_base_status={capital_base_status} "
                f"runtime_risk_status={runtime_risk_status} "
                f"risk_flags={','.join(str(flag) for flag in decision.risk_flags)} "
                f"reason={diagnostics.get('reason')} "
                f"hold={diagnostics.get('runtime_risk_small_account_hold')} "
                f"broker_nlv={broker_nlv} "
                f"portfolio_total_equity={getattr(portfolio_snapshot, 'total_equity', None)} "
                f"target_value_sum={target_value_sum}"
            )
        return StrategyEvaluationResult(
            decision=decision,
            metadata={
                "strategy_profile": self.profile,
                "capital_base_status": capital_base_status,
                "runtime_risk_status": runtime_risk_status,
                **build_execution_timing_metadata(
                    signal_date=as_of,
                    signal_effective_after_trading_days=(
                        self.runtime_adapter.runtime_policy.signal_effective_after_trading_days
                    ),
                ),
            },
        )

    def _evaluate_feature_snapshot_strategy(
        self,
        *,
        runtime_config: Mapping[str, Any],
        available_inputs: Mapping[str, Any],
    ) -> StrategyEvaluationResult:
        result = evaluate_feature_snapshot_strategy(
            entrypoint=self.entrypoint,
            runtime_adapter=self.runtime_adapter,
            runtime_settings=FeatureSnapshotRuntimeSettings(
                feature_snapshot_path=self.runtime_settings.feature_snapshot_path,
                feature_snapshot_manifest_path=self.runtime_settings.feature_snapshot_manifest_path,
                feature_snapshot_fallback_mode=self.runtime_settings.feature_snapshot_fallback_mode,
                feature_snapshot_fallback_cache_dir=self.runtime_settings.feature_snapshot_fallback_cache_dir,
                feature_snapshot_fallback_max_stale_days=(
                    self.runtime_settings.feature_snapshot_fallback_max_stale_days
                ),
                strategy_config_path=self.runtime_settings.strategy_config_path,
                strategy_config_source=self.runtime_settings.strategy_config_source,
                dry_run_only=self.runtime_settings.dry_run_only,
            ),
            runtime_config=runtime_config,
            merged_runtime_config=self.merged_runtime_config,
            available_inputs=available_inputs,
            base_managed_symbols=self.managed_symbols,
            snapshot_loader=load_feature_snapshot_guarded,
        )
        return StrategyEvaluationResult(
            decision=result.decision,
            metadata=result.metadata,
        )

    def load_runtime_parameters(self) -> dict[str, Any]:
        runtime_loader = self.runtime_adapter.runtime_parameter_loader
        if not callable(runtime_loader):
            return {}
        return dict(
            runtime_loader(
                config_path=self.runtime_settings.strategy_config_path,
                logger=self.logger,
            )
            or {}
        )


def load_strategy_runtime(
    raw_profile: str | None,
    *,
    runtime_settings: PlatformRuntimeSettings,
    runtime_overrides: Mapping[str, Any] | None = None,
    logger: Callable[[str], None] = print,
) -> LoadedStrategyRuntime:
    entrypoint = load_strategy_entrypoint_for_profile(raw_profile)
    runtime_adapter = load_strategy_runtime_adapter_for_profile(raw_profile)
    overrides: dict[str, Any] = {}
    reserved_cash_floor_usd = getattr(runtime_settings, "reserved_cash_floor_usd", 0.0)
    reserved_cash_ratio = getattr(runtime_settings, "reserved_cash_ratio", None)
    if float(reserved_cash_floor_usd or 0.0) > 0.0:
        overrides["reserved_cash_floor_usd"] = float(reserved_cash_floor_usd)
    if reserved_cash_ratio is not None and float(reserved_cash_ratio or 0.0) > 0.0:
        overrides["reserved_cash_ratio"] = float(reserved_cash_ratio)
        # Keep the manifest key used by runtime-risk binding in lockstep with the
        # platform reserved-cash setting (both must stay at the approved 0.03).
        overrides["cash_reserve_ratio"] = float(reserved_cash_ratio)
    overrides.update(runtime_overrides or {})
    if getattr(runtime_settings, "cash_only_execution", False) is True:
        # Approved SOXL recovery is cash-only / options-off. UES profile defaults
        # still enable option overlays; force them closed after other overrides.
        overrides["option_overlay_enabled"] = False
        overrides["option_growth_overlay_enabled"] = False
        overrides["option_income_overlay_enabled"] = False
    runtime = LoadedStrategyRuntime(
        entrypoint=entrypoint,
        runtime_adapter=runtime_adapter,
        runtime_settings=runtime_settings,
        runtime_overrides=overrides,
        logger=logger,
    )
    runtime_config = runtime.load_runtime_parameters()
    merged_runtime_config = dict(entrypoint.manifest.default_config)
    merged_runtime_config.update(runtime_config)
    merged_runtime_config.update(overrides)
    return LoadedStrategyRuntime(
        entrypoint=entrypoint,
        runtime_adapter=runtime_adapter,
        runtime_settings=runtime_settings,
        runtime_overrides=overrides,
        runtime_config=runtime_config,
        merged_runtime_config=merged_runtime_config,
        logger=logger,
    )
