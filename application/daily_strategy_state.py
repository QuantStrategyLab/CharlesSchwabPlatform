"""Explicitly enrolled daily strategy-state coordination for runtime decisions."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from typing import Any, Mapping




@dataclass(frozen=True)
class DailyStateBinding:
    """Caller-supplied evidence; this object grants no execution authority."""

    store: Any
    identity: Any
    effective_session: str
    input_sha256: str
    expected_previous_session: str | None
    bootstrap_authorization: Any = None
    fresh_intent: Mapping[str, Any] | None = None
    joint_planning_session: Any = None
    joint_account_facts: Any = None


def evaluate_with_daily_state(entrypoint, ctx, binding: DailyStateBinding | None):
    """Evaluate, durably confirm a proposal, and re-evaluate against the head.

    A failed read, malformed proposal or unconfirmed append triggers a separate
    evaluation with unavailable state, preserving the strategy's own reduction
    path. No configuration, identity, bootstrap authority or session is inferred.
    """
    from application.decision_cycle import receipt_capabilities

    ctx = replace(ctx, capabilities={**dict(ctx.capabilities), **receipt_capabilities(ctx)})
    if binding is None:
        return entrypoint.evaluate(ctx)
    if getattr(getattr(entrypoint, "manifest", None), "profile", None) != "tqqq_growth_income":
        raise RuntimeError("daily state binding requires the tqqq_growth_income entrypoint")
    try:
        from us_equity_strategies import tqqq_daily_state as state_contract
        if not all(callable(getattr(state_contract, name, None)) for name in (
            "prepare_tqqq_daily_state", "finish_tqqq_daily_state",
        )):
            raise ImportError("daily state contract unavailable")
    except ImportError as exc:
        # A legacy strategy may ignore the unavailable-state marker entirely.
        # Explicit enrollment must never fall back to that unguarded decision.
        raise RuntimeError("supported daily state strategy contract required") from exc
    if binding.joint_planning_session is not None:
        try:
            from application.joint_strategy_execution import evaluate_joint_tqqq
            return evaluate_joint_tqqq(entrypoint, ctx, binding)
        except Exception:
            return entrypoint.evaluate(replace(ctx, state={**dict(ctx.state),
                "tqqq_daily_state": {"status": "unavailable"}}))
    try:
        from quant_platform_kit.common.strategy_risk_state import (
            StrategyRiskStateIdentity, StrategyRiskStateTransition,
            coordinate_daily_strategy_risk_state, read_daily_strategy_risk_state,
        )
    except ImportError:
        return entrypoint.evaluate(replace(ctx, state={**dict(ctx.state),
            "tqqq_daily_state": {"status": "unavailable"}}))

    def evaluate(state):
        return entrypoint.evaluate(replace(ctx, state={**dict(ctx.state), "tqqq_daily_state": state}))

    unavailable = {"status": "unavailable"}
    try:
        if (not isinstance(binding.identity, StrategyRiskStateIdentity)
                or binding.identity.strategy_profile != entrypoint.manifest.profile
                or date.fromisoformat(binding.effective_session).isoformat() != binding.effective_session
                or str(ctx.as_of)[:10] != binding.effective_session):
            return evaluate(unavailable)
        read = read_daily_strategy_risk_state(binding.store, binding.identity)
        if read.status == "unavailable":
            return evaluate(unavailable)
        if read.status == "missing" and binding.bootstrap_authorization is None:
            return evaluate(unavailable)
        request = {
            "status": "ready", "identity": binding.identity.to_dict(),
            "previous_transition": read.transition.to_dict() if read.transition is not None else None,
            "bootstrap": read.status == "missing" and binding.bootstrap_authorization is not None,
            "input_sha256": binding.input_sha256,
            "expected_previous_session": binding.expected_previous_session,
            "fresh_intent": binding.fresh_intent,
            "execution_fence_confirmed": False,
        }
        proposal = evaluate(request)
        raw = proposal.diagnostics.get("tqqq_daily_state_transition")
        if raw is None:
            return evaluate(unavailable)
        transition = StrategyRiskStateTransition.from_dict(raw)
        if (transition.identity != binding.identity or transition.effective_session != binding.effective_session
                or transition.input_sha256 != binding.input_sha256):
            return evaluate(unavailable)
        result = coordinate_daily_strategy_risk_state(
            binding.store, transition, bootstrap_authorization=binding.bootstrap_authorization,
        )
        if result.status != "confirmed" or result.transition != transition:
            return evaluate(unavailable)
        request["previous_transition"] = transition.to_dict()
        request["bootstrap"] = False
        confirmed_decision = evaluate(request)
        confirmed_raw = confirmed_decision.diagnostics.get("tqqq_daily_state_transition")
        if confirmed_raw is None or StrategyRiskStateTransition.from_dict(confirmed_raw) != transition:
            return evaluate(unavailable)
        # The confirmed head preserves stateful reductions. No supported fence
        # binds it to submission, so the request explicitly denies increases.
        return replace(confirmed_decision, diagnostics={**dict(confirmed_decision.diagnostics),
            "tqqq_daily_state_persistence_status": "confirmed",
            "tqqq_daily_state_execution_fence": "unavailable"})
    except (AttributeError, KeyError, TypeError, ValueError):
        return evaluate(unavailable)
