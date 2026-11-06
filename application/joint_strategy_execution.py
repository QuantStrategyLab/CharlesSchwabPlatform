"""Opt-in conditional TQQQ planning bound to a durable account journal."""
from __future__ import annotations

from dataclasses import replace

from application.decision_cycle import current_cycle


def evaluate_joint_tqqq(entrypoint, ctx, binding):
    """Build a gated conditional decision only for a protected cycle.

    The journal session must already be independently enrolled. The returned
    decision remains subject to the platform's joint reserve/dispatch path.
    This function cannot create a broker grant or authorize live configuration.
    """
    from quant_platform_kit.common.execution_state import AccountSubmissionSession, submission_binding_digest
    from quant_platform_kit.common.strategy_contracts import BudgetIntent, PositionTarget, StrategyDecision
    from quant_platform_kit.common.strategy_risk_state import StrategyRiskStateTransition
    from us_equity_strategies.conditional_tqqq_candidate import build_tqqq_conditional_candidate, canonical_tqqq_context_sha256
    from us_equity_strategies.decision_observation import attach_optional_decision_receipt
    from us_equity_strategies.entrypoints._common import apply_risk_gate

    cycle = current_cycle()
    session = binding.joint_planning_session
    if cycle is None or type(session) is not AccountSubmissionSession or session.profile != "tqqq_growth_income":
        raise ValueError("joint planning requires an enrolled cycle")
    if str(ctx.as_of)[:10] != binding.effective_session:
        raise ValueError("joint planning session mismatch")
    state = session.read_state()
    if state.get("strategy_enrollment") is None:
        raise ValueError("joint strategy state is not enrolled")
    head = StrategyRiskStateTransition.from_dict(state["strategy_state"])
    if head.identity != binding.identity:
        raise ValueError("joint strategy identity mismatch")
    request = {"status": "ready", "identity": binding.identity.to_dict(),
        "previous_transition": head.to_dict(), "bootstrap": False,
        "input_sha256": binding.input_sha256, "expected_previous_session": binding.expected_previous_session,
        "fresh_intent": binding.fresh_intent, "execution_fence_confirmed": False}
    planning_ctx = replace(ctx, as_of=binding.effective_session,
        state={**dict(ctx.state), "tqqq_daily_state": request})
    if canonical_tqqq_context_sha256(planning_ctx) != binding.input_sha256:
        raise ValueError("joint actual input digest mismatch")
    if head.effective_session != binding.effective_session:
        proposal = entrypoint.evaluate(planning_ctx)
        raw = proposal.diagnostics.get("tqqq_daily_state_transition")
        if raw is None:
            raise ValueError("joint state transition unavailable")
        transition = StrategyRiskStateTransition.from_dict(raw)
        if (transition.identity != binding.identity or transition.input_sha256 != binding.input_sha256
                or transition.effective_session != binding.effective_session
                or transition.previous_transition_sha256 != head.transition_sha256):
            raise ValueError("joint state transition mismatch")
        session.advance_strategy_state(transition)
        head = StrategyRiskStateTransition.from_dict(session.read_state()["strategy_state"])
        if head != transition:
            raise ValueError("joint state confirmation failed")
        request["previous_transition"] = head.to_dict()
        planning_ctx = replace(planning_ctx, state={**dict(planning_ctx.state), "tqqq_daily_state": request})
    elif head.input_sha256 != binding.input_sha256:
        raise ValueError("joint same-session input mismatch")
    intent = head.state.get("pending_intent")
    if not isinstance(intent, dict) or intent != binding.fresh_intent:
        raise ValueError("joint intent unavailable")
    intent_sha = submission_binding_digest(intent)
    handle = session.conditional_planning_handle(strategy_state_sha256=head.transition_sha256,
        strategy_intent_sha256=intent_sha, strategy_session=head.effective_session, input_sha256=head.input_sha256)
    candidate = build_tqqq_conditional_candidate(planning_ctx, journal_session=session, planning_handle=handle)
    if candidate.executable is not False or candidate.order_authorized is not False:
        raise ValueError("conditional candidate is not research-only")
    pre = StrategyDecision(positions=tuple(PositionTarget(**row) for row in candidate.positions),
        budgets=tuple(BudgetIntent(**row) for row in candidate.budgets), risk_flags=candidate.risk_flags,
        diagnostics=candidate.diagnostics)
    post = apply_risk_gate(pre, ctx=planning_ctx, max_single_weight=0.20)
    post = attach_optional_decision_receipt(planning_ctx, pre, post, profile="tqqq_growth_income")
    if post.diagnostics.get("risk_gate") != "APPROVE" or "risk_gate:passed" not in post.risk_flags:
        return post
    if canonical_tqqq_context_sha256(planning_ctx) != binding.input_sha256:
        raise ValueError("joint input changed during planning")
    cycle["joint_strategy_binding"] = {"planning_session": session, "context": planning_ctx, "input_sha256": binding.input_sha256,
        "account_facts": binding.joint_account_facts, "strategy_state_sha256": head.transition_sha256,
        "strategy_intent_sha256": intent_sha, "strategy_session": head.effective_session}
    result = replace(post, diagnostics={**dict(post.diagnostics), "joint_execution_required": True})
    cycle["joint_strategy_binding"]["gated_decision"] = result
    return result
