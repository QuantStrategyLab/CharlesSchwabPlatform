from candidate_dependency_guard import require_candidate_dependencies

require_candidate_dependencies()

from datetime import date
from types import SimpleNamespace

import pytest

from application.daily_strategy_state import DailyStateBinding, evaluate_with_daily_state
from quant_platform_kit.common.strategy_contracts import StrategyContext, StrategyDecision
from quant_platform_kit.common.strategy_risk_state import (
    DailyRiskStateBootstrapAuthorization, StrategyRiskStateIdentity, StrategyRiskStateStore,
)
from us_equity_strategies.tqqq_daily_state import prepare_tqqq_daily_state, finish_tqqq_daily_state


class Entrypoint:
    manifest = SimpleNamespace(profile="tqqq_growth_income")

    def evaluate(self, ctx):
        request = ctx.state["tqqq_daily_state"]
        prepared = prepare_tqqq_daily_state(request, ctx.as_of)
        result = finish_tqqq_daily_state(prepared, request, alpha_active=True, volatility_latch=False)
        return StrategyDecision(diagnostics={"tqqq_daily_state_transition": result["transition"],
            "increase": result["risk_increase_allowed"], "reduction_retained": True})


@pytest.mark.parametrize("profile", ["soxl_soxx_trend_income", "hk_equity", "unknown_profile"])
@pytest.mark.parametrize("joint", [False, True])
def test_explicit_daily_state_binding_cannot_call_an_unsupported_strategy(tmp_path, profile, joint):
    identity = StrategyRiskStateIdentity(profile, "synthetic", "candidate", "a" * 64)
    binding = DailyStateBinding(StrategyRiskStateStore(local_dir=tmp_path), identity,
        "2026-06-19", "b" * 64, None, joint_planning_session=object() if joint else None)
    ctx = StrategyContext(as_of=date(2026, 6, 19))
    calls = []
    legacy_decision = StrategyDecision(diagnostics={"ignores_tqqq_daily_state": True})
    entrypoint = SimpleNamespace(manifest=SimpleNamespace(profile=profile),
        evaluate=lambda value: (calls.append(value), legacy_decision)[1])
    assert evaluate_with_daily_state(entrypoint, ctx, None) is legacy_decision
    with pytest.raises(RuntimeError, match="daily state binding requires the tqqq_growth_income entrypoint"):
        evaluate_with_daily_state(entrypoint, ctx, binding)
    assert len(calls) == 1


def test_confirmed_state_still_preserves_reductions_without_execution_fence(tmp_path):
    identity = StrategyRiskStateIdentity("tqqq_growth_income", "synthetic", "candidate", "a" * 64)
    store = StrategyRiskStateStore(local_dir=tmp_path)
    ctx = StrategyContext(as_of=date(2026, 6, 19))
    request = {"status": "ready", "identity": identity.to_dict(), "previous_transition": None,
        "bootstrap": True, "input_sha256": "b" * 64, "expected_previous_session": None,
        "fresh_intent": {"id": "intent", "alpha_epoch": 1, "signal_session": "2026-06-19"}}
    prepared = prepare_tqqq_daily_state(request, ctx.as_of)
    proposed = finish_tqqq_daily_state(prepared, request, alpha_active=True, volatility_latch=False)["transition"]
    auth = DailyRiskStateBootstrapAuthorization(identity, "2026-06-19", "b" * 64, proposed["transition_sha256"])
    binding = DailyStateBinding(store, identity, "2026-06-19", "b" * 64, None, auth, request["fresh_intent"])
    decision = evaluate_with_daily_state(Entrypoint(), ctx, binding)
    assert len(store.load_chain(identity)) == 1
    assert decision.diagnostics["increase"] is False
    assert decision.diagnostics["reduction_retained"] is True
    assert decision.diagnostics["tqqq_daily_state_execution_fence"] == "unavailable"
    retry = evaluate_with_daily_state(Entrypoint(), ctx, binding)
    assert len(store.load_chain(identity)) == 1 and retry.diagnostics["increase"] is False


def test_missing_and_unavailable_stores_never_authorize_increase(tmp_path):
    identity = StrategyRiskStateIdentity("tqqq_growth_income", "synthetic", "candidate", "a" * 64)
    ctx = StrategyContext(as_of=date(2026, 6, 19))
    class BrokenStore:
        def load_chain(self, identity):
            raise OSError("unavailable")
    for store in (StrategyRiskStateStore(local_dir=tmp_path), BrokenStore()):
        binding = DailyStateBinding(store, identity, "2026-06-19", "b" * 64, None)
        decision = evaluate_with_daily_state(Entrypoint(), ctx, binding)
        assert decision.diagnostics["increase"] is False
        assert decision.diagnostics["reduction_retained"] is True


def test_conflicting_session_or_bootstrap_binding_does_not_write(tmp_path):
    identity = StrategyRiskStateIdentity("tqqq_growth_income", "synthetic", "candidate", "a" * 64)
    store = StrategyRiskStateStore(local_dir=tmp_path)
    ctx = StrategyContext(as_of=date(2026, 6, 19))
    auth = DailyRiskStateBootstrapAuthorization(identity, "2026-06-19", "b" * 64, "0" * 64)
    for binding in (
        DailyStateBinding(store, identity, "2026-06-20", "b" * 64, None, auth),
        DailyStateBinding(store, identity, "2026-06-19", "b" * 64, None, auth),
    ):
        decision = evaluate_with_daily_state(Entrypoint(), ctx, binding)
        assert decision.diagnostics["increase"] is False
        assert decision.diagnostics["reduction_retained"] is True
        assert store.load_chain(identity) == ()
