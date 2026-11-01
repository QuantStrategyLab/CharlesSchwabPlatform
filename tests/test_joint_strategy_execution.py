from candidate_dependency_guard import require_candidate_dependencies

require_candidate_dependencies()

BROKER = "schwab"

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from application.daily_strategy_state import DailyStateBinding
from application.decision_cycle import decision_cycle
from application.joint_strategy_execution import evaluate_joint_tqqq
from quant_platform_kit.common.account_domain import account_domain_key
from quant_platform_kit.common.account_identity import BrokerAccountIdentity
from quant_platform_kit.common.execution_state import AccountSubmissionSession, ExecutionMarkerStore, submission_binding_digest
from quant_platform_kit.common.models import PortfolioSnapshot, Position
from quant_platform_kit.common.strategy_contracts import StrategyContext
from quant_platform_kit.common.strategy_risk_state import StrategyRiskStateIdentity, StrategyRiskStateStore, build_strategy_risk_state_transition
from us_equity_strategies.entrypoints import compute_tqqq_growth_income_decision


def fixture(tmp_path, broker, release_id="b"*64):
    account = "U00000001" if broker == "ibkr" else "synthetic-account-hash"
    domain = {"identity": {"environment": "LIVE", "platform_id": broker, "server": "synthetic",
        "account_id_fingerprint": "sha256:" + "1"*64, "lifecycle_id": "one"},
        "policy": {"capital_kind": "OWN_REAL", "product": "BROKERAGE", "phase": "LIVE", "account_type": "CASH",
            "style": "INTRADAY", "preference": "CAPITAL_PRESERVATION", "currency": "USD", "source": "synthetic",
            "effective_at": "2026-10-01T00:00:00Z", "valid_until": "2026-11-01T00:00:00Z", "version": "one"}}
    observed = BrokerAccountIdentity(platform_id=broker, account_types=("CASH",), account_modes=("LIVE",),
        account_id_fingerprint="sha256:" + "1"*64)
    store = ExecutionMarkerStore(local_dir=tmp_path)
    session = AccountSubmissionSession(store, broker=broker, account_id=account, owner_id="owner",
        profile="tqqq_growth_income", intent_id="a"*64, release_id=release_id, account_domain=domain,
        observed_account_id=account, broker_identity=observed, observed_at="2026-10-30T02:00:00Z",
        decision_at="2026-10-30T02:00:00Z", observed_environment="LIVE", observed_server="synthetic",
        observed_currency="USD", observed_lifecycle_id="one", clock=lambda: "2026-10-30T02:00:00Z")
    identity = StrategyRiskStateIdentity("tqqq_growth_income", "synthetic", "candidate", "c"*64)
    intent = {"id": "intent-1", "alpha_epoch": 1, "signal_session": "2026-10-30"}
    ctx = StrategyContext(as_of="2026-10-30", portfolio=PortfolioSnapshot(as_of=datetime(2026,10,30,tzinfo=timezone.utc),
        total_equity=28000., buying_power=10000., cash_balance=10000., metadata={"account_hash": account, "currency": "USD", "source_digest": "sha256:" + "2"*64}, positions=(Position(symbol="TQQQ", quantity=10, market_value=8000.),
            Position(symbol="QQQM", quantity=20, market_value=10000.))),
        market_data={"benchmark_history": [{"close": 100+i*.5} for i in range(260)]}, runtime_config={"income_layer_enabled": False})
    from us_equity_strategies.conditional_tqqq_candidate import canonical_tqqq_context_sha256
    input_sha = canonical_tqqq_context_sha256(ctx)
    transition = build_strategy_risk_state_transition(identity=identity, effective_session="2026-10-30", input_sha256=input_sha,
        state={"schema_version": "tqqq_daily_state.v1", "session": "2026-10-30", "alpha_trend_active": True,
            "alpha_epoch": 1, "volatility_latch_active": False, "pending_intent": intent, "intent_invalidation_reason": None})
    legacy = StrategyRiskStateStore(local_dir=tmp_path / "legacy")
    legacy.append(transition)
    session.enroll_strategy_state(transition, legacy_store=legacy, migration_authorization={
        "account_domain_key": account_domain_key(domain), "account_binding_sha256": submission_binding_digest({"broker": broker, "account_id": account}),
        "owner_id": "owner", "release_id": release_id, "legacy_head_sha256": transition.transition_sha256,
        "strategy_identity": identity.to_dict(), "authority_receipt_sha256": "e"*64})
    binding = DailyStateBinding(legacy, identity, "2026-10-30", input_sha, None, fresh_intent=intent, joint_planning_session=session)
    entrypoint = SimpleNamespace(manifest=SimpleNamespace(profile="tqqq_growth_income"), evaluate=compute_tqqq_growth_income_decision)
    return session, transition, ctx, binding, entrypoint


def test_joint_candidate_requires_a_cycle_and_retains_submission_binding(tmp_path):
    session, transition, ctx, binding, entrypoint = fixture(tmp_path, BROKER)
    with pytest.raises(ValueError, match="enrolled cycle"):
        evaluate_joint_tqqq(entrypoint, ctx, binding)
    with decision_cycle() as cycle:
        decision = evaluate_joint_tqqq(entrypoint, ctx, binding)
        assert decision.diagnostics["joint_execution_required"] is True
        assert cycle["joint_strategy_binding"]["strategy_state_sha256"] == transition.transition_sha256
        assert cycle["joint_strategy_binding"]["planning_session"] is session


def test_wrong_frozen_input_cannot_create_conditional_decision(tmp_path):
    from dataclasses import replace
    _, _, ctx, binding, entrypoint = fixture(tmp_path, BROKER)
    with decision_cycle(), pytest.raises(ValueError, match="input digest mismatch"):
        evaluate_joint_tqqq(entrypoint, ctx, replace(binding, input_sha256="f"*64))


def test_joint_account_facts_reserve_real_journal_and_keep_unknown(tmp_path):
    import json
    from application.joint_account_facts import JointAccountFacts, reserve_joint_order
    from quant_platform_kit.common.models import OrderIntent
    session, transition, ctx, _, _ = fixture(tmp_path, BROKER)
    snapshot = {"balance": 28000, "equity": 28000, "cash": 10000, "currency": "USD",
        "valuation_at": "2026-10-30T00:00:00Z", "source_digest": "sha256:" + "2" * 64,
        "complete": True, "position_risk": 0, "position_3x_exposure": 0,
        "account_domain_key": account_domain_key(session.account_domain), "account_id": session.account_id,
        "valid_until": "2026-10-30T04:00:00Z", "pending_3x_exposure": 0,
        "open_orders_complete": True, "positions": {"TQQQ":10., "QQQM":20.}, "open_orders_empty": True}
    session.record_account_equity(snapshot=snapshot, cash_flow=0, baseline_confirmed=True)
    # This case validates journal uncertainty, not leveraged-product admission.
    order = OrderIntent(symbol="QQQM", side="buy", quantity=1, account_id=session.account_id)
    risk = {"floor": 25000, "cost_buffer": 0, "amount": 10, "risk_fraction": 0.005,
        "loss_per_unit": 1, "estimated_fee": 0, "quantity_step": 1, "product_leverage_factor": 1,
        "unit_price": 100, "existing_3x_exposure": 0, "pending_3x_exposure": 0,
        "leverage_hitl_approved": False, "account_budget_policy": {"preference": "CAPITAL_PRESERVATION",
        "version": "one", "source": "synthetic", "per_trade_fraction": 0.005,
        "total_future_risk_limit": 1000, "contract_floor": None, "floor": 25000, "cost_buffer": 0}}
    def frozen(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    facts = JointAccountFacts(frozen(snapshot), 0, {session.order_binding_digest(order): risk})
    binding = {"context": ctx, "account_facts": facts, "strategy_state_sha256": transition.transition_sha256,
        "strategy_intent_sha256": submission_binding_digest(transition.state["pending_intent"]),
        "strategy_session": transition.effective_session}
    bad_snapshot = {**snapshot, "positions": {"TQQQ": 999}}
    with pytest.raises(ValueError, match="observation mismatch"):
        reserve_joint_order(session, order, {**binding, "account_facts": JointAccountFacts(frozen(bad_snapshot), 0, {})})
    from dataclasses import replace
    duplicated = replace(ctx, portfolio=replace(ctx.portfolio, positions=ctx.portfolio.positions + (ctx.portfolio.positions[0],)))
    with pytest.raises(ValueError, match="observation mismatch"):
        reserve_joint_order(session, order, {**binding, "context": duplicated})
    grant = reserve_joint_order(session, order, binding)
    session.dispatch(grant, order, lambda: None)
    assert session.read_state()["attempts"][grant.attempt_id]["certainty"] == "UNKNOWN"
