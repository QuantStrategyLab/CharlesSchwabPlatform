from candidate_dependency_guard import require_candidate_dependencies
require_candidate_dependencies()

import json
from dataclasses import replace
import pytest
from application.decision_cycle import decision_cycle
from application.joint_strategy_execution import evaluate_joint_tqqq
from application.joint_account_facts import JointAccountFacts
from application.execution_service import execute_rebalance_cycle
from quant_platform_kit.common.execution_state import submission_release_digest, AccountSubmissionSession, ExecutionMarkerStore, AccountSubmissionError
from quant_platform_kit.common.strategy_risk_state import build_strategy_risk_state_transition
from quant_platform_kit.common.account_domain import account_domain_key
from quant_platform_kit.common.models import OrderIntent, ExecutionReport, QuoteSnapshot
from quant_platform_kit.common.port_adapters import CallableExecutionPort, CallableMarketDataPort
from notifications.telegram import build_translator
from test_joint_strategy_execution import fixture
from decision_mapper import map_strategy_decision_to_plan


@pytest.mark.parametrize('route', ['port', 'sdk'])
@pytest.mark.parametrize('status_code', [201, 408, 503, 'timeout', 'missing_order_id', 'invalid_order_id'])
@pytest.mark.parametrize('tamper', ['none', 'context', 'allocation', 'order', 'risk'])
def test_joint_gated_decision_reaches_real_submit_boundary(tmp_path, route, status_code, tamper):
    receipt = {'attestation_state':'self_attested', 'release_id':'test', 'strategy_release':{'revision':'test'}}
    session, transition, ctx, binding, entrypoint = fixture(tmp_path, 'schwab', submission_release_digest(receipt))
    snapshot = {'balance':28000, 'equity':28000, 'cash':10000, 'currency':'USD',
        'valuation_at':'2026-10-30T00:00:00Z', 'source_digest':'sha256:'+'2'*64, 'complete':True,
        'position_risk':100, 'position_3x_exposure':8000, 'account_domain_key':account_domain_key(session.account_domain),
        'account_id':session.account_id, 'valid_until':'2026-10-30T04:00:00Z', 'pending_3x_exposure':0,
        'open_orders_complete':True, 'positions':{'TQQQ':10.,'QQQM':20.}, 'open_orders_empty':True}
    session.record_account_equity(snapshot=snapshot, cash_flow=0, baseline_confirmed=True)
    risk = {'floor':25000, 'cost_buffer':0, 'amount':100, 'risk_fraction':0.005, 'loss_per_unit':0.01,
        'estimated_fee':0, 'quantity_step':1, 'product_leverage_factor':1, 'unit_price':100,
        'existing_3x_exposure':8000, 'pending_3x_exposure':0, 'leverage_hitl_approved':False,
        'account_budget_policy':{'preference':'CAPITAL_PRESERVATION','version':'one','source':'synthetic',
        'per_trade_fraction':0.005,'total_future_risk_limit':1000,'contract_floor':None,'floor':25000,'cost_buffer':0}}
    expected_order = OrderIntent('QQQM', 'buy', 26, order_type='limit', limit_price=100., account_id=session.account_id)
    keyed = {session.order_binding_digest(expected_order): risk}
    binding = replace(binding, joint_account_facts=JointAccountFacts(json.dumps(snapshot, sort_keys=True, separators=(',',':')).encode(), 0, keyed))
    calls = []
    def submit(order):
        calls.append(order)
        if status_code == 'timeout':
            raise TimeoutError('synthetic transport timeout')
        accepted = status_code in (201, 'missing_order_id', 'invalid_order_id')
        order_id = '123456789' if status_code == 201 else ('orders' if status_code == 'invalid_order_id' else None)
        return ExecutionReport(symbol=order.symbol, side=order.side, quantity=order.quantity,
            status='accepted' if accepted else 'rejected', broker_order_id=order_id,
            raw_payload={'status_code':201 if accepted else status_code})
    with decision_cycle() as cycle:
        cycle['report']={'runtime_release_receipt':receipt}
        decision=evaluate_joint_tqqq(entrypoint,ctx,binding)
        plan=map_strategy_decision_to_plan(decision,snapshot=ctx.portfolio,strategy_profile='tqqq_growth_income')
        if tamper == 'context':
            ctx.market_data['benchmark_history'][0]['close'] += 1
        elif tamper == 'allocation':
            plan['allocation']['targets']['QQQM'] += 1
        elif tamper == 'risk':
            risk['amount'] += 1
        kwargs = {'execution_port':CallableExecutionPort(submit)} if route=='port' else {'submit_equity_order':lambda client,account,order: submit(order)}
        result=execute_rebalance_cycle(client=object(), plan=plan, portfolio=plan['portfolio'], execution=plan['execution'], allocation=plan['allocation'],
            fetch_managed_snapshot=lambda _:None, market_data_port=CallableMarketDataPort(quote_loader=lambda symbol: QuoteSnapshot(symbol=symbol,as_of='2026-10-30',last_price=101. if tamper == 'order' else 100.,ask_price=101. if tamper == 'order' else 100.)),
            load_plan=lambda _: (plan,plan['portfolio'],plan['execution'],plan['allocation']), translator=build_translator('en'), limit_buy_premium=1.,
            sell_settle_delay_sec=0, dry_run_only=False, publish_order_issue=lambda _:None,
            joint_execution_state_store=session.store, joint_strategy_profile='tqqq_growth_income', **kwargs)
    restarted = AccountSubmissionSession(ExecutionMarkerStore(local_dir=tmp_path), broker=session.broker,
        account_id=session.account_id, owner_id=session.owner_id, profile=session.profile, intent_id='f'*64,
        release_id=session.release_id, account_domain=session.account_domain, observed_account_id=session.observed_account_id,
        broker_identity=session.broker_identity, observed_at=session.observed_at, decision_at=session.decision_at,
        observed_environment=session.observed_environment, observed_server=session.observed_server,
        observed_currency=session.observed_currency, observed_lifecycle_id=session.observed_lifecycle_id, clock=session.clock)
    attempts=restarted.read_state()['attempts']
    if tamper != 'none':
        assert not calls
        assert not attempts
    else:
        assert len(calls)==1, result
        assert calls[0] == expected_order
        assert plan["allocation"]["targets"]["QQQM"] == 12600.
        assert len(attempts)==1
        assert next(iter(attempts.values()))['certainty'] == ('KNOWN_PENDING' if status_code==201 else 'UNKNOWN')
        if status_code != 201:
            assert result.execution['broker_submission_done'] is False
            assert result.execution['execution_status'] == 'pending_reconciliation'
            assert result.submitted_orders[0]['status'] == 'unknown'
            if status_code == 'invalid_order_id':
                assert result.submitted_orders[0]['broker_order_id'] == 'orders'
        from quant_platform_kit.common.execution_state import submission_binding_digest
        with pytest.raises(AccountSubmissionError):
            restarted.reserve(expected_order,
                strategy_state_sha256=transition.transition_sha256,
                strategy_intent_sha256=submission_binding_digest(transition.state['pending_intent']),
                strategy_session=transition.effective_session)
        successor = build_strategy_risk_state_transition(identity=transition.identity, effective_session='2026-10-31',
            input_sha256='f'*64, state={**transition.state, 'session':'2026-10-31', 'pending_intent':None,
            'intent_invalidation_reason':'new_session'}, previous_transition=transition)
        with pytest.raises(AccountSubmissionError):
            restarted.advance_strategy_state(successor)
