"""Old-base integration boundaries: synthetic IO and fault injection only."""
import copy
import json
from types import SimpleNamespace

import pytest

from notifications import boundary, unified
from notif_equivalence_harness import record_cycle
from test_notification_qrs_roundtrip import producer as make_producer, qrs_roundtrip


@pytest.fixture
def producer(monkeypatch):
    return make_producer.__wrapped__(monkeypatch)


@pytest.mark.parametrize("scenario", ["healthy", "blocked", "order", "dry_run"])
def test_new_cycle_failure_does_not_change_old_business_or_persistence(monkeypatch, scenario):
    monkeypatch.setenv("ACCOUNT_NEW_RISK_GATE", "0")
    monkeypatch.setenv(unified.FLAG, "true")
    def fail(*args, **kwargs):
        raise ValueError("synthetic notification failure")
    monkeypatch.setattr(unified, "publish_cycle", fail)
    actual = json.loads(json.dumps(record_cycle(scenario), default=str))
    from pathlib import Path
    expected = json.loads((Path(__file__).parent/'fixtures/notif_main_calls.json').read_text())[scenario+':en']
    assert actual["result"] == expected["result"]
    assert [call for call in actual["calls"] if call[0] != "send"] == [
        call for call in expected["calls"] if call[0] != "send"]


def test_callback_is_not_retried_after_new_notification_failure(monkeypatch):
    calls=[]
    def fail_after(result, *, config, legacy):
        legacy()
        raise ValueError("synthetic post-send error")
    monkeypatch.setattr(unified,"publish_cycle",fail_after)
    boundary.publish_cycle_safely(None,config=None,legacy=lambda:calls.append('original'))
    assert calls == ['original']


def test_context_failure_does_not_skip_cycle_and_business_errors_still_propagate(monkeypatch):
    def fail(*args,**kwargs):
        raise ValueError('synthetic context failure')
    monkeypatch.setattr(unified,'notification_context',fail)
    calls=[]
    with boundary.notification_context({}):
        calls.append('cycle')
    assert calls == ['cycle']
    with pytest.raises(RuntimeError,match='business'):
        with boundary.notification_context({}):
            raise RuntimeError('business')


def test_bad_gate_observation_leaves_plan_and_decision_unchanged(monkeypatch):
    monkeypatch.setenv(unified.FLAG,'true')
    from notifications import approved_gates
    def fail(*args,**kwargs):
        raise ValueError('synthetic annotation failure')
    monkeypatch.setattr(approved_gates,'decision_gate_reasons',fail)
    plan={'execution':{'execution_status':'blocked'}}
    decision=SimpleNamespace(risk_flags=(),diagnostics={})
    before=copy.deepcopy(plan)
    boundary.annotate_decision(plan,decision)
    assert plan==before


@pytest.mark.parametrize('order_type',['MARKET','LIMIT','STOP','STOP_LIMIT','TRAILING_STOP'])
def test_real_composer_order_types_keep_fake_immediate_send(producer,order_type):
    order={'status':'filled','broker_order_id':'synthetic-order','order_type':order_type,
        'fills':[{'execution_id':'synthetic-fill'}]}
    report=producer.run('order-business',(order,))
    assert report['platform']=='charles_schwab'
    assert producer.sends==['order-business']
    assert next(iter(producer.store.facts.values()))[0]['event_kind']=='order_submitted'


def test_real_composer_unknown_fill_count_is_not_zero(monkeypatch,tmp_path,producer):
    report=producer.run('order-unknown',({'status':'filled','broker_order_id':'synthetic-order'},))
    assert report['summary']['notification_digest'].get('fill_count') is None
    producer.export(producer.env)
    runs,summary,text,receipt,_=qrs_roundtrip(monkeypatch,tmp_path,producer.uploads)
    assert runs[0]['fill_count'] is None and summary['counts_unknown'] is True
    assert receipt['total_fills'] is None
    assert producer.sends==['order-unknown']


def test_old_composer_archive_namespace_is_read_without_rewriting_report(producer):
    report=producer.run('real-archive-path')
    before=copy.deepcopy(report)
    assert producer.export(producer.env)['uploaded'] is True
    assert report==before and report['platform']=='charles_schwab'
    assert producer.uploads[-1]['runs'][0]['source_run_id']=='real-archive-path'


def test_old_execution_missing_block_outcome_stays_unknown_and_immediate(monkeypatch):
    """The old engine omits a final reason; notification code must not invent it."""
    from backport_helpers import configure
    from test_account_new_risk_gate import AccountNewRiskGateExecutionCycleTests
    from notifications.approved_gates import approved_block_reasons
    from application import execution_service
    configure(monkeypatch)
    monkeypatch.setenv('ACCOUNT_NEW_RISK_GATE','1')
    monkeypatch.setenv('RUNTIME_TARGET_JSON',json.dumps({'strategy_profile':'synthetic_profile','runtime_risk_limits':{
        'max_daily_loss_equity_formula':{'pct_max':0.05,'pct_min':0.01,'equity_scale_usd':2000}}}))
    monkeypatch.setattr(execution_service,'attach_daily_loss_fact_to_portfolio',lambda portfolio,**kwargs:portfolio)
    case=AccountNewRiskGateExecutionCycleTests()
    result,orders=case._run_buy_cycle(portfolio_overrides={'total_equity':2000,'account_new_risk_snapshot':{
        'observation_status':'COMPLETE','reconciliation_status':'VERIFIED','circuit_breaker_state':'CLOSED',
        'daily_loss_usd':80,'equity_usd':2000,'daily_loss_baseline_equity_usd':2000}})
    assert orders==[]
    assert result.execution.get('execution_status') is None
    assert approved_block_reasons(result.execution,order_count=0)==[]
    sends=[]
    unified.publish_cycle(result,config=SimpleNamespace(dry_run_only=False,strategy_profile='synthetic_profile'),
        legacy=lambda:sends.append('unconfirmed') or True)
    assert sends==['unconfirmed']


@pytest.mark.parametrize('broken',[{}, {'schema_version':'qsl.notification_observation.v1'}, {'incident_reasons':[]}])
def test_corrupt_marker_never_authorizes_fake_send(monkeypatch,broken):
    from test_unified_notifications import MemoryStore, event
    class BrokenStore(MemoryStore):
        def read(self,key):
            return 1,copy.deepcopy(broken)
    sends=[]
    unified.route(event(),env={unified.FLAG:'true'},store=BrokenStore(),legacy=lambda:sends.append('send'))
    assert sends==[]


@pytest.mark.parametrize('kind',['event','transition'])
def test_local_identity_is_not_a_delivery_receipt_and_is_not_resent(kind):
    from test_unified_notifications import MemoryStore, event
    store=MemoryStore();sends=[];fact=event()
    def ack():
        sends.append('send')
        marker=next(value for _,value in store.values.values() if value.get('transition_key'))
        return {'acknowledged':True,'receipt_id':fact.to_candidate()['dedupe_key'] if kind=='event' else marker['transition_key']}
    for _ in range(2):
        unified.route(fact,env={unified.FLAG:'true'},store=store,legacy=ack)
    marker=next(value for _,value in store.values.values() if 'delivery_status' in value)
    assert sends==['send'] and marker['delivery_status']=='uncertain' and marker['receipt_id'] is None
