import copy
import json
from dataclasses import asdict
from pathlib import Path

import pytest

from actual_gate_helpers import run_actual, old_executor, strip_notifications, PROFILE
from notifications import execution_observation as observation
from notifications import unified
from test_notification_qrs_roundtrip import qrs_roundtrip


def receive(mp,tmp_path,case,locale='zh'):
    # Existing pinned receiver helper has a fixed synthetic profile; preserve
    # the actual production profile by using the exact receiver entry points.
    mp.syspath_prepend(str(Path(__file__).parent/'qrs_snapshot'))
    mp.setenv('QRS_UNIFIED_NOTIFICATION_ENABLED','true')
    mp.setenv('DIGEST_CANDIDATES_STORE_STATUS','loaded')
    expected=[dict(platform_id='schwab',strategy_profile=PROFILE,opaque_account_uid='opaque-a',target_id='target-a')]
    mp.setenv('DIGEST_EXPECTED_TARGETS_JSON',json.dumps(expected))
    import merge_digest_candidates as intake
    import notification_candidate_adapter as adapter
    import daily_digest_aggregator as aggregator
    directory=tmp_path/'observations';directory.mkdir()
    payload=case.producer.uploads[-1]
    for i in range(2):(directory/f'{i}.json').write_text(json.dumps(payload))
    merged,stats=intake.merge(None,directory);rows=intake._rows(merged)
    adapter.validate_expected(expected)
    current,stale=adapter.partition(rows,'2026-10-16');assert not stale
    runs,summary=adapter.aggregate(current,expected)
    path=tmp_path/'merged.json';path.write_text(json.dumps(merged))
    cfg=tmp_path/'config.json';cfg.write_text(json.dumps({'notifications':{'quant_sentinel':{'daily_digest':{}}}}))
    payload,text,collected=aggregator.build_digest_payload(business_day='2026-10-16',locale=locale,candidates_path=path,config_path=cfg,include_github=False)
    receipt=aggregator.build_receipt(payload,collected,message_chars=len(text))
    assert receipt['run_count']==1 and summary['source_inventory']['schwab']['runs']==1
    assert adapter.aggregate(current+current,expected)[1]==summary
    return runs,summary,text,receipt


@pytest.mark.parametrize('kind',['daily','consecutive','both'])
@pytest.mark.parametrize('locale',['zh','en'])
def test_actual_executor_composer_receipt_finalizer_gates(mp, tmp_path, kind, locale):
    case=run_actual(mp,kind,locale=locale)
    report=case.source;before=copy.deepcopy(report)
    assert case.producer.sends==[] and case.calls==([['quote','SOXL']] if kind=='daily' else [])
    assert case.result.submitted_orders==()
    assert report['platform']=='charles_schwab'
    assert report['execution_receipt']==case.receipt_before
    assert report['execution_receipt']['outcome']==('no_action' if kind=='daily' else 'risk_blocked')
    if kind=='daily':
        assert case.result.execution.get('execution_status') is None and case.result.execution.get('no_op_reason') is None
    runs,summary,text,receipt=receive(mp,tmp_path,case,locale)
    assert report==before and summary['immediate_events']==0
    assert summary['counts_unknown'] is True and receipt['total_fills'] is None
    assert runs[0]['order_count']==0 and runs[0]['fill_count'] is None
    reasons=receipt['runs'][0]['approved_gate_reasons']
    assert len(reasons)==(2 if kind=='both' else 1)
    if kind in {'daily','both'}:
        assert ('日亏损上限' if locale=='zh' else 'Daily-loss gate') in text
        assert ('80.0≥60.0' if locale=='zh' else '80.0>=60.0') in text and '2000/(2000+E)' in text
    if kind in {'consecutive','both'}:
        assert ('连亏熔断' if locale=='zh' else 'Consecutive-loss gate') in text and '6>5' in text
        assert case.gate_decision['diagnostics']['consecutive_losses']==6
    assert ('本轮未下单' if locale=='zh' else 'No order this run') in text


@pytest.fixture
def mp(monkeypatch):return monkeypatch


@pytest.mark.parametrize('flag',[None,'false','invalid','true'])
@pytest.mark.parametrize('change',[None,'sell','rejected_sell','error_sell','other_order','unknown_block'])
def test_actual_executor_return_broker_order_and_receipt_match_frozen_old(flag,change):
    with pytest.MonkeyPatch.context() as mp:
        old=run_actual(mp,change=change,flag=flag,executor=old_executor())
    with pytest.MonkeyPatch.context() as mp:
        new=run_actual(mp,change=change,flag=flag)
    if flag=='true':
        assert strip_notifications(new.execution_before_publish)==strip_notifications(old.execution_before_publish)
    else:assert new.execution_before_publish==old.execution_before_publish
    assert new.calls==old.calls and new.result.submitted_orders==old.result.submitted_orders
    assert new.receipt_before==old.receipt_before


@pytest.mark.parametrize('change',['sell','rejected_sell','error_sell','other_order','unknown_block','unrelated_gate','missing_receipt'])
def test_actual_orders_errors_missing_receipt_and_other_blocks_stay_immediate(mp,tmp_path,change):
    case=run_actual(mp,change=change)
    if change=='missing_receipt':
        from scripts import execution_report_heartbeat as heartbeat
        from backport_helpers import mock_heartbeat
        from test_notification_qrs_roundtrip import SOURCE_AT
        sent=mock_heartbeat(mp,case.source)
        mp.setattr(heartbeat,'_list_gcs_objects',lambda *args,**kwargs:[{'url':'gs://synthetic/report.json','metadata':{'updated':SOURCE_AT.isoformat()}}])
        assert heartbeat.main(SOURCE_AT.replace(hour=23))==0  # old acceptance/exit unchanged
        assert len(sent)==1
    else:assert case.producer.sends
    assert not observation.report_reasons(case.source)
    runs,summary,text,receipt=receive(mp,tmp_path,case)
    assert summary['immediate_events']>0 and not receipt['runs'][0]['approved_gate_reasons']
    assert receipt['total_fills'] is None


@pytest.mark.parametrize('field,value',[
    ('source_run_id','other-run'),('branch','other_gate'),('order_path_count',True),('order_path_count',1),
    ('submitted_order_count',1),('needed_buy_count',0),('admission',{'disposition':'NEW_RISK_PROHIBITED','reason_codes':['UNKNOWN']}),
    ('runtime_target',{}),('approved_gate_reasons',[{'reason_code':'daily_loss_limit_exceeded','observed_value':80,'limit':61,'equity_usd':2000}]),
])
def test_completion_proof_is_run_target_value_and_branch_bound(mp,field,value):
    case=run_actual(mp)
    report=copy.deepcopy(case.source)
    report['summary'][observation.FIELD][field]=value
    assert observation.report_reasons(report)==[]


@pytest.mark.parametrize('method',['order_path','complete'])
def test_new_observer_failure_cannot_change_old_order_calls_or_results(mp,method):
    def fail(*args,**kwargs):raise RuntimeError('synthetic notification failure')
    mp.setattr(observation._Observation,method,fail)
    case=run_actual(mp,change='rejected_sell')
    assert [call[0] for call in case.calls]==['quote','broker']
    assert case.producer.sends and not case.result.execution.get(observation.FIELD)


@pytest.mark.parametrize('kind',['daily','consecutive','both'])
@pytest.mark.parametrize('locale',['zh','en'])
def test_actual_heartbeat_defers_gate_without_changing_acceptance_or_exit(mp,kind,locale):
    from scripts import execution_report_heartbeat as heartbeat
    from backport_helpers import mock_heartbeat
    from test_notification_qrs_roundtrip import SOURCE_AT
    case=run_actual(mp,kind,locale=locale)
    before=copy.deepcopy(case.source)
    accepted=heartbeat._is_accepted_report(case.source)
    sent=mock_heartbeat(mp,case.source)
    mp.setattr(heartbeat,'_list_gcs_objects',lambda *args,**kwargs:[{'url':'gs://synthetic/report.json','metadata':{'updated':SOURCE_AT.isoformat()}}])
    for _ in range(2):
        assert heartbeat.main(SOURCE_AT.replace(hour=23))==(0 if kind=='daily' else 1)
    assert sent==[] and case.producer.sends==[] and case.source==before
    assert heartbeat._is_accepted_report(case.source)==accepted
    assert accepted[0] is (kind=='daily')


@pytest.mark.parametrize('change',['missing','other_receipt','receipt_time','free_text','boolean'])
def test_no_action_receipt_or_unproven_metadata_never_manufactures_gate_approval(mp,change):
    from quant_platform_kit.common.execution_receipts import build_execution_receipt
    case=run_actual(mp)
    report=copy.deepcopy(case.source)
    if change=='missing':report.pop('execution_receipt')
    if change=='other_receipt':
        report['execution_receipt']=build_execution_receipt(platform='schwab',strategy_profile=PROFILE,strategy_revision='a'*40,
            execution_mode='live',outcome='risk_blocked',observed_at=report['finished_at'])
    if change=='receipt_time':report['execution_receipt']['observed_at']='2020-01-01T00:00:00Z'
    if change=='free_text':report['summary'][observation.FIELD]='approved daily loss 80>=60'
    if change=='boolean':report['summary'][observation.FIELD]=True
    assert observation.report_reasons(report)==[]


def test_start_observer_failure_keeps_original_executor_result_and_immediate_fallback(mp):
    def fail():raise ValueError('synthetic start failure')
    mp.setattr(observation,'start_observation',fail)
    case=run_actual(mp)
    assert case.result.execution.get('execution_status') is None
    assert case.receipt_before['outcome']=='no_action'
    assert case.producer.sends==['cycle'] and observation.FIELD not in case.result.execution
