"""Actual pinned gate/mapper/executor/composer/receipt/finalizer; fake IO only."""
import ast
import copy
import json
import subprocess
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from pathlib import Path

from application import execution_service
from application.execution_receipt_adapter import attach_cycle_execution_receipt
from application.runtime_report_summary import summarize_execution_cycle_result
from decision_mapper import map_strategy_decision_to_plan
from notifications import unified
from notifications.boundary import annotate_decision
from quant_platform_kit.common.models import PortfolioSnapshot, Position, QuoteSnapshot, ExecutionReport
from quant_platform_kit.common.port_adapters import CallableExecutionPort, CallableMarketDataPort
from quant_platform_kit.common.strategy_contracts import StrategyDecision, PositionTarget
from quant_platform_kit.common.runtime_reports import finalize_runtime_report
from quant_platform_kit.risk.gate import apply_risk_gate
import quant_platform_kit.common.execution_receipts as receipts
from quant_platform_kit.risk import attention_notify
from notifications.telegram import build_translator
from test_notification_qrs_roundtrip import producer as make_producer, SOURCE_AT
from composer_helpers import composer_report

PROFILE = 'soxl_soxx_trend_income'
OLD = 'aee876f6464bfe34816ae93c2a2c1f4449964fa4'
FORMULA = {'pct_max':0.05,'pct_min':0.01,'equity_scale_usd':2000}


def old_executor():
    repo=Path(execution_service.__file__).resolve().parents[1]
    text=subprocess.check_output(['git','-C',str(repo),'show',OLD+':application/execution_service.py'],text=True)
    fn=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='execute_rebalance_cycle')
    namespace=dict(execution_service.__dict__)
    exec(compile(ast.Module([fn],[]),'frozen_old_executor','exec'),namespace)
    return namespace['execute_rebalance_cycle']


def strip_notifications(value):
    if isinstance(value,dict):return {k:strip_notifications(v) for k,v in value.items() if not k.startswith('notification_')}
    if isinstance(value,(list,tuple)):return [strip_notifications(v) for v in value]
    return value


def run_actual(mp, kind='daily', *, run_id='actual-business', change=None, locale='zh', executor=None, flag='true'):
    from application.account_new_risk_gate_support import reset_attention_sent_keys_for_tests, set_cycle_snapshot
    reset_attention_sent_keys_for_tests()
    set_cycle_snapshot(None)
    p=make_producer.__wrapped__(mp)
    if flag is None:mp.delenv(unified.FLAG,raising=False)
    else:mp.setenv(unified.FLAG,flag)
    mp.setenv('ACCOUNT_NEW_RISK_GATE','1')
    target=json.loads(p.env['RUNTIME_TARGET_JSON']);target['strategy_profile']=PROFILE;target['execution_mode']='live'
    target['runtime_risk_limits']={'max_daily_loss_equity_formula':FORMULA}
    p.env['RUNTIME_TARGET_JSON']=json.dumps(target);mp.setenv('RUNTIME_TARGET_JSON',p.env['RUNTIME_TARGET_JSON'])
    mp.setenv('NOTIFY_LANG',locale)
    source=composer_report(p.env,SOURCE_AT,run_id)
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):return SOURCE_AT.astimezone(tz) if tz else SOURCE_AT.replace(tzinfo=None)
    mp.setattr(receipts,'datetime',Clock)
    mp.setattr(execution_service,'attach_daily_loss_fact_to_portfolio',lambda portfolio,**kw:portfolio)
    mp.setattr(attention_notify,'_default_telegram_sender',lambda **kw:p.sends.append('attention') or True)
    calls=[];decision=None
    if kind in {'consecutive','both'}:
        snapshot=PortfolioSnapshot(as_of=SOURCE_AT,total_equity=2000,buying_power=2000,
            positions=(Position(symbol='SOXL',quantity=0,market_value=0),),metadata={'account_hash':'synthetic'})
        decision=apply_risk_gate(StrategyDecision(positions=(PositionTarget(symbol='SOXL',target_value=400,role='risk'),),
            diagnostics={'consecutive_losses':6}),portfolio_snapshot=snapshot)
        assert decision.risk_flags==('rejected:circuit_breaker',)
        plan=map_strategy_decision_to_plan(decision,snapshot=snapshot,strategy_profile=PROFILE)
        annotate_decision(plan,decision)
    else:
        plan={'account_hash':'synthetic','strategy_profile':PROFILE,
            'allocation':{'target_mode':'value','strategy_symbols':('SOXL',),'risk_symbols':('SOXL',),
                'income_symbols':(),'safe_haven_symbols':(),'targets':{'SOXL':400.0}},
            'portfolio':{'market_values':{'SOXL':0.0},'quantities':{'SOXL':0},'total_equity':2000.,'liquid_cash':500.,'cash_sweep_symbol':None},
            'execution':{'trade_threshold_value':10.,'reserved_cash':0.}}
    plan['portfolio']['account_new_risk_snapshot']={'observation_status':'COMPLETE','reconciliation_status':'VERIFIED',
        'circuit_breaker_state':'CLOSED','daily_loss_usd':0 if kind=='consecutive' else 80,'equity_usd':2000.,'daily_loss_baseline_equity_usd':2000.}
    if change in {'sell','rejected_sell','error_sell'}:
        plan['portfolio'].update(market_values={'SOXL':600.},quantities={'SOXL':6})
    if change=='other_order':plan['portfolio']['account_new_risk_snapshot']['daily_loss_usd']=0
    if change=='unknown_block':plan['execution'].update(execution_status='blocked',no_op_reason='unknown_risk_gate')
    if change=='unrelated_gate':plan['portfolio']['account_new_risk_snapshot']['production_drift_status']='review'
    def submit(intent):
        calls.append(['broker',asdict(intent)])
        if change=='error_sell':raise RuntimeError('synthetic broker error')
        return ExecutionReport(symbol=intent.symbol,side=intent.side,quantity=intent.quantity,
            status='rejected' if change=='rejected_sell' else 'accepted',broker_order_id='synthetic-order',raw_payload={'status_code':400} if change=='rejected_sell' else {})
    def quote(symbol):
        calls.append(['quote',symbol]);return QuoteSnapshot(symbol=symbol,as_of=SOURCE_AT,last_price=100.,ask_price=100.)
    def load(snapshot):
        calls.append(['refresh',None]);return plan,plan['portfolio'],plan['execution'],plan['allocation']
    def issue(message):p.sends.append('order_issue')
    with unified.notification_context(source):
        result=(executor or execution_service.execute_rebalance_cycle)(client=object(),plan=plan,portfolio=plan['portfolio'],
            execution=plan['execution'],allocation=plan['allocation'],fetch_managed_snapshot=lambda client:None,
            market_data_port=CallableMarketDataPort(quote_loader=quote),load_plan=load,
            execution_port=CallableExecutionPort(submit),translator=build_translator(locale),limit_buy_premium=1.,sell_settle_delay_sec=0,
            sleeper=lambda seconds:calls.append(['sleep',seconds]),publish_order_issue=issue)
        before_notification=copy.deepcopy(asdict(result))
        unified.publish_cycle(result,config=SimpleNamespace(dry_run_only=False,strategy_profile=PROFILE),
            legacy=lambda:p.sends.append('cycle') or True)
    source_before_attach=copy.deepcopy(source)
    attach_cycle_execution_receipt(source,result)
    receipt_before=copy.deepcopy(source['execution_receipt'])
    finalize_runtime_report(source,status='ok',finished_at=SOURCE_AT+timedelta(minutes=1),
        summary=summarize_execution_cycle_result(result,dry_run=False))
    if change=='missing_receipt':source.pop('execution_receipt')
    p.reports.append(source)
    if flag=='true':p.export(p.env)
    return SimpleNamespace(producer=p,source=source,result=result,execution_before_publish=before_notification,
        calls=calls,receipt_before=receipt_before,source_before_attach=source_before_attach,gate_decision=asdict(decision) if decision else None)
