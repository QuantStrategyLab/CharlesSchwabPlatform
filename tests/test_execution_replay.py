"""Same-key replay keeps recorded uncertainty without executing a second cycle."""
from dataclasses import replace
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

from application import rebalance_service
from application.runtime_dependencies import SchwabRebalanceConfig, SchwabRebalanceRuntime
from application.runtime_report_summary import summarize_execution_cycle_result
from notifications.telegram import build_translator
from quant_platform_kit.common.execution_state import ExecutionMarkerStore
from quant_platform_kit.common.execution_state import claim_account_owner
from quant_platform_kit.common.models import ExecutionReport, OrderIntent, QuoteSnapshot
from quant_platform_kit.common.port_adapters import (
    CallableExecutionPort, CallableMarketDataPort, CallableNotificationPort, CallablePortfolioPort,
)


def plan(*, buy=False):
    symbols = ("SOXL", "SOXX")
    return {
        "account_hash": "synthetic-only",
        "portfolio": {"strategy_symbols": symbols, "portfolio_rows": (symbols,),
            "market_values": dict.fromkeys(symbols, 0.0 if buy else 400.0),
            "quantities": dict.fromkeys(symbols, 0 if buy else 4), "total_equity": 50000.0,
            "liquid_cash": 1500.0, "cash_sweep_symbol": None, "daily_loss_usd": 0.0},
        "allocation": {"target_mode": "value", "strategy_symbols": symbols,
            "risk_symbols": symbols, "income_symbols": (), "safe_haven_symbols": (),
            "targets": dict.fromkeys(symbols, 400.0 if buy else 0.0)},
        "execution": {"trade_threshold_value": 10.0, "reserved_cash": 0.0,
            "signal_date": "2026-10-16", "effective_date": "2026-10-16",
            "execution_timing_contract": "synthetic_close", "signal_display": "synthetic",
            "dashboard_text": "synthetic", "separator": "---"},
    }


@pytest.fixture
def setup_cycle(tmp_path, monkeypatch):
    monkeypatch.setattr(rebalance_service, "_record_platform_execution_telemetry", lambda *_: None)
    calls = []
    outcome = {"kind": "unknown"}
    def submit(intent):
        calls.append(intent)
        if outcome["kind"] == "unknown":
            raise TimeoutError("synthetic-private-provider-detail")
        return ExecutionReport(symbol=intent.symbol, side=intent.side, quantity=intent.quantity,
            status="accepted", broker_order_id={"SOXL": "400000001", "SOXX": "400000002"}[intent.symbol])
    from test_rebalance_service import synthetic_sell_session
    native, _session = synthetic_sell_session(tmp_path / "native", plan(), OrderIntent("SOXL", "sell", 4),
                                              profile_id="soxl_soxx_trend_income")
    # Accepted replay keeps two real ACKs via legitimate BUYs. One flat SELL
    # proof cannot authorize two different SELLs in a single current cycle.
    # Unknown replay still really submits the single native-qualified SELL.
    runtime = SchwabRebalanceRuntime(
        fetch_reference_history=lambda: [],
        portfolio_port=CallablePortfolioPort(lambda: replace(native.snapshot, positions=())
                                            if outcome["kind"] == "accepted" else native.snapshot),
        market_data_port=CallableMarketDataPort(quote_loader=lambda symbol: QuoteSnapshot(
            symbol=symbol, as_of=datetime(2026,10,16,tzinfo=timezone.utc),
            last_price=100.0, ask_price=100.0)),
        resolve_rebalance_plan=lambda **_: plan(buy=outcome["kind"] == "accepted"),
        notifications=CallableNotificationPort(lambda _: None),
        execution_port_factory=lambda _: CallableExecutionPort(submit),
    )
    store = ExecutionMarkerStore(local_dir=str(tmp_path), cloud_prefix_uri=None)
    config = SchwabRebalanceConfig(translator=build_translator("en"),
        strategy_display_name="synthetic", strategy_profile="soxl_soxx_trend_income",
        limit_buy_premium=1.0, sell_settle_delay_sec=0,
        execution_dedup_enabled=True, execution_state_store=store, notify_no_trade_cycles=False,
        intent_continuity=native.context)
    key = rebalance_service._build_execution_marker_key(config=config, plan=plan(), execution=plan()["execution"])
    def run(config_override=None):
        return rebalance_service.run_strategy_core(runtime=runtime, config=config_override or config)
    return SimpleNamespace(store=store, config=config, key=key, calls=calls, run=run,
                           outcome=outcome, tmp_path=tmp_path)


def metadata():
    return {"strategy_profile":"soxl_soxx_trend_income", "account_scope":"synthetic-only",
        "dry_run_only":False, "signal_date":"2026-10-16", "effective_date":"2026-10-16",
        "execution_status":"pending_reconciliation", "broker_submission_done":False,
        "orders_pending_count":1, "submitted_orders":[{"symbol":"SOXL", "side":"sell",
            "quantity":4, "order_type":"market", "status":"unknown"}]}


def seed(case, data=None):
    assert case.store.claim_marker(case.key)
    case.store.record_outcome(case.key, metadata=data or metadata())


def assert_pending_replay(case, result, count):
    summary = summarize_execution_cycle_result(result, dry_run=False)
    assert summary["result"] == "pending_reconciliation"
    assert summary["orders_pending_count"] == count
    assert summary["orders_submitted_count"] == 0
    assert summary["broker_submission_done"] is False
    assert result.submitted_orders == ()


@pytest.mark.parametrize("enforce", [False, True])
@pytest.mark.parametrize("outcome", ["unknown", "accepted"])
def test_full_caller_replay_retains_pending_without_new_submission(setup_cycle, enforce, outcome):
    case = setup_cycle
    case.outcome["kind"] = outcome
    config = replace(case.config, execution_kernel_t1_enforce=enforce)
    first = case.run(config)
    count = len(first.submitted_orders)
    assert {order["side"] for order in first.submitted_orders} == ({"buy"} if outcome == "accepted" else {"sell"})
    assert count == (1 if outcome == "unknown" else 2)
    before = {p.relative_to(case.tmp_path):p.read_bytes() for p in case.tmp_path.rglob("*.json")}
    calls_before = len(case.calls)
    # New adapter instance uses the same durable state, like a restarted caller.
    replay = case.run(replace(config, execution_state_store=replace(case.store)))
    assert len(case.calls) == calls_before
    assert_pending_replay(case, replay, count)
    assert replay.execution["recorded_submitted_orders"] == list(first.submitted_orders)
    assert {p.relative_to(case.tmp_path):p.read_bytes() for p in case.tmp_path.rglob("*.json")} == before


def test_claim_without_outcome_is_uncertain_not_success(setup_cycle):
    case=setup_cycle
    assert case.store.claim_marker(case.key)
    result=case.run()
    assert case.calls == []
    assert_pending_replay(case,result,0)
    assert result.execution["no_op_reason"] == "execution_outcome_unavailable"
    assert "recorded_submitted_orders" not in result.execution


@pytest.mark.parametrize("field,bad", [("strategy_profile","other"), ("account_scope","other"),
    ("dry_run_only",True), ("signal_date","2026-10-15"), ("effective_date","2026-10-15"),
    ("orders_pending_count",True), ("orders_pending_count",-1), ("submitted_orders","invalid")])
def test_invalid_recorded_identity_or_facts_remain_uncertain(setup_cycle,field,bad):
    case=setup_cycle
    data=metadata();data[field]=bad;seed(case,data)
    result=case.run()
    assert case.calls == []
    assert_pending_replay(case,result,0)
    assert "recorded_submitted_orders" not in result.execution


@pytest.mark.parametrize("corruption", ["json", "schema", "key"])
def test_corrupt_record_does_not_turn_claim_into_success(setup_cycle,corruption):
    case=setup_cycle;seed(case)
    path=case.store._outcome_local_path(case.key)
    if corruption=="json":path.write_text("not json")
    else:
        value=json.loads(path.read_text())
        value["schema_version" if corruption=="schema" else "marker_key"]="invalid"
        path.write_text(json.dumps(value))
    before=path.read_bytes();result=case.run()
    assert_pending_replay(case,result,0)
    assert case.calls == [] and path.read_bytes()==before


def test_outcome_read_failure_keeps_unknown_and_sanitizes_detail(setup_cycle,monkeypatch,capsys):
    case=setup_cycle;seed(case)
    original=ExecutionMarkerStore.read_marker
    def read(store,key):
        if store.namespace==store.outcome_namespace:
            raise OSError("synthetic-private-store-detail")
        return original(store,key)
    monkeypatch.setattr(ExecutionMarkerStore,"read_marker",read)
    result=case.run()
    assert_pending_replay(case,result,0)
    assert case.calls == []
    assert "synthetic-private-store-detail" not in str(result.execution)+capsys.readouterr().out


@pytest.mark.parametrize("status", ["completed", "no_action", "no_op"])
def test_confirmed_no_action_outcome_does_not_invent_pending(setup_cycle,status):
    case=setup_cycle;data=metadata()
    data.update(execution_status=status,orders_pending_count=0,submitted_orders=[])
    seed(case,data);result=case.run()
    assert case.calls == []
    assert summarize_execution_cycle_result(result,dry_run=False)["result"]=="ok"
    assert "recorded_submitted_orders" not in result.execution


@pytest.mark.parametrize("status", ["unknown", "failed", "rejected", "blocked", "invalid", ""])
def test_zero_count_is_not_a_success_certificate(setup_cycle,status):
    case=setup_cycle;data=metadata()
    data.update(execution_status=status,orders_pending_count=0,submitted_orders=[])
    seed(case,data);result=case.run()
    assert case.calls==[]
    assert_pending_replay(case,result,0)
    assert result.execution["no_op_reason"]=="execution_outcome_unavailable"


def test_dry_run_dedup_does_not_read_live_outcome(setup_cycle,monkeypatch):
    case=setup_cycle;config=replace(case.config,dry_run_only=True)
    key=rebalance_service._build_execution_marker_key(config=config,plan=plan(),execution=plan()["execution"])
    assert case.store.claim_marker(key)
    original=ExecutionMarkerStore.read_marker
    def read(store,key):
        assert store.namespace!=store.outcome_namespace
        return original(store,key)
    monkeypatch.setattr(ExecutionMarkerStore,"read_marker",read)
    result=case.run(config)
    assert case.calls==[] and result.submitted_orders==()


def test_existing_object_store_outcome_namespace_is_read_without_writes(setup_cycle,monkeypatch):
    case=setup_cycle
    objects={}
    reads=[]
    class FakeObjectStore:
        def exists(self,uri): return uri in objects
        def read_text(self,uri):
            reads.append(uri)
            return objects[uri]
        def create_text(self,uri,text,content_type):
            if uri in objects: return False
            objects[uri]=text
            return True
        def list(self,prefix): return sorted(uri for uri in objects if uri.startswith(prefix))
    monkeypatch.setattr(ExecutionMarkerStore,"_object_store",lambda _:FakeObjectStore())
    store=replace(case.store,local_dir=None,cloud_prefix_uri="gs://synthetic-only/offline")
    config=replace(case.config,execution_state_store=store)
    case.run(config)
    before=dict(objects);calls=len(case.calls)
    result=case.run(config)
    assert len(case.calls)==calls and objects==before
    assert any("/execution_outcomes/" in uri for uri in reads)
    assert_pending_replay(case,result,1)


def test_lost_atomic_claim_race_also_preserves_same_key_outcome(setup_cycle,monkeypatch):
    case=setup_cycle;seed(case)
    monkeypatch.setattr(ExecutionMarkerStore,"has_marker",lambda *_:False)
    result=case.run()
    assert case.calls==[]
    assert_pending_replay(case,result,1)


def test_report_only_dedup_does_not_infer_a_same_key_outcome(setup_cycle,monkeypatch):
    case=setup_cycle
    monkeypatch.setattr(ExecutionMarkerStore,"has_prior_execution_report",lambda *_args,**_kwargs:True)
    original=ExecutionMarkerStore.read_marker
    def read(store,key):
        assert store.namespace!=store.outcome_namespace
        return original(store,key)
    monkeypatch.setattr(ExecutionMarkerStore,"read_marker",read)
    result=case.run()
    assert case.calls==[]
    assert "recorded_submitted_orders" not in result.execution
    assert summarize_execution_cycle_result(result,dry_run=False)["result"]=="ok"


def test_contested_account_owner_does_not_expose_a_same_key_outcome(setup_cycle,monkeypatch):
    case=setup_cycle
    claim_account_owner(case.store,broker="schwab",account_id="synthetic-only",owner_id="other-profile")
    seed(case)
    original=ExecutionMarkerStore.read_marker
    def read(store,key):
        assert store.namespace!=store.outcome_namespace
        return original(store,key)
    monkeypatch.setattr(ExecutionMarkerStore,"read_marker",read)
    result=case.run()
    assert case.calls==[]
    assert "recorded_submitted_orders" not in result.execution
    assert "account_owner_contested" in result.execution["notification_attention_reason_codes"]


def test_dedup_off_still_uses_existing_submit_admission(setup_cycle,monkeypatch):
    case=setup_cycle
    def unexpected(*_,**__): raise AssertionError("dedup-off must not read markers/outcomes")
    monkeypatch.setattr(ExecutionMarkerStore,"read_marker",unexpected)
    monkeypatch.setattr(ExecutionMarkerStore,"has_marker",unexpected)
    result=case.run(replace(case.config,execution_dedup_enabled=False,execution_kernel_t1_enforce=True))
    assert len(case.calls)==1 and result.submitted_orders[0]["status"]=="unknown"
    assert "recorded_submitted_orders" not in result.execution
