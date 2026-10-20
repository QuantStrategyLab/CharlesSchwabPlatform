from types import SimpleNamespace

import pytest

from application.execution_service import execute_rebalance_cycle
from application.rebalance_service import _record_execution_outcome
from notifications.telegram import build_translator
from quant_platform_kit.common.execution_state import ExecutionMarkerStore
from quant_platform_kit.common.models import ExecutionReport, QuoteSnapshot
from quant_platform_kit.common.port_adapters import CallableExecutionPort, CallableMarketDataPort


def run_cycle(submit, *, sell=False, notional=False, notify=lambda _: None, translator=None, native_root=None):
    symbols = ("SOXL", "SOXX", "QQQM")
    portfolio = {
        "market_values": dict.fromkeys(symbols, 400.0 if sell else 0.0),
        "quantities": dict.fromkeys(symbols, 4 if sell else 0),
        "total_equity": 50000.0,
        "liquid_cash": 1500.0,
        "cash_sweep_symbol": None,
    }
    allocation = {
        "target_mode": "value", "strategy_symbols": symbols, "risk_symbols": symbols,
        "income_symbols": (), "safe_haven_symbols": (),
        "targets": dict.fromkeys(symbols, 0.0 if sell else 400.0),
    }
    execution = {"trade_threshold_value": 10.0, "reserved_cash": 0.0}
    plan = {"account_hash": "synthetic", "portfolio": portfolio,
            "allocation": allocation, "execution": execution}

    def no_refresh(_):
        raise AssertionError("an uncertain cycle must not refresh and resume")

    session = None
    refreshed = None
    last_submission_accepted = False

    def observed_submit(intent):
        nonlocal last_submission_accepted
        last_submission_accepted = False
        outcome = submit(intent)
        last_submission_accepted = getattr(outcome, "status", None) == "accepted"
        return outcome

    def refreshed_snapshot(_):
        assert last_submission_accepted, "an uncertain SELL must not refresh and resume"
        return observed

    if sell:
        from dataclasses import replace
        from datetime import timedelta
        from quant_platform_kit.common.models import OrderIntent
        from test_rebalance_service import synthetic_sell_session
        assert native_root is not None
        # One qualified SELL, followed (only after observed cash refresh) by
        # BUYs. This does not claim one proof supports multiple different SELLs.
        for symbol in symbols[1:]:
            portfolio["market_values"][symbol] = 0
            portfolio["quantities"][symbol] = 0
            allocation["targets"][symbol] = 400
        native, session = synthetic_sell_session(native_root, plan, OrderIntent("SOXL", "sell", 4))
        observed = replace(native.snapshot, as_of=native.snapshot.as_of+timedelta(minutes=1),
                           positions=(), cash_balance=1900, buying_power=1900)
        refreshed_portfolio = {**portfolio, "market_values": dict.fromkeys(symbols, 0),
                               "quantities": dict.fromkeys(symbols, 0), "liquid_cash": 1900}
        refreshed = {**plan, "portfolio": refreshed_portfolio}
        session.register_plan_source(refreshed, observed)

    return execute_rebalance_cycle(
        client=object(), plan=plan, portfolio=portfolio, execution=execution,
        allocation=allocation, fetch_managed_snapshot=refreshed_snapshot if sell else no_refresh,
        intent_continuity=session,
        market_data_port=CallableMarketDataPort(quote_loader=lambda symbol: QuoteSnapshot(
            symbol=symbol, as_of="2026-09-08", last_price=100.0, ask_price=100.0,
        )),
        load_plan=lambda _: (refreshed, refreshed["portfolio"], execution, allocation) if sell else (plan, portfolio, execution, allocation),
        execution_port=CallableExecutionPort(observed_submit if sell else submit),
        translator=translator or build_translator("en"), limit_buy_premium=1.0,
        sell_settle_delay_sec=0, publish_order_issue=notify,
        notional_buy_execution=notional,
    )


def report(intent, status="accepted", **kwargs):
    return ExecutionReport(symbol=intent.symbol, side=intent.side, quantity=intent.quantity,
                           status=status, broker_order_id="synthetic-order", **kwargs)


@pytest.mark.parametrize("sell,notional", [(False, False), (True, False), (False, True)])
@pytest.mark.parametrize("prior_accepted", [False, True])
def test_uncertain_submission_stops_cycle_and_preserves_original_intent(tmp_path, sell, notional, prior_accepted):
    calls = []

    def submit(intent):
        calls.append(intent)
        if prior_accepted and len(calls) == 1:
            return report(intent)
        raise TimeoutError("synthetic-private-provider-detail")

    result = run_cycle(submit, sell=sell, notional=notional, native_root=tmp_path)
    assert len(calls) == 1 + int(prior_accepted)
    unknown = result.submitted_orders[-1]
    assert unknown["status"] == "unknown"
    assert unknown["symbol"] == calls[-1].symbol
    assert unknown["side"] == calls[-1].side
    assert "broker_order_id" not in unknown
    if notional:
        assert unknown["notional_usd"] == calls[-1].metadata["notional_usd"]
    else:
        assert unknown["quantity"] == calls[-1].quantity
    if prior_accepted:
        assert result.submitted_orders[0]["status"] == "accepted"
    assert result.execution["execution_status"] == "pending_reconciliation"
    assert result.execution["broker_submission_done"] is False
    assert result.execution["orders_pending_count"] == len(calls)
    assert "synthetic-private-provider-detail" not in str(result.trade_logs)


@pytest.mark.parametrize("outcome", ["unknown", "malformed", "server_error"])
def test_unconfirmed_reports_also_stop_further_submissions(outcome):
    calls = []

    def submit(intent):
        calls.append(intent)
        if outcome == "malformed":
            return None
        if outcome == "server_error":
            return report(intent, "rejected", raw_payload={"status_code": 500})
        return report(intent, "unknown")

    result = run_cycle(submit)
    assert len(calls) == 1
    assert result.submitted_orders[0]["status"] == "unknown"
    if outcome == "unknown":
        assert result.submitted_orders[0]["broker_order_id"] == "synthetic-order"


def test_explicit_rejection_remains_distinct_from_unknown():
    calls = []

    def submit(intent):
        calls.append(intent)
        return report(intent, "rejected" if len(calls) == 1 else "accepted")

    result = run_cycle(submit)
    assert len(calls) == 3
    assert all(order["status"] == "accepted" for order in result.submitted_orders)


def test_notification_failure_does_not_drop_unknown_or_resume():
    calls = []

    def fail_notify(_):
        raise RuntimeError("synthetic-notification-detail")

    def submit(intent):
        calls.append(intent)
        raise TimeoutError("synthetic-provider-detail")

    result = run_cycle(submit, notify=fail_notify)
    assert len(calls) == 1
    assert result.submitted_orders[0]["status"] == "unknown"
    assert "synthetic-provider-detail" not in str(result.trade_logs)


def test_post_ack_failure_preserves_ack_without_resubmission():
    calls = []
    translate = build_translator("en")

    def fail_after_ack(key, **kwargs):
        if key == "order_id_suffix":
            raise RuntimeError("synthetic-formatting-detail")
        return translate(key, **kwargs)

    result = run_cycle(lambda intent: (calls.append(intent), report(intent))[1], translator=fail_after_ack)
    assert len(calls) == 1
    assert result.submitted_orders[0]["status"] == "accepted"
    assert result.submitted_orders[0]["broker_order_id"] == "synthetic-order"


def test_unknown_outcome_preserves_claim_and_durable_intent(tmp_path):
    def submit(_):
        raise TimeoutError("synthetic")

    result = run_cycle(submit)
    store = ExecutionMarkerStore(local_dir=str(tmp_path), cloud_prefix_uri=None)
    assert store.claim_marker("synthetic-claim", metadata={"original": True})
    original_claim = store._local_path("synthetic-claim").read_bytes()
    _record_execution_outcome(
        config=SimpleNamespace(execution_state_store=store, strategy_profile="synthetic",
                               execution_state_account_scope="SYNTHETIC", dry_run_only=False),
        marker_key="synthetic-claim", result=result, plan=result.plan,
        notify_issue=lambda *_: None,
    )
    import json
    outcome = json.loads(store._outcome_local_path("synthetic-claim").read_text())
    assert outcome["metadata"]["submitted_orders"] == list(result.submitted_orders)
    assert store._local_path("synthetic-claim").read_bytes() == original_claim
    assert store.claim_marker("synthetic-claim", metadata={}) is False


def test_outcome_persistence_failure_is_sanitized_even_if_notification_fails():
    notifications = []

    def fail_store(*_, **__):
        raise OSError("synthetic-private-store-detail")

    def fail_notify(title, detail):
        notifications.append((title, detail))
        raise OSError("synthetic-private-notify-detail")

    _record_execution_outcome(
        config=SimpleNamespace(execution_state_store=SimpleNamespace(record_outcome=fail_store)),
        marker_key="synthetic", result=SimpleNamespace(execution={}), plan={},
        notify_issue=fail_notify,
    )
    assert "synthetic-private" not in str(notifications)


def test_caller_summary_notification_failure_does_not_drop_unknown(monkeypatch):
    from application import rebalance_service
    from application.runtime_dependencies import SchwabRebalanceConfig, SchwabRebalanceRuntime
    from quant_platform_kit.common.port_adapters import CallableNotificationPort, CallablePortfolioPort

    def submit(_):
        raise TimeoutError("synthetic")

    expected = run_cycle(submit)
    expected.execution.update(signal_display="synthetic", dashboard_text="synthetic", separator="---")
    monkeypatch.setattr(rebalance_service, "execute_rebalance_cycle", lambda **_: expected)

    def fail_publish(*_):
        raise RuntimeError("synthetic-private-notification-detail")

    monkeypatch.setattr(rebalance_service.NotificationPublisher, "publish", fail_publish)
    actual = rebalance_service.run_strategy_core(
        runtime=SchwabRebalanceRuntime(
            fetch_reference_history=lambda: [],
            portfolio_port=CallablePortfolioPort(lambda: SimpleNamespace(metadata={})),
            market_data_port=CallableMarketDataPort(quote_loader=lambda _: None),
            resolve_rebalance_plan=lambda **_: expected.plan,
            notifications=CallableNotificationPort(lambda _: None),
        ),
        config=SchwabRebalanceConfig(
            translator=build_translator("en"), strategy_display_name="synthetic",
            limit_buy_premium=1.0, sell_settle_delay_sec=0,
        ),
    )
    assert actual is expected
    assert actual.submitted_orders[0]["status"] == "unknown"


from dataclasses import replace
from unittest.mock import patch
from test_rebalance_service import continuity_case, run_strategy_core
from application.execution_intent_continuity import _IntentSession


@pytest.mark.parametrize("boundary", ["descriptor", "descriptor_lost_ack", "locator", "locator_lost_ack", "claim", "claim_lost_ack", "partial_list", "list_error"])
def test_actual_managed_caller_boundary_faults_have_zero_transport(tmp_path, boundary):
    c = continuity_case(tmp_path)
    original_create = c.store.object_store.create_text
    original_cas = c.locator.compare_update
    def create(uri, *args):
        hit = (("descriptor" in boundary and uri.endswith("command.json")) or
               ("claim" in boundary and uri.endswith("00000001.json")))
        if hit:
            if "lost_ack" in boundary:
                original_create(uri, *args)
            raise TimeoutError("synthetic lost acknowledgement")
        return original_create(uri, *args)
    def cas(*args, **kwargs):
        if boundary == "locator_lost_ack":
            original_cas(*args, **kwargs)
        raise TimeoutError("synthetic locator reply loss")
    if boundary == "partial_list":
        c.client.partial = True
    if boundary == "list_error":
        c.client.list_error = TimeoutError()
    with patch.object(c.store.object_store, "create_text", create), patch.object(c.locator, "compare_update", cas if boundary.startswith("locator") else original_cas):
        result = run_strategy_core(runtime=c.runtime, config=c.config)
    assert c.calls == [] and result.submitted_orders == ()
    assert result.execution["continuity_coverage"] == "evidence_required"
    assert result.execution["broker_submission_done"] is False


@pytest.mark.parametrize("boundary", ["ack_event", "final_cycle_outcome"])
def test_actual_ack_persistence_failure_keeps_current_ack_and_blocks_restart(tmp_path, boundary):
    c = continuity_case(tmp_path)
    from test_rebalance_service import qualify_synthetic_sell
    qualify_synthetic_sell(c)
    original_create = c.store.object_store.create_text
    fault_hits = []
    def create(uri, *args):
        if uri.endswith("00000002.json"):
            fault_hits.append(uri)
            raise TimeoutError("synthetic event write failure")
        return original_create(uri, *args)
    if boundary == "ack_event":
        with patch.object(c.store.object_store, "create_text", create):
            result = run_strategy_core(runtime=c.runtime, config=c.config)
        assert fault_hits
        assert result.execution["broker_submission_done"] is False
    else:
        with patch.object(type(c.config.execution_state_store), "record_outcome", side_effect=OSError("synthetic")) as persistence_fault:
            result = run_strategy_core(runtime=c.runtime, config=c.config)
        assert persistence_fault.call_count == 1
    assert len(c.calls) == 1
    assert result.submitted_orders[0]["status"] == "accepted"
    later = continuity_case(tmp_path, "2026-10-01")
    result2 = run_strategy_core(runtime=later.runtime, config=later.config)
    assert later.calls == [] and result2.submitted_orders == ()
    assert result2.execution["broker_submission_done"] is False


def test_reference_corruption_never_turns_into_empty_history(tmp_path):
    import json
    c = continuity_case(tmp_path)
    run_strategy_core(runtime=c.runtime, config=c.config)
    ref = json.loads(c.locator.read_registered().payload_json)["references"][0]
    key = ref["descriptor_uri"][5:].split("/", 1)[1]
    c.client.owner(key).path.unlink()
    later = continuity_case(tmp_path, "2026-10-01")
    result = run_strategy_core(runtime=later.runtime, config=later.config)
    assert later.calls == [] and result.execution["continuity_coverage"] == "evidence_required"


def test_same_native_id_preserves_first_created_at_and_validates_existing_content(tmp_path):
    import json
    c = continuity_case(tmp_path)
    first_session = _IntentSession(c.context, snapshot=c.snapshot, profile_id=c.scope.profile_id)
    order = __import__("quant_platform_kit.common.models", fromlist=["OrderIntent"]).OrderIntent("SOXL", "sell", 4)
    # Queue a complete descriptor but make claim unavailable. A later explicit
    # invocation with the same source may claim; creation time stays immutable.
    with patch.object(type(c.store), "claim_due", return_value=None):
        assert first_session.prepare(order=order, plan=c.plan, action_type="SELL") is None
    ref = json.loads(c.locator.read_registered().payload_json)["references"][0]
    original = c.store.object_store.read_text(ref["descriptor_uri"])
    ctx = replace(c.context, consumption_clock=lambda: __import__("datetime").datetime.fromisoformat("2026-09-30T16:00:00+00:00"))
    second = _IntentSession(ctx, snapshot=c.snapshot, profile_id=c.scope.profile_id)
    ticket = second.prepare(order=order, plan=c.plan, action_type="SELL")
    assert ticket is not None
    assert c.store.object_store.read_text(ref["descriptor_uri"]) == original
    assert ticket.command.created_at == json.loads(original)["created_at"]


# Reuse explicit test-only external ports under ordinary pytest as well.
from test_rebalance_service import _offline_caller_external_ports


@pytest.mark.parametrize("quantity", [None, 0, -1, True, False, float("nan"), float("inf"), -float("inf"), "4"])
@pytest.mark.parametrize("preview", [False, True])
def test_untrusted_long_quantity_never_reconstructs_sell_from_value(tmp_path, quantity, preview):
    from test_rebalance_service import continuity_case
    c = continuity_case(tmp_path, selected=False)
    c.plan["portfolio"]["market_values"]["SOXL"] = 500.0
    if quantity is None:
        c.plan["portfolio"]["quantities"].pop("SOXL")
    else:
        c.plan["portfolio"]["quantities"]["SOXL"] = quantity
    result = __import__("application.rebalance_service", fromlist=["run_strategy_core"]).run_strategy_core(
        runtime=c.runtime, config=replace(c.config, dry_run_only=preview))
    assert c.calls == []
    assert result.submitted_orders == ()


@pytest.mark.parametrize("held,target,value,expected", [
    (3, 0, 524.10, 3), (3.9, -1000, 10000, 0),
    (0.9, 0, 500, 0), (4, 200, 400, 2), (4, 0, 1000000, 4),
])
def test_preview_sell_quantity_stays_within_whole_long_holding(tmp_path, held, target, value, expected):
    c = continuity_case(tmp_path, selected=False)
    c.plan["portfolio"].update(quantities={"SOXL": held}, market_values={"SOXL": value})
    c.plan["allocation"]["targets"] = {"SOXL": target}
    # Preview isolates sizing from the independently required live resource
    # proof. It does not authorize or simulate broker acceptance.
    result = run_strategy_core(runtime=c.runtime, config=replace(c.config, dry_run_only=True))
    assert c.calls == []
    assert sum(order["quantity"] for order in result.submitted_orders if order["side"] == "sell") <= int(held)
    if expected:
        assert result.submitted_orders[0]["quantity"] == expected
    else:
        assert result.submitted_orders == ()


@pytest.mark.parametrize("selected", [False, True])
def test_live_sell_requires_complete_resource_proof_even_with_continuity(tmp_path, selected):
    from test_rebalance_service import continuity_case, run_strategy_core
    c = continuity_case(tmp_path, selected=selected)
    result = run_strategy_core(runtime=c.runtime, config=c.config)
    assert c.calls == []
    assert result.submitted_orders == ()
    assert "sell_resource_evidence_required" in result.execution["notification_attention_reason_codes"]
