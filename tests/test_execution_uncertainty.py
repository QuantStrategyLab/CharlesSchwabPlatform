from types import SimpleNamespace

import pytest

from application.execution_service import execute_rebalance_cycle
from application.rebalance_service import _record_execution_outcome
from notifications.telegram import build_translator
from quant_platform_kit.common.execution_state import ExecutionMarkerStore
from quant_platform_kit.common.models import ExecutionReport, QuoteSnapshot
from quant_platform_kit.common.port_adapters import CallableExecutionPort, CallableMarketDataPort


def run_cycle(submit, *, sell=False, notional=False, notify=lambda _: None, translator=None):
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

    return execute_rebalance_cycle(
        client=object(), plan=plan, portfolio=portfolio, execution=execution,
        allocation=allocation, fetch_managed_snapshot=no_refresh,
        market_data_port=CallableMarketDataPort(quote_loader=lambda symbol: QuoteSnapshot(
            symbol=symbol, as_of="2026-09-08", last_price=100.0, ask_price=100.0,
        )),
        load_plan=lambda _: (plan, portfolio, execution, allocation),
        execution_port=CallableExecutionPort(submit),
        translator=translator or build_translator("en"), limit_buy_premium=1.0,
        sell_settle_delay_sec=0, publish_order_issue=notify,
        notional_buy_execution=notional,
    )


def report(intent, status="accepted", **kwargs):
    return ExecutionReport(symbol=intent.symbol, side=intent.side, quantity=intent.quantity,
                           status=status, broker_order_id="synthetic-order", **kwargs)


@pytest.mark.parametrize("sell,notional", [(False, False), (True, False), (False, True)])
@pytest.mark.parametrize("prior_accepted", [False, True])
def test_uncertain_submission_stops_cycle_and_preserves_original_intent(sell, notional, prior_accepted):
    calls = []

    def submit(intent):
        calls.append(intent)
        if prior_accepted and len(calls) == 1:
            return report(intent)
        raise TimeoutError("synthetic-private-provider-detail")

    result = run_cycle(submit, sell=sell, notional=notional)
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
