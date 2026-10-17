"""Approved notification classification; trading gates/receipt policy stay intact."""
import copy
import json
from types import SimpleNamespace

import pytest

from application import account_new_risk_gate_support as support
from application.runtime_report_summary import summarize_execution_cycle_result
from notifications import unified
from notifications.approved_gates import (
    approved_block_reasons, clean_gate_reasons, daily_gate_reasons, decision_gate_reasons,
    render_gate_reasons, report_gate_reasons,
)
from scripts import execution_report_heartbeat as heartbeat
from backport_helpers import decision
from backport_helpers import NOW, mock_heartbeat
from backport_helpers import report
from backport_helpers import configure

CONSECUTIVE = dict(reason_code="consecutive_loss_limit_exceeded", observed_value=6, limit=5)
DAILY = dict(reason_code="daily_loss_limit_exceeded", observed_value=80, limit=60, equity_usd=2000)
FORMULA = dict(pct_max=0.05, pct_min=0.01, equity_scale_usd=2000)


def execution(reasons):
    return dict(execution_status="blocked", no_op_reason="strategy_risk_rejected",
        broker_submission_done=False, notification_approved_gates=copy.deepcopy(reasons))


def source_report(reasons):
    summary = dict(execution(reasons), orders_submitted_count=0, orders_pending_count=0)
    source = report("risk_blocked", started_at="2026-10-05T12:00:00Z", finished_at="2026-10-05T12:01:00Z", summary=summary)
    source["runtime_target"].update(platform_id="schwab", service_name=source["service_name"],
        deployment_selector="synthetic-slot", dry_run_only=False)
    return source


@pytest.mark.parametrize("reasons", [[CONSECUTIVE], [DAILY], [CONSECUTIVE, DAILY]])
@pytest.mark.parametrize("locale", ["zh", "en"])
def test_approved_gates_are_digest_candidates_with_every_reason_and_no_order(monkeypatch, reasons, locale):
    store = configure(monkeypatch)
    source = source_report(reasons)
    target = dict(platform_id="schwab", strategy_profile="synthetic_profile", service_name="synthetic-service",
        account_scope="synthetic", account_selector=["synthetic"], deployment_selector="synthetic-slot", dry_run_only=False)
    source.update(runtime_target=target, service_name="synthetic-service", strategy_profile="synthetic_profile")
    # Cycle projection checks source age; use current receipt start for this producer test.
    from datetime import datetime, timezone
    source["started_at"] = datetime.now(timezone.utc).isoformat()
    result = SimpleNamespace(execution=execution(reasons), submitted_orders=(), trade_logs=())
    original = copy.deepcopy(result.execution)
    with unified.notification_context(source):
        unified.publish_cycle(result, config=SimpleNamespace(dry_run_only=False, strategy_profile="synthetic_profile"),
            legacy=lambda: pytest.fail("approved block must defer after candidate write"))
    fact, _ = next(iter(store.facts.values()))
    assert fact["event_kind"] == "routine_cycle"
    assert fact["approved_gate_reasons"] == reasons
    text = fact["reason_text_" + locale]
    assert ("本轮未下单" if locale == "zh" else "No order this run") in text
    if CONSECUTIVE in reasons:
        assert ("连亏熔断" if locale == "zh" else "Consecutive-loss gate") in text
        assert "6>5" in text
    if DAILY in reasons:
        assert ("日亏损上限" if locale == "zh" else "Daily-loss gate") in text
        assert ("80≥60" if locale == "zh" else "80>=60") in text
    result.execution.pop("notification_events")
    assert result.execution == original
    summary = summarize_execution_cycle_result(result, dry_run=False)
    assert summary["notification_approved_gates"] == reasons


@pytest.mark.parametrize("reasons", [[CONSECUTIVE], [DAILY], [CONSECUTIVE, DAILY]])
def test_approved_block_receipt_suppresses_alert_only_after_candidate_and_still_fails(monkeypatch, reasons):
    store = configure(monkeypatch)
    source = source_report(reasons)
    assert report_gate_reasons(source) == reasons
    before = copy.deepcopy(source)
    sent = mock_heartbeat(monkeypatch, source)
    monkeypatch.setenv("RUNTIME_TARGET_JSON", json.dumps(source["runtime_target"]))
    monkeypatch.setattr(unified, "emit_completed_candidates", lambda env, **kwargs: {"uploaded": True})
    assert heartbeat.main(NOW) == 1
    assert not sent and source == before
    assert heartbeat._is_accepted_report(source) == (False, "rejected inner execution status")
    assert store.facts == {}  # Archive coverage owns this receipt; no second cycle fact.


@pytest.mark.parametrize("change", ["missing_receipt", "wrong_receipt", "order", "error", "pending", "other_reason", "bad_limit", "producer_failed"])
def test_other_receipt_failures_stay_immediate(monkeypatch, change):
    configure(monkeypatch)
    source = source_report([CONSECUTIVE])
    if change == "missing_receipt": source.pop("execution_receipt")
    if change == "wrong_receipt": source["execution_receipt"]["strategy_profile"] = "another-strategy"
    if change == "order": source["summary"].update(orders_submitted_count=1, broker_submission_done=True)
    if change == "error": source["errors"] = ["synthetic order error"]
    if change == "pending": source["summary"]["orders_pending_count"] = 1
    if change == "other_reason": source["summary"]["no_op_reason"] = "other_risk_rejected"
    if change == "bad_limit": source["summary"]["notification_approved_gates"][0]["limit"] = 4
    sent = mock_heartbeat(monkeypatch, source)
    monkeypatch.setattr(unified, "emit_completed_candidates", lambda env, **kwargs: {"uploaded": change != "producer_failed"})
    assert heartbeat.main(NOW) == 1
    assert len(sent) == 1


@pytest.mark.parametrize("flag", [None, "", "false", "invalid"])
def test_approved_gate_flag_off_preserves_original_alert_and_receipt_disposition(monkeypatch, flag):
    if flag is None: monkeypatch.delenv(unified.FLAG, raising=False)
    else: monkeypatch.setenv(unified.FLAG, flag)
    source = source_report([CONSECUTIVE, DAILY])
    sent = mock_heartbeat(monkeypatch, source)
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda *a, **kw: pytest.fail("OFF storage"))
    assert heartbeat.main(NOW) == 1
    assert len(sent) == 1 and heartbeat._is_accepted_report(source)[0] is False


def test_only_the_approved_completed_decision_threshold_is_exempt():
    assert decision_gate_reasons(decision()) == [CONSECUTIVE]
    assert decision_gate_reasons(decision(6, 4)) == []
    value = decision()
    value.risk_flags += ("rejected:runtime_risk_limits",)
    assert decision_gate_reasons(value) == []
    value = execution([CONSECUTIVE])
    value["notification_attention_reason_codes"] = ["negative_cash"]
    assert approved_block_reasons(value, order_count=0) == []
    assert approved_block_reasons(execution([CONSECUTIVE]), order_count=1) == []
    assert approved_block_reasons(execution([CONSECUTIVE]), order_count=0, dry_run=True) == []
    assert clean_gate_reasons([DAILY, DAILY]) == []


def test_daily_loss_uses_completed_admission_exact_formula_and_actual_values():
    admission = SimpleNamespace(reason_codes=("DAILY_LOSS_LIMIT_EXCEEDED",))
    snapshot = SimpleNamespace(daily_loss_usd=80, equity_usd=2000)
    resolution = support.MaxDailyLossResolution(status="valid", limit_usd=60, reason="formula")
    assert daily_gate_reasons(admission, snapshot, resolution, FORMULA) == [DAILY]
    assert daily_gate_reasons(admission, snapshot, resolution, {**FORMULA, "pct_max": 0.06}) == []
    admission.reason_codes += ("RECONCILIATION_NOT_VERIFIED",)
    assert daily_gate_reasons(admission, snapshot, resolution, FORMULA) == []


def test_actual_daily_admission_notification_seam_does_not_page_or_change_decision(monkeypatch):
    from quant_platform_kit.risk.account_new_risk_gate import NewRiskAdmissionResult, NewRiskDisposition
    store = configure(monkeypatch)
    target = {"strategy_profile": "synthetic_profile", "runtime_risk_limits": {"max_daily_loss_equity_formula": FORMULA}}
    monkeypatch.setenv("RUNTIME_TARGET_JSON", json.dumps(target))
    admission = NewRiskAdmissionResult(disposition=NewRiskDisposition.NEW_RISK_PROHIBITED,
        reason_codes=("DAILY_LOSS_LIMIT_EXCEEDED",))
    snapshot = SimpleNamespace(daily_loss_usd=80, equity_usd=2000, drawdown_from_peak=None, production_drift_status="healthy")
    data = execution([])
    before = copy.deepcopy(admission)
    result = support.maybe_publish_attention_for_admission(admission,
        portfolio={"account_new_risk_snapshot": {"equity_usd": 2000}}, execution=data, snapshot=snapshot,
        telegram_sender=lambda **kw: pytest.fail("approved daily-loss attention must defer"))
    assert result["sent"] == 0 and admission == before
    assert data["notification_approved_gates"] == [DAILY]
    assert next(iter(store.facts.values()))[0]["event_kind"] == "expected_block_observed"


@pytest.mark.parametrize("key", ["fail_reason", "plugin_error", "persistence_error", "market_data_retry_required", "notification_error"])
def test_approved_gate_cannot_hide_other_execution_failures(key):
    value = execution([CONSECUTIVE])
    value[key] = "synthetic unrelated issue"
    assert approved_block_reasons(value, order_count=0) == []
    value = execution([CONSECUTIVE])
    value["signal_metadata"] = {key: "synthetic unrelated issue"}
    assert approved_block_reasons(value, order_count=0) == []


def test_missing_receipt_in_legacy_accepted_report_still_alerts_when_on(monkeypatch):
    configure(monkeypatch)
    from backport_helpers import legacy_report
    source = {**legacy_report(), "run_id": "synthetic-business"}
    sent = mock_heartbeat(monkeypatch, source)
    assert heartbeat._is_accepted_report(source)[0] is True
    assert heartbeat.main(NOW) == 0
    assert len(sent) == 1


@pytest.mark.parametrize("flag", ["false", "true"])
def test_real_strategy_adapter_only_adds_approved_annotation_when_on(monkeypatch, flag):
    from dataclasses import replace
    from unittest.mock import Mock
    from backport_helpers import _adapters
    monkeypatch.setenv(unified.FLAG, flag)
    original = {"execution": {"execution_status": "blocked", "no_op_reason": "strategy_risk_rejected"},
        "allocation": {"target_mode": "value"}}
    mapper = Mock(return_value=copy.deepcopy(original))
    value = decision()
    adapters = replace(_adapters(), strategy_runtime=SimpleNamespace(evaluate=Mock(return_value=SimpleNamespace(
        decision=value, metadata={"marker": "synthetic"}))), map_strategy_decision_to_plan_fn=mapper)
    snapshot = object()
    plan = adapters.resolve_rebalance_plan(qqq_history={}, snapshot=snapshot)
    assert mapper.call_args.args == (value,)
    assert mapper.call_args.kwargs["snapshot"] is snapshot
    assert mapper.call_args.kwargs["strategy_profile"] == adapters.strategy_profile
    if flag == "true":
        assert plan["execution"].pop("notification_block_reason") == CONSECUTIVE
        assert plan["execution"].pop("notification_approved_gates") == [CONSECUTIVE]
    assert plan == original


@pytest.mark.parametrize("approved", [False, True])
def test_attention_and_cycle_share_immutable_store_without_event_key_collision(monkeypatch, approved):
    from datetime import datetime, timezone
    from notifications.attention import publish_attention_telegram_transition
    from quant_platform_kit.risk.attention import AttentionAxes, evaluate_attention
    from test_unified_notifications import MemoryStore
    class ImmutableStore(MemoryStore):
        def persist(self, fact, projection=None):
            previous = self.facts.get(fact["dedupe_key"])
            if previous is not None and previous != (fact, projection):
                raise ValueError("notification_candidate_conflict")
            return super().persist(fact, projection)
    configure(monkeypatch)
    store, sent = ImmutableStore(), []
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda *a, **kw: store)
    target = dict(platform_id="schwab", strategy_profile="synthetic_profile", service_name="synthetic-service",
        account_scope="synthetic", account_selector=["synthetic"], deployment_selector="synthetic-slot", dry_run_only=False)
    monkeypatch.setenv("RUNTIME_TARGET_JSON", json.dumps(target))
    source = dict(schema_version="runtime_report.v1", platform="schwab", strategy_profile="synthetic_profile",
        service_name="synthetic-service", runtime_target=target, dry_run=False, status="ok", run_id="same-business-run",
        started_at=datetime.now(timezone.utc).isoformat())
    data = execution([DAILY]) if approved else dict(execution_status="no_action", broker_submission_done=False)
    with unified.notification_context(source):
        publish_attention_telegram_transition(decision=evaluate_attention(AttentionAxes(new_risk_prohibited=approved)),
            platform="schwab", account_alias="synthetic", strategy_profile="synthetic_profile",
            guard_reason_codes=("DAILY_LOSS_LIMIT_EXCEEDED",) if approved else (),
            approved_gate_reasons=[DAILY] if approved else [],
            telegram_sender=lambda **kw: sent.append("attention") or True, locale="zh")
        unified.publish_cycle(SimpleNamespace(execution=data, submitted_orders=(), trade_logs=()),
            config=SimpleNamespace(dry_run_only=False, strategy_profile="synthetic_profile"),
            legacy=lambda: sent.append("cycle") or True)
    assert sent == []
    kinds = {fact["event_kind"] for fact, _ in store.facts.values()}
    assert kinds == ({"expected_block_observed", "routine_cycle"} if approved else {"routine_cycle"})


@pytest.mark.parametrize("kind", ["receipt_missing", "receipt_stale", "receipt_unacceptable", "real_error"])
def test_receipt_incidents_without_business_run_never_stage_monitor_as_candidate(monkeypatch, kind):
    store = configure(monkeypatch)
    sent = []
    for monitor in ("monitor-a", "monitor-b"):
        monkeypatch.setenv("GITHUB_RUN_ID", monitor)
        unified.publish_receipt(lambda: sent.append("alert") or True, kind=kind, reason="receipt_incident")
    assert sent == ["alert"]
    assert store.facts == {}
    marker = next(value for _, value in store.values.values() if "delivery_status" in value)
    assert marker["delivery_status"] == "uncertain" and marker["receipt_id"] is None
