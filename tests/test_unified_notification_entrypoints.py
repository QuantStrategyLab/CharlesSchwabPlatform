import datetime as dt
import json
from types import SimpleNamespace

from notifications import unified
from notifications.attention import publish_attention_telegram_transition
from quant_platform_kit.risk.attention import AttentionAxes, evaluate_attention
from scripts import execution_report_heartbeat as heartbeat
from test_notification_quiet import NOW, mock_heartbeat, report
from test_unified_notifications import MemoryStore


def configure(monkeypatch):
    store = MemoryStore()
    monkeypatch.setenv(unified.FLAG, "true")
    monkeypatch.setenv("NOTIFY_LANG", "zh")
    monkeypatch.setenv("DIGEST_OPAQUE_ACCOUNT_UID", "opaque-a")
    monkeypatch.setenv("DIGEST_TARGET_ID", "target-a")
    monkeypatch.setenv("GITHUB_RUN_ID", "run-synthetic")
    monkeypatch.setenv("RUNTIME_TARGET_JSON", json.dumps({"strategy_profile": "synthetic_profile"}))
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda *a, **kw: store)
    return store


def test_completed_real_order_is_immediate_and_only_counted_in_candidate(monkeypatch):
    store = configure(monkeypatch)
    calls = []
    result = SimpleNamespace(execution={"execution_status": "completed", "broker_submission_done": True},
        submitted_orders=({"status": "filled", "broker_order_id": "synthetic-order"},))
    config = SimpleNamespace(dry_run_only=False, strategy_profile="synthetic_profile")
    unified.publish_cycle(result, config=config, legacy=lambda: calls.append("order") or True)
    assert calls == ["order"]
    fact, _ = next(iter(store.facts.values()))
    assert fact["event_kind"] == "order_submitted" and fact["order_count"] == 1
    assert fact["fill_count"] is None
    assert "synthetic-order" not in fact["order_execution_digest"]
    assert not any("delivery_status" in value for _, value in store.values.values())


def test_attention_real_publisher_cold_start_recovery_and_reworsening(monkeypatch):
    store = configure(monkeypatch)
    sent = []
    kwargs = dict(platform="schwab", account_alias="synthetic", strategy_profile="synthetic_profile",
        guard_reason_codes=("OBSERVATION_NOT_COMPLETE",), already_sent_keys=["irrelevant-legacy-key"],
        record_sent_key=lambda key: (_ for _ in ()).throw(AssertionError("ON must use durable markers")),
        telegram_sender=lambda **kw: sent.append(kw) or True, locale="zh")
    decision = evaluate_attention(AttentionAxes(new_risk_prohibited=True))
    for run in ("a", "b"):
        monkeypatch.setenv("GITHUB_RUN_ID", run)
        publish_attention_telegram_transition(decision=decision, **kwargs)
    assert len(sent) == 1
    monkeypatch.setenv("GITHUB_RUN_ID", "healthy")
    publish_attention_telegram_transition(decision=evaluate_attention(AttentionAxes()), **{**kwargs, "guard_reason_codes": ()})
    monkeypatch.setenv("GITHUB_RUN_ID", "worse-again")
    publish_attention_telegram_transition(decision=decision, **kwargs)
    assert len(sent) == 2
    assert "账户快照不完整" in sent[0]["text"]
    assert store.values


def healthy_business_receipt(monkeypatch):
    from quant_platform_kit.common.execution_receipts import build_execution_receipt
    payload = {**report(), "run_id": "business-run", "platform": "schwab"}
    target = dict(platform_id="schwab", strategy_profile=payload["strategy_profile"], service_name=payload["service_name"],
        account_scope=payload["account_scope"], account_selector=["synthetic"], deployment_selector="synthetic-slot", dry_run_only=False)
    payload["runtime_target"] = target
    payload["execution_receipt"] = build_execution_receipt(platform="schwab", strategy_profile=payload["strategy_profile"],
        strategy_revision="a" * 40, execution_mode="live", outcome="no_action", observed_at=payload["finished_at"])
    monkeypatch.setenv("RUNTIME_TARGET_JSON", json.dumps(target))
    return payload


def test_heartbeat_healthy_uses_real_producer_before_suppressing_manual_send(monkeypatch):
    store = configure(monkeypatch)
    payload = healthy_business_receipt(monkeypatch)
    sent = mock_heartbeat(monkeypatch, payload)
    monkeypatch.setenv("RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS", "true")
    def produce(env):
        store.operations.append("completed_archive_upload")
        return {"uploaded": True}
    monkeypatch.setattr(unified, "emit_completed_candidates", produce)
    assert heartbeat.main(NOW) == 0
    assert not sent
    assert store.operations.index("candidate") < store.operations.index("completed_archive_upload")
    fact, _ = next(iter(store.facts.values()))
    assert fact["event_kind"] == "healthy_heartbeat"
    assert fact["source_run_id"] == "business-run"
    assert fact["occurred_at"] == "2026-10-05T12:00:00+00:00"
    assert fact["order_count"] is None and fact["fill_count"] is None


def test_heartbeat_failed_producer_keeps_manual_send_and_reports_unknown(monkeypatch, capsys):
    configure(monkeypatch)
    sent = mock_heartbeat(monkeypatch, healthy_business_receipt(monkeypatch))
    monkeypatch.setenv("RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS", "true")
    monkeypatch.setattr(unified, "emit_completed_candidates", lambda env: {"uploaded": False})
    assert heartbeat.main(NOW) == 0
    assert len(sent) == 1
    assert "coverage=unknown" in capsys.readouterr().out


def test_blocked_receipt_real_entrypoint_remains_failure_but_pages_once_across_runs(monkeypatch):
    configure(monkeypatch)
    payload = report()
    payload["summary"].update(execution_status="blocked", no_op_reason="strategy_risk_rejected", orders_submitted_count=0)
    sent = mock_heartbeat(monkeypatch, payload)
    for run in ("a", "b"):
        monkeypatch.setenv("GITHUB_RUN_ID", run)
        assert heartbeat.main(NOW) == 1
    assert len(sent) == 1
    assert "gs://" not in sent[0]
    assert "被策略风控阻断" in sent[0]
    assert heartbeat._is_accepted_report(payload)[0] is False


def test_pending_status_is_immediate_even_when_legacy_quiet_rule_accepts_it(monkeypatch):
    configure(monkeypatch)
    payload = report()
    payload["summary"].update(execution_status="pending", orders_submitted_count=1)
    assert heartbeat._is_quiet_report(payload, now=NOW)
    sent = mock_heartbeat(monkeypatch, payload)
    assert heartbeat.main(NOW) == 0  # Acceptance and exit code remain the original result.
    assert len(sent) == 1


def test_runtime_failure_on_uses_one_attempt_without_legacy_fallback(monkeypatch):
    from test_runtime_notification_i18n import runtime_function

    configure(monkeypatch)
    calls = []
    fn = runtime_function("_publish_runtime_failure_notification",
        publish_notification=lambda **kw: calls.append("primary") or False,
        _publish_runtime_failure_notification_legacy=lambda **kw: calls.append("legacy-fallback") or True)
    assert fn(detailed_text="Synthetic failure", compact_text="Synthetic failure", exc=RuntimeError("synthetic")) is False
    assert calls == ["primary"]
