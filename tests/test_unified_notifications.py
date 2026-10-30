"""Notification-only fakes: no broker, HTTP, cloud credentials or real sends."""
import copy
import datetime as dt
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from notifications import unified
from notifications.digest_snapshot.unified_contract import NotificationEvent

NOW = dt.datetime(2026, 10, 16, 19, 45, tzinfo=dt.timezone.utc)


class MemoryStore:
    def __init__(self):
        self.values, self.facts, self.operations = {}, {}, []
        self.fail_candidate = False
        self.fail_read = False

    def read(self, key):
        if self.fail_read:
            raise RuntimeError("incomplete_history")
        version, value = self.values.get(key, (None, None))
        self.operations.append("read")
        return version, copy.deepcopy(value)

    def compare_and_set(self, key, version, value):
        current, _ = self.values.get(key, (None, None))
        if current != version:
            return False
        self.values[key] = ((current or 0) + 1, copy.deepcopy(value))
        self.operations.append("cas")
        return True

    def persist(self, fact, projection=None):
        self.operations.append("candidate")
        if self.fail_candidate:
            raise RuntimeError("candidate_write_failed")
        if fact["coverage_status"] != "known":
            return False
        self.facts[fact["dedupe_key"]] = (fact, projection)
        return True


def event(kind="real_error", run="run-a", **kwargs):
    return NotificationEvent(kind, NOW, platform_id="schwab", target_id="target-a",
        opaque_account_uid="opaque-a", strategy_profile="strategy-a", source_run_id=run,
        incident_reason="synthetic_incident", **kwargs)


@pytest.mark.parametrize("flag", [None, "", "false", "invalid", "1", "yes"])
def test_off_calls_only_legacy_with_no_candidate_or_marker(flag):
    env = {} if flag is None else {unified.FLAG: flag}
    store, calls = MemoryStore(), []
    unified.route(event(), env=env, store=store, legacy=lambda: calls.append("legacy"))
    assert calls == ["legacy"]
    assert store.operations == []


def test_true_maps_single_mode_and_persists_before_routine_deferral():
    store, sent = MemoryStore(), []
    env = {unified.FLAG: " true ", "QSL_ROUTINE_NOTIFICATION_MODE": "immediate"}
    unified.route(event("routine_cycle", order_count=0), env=env, store=store, legacy=lambda: sent.append("direct"))
    assert not sent
    assert store.operations == ["candidate"]
    assert next(iter(store.facts.values()))[0]["window_id"] == "sgt-0600:2026-10-16"


def test_routine_write_failure_is_unresolved_and_records_unknown(capsys):
    store, sent = MemoryStore(), []
    store.fail_candidate = True
    with pytest.raises(unified.CandidatePersistenceError):
        unified.route(event("routine_cycle"), env={unified.FLAG: "true"}, store=store, legacy=lambda: sent.append("direct"))
    assert sent == []
    assert "coverage=unknown" in capsys.readouterr().out


def test_cold_starts_share_reservation_and_only_one_acknowledged_alert():
    store, sent = MemoryStore(), []
    def send():
        assert "cas" in store.operations
        sent.append("alert")
        return True
    for run in ("run-a", "run-b", "workflow-retry"):
        unified.route(event(run=run), env={unified.FLAG: "true"}, store=store, legacy=send)
    assert sent == ["alert"]
    markers = [value for _, value in store.values.values() if "delivery_status" in value]
    assert markers[0]["delivery_status"] == "uncertain"
    assert markers[0]["receipt_id"] is None
    assert markers[0]["transport_acknowledged"] is True


def test_candidate_failure_does_not_suppress_real_alert_with_available_marker():
    store, sent = MemoryStore(), []
    store.fail_candidate = True
    unified.route(event(), env={unified.FLAG: "true"}, store=store, legacy=lambda: sent.append("alert") or True)
    assert sent == ["alert"]


@pytest.mark.parametrize("ack", [False, None, "exception"])
def test_uncertain_or_failed_delivery_never_blindly_resends(ack):
    store, calls = MemoryStore(), []
    def send():
        calls.append("attempt")
        if ack == "exception":
            raise TimeoutError("unknown_result")
        return ack
    for run in ("a", "b"):
        unified.route(event(run=run), env={unified.FLAG: "true"}, store=store, legacy=send)
    assert calls == ["attempt"]
    marker = next(value for _, value in store.values.values() if "delivery_status" in value)
    assert marker["delivery_status"] == "uncertain"


def test_marker_read_failure_never_falls_back_to_untracked_send(capsys):
    store, calls = MemoryStore(), []
    store.fail_read = True
    unified.route(event(), env={unified.FLAG: "true"}, store=store, legacy=lambda: calls.append("unsafe"))
    assert not calls
    assert "marker_or_producer_unavailable" in capsys.readouterr().out


def test_recovery_then_reworsening_and_severity_up_are_new_events():
    store, calls = MemoryStore(), []
    def observe(fact, **kw):
        return unified.route(fact, env={unified.FLAG: "true"}, store=store,
            legacy=lambda: calls.append("alert") or True, **kw)
    observe(event("risk_escalation"), severity=2)
    observe(replace(event("risk_escalation", "b"), occurred_at=NOW + dt.timedelta(seconds=1)), severity=2)
    observe(replace(event("risk_escalation", "c"), occurred_at=NOW + dt.timedelta(seconds=2)), severity=3)
    observe(replace(event("routine_cycle", "healthy"), occurred_at=NOW + dt.timedelta(seconds=3)), recovered=True)
    observe(replace(event("risk_escalation", "d"), occurred_at=NOW + dt.timedelta(seconds=4)), severity=2)
    assert calls == ["alert"] * 3


def test_no_due_observation_and_stale_recovery_do_not_clear_incident():
    store, calls = MemoryStore(), []
    env = {unified.FLAG: "true"}
    def send():
        return calls.append("alert") or True
    unified.route(event(), env=env, store=store, legacy=send)
    unified.route(replace(event("routine_cycle", "idle"), occurred_at=NOW + dt.timedelta(seconds=1)),
        env=env, store=store, legacy=send, recovered=False)
    unified.route(replace(event("routine_cycle", "old"), occurred_at=NOW - dt.timedelta(days=1)),
        env=env, store=store, legacy=send, recovered=True)
    unified.route(replace(event(run="new"), occurred_at=NOW + dt.timedelta(seconds=2)), env=env, store=store, legacy=send)
    assert len(calls) == 1


def test_different_targets_accounts_and_new_reasons_are_independent():
    store, calls = MemoryStore(), []
    for fact in (event(), replace(event(), target_id="target-b"), replace(event(), opaque_account_uid="opaque-b"),
                 replace(event(), strategy_profile="strategy-b"), replace(event(), incident_reason="new_reason")):
        unified.route(fact, env={unified.FLAG: "true"}, store=store, legacy=lambda: calls.append("alert") or True)
    assert len(calls) == 5


def test_unknown_identity_keeps_unknown_counts_and_cannot_claim_delivery():
    store, calls = MemoryStore(), []
    fact = replace(event(order_count=0), opaque_account_uid=None)
    assert fact.to_candidate()["order_count"] is None
    assert fact.to_candidate()["dedupe_key"] is None
    unified.route(fact, env={unified.FLAG: "true"}, store=store, legacy=lambda: calls.append("alert"))
    assert not calls


def test_orders_default_to_daily_candidate_in_contract():
    from notifications.digest_snapshot.unified_contract import select_notification_route
    assert select_notification_route(event_kind="fill", env={unified.FLAG: "true"}, flag_key=unified.FLAG) == "digest_candidate"


def test_real_rebalance_entrypoint_on_stages_producer_and_keeps_broker_calls(monkeypatch):
    from notif_equivalence_harness import record_cycle
    from pathlib import Path

    store = MemoryStore()
    monkeypatch.setenv("ACCOUNT_NEW_RISK_GATE", "0")
    monkeypatch.setenv(unified.FLAG, "true")
    monkeypatch.setenv("DIGEST_OPAQUE_ACCOUNT_UID", "opaque-a")
    monkeypatch.setenv("DIGEST_TARGET_ID", "target-a")
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda *a, **kw: store)
    now = dt.datetime.now(dt.timezone.utc)
    target = {"platform_id": "schwab", "strategy_profile": "synthetic_profile", "service_name": "synthetic-service",
        "account_scope": "synthetic", "account_selector": ["synthetic"], "deployment_selector": "synthetic-deploy", "dry_run_only": False}
    report = {"schema_version": "runtime_report.v1", "platform": "charles_schwab", "service_name": "synthetic-service",
        "strategy_profile": "synthetic_profile", "runtime_target": target, "started_at": now.isoformat(), "run_id": "run-synthetic", "dry_run": False}
    with unified.notification_context(report):
        actual = json.loads(json.dumps(record_cycle("healthy"), default=str))
    expected = json.loads((Path(__file__).parent / "fixtures" / "notif_main_calls.json").read_text())["healthy:en"]
    assert [call for call in actual["calls"] if call[0] != "send"] == [call for call in expected["calls"] if call[0] != "send"]
    assert not any(call[0] == "send" for call in actual["calls"])
    fact, projection = next(iter(store.facts.values()))
    assert fact["source_run_id"] == "run-synthetic"
    assert projection["runs"][0]["source_run_id"] == "run-synthetic"
    assert projection["runs"][0]["cycle_count"] == 1
    assert projection["runs"][0]["order_count"] == 0
    assert projection["runs"][0]["notification_events"] == [fact]


@pytest.mark.parametrize("dry_run", [False, True])
def test_metadata_error_is_never_hidden_as_routine(monkeypatch, dry_run):
    store, sent = MemoryStore(), []
    monkeypatch.setenv(unified.FLAG, "true")
    monkeypatch.setenv("DIGEST_OPAQUE_ACCOUNT_UID", "opaque-a")
    monkeypatch.setenv("DIGEST_TARGET_ID", "target-a")
    monkeypatch.setenv("GITHUB_RUN_ID", "synthetic-run")
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda *a, **kw: store)
    result = SimpleNamespace(execution={"execution_status": "dry_run" if dry_run else "no_action",
        "signal_metadata": {"errors": ["synthetic failure"]}}, submitted_orders=())
    unified.publish_cycle(result, config=SimpleNamespace(dry_run_only=dry_run, strategy_profile="strategy-a"),
        legacy=lambda: sent.append("alert") or True)
    assert sent == ["alert"]
    assert next(iter(store.facts.values()))[0]["event_kind"] != "routine_cycle"


@pytest.mark.parametrize("response,delivered", [
    ({"acknowledged": True, "receipt_id": "telegram:synthetic-message-42"}, True),
    ({"acknowledged": True, "receipt_id": "transport-ack:synthetic"}, False),
    ({"acknowledged": True}, False),
    (True, False),
])
def test_delivery_requires_sender_receipt_and_uncertain_ack_is_not_retried(response, delivered):
    store, calls = MemoryStore(), []
    for run in ("a", "b"):
        unified.route(event(run=run), env={unified.FLAG: "true"}, store=store,
            legacy=lambda: calls.append("send") or response)
    marker = next(v for _, v in store.values.values() if "delivery_status" in v)
    assert calls == ["send"]
    assert marker["delivery_status"] == ("delivered" if delivered else "uncertain")
    assert marker["receipt_id"] == (response["receipt_id"] if delivered else None)
