"""Offline regression checks for routine Schwab ACK evidence."""
import copy
import datetime as dt

import pytest

from notifications.accepted_orders import confirmed_acceptances


def acknowledged():
    return ({"execution_status": "pending_reconciliation", "broker_submission_done": True,
             "orders_pending_count": 1}, [{"status": "accepted", "broker_order_id": "synthetic"}])


def test_acknowledgement_requires_complete_positive_evidence():
    execution, orders = acknowledged()
    assert confirmed_acceptances(execution, orders)
    for status in ("submitted", "broker_acknowledged", "pending", "partially_filled"):
        assert confirmed_acceptances(execution, [{"status": status, "broker_order_id": "synthetic"}])
    mutations = [
        ("broker_submission_done", False), ("orders_pending_count", 2),
        ("notification_order_issue", True), ("reconciliation_required", True),
        ("notification_attention_reason_codes", ["risk"]),
    ]
    for key, value in mutations:
        changed = copy.deepcopy(execution)
        changed[key] = value
        assert not confirmed_acceptances(changed, orders)
    for order in ({"status": "unknown", "broker_order_id": "synthetic"},
                  {"status": "accepted"}, {"status": "rejected", "broker_order_id": "synthetic"}):
        assert not confirmed_acceptances(execution, [order])


def test_empty_execution_is_not_invented_as_a_completed_cycle():
    from notifications.accepted_orders import cycle_notification_evidence
    assert cycle_notification_evidence({}, [])["routine"] is False
    assert cycle_notification_evidence({"notification_cycle_completed": True, "errors": ["error"]}, [])["routine"] is False
    assert cycle_notification_evidence({"notification_cycle_completed": True, "signal_metadata": {"errors": ["error"]}}, [])["routine"] is False
    assert cycle_notification_evidence({"notification_cycle_completed": True}, [])["routine"] is True
    blocked = cycle_notification_evidence({"execution_status": "blocked"}, [])
    assert blocked["routine"] is False
    assert blocked["order_count"] is None and blocked["fill_count"] is None


@pytest.mark.parametrize("flag", [None, "false", "true"])
def test_healthy_dry_run_does_not_require_candidate_or_send(flag, monkeypatch):
    from notifications.unified import publish_cycle
    from types import SimpleNamespace
    if flag is None:
        monkeypatch.delenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", flag)
    calls = []
    result = SimpleNamespace(execution={"execution_status": "dry_run", "signal_metadata": {}},
                             submitted_orders=[{"status": "dry_run"}])
    config = SimpleNamespace(dry_run_only=True, strategy_profile="synthetic", notification_attention_reason_codes=())
    monkeypatch.setattr("notifications.unified.GCSNotificationStore", lambda *a: pytest.fail("preview tried to access storage"))
    assert publish_cycle(result, config=config, legacy=lambda: calls.append("normal"),
                         coverage_failure=lambda: calls.append("coverage")) is None
    assert calls == []


def test_uncertain_dry_run_keeps_existing_error_route(monkeypatch):
    from notifications.unified import publish_cycle
    from types import SimpleNamespace
    monkeypatch.setenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", "false")
    calls = []
    result = SimpleNamespace(execution={"execution_status": "unknown"}, submitted_orders=[])
    config = SimpleNamespace(dry_run_only=True, strategy_profile="synthetic", notification_attention_reason_codes=())
    publish_cycle(result, config=config, legacy=lambda: calls.append("attention"))
    assert calls == ["attention"]


def test_archived_ack_proof_preserves_unknown_fill_and_rejects_conflicts():
    from types import SimpleNamespace
    from application.runtime_report_summary import summarize_execution_cycle_result
    from quant_platform_kit.common.execution_receipts import build_execution_receipt
    from scripts.execution_report_heartbeat import _is_proven_accepted_report

    execution, orders = acknowledged()
    now = dt.datetime(2026, 10, 30, 2, tzinfo=dt.timezone.utc)
    summary = summarize_execution_cycle_result(SimpleNamespace(execution=execution, submitted_orders=orders, trade_logs=[]), dry_run=False)
    report = {"status": "ok", "platform": "charles_schwab", "strategy_profile": "synthetic",
              "dry_run": False, "started_at": (now - dt.timedelta(minutes=2)).isoformat(),
              "finished_at": (now - dt.timedelta(minutes=1)).isoformat(), "errors": [],
              "runtime_target": {"execution_mode": "live"},
              "runtime_release_receipt": {"attestation_state": "self_attested", "strategy_release": {"strategy_revision": "a" * 40}},
              "diagnostics": {}, "summary": summary,
              "execution_receipt": build_execution_receipt(platform="schwab", strategy_profile="synthetic",
                  strategy_revision="a" * 40, execution_mode="live", outcome="reconciliation_required", observed_at=now)}
    assert summary["notification_digest"]["routine"] is True
    assert summary["notification_digest"]["fill_count"] is None
    assert report["execution_receipt"]["outcome"] == "reconciliation_required"
    assert _is_proven_accepted_report(report, now=now)
    for field, value in (("broker_submission_done", False), ("orders_pending_count", 2),
                         ("submitted_order_status_counts", {"unknown": 1})):
        changed = copy.deepcopy(report)
        changed["summary"][field] = value
        assert not _is_proven_accepted_report(changed, now=now)
    changed = copy.deepcopy(report)
    changed["summary"].pop("notification_digest")
    assert not _is_proven_accepted_report(changed, now=now)


def test_archive_projection_preserves_ack_count_and_unknown_fill():
    from types import SimpleNamespace
    from application.runtime_report_summary import summarize_execution_cycle_result
    from notifications.unified import _project_runs
    execution, orders = acknowledged()
    now = dt.datetime.now(dt.timezone.utc)
    target = {"platform_id": "schwab", "strategy_profile": "synthetic", "service_name": "synthetic-service",
              "account_scope": "live", "account_selector": ["live"], "deployment_selector": "live", "dry_run_only": False}
    report = {"schema_version": "runtime_report.v1", "platform": "charles_schwab", "strategy_profile": "synthetic",
              "service_name": "synthetic-service", "runtime_target": target, "status": "ok", "dry_run": False,
              "started_at": now.isoformat(), "run_id": "synthetic-run", "errors": [],
              "summary": summarize_execution_cycle_result(SimpleNamespace(execution=execution, submitted_orders=orders, trade_logs=[]), dry_run=False)}
    report["summary"]["routine_observation_counts"] = {"watch": 1}
    rows = _project_runs([report], expected_target=target, opaque_account_uid="synthetic-account", target_id="synthetic-target", now=now)["runs"]
    assert len(rows) == 1
    assert rows[0]["source_run_id"] == "synthetic-run"
    assert rows[0]["order_count"] == 1
    assert rows[0]["fill_count"] is None
    assert rows[0]["execution_status"] == "order_submitted"
    assert rows[0]["routine_observation_counts"] == {"watch": 1}


def test_completed_window_export_reads_schwab_alias_with_bound_target():
    import json
    from types import SimpleNamespace
    from application.runtime_report_summary import summarize_execution_cycle_result
    from notifications.unified import emit_completed_candidates
    execution, orders = acknowledged()
    now = dt.datetime(2026, 10, 30, 3, tzinfo=dt.timezone.utc)
    start = dt.datetime(2026, 10, 29, 20, tzinfo=dt.timezone.utc)
    target = {"platform_id": "schwab", "strategy_profile": "synthetic", "service_name": "synthetic-service",
              "account_scope": "live", "account_selector": ["live"], "deployment_selector": "live", "dry_run_only": False}
    report = {"schema_version": "runtime_report.v1", "platform": "charles_schwab", "strategy_profile": "synthetic",
              "service_name": "synthetic-service", "runtime_target": target, "status": "ok", "dry_run": False,
              "started_at": start.isoformat(), "run_id": "synthetic-run", "errors": [],
              "summary": summarize_execution_cycle_result(SimpleNamespace(execution=execution, submitted_orders=orders, trade_logs=[]), dry_run=False)}
    uri = "gs://synthetic-reports/reports/charles_schwab/synthetic/2026-10/run.json"
    env = {"RUNTIME_TARGET_ENABLED": "true", "RUNTIME_TARGET_JSON": json.dumps(target),
           "EXECUTION_REPORT_GCS_URI": "gs://synthetic-reports/reports", "DIGEST_CANDIDATES_GCS_URI": "gs://synthetic-output/candidates",
           "DIGEST_STORE_TARGET": "synthetic", "DIGEST_OPAQUE_ACCOUNT_UID": "opaque", "DIGEST_TARGET_ID": "target"}
    listed, uploaded = [], []
    def read(args):
        if args[0] == "ls":
            listed.append(args[-1])
            return [{"url": uri, "metadata": {"updated": start.isoformat()}}] if "/2026-10/" in args[-1] else []
        assert args == ["cat", uri]
        return report
    result = emit_completed_candidates(env, now=now, read=read, upload=lambda uri, body: uploaded.append((uri, json.loads(body))))
    assert result["uploaded"] is True
    assert all("/charles_schwab/" in path for path in listed)
    assert len(uploaded) == 1 and uploaded[0][1]["runs"][0]["order_count"] == 1
    assert uploaded[0][1]["runs"][0]["fill_count"] is None


@pytest.mark.parametrize("flag", [None, "false", "true"])
def test_routine_ack_is_deferred_for_every_flag(flag, monkeypatch):
    from notifications.unified import publish_cycle
    from types import SimpleNamespace
    execution, orders = acknowledged()
    if flag is None:
        monkeypatch.delenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", flag)
    calls = []
    persisted = []
    result = SimpleNamespace(execution=execution, submitted_orders=orders)
    config = SimpleNamespace(dry_run_only=False, strategy_profile="synthetic", notification_attention_reason_codes=())
    monkeypatch.setenv("DIGEST_OPAQUE_ACCOUNT_UID", "synthetic-account")
    monkeypatch.setenv("DIGEST_TARGET_ID", "synthetic-target")
    monkeypatch.setenv("GITHUB_RUN_ID", "synthetic-run")
    monkeypatch.setattr("notifications.unified._cycle_projection", lambda *a: {"runs": ["synthetic"]})
    monkeypatch.setattr("notifications.unified.GCSNotificationStore", lambda: SimpleNamespace(
        persist=lambda fact, projection: persisted.append((fact, projection)) or True))
    monkeypatch.setattr("notifications.unified._incident", lambda *a, **kw: None)
    publish_cycle(result, config=config, legacy=lambda: calls.append("sent"))
    assert calls == []
    assert len(persisted) == 1
    assert persisted[0][0]["coverage_status"] == "known"
    assert persisted[0][0]["order_count"] == 1
    assert persisted[0][0]["fill_count"] is None


@pytest.mark.parametrize("flag", [None, "false", "true"])
def test_unknown_submission_stays_urgent(flag, monkeypatch):
    from notifications.unified import publish_cycle
    from types import SimpleNamespace
    execution, orders = acknowledged()
    execution["broker_submission_done"] = False
    orders[0]["status"] = "unknown"
    if flag is None:
        monkeypatch.delenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", flag)
    calls = []
    config = SimpleNamespace(dry_run_only=False, strategy_profile="synthetic", notification_attention_reason_codes=())
    monkeypatch.setenv("DIGEST_OPAQUE_ACCOUNT_UID", "synthetic-account")
    monkeypatch.setenv("DIGEST_TARGET_ID", "synthetic-target")
    monkeypatch.setenv("GITHUB_RUN_ID", "synthetic-run")
    monkeypatch.setattr("notifications.unified._cycle_projection", lambda *a: None)
    monkeypatch.setattr("notifications.unified.GCSNotificationStore", lambda *a: SimpleNamespace(persist=lambda *a: True))
    monkeypatch.setattr("notifications.unified._incident", lambda event, **kw: kw["sender"]())
    publish_cycle(SimpleNamespace(execution=execution, submitted_orders=orders), config=config,
                  legacy=lambda: calls.append("urgent"))
    assert calls == ["urgent"]
    assert execution["notification_events"][0]["event_kind"] == "order_status_unknown"


@pytest.mark.parametrize("flag", [None, "false", "true"])
@pytest.mark.parametrize("status", [None, "no_action"])
def test_no_trade_cycle_is_staged_and_not_pushed(flag, status, monkeypatch):
    from notifications.unified import publish_cycle
    from types import SimpleNamespace
    if flag is None:
        monkeypatch.delenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", flag)
    monkeypatch.setenv("DIGEST_OPAQUE_ACCOUNT_UID", "synthetic-account")
    monkeypatch.setenv("DIGEST_TARGET_ID", "synthetic-target")
    monkeypatch.setenv("GITHUB_RUN_ID", "synthetic-run")
    staged, sent = [], []
    monkeypatch.setattr("notifications.unified._cycle_projection", lambda *a: {"runs": ["synthetic"]})
    monkeypatch.setattr("notifications.unified.GCSNotificationStore", lambda: SimpleNamespace(
        persist=lambda fact, projection: staged.append(fact) or True))
    monkeypatch.setattr("notifications.unified._incident", lambda *a, **kw: None)
    result = SimpleNamespace(execution={"execution_status": status, "notification_cycle_completed": True}, submitted_orders=[])
    config = SimpleNamespace(dry_run_only=False, strategy_profile="synthetic", notification_attention_reason_codes=())
    publish_cycle(result, config=config, legacy=lambda: sent.append("routine"))
    assert sent == []
    assert len(staged) == 1 and staged[0]["event_kind"] == "routine_cycle"


@pytest.mark.parametrize("locale", ["zh", "en"])
@pytest.mark.parametrize("flag", [None, "false", "true"])
def test_routine_coverage_failure_uses_fixed_error_only(flag, locale, monkeypatch):
    from notifications.unified import publish_cycle
    from types import SimpleNamespace
    execution, orders = acknowledged()
    if flag is None:
        monkeypatch.delenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", flag)
    sent = []
    config = SimpleNamespace(dry_run_only=False, strategy_profile="synthetic", notification_attention_reason_codes=())
    monkeypatch.setattr("notifications.unified._cycle_projection", lambda *a: (_ for _ in ()).throw(ValueError("private order detail")))
    publish_cycle(SimpleNamespace(execution=execution, submitted_orders=orders), config=config,
                  legacy=lambda: sent.append("private trade body"),
                  coverage_failure=lambda: sent.append("摘要记录未确认" if locale == "zh" else "coverage unconfirmed"))
    assert sent == ["摘要记录未确认" if locale == "zh" else "coverage unconfirmed"]


@pytest.mark.parametrize("locale", ["zh", "en"])
def test_healthy_heartbeat_coverage_failure_is_urgent_and_private(locale, monkeypatch):
    from scripts import execution_report_heartbeat as heartbeat
    sent = []
    monkeypatch.setenv("NOTIFY_LANG", locale)
    monkeypatch.setenv("RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS", "true")
    monkeypatch.setattr(heartbeat, "unified_enabled", lambda: False)
    monkeypatch.setattr(heartbeat, "publish_receipt", lambda *a, **kw: (_ for _ in ()).throw(ValueError("private order detail")))
    monkeypatch.setattr(heartbeat, "_send_telegram", lambda message: sent.append(message) or True)
    result = heartbeat._send_normal_heartbeat("synthetic", "private order detail", quiet_eligible=True,
                                               source_reports=[{"execution_receipt": {"synthetic": True}}])
    assert result == 1
    assert len(sent) == 1
    assert ("摘要记录未确认" if locale == "zh" else "coverage is unconfirmed") in sent[0]
    assert "private order detail" not in sent[0]


@pytest.mark.parametrize("flag", [None, "false", "true"])
def test_receipt_fact_persists_and_links_to_archive_for_all_flags(flag, monkeypatch):
    from types import SimpleNamespace
    from notifications import unified
    from notifications.digest_snapshot.unified_contract import NotificationEvent
    if flag is None:
        monkeypatch.delenv(unified.FLAG, raising=False)
    else:
        monkeypatch.setenv(unified.FLAG, flag)
    stamp = dt.datetime(2026, 10, 29, 20, tzinfo=dt.timezone.utc)
    event = NotificationEvent("healthy_heartbeat", stamp, platform_id="schwab", target_id="target",
                              opaque_account_uid="opaque", strategy_profile="synthetic", source_run_id="run")
    staged, exported, sent = [], [], []
    monkeypatch.setattr(unified, "_receipt_event", lambda *a: event)
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda: SimpleNamespace(persist=lambda fact: staged.append(fact) or True))
    monkeypatch.setattr(unified, "emit_completed_candidates", lambda *a, **kw: exported.append(kw) or {"uploaded": True})
    assert unified.publish_receipt(lambda: sent.append("routine"), kind="healthy_heartbeat", reason="observed", source_report={}) is None
    assert sent == [] and len(staged) == 1 and len(exported) == 1
    assert exported[0]["required_runs"] == [unified._run_identity(event.to_candidate())]


def test_manual_healthy_heartbeat_never_pushes(monkeypatch):
    from scripts import execution_report_heartbeat as heartbeat
    monkeypatch.setenv("RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS", "true")
    monkeypatch.setattr(heartbeat, "_send_telegram", lambda _: pytest.fail("routine heartbeat pushed"))
    assert heartbeat._send_normal_heartbeat_legacy("synthetic", "detail", quiet_eligible=True) == 0


def test_plugin_filter_keeps_errors_and_defers_routine_regardless_of_default_mode():
    from types import SimpleNamespace
    from notifications.digest_snapshot.digest_routing import immediate_plugin_signals
    watch = SimpleNamespace(canonical_route="watch", suggested_action="observe")
    urgent = SimpleNamespace(canonical_route="watch", suggested_action="observe", error="synthetic error")
    assert immediate_plugin_signals([watch, urgent], mode="daily_digest") == (urgent,)


@pytest.mark.parametrize("unified", [False, True])
def test_actual_heartbeat_uses_archived_ack_as_routine(monkeypatch, unified):
    from types import SimpleNamespace
    from application.runtime_report_summary import summarize_execution_cycle_result
    from quant_platform_kit.common.execution_receipts import build_execution_receipt
    from scripts import execution_report_heartbeat as heartbeat
    execution, orders = acknowledged()
    now = dt.datetime(2026, 10, 30, 2, tzinfo=dt.timezone.utc)
    report = {"status": "ok", "platform": "charles_schwab", "strategy_profile": "synthetic", "dry_run": False,
              "started_at": (now - dt.timedelta(minutes=2)).isoformat(),
              "finished_at": (now - dt.timedelta(minutes=1)).isoformat(), "errors": [], "diagnostics": {},
              "runtime_target": {"execution_mode": "live"},
              "runtime_release_receipt": {"attestation_state": "self_attested", "strategy_release": {"strategy_revision": "a" * 40}},
              "summary": summarize_execution_cycle_result(SimpleNamespace(execution=execution, submitted_orders=orders, trade_logs=[]), dry_run=False),
              "execution_receipt": build_execution_receipt(platform="schwab", strategy_profile="synthetic",
                  strategy_revision="a" * 40, execution_mode="live", outcome="reconciliation_required", observed_at=now)}
    sent, recorded = [], []
    monkeypatch.setenv("RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS", "true")
    monkeypatch.setattr(heartbeat, "_runtime_target_enabled", lambda: True)
    monkeypatch.setattr(heartbeat, "runtime_target_configuration_present", lambda _: False)
    monkeypatch.setattr(heartbeat, "_load_required_services", lambda: [])
    monkeypatch.setattr(heartbeat, "load_runtime_targets", lambda _: [])
    monkeypatch.setattr(heartbeat, "filter_due_targets", lambda *a, **kw: ([], False))
    monkeypatch.setattr(heartbeat, "_heartbeat_skip_reason_for_schedule", lambda *a: None)
    monkeypatch.setattr(heartbeat, "_report_globs", lambda *a: ["synthetic"])
    monkeypatch.setattr(heartbeat, "_list_gcs_objects", lambda *a, **kw: [{"url": "gs://synthetic/report", "metadata": {"updated": now.isoformat()}}])
    monkeypatch.setattr(heartbeat, "_cat_gcs_json", lambda *a, **kw: report)
    monkeypatch.setattr(heartbeat, "_payload_matches", lambda *a, **kw: (True, "", ""))
    monkeypatch.setattr(heartbeat, "unified_enabled", lambda: unified)
    monkeypatch.setattr(heartbeat, "_send_telegram", lambda message: sent.append(message) or True)
    monkeypatch.setattr(heartbeat, "publish_receipt", lambda sender, **kwargs: recorded.append(kwargs) or None)
    assert heartbeat.main(now=now) == 0
    assert sent == []
    assert len(recorded) == 1
    assert recorded[0]["kind"] == "healthy_heartbeat"
    assert recorded[0]["source_report"] is report


@pytest.mark.parametrize("status", ["submitted", "broker_acknowledged", "partially_filled", "pending"])
@pytest.mark.parametrize("unified", [False, True])
def test_legacy_complete_unambiguous_status_stays_routine(monkeypatch, status, unified):
    from test_notification_quiet import NOW, mock_heartbeat, report
    from scripts import execution_report_heartbeat as heartbeat
    from quant_platform_kit.common.execution_receipts import build_execution_receipt
    payload = report()
    payload["summary"].update(execution_status=status, orders_submitted_count=1)
    payload["platform"] = "charles_schwab"
    outcome = status if status in {"submitted", "broker_acknowledged", "partially_filled"} else "submitted"
    payload["execution_receipt"] = build_execution_receipt(platform="schwab", strategy_profile=payload["strategy_profile"],
        strategy_revision="a" * 40, execution_mode="live", outcome=outcome, observed_at=NOW)
    sent = mock_heartbeat(monkeypatch, payload)
    staged = []
    monkeypatch.setattr(heartbeat, "unified_enabled", lambda: unified)
    monkeypatch.setattr(heartbeat, "publish_receipt", lambda sender, **kwargs: staged.append(kwargs) or None)
    assert heartbeat.main(NOW) == 0
    assert sent == []
    assert staged[0]["kind"] == "healthy_heartbeat"
