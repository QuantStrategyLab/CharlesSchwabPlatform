import copy
import datetime as dt

import pytest

from notifications.receipt_alerts import render_receipt_alert
from notifications.attention import render_attention
from quant_platform_kit.risk.attention import AttentionAxes, evaluate_attention
from scripts import execution_report_heartbeat as heartbeat


@pytest.mark.parametrize("locale", ["zh", "en"])
def test_receipt_sample_is_one_blocked_plus_five_time_skips(locale):
    stamp = dt.datetime(2026, 10, 16, 19, 46, 10, tzinfo=dt.timezone.utc)
    observations = [{"category": "blocked", "updated": stamp}] + [
        {"category": "time_skipped", "updated": stamp - dt.timedelta(hours=n)} for n in range(1, 6)
    ]
    text = render_receipt_alert(locale=locale, observations=observations, missing=True,
        due_at=stamp.replace(minute=45, second=0), workflow_url="https://example.invalid/run/1")
    assert ("1 份被策略风控阻断" if locale == "zh" else "1 report(s) blocked by strategy risk") in text
    assert ("5 份因时间不符跳过" if locale == "zh" else "5 report(s) skipped by schedule time") in text
    assert "2026-10-17 03:46:10 SGT" in text
    assert "2026-10-17 03:45:00 SGT" in text
    assert "https://example.invalid/run/1" in text
    assert "gs://" not in text


@pytest.mark.parametrize("locale", ["zh", "en"])
def test_blocked_receipt_acceptance_is_unchanged(locale, monkeypatch):
    monkeypatch.setenv("NOTIFY_LANG", locale)
    payload = {"status": "ok", "errors": [], "dry_run": False,
               "summary": {"execution_status": "blocked", "no_op_reason": "strategy_risk_rejected", "orders_submitted_count": 0}}
    original = copy.deepcopy(payload)
    assert heartbeat._is_accepted_report(payload) == (False, "rejected inner execution status")
    assert payload == original


@pytest.mark.parametrize("locale", ["zh", "en"])
@pytest.mark.parametrize("reason", [(), ("OBSERVATION_NOT_COMPLETE",), ("unrecognized_guard",)])
def test_attention_result_and_actual_reason_are_separate(locale, reason):
    decision = evaluate_attention(AttentionAxes(new_risk_prohibited=True))
    original = copy.deepcopy(decision)
    text = render_attention(decision=decision, platform="schwab", account_alias="private-account-synthetic",
        strategy_profile="unknown_private_strategy", guard_reason_codes=reason, locale=locale)
    lines = text.splitlines()
    assert len(lines) == 4
    assert ("已禁止新增风险" if locale == "zh" else "new risk additions prohibited") in lines[1]
    expected = ("账户快照不完整" if locale == "zh" else "account snapshot is incomplete") if reason == ("OBSERVATION_NOT_COMPLETE",) else (
        ("原因尚未识别" if locale == "zh" else "Reason unrecognized") if reason else (
        "原因未提供" if locale == "zh" else "Reason not provided"))
    assert expected in lines[2]
    assert "private-account-synthetic" not in text
    assert "unknown_private_strategy" not in text
    assert "new_risk_prohibited" not in text
    assert decision == original


@pytest.mark.parametrize("locale", ["zh", "en"])
def test_real_heartbeat_entrypoint_keeps_rejection_exit_and_raw_logs(monkeypatch, capsys, locale):
    from backport_helpers import configure
    configure(monkeypatch)
    now = dt.datetime(2026, 10, 16, 23, tzinfo=dt.timezone.utc)
    due = now.replace(hour=19, minute=45)
    stamps = [due + dt.timedelta(seconds=70), due - dt.timedelta(minutes=10),
              due - dt.timedelta(hours=6), due - dt.timedelta(hours=6, minutes=10),
              due - dt.timedelta(days=1), due - dt.timedelta(days=1, minutes=10)]
    target = {"service": "synthetic-private-service", "_heartbeat_latest_due_at": due}
    payload = {"status": "ok", "errors": [], "dry_run": False,
        "service_name": target["service"], "account_scope": "synthetic-private-account",
        "summary": {"execution_status": "blocked", "no_op_reason": "strategy_risk_rejected", "orders_submitted_count": 0}}
    monkeypatch.setenv("NOTIFY_LANG", locale)
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://example.invalid")
    monkeypatch.setenv("GITHUB_REPOSITORY", "example/platform")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setattr(heartbeat, "_runtime_target_enabled", lambda: True)
    monkeypatch.setattr(heartbeat, "runtime_target_configuration_present", lambda env: False)
    monkeypatch.setattr(heartbeat, "load_runtime_targets", lambda env: [target])
    monkeypatch.setattr(heartbeat, "_hydrate_runtime_target_schedules", lambda targets, **kw: targets)
    monkeypatch.setattr(heartbeat, "filter_due_targets", lambda *a, **kw: ([target], True))
    monkeypatch.setattr(heartbeat, "_load_required_services", lambda: [target["service"]])
    monkeypatch.setattr(heartbeat, "_payload_matches", lambda *a, **kw: (True, target["service"], "matched"))
    monkeypatch.setattr(heartbeat, "target_key", lambda target: target["service"])
    monkeypatch.setattr(heartbeat, "target_label", lambda target: target["service"])
    monkeypatch.setattr(heartbeat, "_report_globs", lambda *a: ["gs://synthetic-reports/*"])
    monkeypatch.setattr(heartbeat, "_list_gcs_objects", lambda *a, **kw: [
        {"url": f"gs://synthetic-reports/{i}.json", "metadata": {"updated": stamp.isoformat()}}
        for i, stamp in enumerate(stamps)])
    monkeypatch.setattr(heartbeat, "_cat_gcs_json", lambda *a, **kw: copy.deepcopy(payload))
    sent = []
    monkeypatch.setattr(heartbeat, "_send_telegram", lambda message: sent.append(message) or True)
    assert heartbeat.main(now) == 1
    assert len(sent) == 1
    assert ("1 份被策略风控阻断" if locale == "zh" else "1 report(s) blocked by strategy risk") in sent[0]
    assert ("5 份因时间不符跳过" if locale == "zh" else "5 report(s) skipped by schedule time") in sent[0]
    assert "2026-10-17 03:45:00 SGT" in sent[0]
    assert "https://example.invalid/example/platform/actions/runs/123" in sent[0]
    assert not any(raw in sent[0] for raw in ("gs://", target["service"], payload["account_scope"]))
    logs = capsys.readouterr().out
    assert "gs://synthetic-reports/0.json" in logs
    assert "rejected inner execution status" in logs


