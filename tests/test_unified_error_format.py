"""Offline P10 tests with fake identities and transports."""
from types import SimpleNamespace
import pytest
from notifications import unified
from notifications.receipt_alerts import render_receipt_alert
from notifications.telegram import build_sender
from scripts.digest_coverage_warning import warn_coverage_unconfirmed
from test_unified_notifications import MemoryStore, event

@pytest.mark.parametrize("locale,title", [("zh", "【资产管理｜运行错误】"), ("en", "[Asset management | Runtime error]")])
def test_receipt_and_coverage_share_error_template(monkeypatch, locale, title):
    monkeypatch.delenv("DIGEST_COVERAGE_ALERT_ATTEMPT_PATH", raising=False)
    texts = [render_receipt_alert(locale=locale, missing=True)]
    assert warn_coverage_unconfirmed(lambda text: texts.append(text) or True, locale)
    assert all(text.startswith(title) for text in texts)
    assert all(("是否提交订单：未知" if locale == "zh" else "Order submission: unknown") in text for text in texts)

@pytest.mark.parametrize("flag", ["false", "true"])
def test_cycle_errors_use_same_body_without_private_dashboard(monkeypatch, flag):
    monkeypatch.setenv("NOTIFY_LANG", "zh")
    sent = []
    class HTTP:
        @staticmethod
        def post(url, *, json, timeout):
            sent.append(json["text"])
            return SimpleNamespace(status_code=200, json=lambda: {"ok": True})
    sender = build_sender("fake-token", "fake-recipient", requests_module=HTTP)
    unified.route(event(), env={unified.FLAG: flag}, store=MemoryStore(),
                  legacy=lambda: sender("private-account-fake private-service-fake healthy dashboard"))
    assert len(sent) == 1
    assert sent[0].startswith("【资产管理｜运行错误】")
    assert "private-" not in sent[0]
    assert "2026-10-17 03:45:00 SGT" in sent[0]
    assert "是否提交订单：未知" in sent[0]

def test_context_is_reset_after_failed_sender(monkeypatch):
    from notifications.unified import current_error_notification
    def fail():
        assert current_error_notification().startswith("【资产管理｜运行错误】")
        raise RuntimeError("fake transport failure")
    monkeypatch.setenv("NOTIFY_LANG", "zh")
    with pytest.raises(RuntimeError):
        unified.route(event(), env={}, legacy=fail)
    assert current_error_notification() is None

def test_unknown_time_and_unsafe_link_are_not_evidence():
    from notifications.receipt_alerts import render_runtime_error
    text = render_runtime_error(reason="private-account-fake", occurred_at="not-a-date",
        workflow_url="https://user:password@example.invalid/private?token=fake", locale="zh")
    assert "时间：未知" in text
    assert "运行链接不可用" in text
    assert "private-account" not in text and "password" not in text and "token" not in text

def test_runtime_guard_uses_same_template():
    from scripts import cloud_run_runtime_guard as guard
    text = guard.render_runtime_error(reason="runtime_guard", locale="en")
    assert text.startswith("[Asset management | Runtime error]")


def test_error_context_isolated_between_concurrent_workers():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from dataclasses import replace
    import datetime as dt
    from notifications.unified import current_error_notification, error_notification_context
    barrier = Barrier(2)
    def worker(day):
        local = replace(event(), occurred_at=dt.datetime(2026, 10, day, tzinfo=dt.timezone.utc))
        with error_notification_context(local):
            barrier.wait(timeout=5)
            body = current_error_notification()
            assert f"2026-10-{day:02d}" in body
        assert current_error_notification() is None
        return body
    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.map(worker, [16, 17])
    assert first != second
    assert current_error_notification() is None
