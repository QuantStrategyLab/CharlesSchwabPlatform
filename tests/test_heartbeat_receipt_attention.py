import datetime as dt
import json
import urllib.parse

import pytest

from scripts import execution_report_heartbeat as heartbeat


@pytest.mark.parametrize("unified", [False, True])
@pytest.mark.parametrize("execution_status", ["pending_reconciliation", "unknown", "pending", "submitted", "broker_acknowledged", "partially_filled"])
@pytest.mark.parametrize("locale,expected", [
    ("zh", "订单状态尚待核对；请在券商核对订单及成交记录。"),
    ("en", "Order status needs review; check orders and fills with the broker."),
])
def test_pending_report_explains_uncertain_order_status(monkeypatch, unified, execution_status, locale, expected):
    now = dt.datetime(2026, 10, 30, 2, tzinfo=dt.timezone.utc)
    report = {
        "status": "ok", "execution_status": execution_status, "errors": [],
        "started_at": (now - dt.timedelta(minutes=2)).isoformat(),
        "finished_at": (now - dt.timedelta(minutes=1)).isoformat(),
        "summary": {"execution_status": execution_status, "orders_submitted_count": 1},
        "diagnostics": {},
    }
    quiet = execution_status in {"pending", "submitted", "broker_acknowledged", "partially_filled"}
    assert heartbeat._is_quiet_report(report, now=now) is quiet
    sent, published = [], []
    monkeypatch.setenv("NOTIFY_LANG", locale)
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "example/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
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
    def publish(sender, **kwargs):
        published.append(kwargs)
        return sender()
    monkeypatch.setattr(heartbeat, "publish_receipt", publish)

    assert heartbeat.main(now=now) == 0
    if quiet and not unified:
        assert sent == []
        return
    assert len(sent) == 1
    assert expected in sent[0]
    assert "https://github.com/example/repo/actions/runs/123" in sent[0]
    if unified:
        assert published[0]["kind"] == "order_status_unknown"
        assert published[0]["reason"] == "order_status_unknown"


@pytest.mark.parametrize("unified", [False, True])
def test_other_nonquiet_evidence_retains_generic_fallback(monkeypatch, unified):
    sent = []
    monkeypatch.setenv("NOTIFY_LANG", "en")
    monkeypatch.setattr(heartbeat, "unified_enabled", lambda: unified)
    monkeypatch.setattr(heartbeat, "publish_receipt", lambda sender, **kwargs: sender())
    monkeypatch.setattr(heartbeat, "_send_telegram", lambda message: sent.append(message) or True)
    assert heartbeat._send_normal_heartbeat("example", "private detail", quiet_eligible=False, attention_reason="unexpected") == 0
    assert "Runtime check evidence is unconfirmed" in sent[0]
    assert "private detail" not in sent[0]
    assert "Order status needs review" not in sent[0]


@pytest.mark.parametrize("unified", [False, True])
def test_healthy_heartbeat_remains_quiet(monkeypatch, unified):
    monkeypatch.delenv("RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS", raising=False)
    monkeypatch.setattr(heartbeat, "unified_enabled", lambda: unified)
    monkeypatch.setattr(heartbeat, "publish_receipt", lambda sender, **kwargs: None)
    monkeypatch.setattr(heartbeat, "_send_telegram", lambda _: pytest.fail("unexpected notification"))
    assert heartbeat._send_normal_heartbeat("example", "healthy") == 0


def test_nonquiet_legacy_attention_preserves_delivery_failure_result(monkeypatch):
    monkeypatch.setattr(heartbeat, "unified_enabled", lambda: False)
    monkeypatch.setattr(heartbeat, "_send_telegram", lambda _: False)
    assert heartbeat._send_normal_heartbeat(
        "example", "detail", quiet_eligible=False, attention_reason="order_status_unknown",
    ) == 1


def test_telegram_disables_link_preview(monkeypatch):
    requests = []
    class Response:
        status = 200
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
    monkeypatch.setenv("GLOBAL_TELEGRAM_CHAT_ID", "synthetic-chat")
    monkeypatch.setattr(heartbeat, "_telegram_token", lambda: "synthetic-token")
    monkeypatch.setattr(heartbeat.urllib.request, "urlopen", lambda request, timeout: requests.append(request) or Response())
    assert heartbeat._send_telegram("https://github.com/example/repo/actions/runs/123")
    fields = urllib.parse.parse_qs(requests[0].data.decode())
    assert json.loads(fields["link_preview_options"][0]) == {"is_disabled": True}
    assert fields["text"] == ["https://github.com/example/repo/actions/runs/123"]
