from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import send_notification_canary as canary

WORKFLOW = (ROOT / ".github/workflows/notification-canary.yml").read_text(
    encoding="utf-8"
)


class FakeRequests:
    def __init__(self, *, status_code=200, payload=None, raise_exc: Exception | None = None):
        self.calls = []
        self.status_code = status_code
        self.payload = {"ok": True} if payload is None else payload
        self.raise_exc = raise_exc

    def post(self, url, json, timeout):
        if self.raise_exc is not None:
            raise self.raise_exc
        self.calls.append((url, json, timeout))
        return FakeResponse(self.status_code, self.payload)


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def test_canary_id_is_required_and_nonempty():
    with pytest.raises(ValueError, match="required"):
        canary.normalize_canary_id("")
    with pytest.raises(ValueError, match="required"):
        canary.normalize_canary_id("   ")
    assert canary.main(["--canary-id", ""]) == 2
    assert canary.main([]) == 2


def test_canary_message_is_fixed_safe_content(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TOKEN", "secret-token-value")
    monkeypatch.setenv("GLOBAL_TELEGRAM_CHAT_ID", "secret-chat-id")
    monkeypatch.setenv("TELEGRAM_TOKEN_SECRET_NAME", "secret-name-should-not-leak")
    monkeypatch.setenv("GCP_PROJECT_ID", "project-should-not-leak")

    message = canary.build_canary_message("manual-20260920a")
    assert "通知投递测试" in message or "notification delivery test" in message
    assert "No order was submitted" in message
    assert "未提交任何订单" in message
    assert "No strategy, position, or configuration was changed" in message
    assert "未更改策略、仓位或配置" in message
    assert "canary_id=manual-20260920a" in message
    assert "TEST" in message

    for forbidden in (
        "secret-token-value",
        "secret-chat-id",
        "secret-name-should-not-leak",
        "project-should-not-leak",
        "api.telegram.org",
        "https://",
        "charlesschwabquant",
        "account",
        "order_id",
        "Traceback",
        "SCHWAB_API_KEY",
        "SCHWAB_APP_SECRET",
    ):
        assert forbidden not in message

    with pytest.raises(ValueError):
        canary.normalize_canary_id("bad id with spaces")
    with pytest.raises(ValueError):
        canary.normalize_canary_id("token=abc/def")


def test_delivery_success_and_failure_without_network(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TOKEN", "token-1")
    monkeypatch.setenv("GLOBAL_TELEGRAM_CHAT_ID", "chat-1")

    ok_requests = FakeRequests()
    assert canary.send_canary(canary_id="ok-1", requests_module=ok_requests) is True
    assert len(ok_requests.calls) == 1
    _url, payload, timeout = ok_requests.calls[0]
    assert payload["chat_id"] == "chat-1"
    assert "canary_id=ok-1" in payload["text"]
    assert "token-1" not in payload["text"]
    assert "notification delivery test" in payload["text"]
    assert timeout == 15

    fail_requests = FakeRequests(payload={"ok": False, "description": "chat not found"})
    assert canary.send_canary(canary_id="fail-1", requests_module=fail_requests) is False

    error_requests = FakeRequests(raise_exc=RuntimeError("boom-should-not-surface"))
    assert canary.send_canary(canary_id="fail-2", requests_module=error_requests) is False

    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TG_TOKEN", raising=False)
    monkeypatch.delenv("GLOBAL_TELEGRAM_CHAT_ID", raising=False)
    assert canary.send_canary(canary_id="missing-cfg", requests_module=FakeRequests()) is False


def test_secret_manager_token_load_mocks_subprocess(monkeypatch):
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TG_TOKEN", raising=False)
    monkeypatch.setenv("TELEGRAM_TOKEN_SECRET_NAME", "platform-telegram-token")
    monkeypatch.setenv("GCP_PROJECT_ID", "charlesschwabquant")
    monkeypatch.setenv("GLOBAL_TELEGRAM_CHAT_ID", "chat-2")

    class Result:
        returncode = 0
        stdout = "secret-token-from-sm\n"
        stderr = ""

    with patch("scripts.send_notification_canary.subprocess.run", return_value=Result()) as mocked:
        ok_requests = FakeRequests()
        assert canary.send_canary(canary_id="sm-1", requests_module=ok_requests) is True
        mocked.assert_called_once()
        command = mocked.call_args.args[0]
        assert command[:6] == [
            "gcloud",
            "secrets",
            "versions",
            "access",
            "latest",
            "--secret",
        ]
        assert "platform-telegram-token" in command
        assert "--project" in command
        assert "charlesschwabquant" in command
        assert len(ok_requests.calls) == 1
        assert "secret-token-from-sm" not in ok_requests.calls[0][1]["text"]

    class FailResult:
        returncode = 1
        stdout = "LEAK-STDOUT"
        stderr = "LEAK-STDERR"

    with patch(
        "scripts.send_notification_canary.subprocess.run", return_value=FailResult()
    ):
        assert canary.send_canary(canary_id="sm-fail", requests_module=FakeRequests()) is False


def test_workflow_static_safety_constraints():
    assert "workflow_dispatch:" in WORKFLOW
    assert "canary_id:" in WORKFLOW
    assert "required: true" in WORKFLOW
    assert "schedule:" not in WORKFLOW
    assert "workflow_run:" not in WORKFLOW
    assert "scripts/send_notification_canary.py" in WORKFLOW
    assert "secrets.GLOBAL_TELEGRAM_CHAT_ID" in WORKFLOW
    assert "secrets.TELEGRAM_TOKEN" in WORKFLOW
    assert "vars.TELEGRAM_TOKEN_SECRET_NAME" in WORKFLOW
    assert "uv sync --frozen --no-dev" in WORKFLOW
    assert "astral-sh/setup-uv@d0cc045d04ccac9d8b7881df0226f9e82c39688e" in WORKFLOW
    assert "google-github-actions/auth@v3" in WORKFLOW
    assert "google-github-actions/setup-gcloud@v3" in WORKFLOW
    assert "actions/checkout@v6" in WORKFLOW

    for forbidden in (
        "gcloud run deploy",
        "gcloud run services",
        "gcloud run jobs",
        "gcloud scheduler",
        "invoke-cloud-run",
        "Cloud Run",
        "retry",
        "max-attempts",
        "continue-on-error: true",
        "SCHWAB_API_KEY",
        "SCHWAB_APP_SECRET",
        "secrets.SCHWAB",
        "strategy_profile",
        "broker",
    ):
        assert forbidden not in WORKFLOW

    assert WORKFLOW.count("google-github-actions/auth@v3") == 1
    assert WORKFLOW.count("Send one notification canary") == 1
    assert "TELEGRAM_TOKEN == '' && env.TELEGRAM_TOKEN_SECRET_NAME != ''" in WORKFLOW
