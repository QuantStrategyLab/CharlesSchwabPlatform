from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import send_paper_notification_preview as preview

WORKFLOW = (ROOT / ".github/workflows/paper-notification-preview.yml").read_text(
    encoding="utf-8"
)
SCRIPT_PATH = ROOT / "scripts" / "send_paper_notification_preview.py"

_FORBIDDEN_IMPORT_ROOTS = (
    "schwab",
    "main",
    "application",
    "strategy_runtime",
    "decision_mapper",
    "entrypoints",
    "runtime_config_support",
)

_FORBIDDEN_TEXT = (
    "api.telegram.org",
    "https://",
    "Traceback",
    "SCHWAB_APP",
    "secret-token",
)


def test_build_preview_message_renders_single_unified_daily_digest():
    message = preview.build_preview_message(locale="zh", business_day="2026-11-02")

    assert isinstance(message, str)
    assert message.count("[PAPER]") == 1
    assert message.startswith("[PAPER]")

    # Unified daily-digest fields (QuantSentinel daily digest contract, zh).
    assert "📡 量化哨兵 · 心跳" in message
    assert "业务日: 2026-11-02" in message
    assert "今日有运行且已验证零成交。" in message
    assert "【当日已运行】" in message
    assert "· schwab / 美股核心收益组合 · 周期 1 · 无交易 · 正常" in message

    # Preview markers stay explicit and safe.
    assert "PREVIEW" in message
    assert "synthetic" in message.lower()
    assert "不会下单" in message
    for forbidden in _FORBIDDEN_TEXT:
        assert forbidden not in message


def test_build_preview_message_supports_english_locale():
    message = preview.build_preview_message(locale="en", business_day="2026-11-02")

    assert "📡 QuantSentinel · Heartbeat" in message
    assert "Business day: 2026-11-02" in message
    assert "Runs completed with verified zero fills today." in message
    assert "[Ran today]" in message
    assert "· schwab / US Core Income Combo · cycles 1 · No trades · ok" in message
    assert "No order will be placed" in message
    assert "今日有运行且已验证零成交。" not in message


def test_send_preview_calls_sender_exactly_once_without_broker_imports(monkeypatch):
    monkeypatch.setenv("TELEGRAM_TOKEN", "token-preview")
    monkeypatch.setenv("GLOBAL_TELEGRAM_CHAT_ID", "chat-preview")
    monkeypatch.setenv("NOTIFY_LANG", "zh")
    monkeypatch.setenv("RUNTIME_TARGET_ENABLED", "false")

    before_modules = {
        name
        for name in sys.modules
        if any(name == root or name.startswith(f"{root}.") for root in _FORBIDDEN_IMPORT_ROOTS)
    }

    sent = []

    def fake_send(text):
        sent.append(text)
        return True

    delivered = preview.send_preview(send_fn=fake_send)

    assert delivered is True
    assert len(sent) == 1
    assert sent[0].startswith("[PAPER]")
    assert "PREVIEW" in sent[0]
    assert "token-preview" not in sent[0]
    assert "chat-preview" not in sent[0]

    after_modules = {
        name
        for name in sys.modules
        if any(name == root or name.startswith(f"{root}.") for root in _FORBIDDEN_IMPORT_ROOTS)
    }
    assert after_modules == before_modules


def test_preview_script_has_no_broker_or_execution_imports():
    tree = ast.parse(SCRIPT_PATH.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported.add(alias.name.split(".")[0])
                imported.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
            imported.add(node.module)

    for forbidden in _FORBIDDEN_IMPORT_ROOTS:
        assert forbidden not in imported
        assert not any(
            name == forbidden or name.startswith(f"{forbidden}.") for name in imported
        )

    assert "notifications.telegram" in imported


def test_send_preview_fails_closed_without_telegram_target(monkeypatch):
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TG_TOKEN", raising=False)
    monkeypatch.delenv("GLOBAL_TELEGRAM_CHAT_ID", raising=False)
    sent = []
    assert preview.send_preview(send_fn=lambda text: sent.append(text) or True) is False
    assert sent == []


def test_main_renders_without_sending_by_default(monkeypatch, capsys):
    monkeypatch.setenv("RUNTIME_TARGET_ENABLED", "false")
    monkeypatch.setenv("NOTIFY_LANG", "zh")
    monkeypatch.delenv("PREVIEW_SEND", raising=False)
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("GLOBAL_TELEGRAM_CHAT_ID", raising=False)

    def explode_send(**_kwargs):
        raise AssertionError("send_preview must not be called when send is false")

    monkeypatch.setattr(preview, "send_preview", explode_send)

    assert preview.main([]) == 0
    out = capsys.readouterr().out
    assert "[PAPER]" in out
    assert "业务日" in out
    assert "no Telegram delivery" in out


def test_main_sends_single_message_when_requested(monkeypatch):
    monkeypatch.setenv("RUNTIME_TARGET_ENABLED", "false")
    monkeypatch.setenv("TELEGRAM_TOKEN", "token-preview")
    monkeypatch.setenv("GLOBAL_TELEGRAM_CHAT_ID", "chat-preview")

    calls = []

    def fake_send_preview(**kwargs):
        calls.append(kwargs)
        return True

    monkeypatch.setattr(preview, "send_preview", fake_send_preview)
    assert preview.main(["--send"]) == 0
    assert len(calls) == 1


def test_main_refuses_enabled_runtime_target(monkeypatch):
    monkeypatch.setenv("RUNTIME_TARGET_ENABLED", "true")
    monkeypatch.setenv("TELEGRAM_TOKEN", "token-preview")
    monkeypatch.setenv("GLOBAL_TELEGRAM_CHAT_ID", "chat-preview")
    assert preview.main([]) == 1


def test_main_returns_nonzero_when_delivery_fails(monkeypatch):
    monkeypatch.setenv("RUNTIME_TARGET_ENABLED", "false")
    monkeypatch.setenv("TELEGRAM_TOKEN", "token-preview")
    monkeypatch.setenv("GLOBAL_TELEGRAM_CHAT_ID", "chat-preview")
    monkeypatch.setattr(preview, "send_preview", lambda **_kwargs: False)
    assert preview.main(["--send"]) == 1


def test_workflow_static_safety_constraints():
    assert "name: PAPER Notification Preview" in WORKFLOW
    assert "workflow_dispatch:" in WORKFLOW
    # Dispatch-only trigger with a send switch that defaults to print-only.
    assert "push:" not in WORKFLOW
    assert "pull_request:" not in WORKFLOW
    assert "schedule:" not in WORKFLOW
    assert "workflow_run:" not in WORKFLOW
    assert "type: boolean" in WORKFLOW
    assert "default: false" in WORKFLOW
    assert "PREVIEW_SEND: ${{ inputs.send }}" in WORKFLOW
    assert "scripts/send_paper_notification_preview.py" in WORKFLOW
    assert 'RUNTIME_TARGET_ENABLED: "false"' in WORKFLOW
    assert "secrets.TELEGRAM_TOKEN" in WORKFLOW
    assert "secrets.GLOBAL_TELEGRAM_CHAT_ID" in WORKFLOW
    assert "vars.GLOBAL_TELEGRAM_CHAT_ID" not in WORKFLOW
    assert "uv sync --frozen --no-dev" in WORKFLOW
    assert "astral-sh/setup-uv@d0cc045d04ccac9d8b7881df0226f9e82c39688e" in WORKFLOW

    for forbidden in (
        "gcloud run deploy",
        "gcloud run services",
        "gcloud run jobs",
        "gcloud scheduler",
        "Cloud Run",
        "continue-on-error: true",
        "strategy_profile",
        "main.py",
        "application/",
        "schwab-py",
        "SCHWAB_APP_KEY",
        "SCHWAB_APP_SECRET",
    ):
        assert forbidden not in WORKFLOW
