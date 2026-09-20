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


def _classify_preview_text(text: str) -> str | None:
    lower = text.lower()
    if "未知状态" in text or "unknown status" in lower:
        return "unknown_status"
    if "订单待确认" in text or "pending confirmation" in lower or "fill not confirmed" in lower:
        return "pending_confirmation"
    if "成交确认" in text or "preview filled" in lower:
        return "filled"
    if "拒单异常" in text or "preview reject" in lower:
        return "rejected_or_exception"
    if "心跳检测" in text or "💓" in text:
        return "heartbeat_no_rebalance"
    if "模拟下单" in text or "dry_run " in lower or "🧪 dry_run" in lower:
        return "rebalance_dry_run"
    return None

def test_build_preview_messages_covers_required_categories_with_safe_markers():
    messages = preview.build_preview_messages(locale="zh")
    assert 1 <= len(messages) <= 6
    assert len(messages) == 6

    categories = {_classify_preview_text(message) for message in messages}
    assert categories == {
        "heartbeat_no_rebalance",
        "rebalance_dry_run",
        "pending_confirmation",
        "filled",
        "rejected_or_exception",
        "unknown_status",
    }

    for message in messages:
        assert message.startswith("[PAPER]")
        assert "PREVIEW" in message
        assert "synthetic" in message.lower() or "合成" in message
        assert "不会下单" in message or "No order will be" in message
        for forbidden in (
            "api.telegram.org",
            "https://",
            "Traceback",
            "SCHWAB_APP",
            "secret-token",
        ):
            assert forbidden not in message


def test_send_preview_calls_sender_once_per_message_without_broker_imports(monkeypatch):
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
    assert len(sent) == 6
    assert len(sent) <= 6

    for text in sent:
        assert text.startswith("[PAPER]")
        assert "PREVIEW" in text
        assert "token-preview" not in text
        assert "chat-preview" not in text

    categories = {_classify_preview_text(text) for text in sent}
    assert categories == {
        "heartbeat_no_rebalance",
        "rebalance_dry_run",
        "pending_confirmation",
        "filled",
        "rejected_or_exception",
        "unknown_status",
    }

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
    assert "notifications.renderers" in imported


def test_send_preview_fails_closed_without_telegram_target(monkeypatch):
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TG_TOKEN", raising=False)
    monkeypatch.delenv("GLOBAL_TELEGRAM_CHAT_ID", raising=False)
    sent = []
    assert preview.send_preview(send_fn=lambda text: sent.append(text) or True) is False
    assert sent == []


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
    assert preview.main([]) == 1


def test_workflow_static_safety_constraints():
    assert "name: PAPER Notification Preview" in WORKFLOW
    assert "workflow_dispatch:" in WORKFLOW
    assert "schedule:" not in WORKFLOW
    assert "workflow_run:" not in WORKFLOW
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
