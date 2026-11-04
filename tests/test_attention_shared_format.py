"""Shared-renderer rollout and actual run context, using synthetic metadata."""
import pytest

from notifications import attention, unified
from quant_platform_kit.risk.attention import AttentionAxes, evaluate_attention


def test_runtime_context_reaches_rendered_notification_without_mutation(monkeypatch):
    from quant_platform_kit.risk import attention as shared
    if not hasattr(shared, "render_attention_notification"):
        pytest.skip("Coordinated QPK format revision has not been pinned yet")
    monkeypatch.delenv(unified.FLAG, raising=False)
    source = {"started_at": "2026-11-02T14:05:00Z", "run_id": "fake-run",
              "console_run_url": "https://console.example.invalid/runs/fake-run"}
    original = source.copy()
    sent = []
    with unified.notification_context(source):
        result = attention.publish_attention_telegram_transition(
            decision=evaluate_attention(AttentionAxes(new_risk_prohibited=True)),
            platform="schwab", account_alias="fake-account-42", strategy_profile="soxl_soxx_trend_income",
            guard_reason_codes=("DAILY_LOSS_UNKNOWN_FAIL_CLOSED",), locale="zh",
            telegram_sender=lambda **kwargs: sent.append(kwargs) or True,
        )
    assert result == {"sent": 1, "skipped": 0, "failed": 0}
    assert sent[0]["text"].startswith("【资产管理｜运行错误】")
    assert "••42" not in sent[0]["text"]
    assert "当日亏损或上限无法核实" in sent[0]["text"]
    assert "2026-11-02 22:05:00 SGT" in sent[0]["text"]
    assert source["console_run_url"] in sent[0]["text"]
    assert source == original
    assert unified.attention_run_context() == {"occurred_at": None, "run_url": None}


def test_old_pin_keeps_delivery_available(monkeypatch):
    from quant_platform_kit.risk import attention as shared
    monkeypatch.delattr(shared, "render_attention_notification", raising=False)
    decision = evaluate_attention(AttentionAxes(new_risk_prohibited=True))
    text = attention.render_attention(
        decision=decision, platform="schwab", account_alias="fake42",
        strategy_profile="soxl_soxx_trend_income", guard_reason_codes=("OBSERVATION_NOT_COMPLETE",), locale="en",
    )
    assert text.startswith("[Asset management | Runtime error]")
    assert "account snapshot is incomplete" in text
    assert "fake42" not in text
