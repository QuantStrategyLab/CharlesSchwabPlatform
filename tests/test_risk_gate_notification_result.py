import copy

import pytest

from notifications.renderers import render_heartbeat_notification
from notifications.telegram import build_translator


def _render(execution, *, locale):
    return render_heartbeat_notification(
        translator=build_translator(locale),
        strategy_display_name="Synthetic strategy",
        dry_run_only=False,
        extra_notification_lines=(),
        execution={"dashboard_text": "", "separator": "---", "signal_display": "", **execution},
        portfolio={"total_equity": 0.0, "portfolio_rows": (), "market_values": {}},
    )


@pytest.mark.parametrize("locale", ["zh", "en"])
@pytest.mark.parametrize("status", [None, "no_op", "completed", "unknown"])
def test_new_risk_gate_attention_does_not_claim_no_rebalance(locale, status):
    execution = {"notification_attention_reason_codes": ("account_new_risk_gate",)}
    if status is not None:
        execution["execution_status"] = status
    original = copy.deepcopy(execution)
    rendered = _render(execution, locale=locale)
    expected = "⚠️ 已禁止新增风险" if locale == "zh" else "⚠️ New risk is prohibited"
    assert rendered.compact_text.splitlines()[-1] == expected
    assert rendered.detailed_text.splitlines()[-1] == expected
    assert build_translator(locale)("no_trades") not in rendered.compact_text
    assert execution == original


@pytest.mark.parametrize("locale", ["zh", "en"])
@pytest.mark.parametrize("status", ["unknown", "pending_reconciliation", "deferred_market_data"])
def test_unverified_result_without_risk_evidence_is_not_a_risk_rejection(locale, status):
    rendered = _render({"execution_status": status}, locale=locale)
    expected = "⚠️ 执行结果尚未核实" if locale == "zh" else "⚠️ Execution result is not yet verified"
    assert rendered.compact_text.splitlines()[-1] == expected
    assert rendered.detailed_text.splitlines()[-1] == expected
    assert build_translator(locale)("risk_rejected") not in rendered.compact_text


@pytest.mark.parametrize("locale", ["zh", "en"])
@pytest.mark.parametrize("status", ["no_op", "no_action", "completed", "executed"])
def test_verified_no_action_keeps_existing_completion(locale, status):
    rendered = _render(
        {"execution_status": status, "notification_attention_reason_codes": ("strategy_plugin_error",)},
        locale=locale,
    )
    assert rendered.compact_text.splitlines()[-1] == build_translator(locale)("no_trades")


@pytest.mark.parametrize("locale", ["zh", "en"])
@pytest.mark.parametrize("reason", ["source_unavailable", "rejected:too_many_positions"])
def test_existing_blocked_completion_is_preserved(locale, reason):
    rendered = _render({"execution_status": "blocked", "no_op_reason": reason}, locale=locale)
    key = "risk_rejected_too_many_positions" if reason == "rejected:too_many_positions" else "risk_rejected"
    assert rendered.compact_text.splitlines()[-1] == build_translator(locale)(key)


def test_free_text_or_similar_reason_does_not_invent_risk_rejection():
    rendered = _render(
        {
            "execution_status": "no_action",
            "notification_attention_reason_codes": ("account_new_risk_gate_unverified",),
            "status_display": "account_new_risk_gate",
        },
        locale="zh",
    )
    assert rendered.compact_text.splitlines()[-1] == "✅ 无需调仓"
