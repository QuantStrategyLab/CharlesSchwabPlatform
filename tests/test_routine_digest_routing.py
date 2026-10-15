from types import SimpleNamespace

import pytest

from application.account_new_risk_gate_support import maybe_publish_attention_for_admission
from application.rebalance_service import _should_publish_cycle_notification
from quant_platform_kit.risk.account_new_risk_gate import NewRiskAdmissionResult, NewRiskDisposition


def _publish(execution, *, orders=()):
    return _should_publish_cycle_notification(
        SimpleNamespace(execution=execution, submitted_orders=orders),
        config=SimpleNamespace(dry_run_only=False, notify_no_trade_cycles=True),
    )


def test_opt_in_moves_routine_no_order_to_digest_without_changing_execution(monkeypatch):
    evidence = {"execution_status": "no_action"}
    monkeypatch.delenv("QSL_ROUTINE_NOTIFICATION_MODE", raising=False)
    assert _publish(evidence)
    monkeypatch.setenv("QSL_ROUTINE_NOTIFICATION_MODE", "daily_digest")
    assert not _publish(evidence)
    assert evidence == {"execution_status": "no_action"}


@pytest.mark.parametrize("evidence", [
    {"execution_status": "blocked"}, {"execution_status": "unknown"},
    {"execution_status": "pending_reconciliation"},
    {"execution_status": "no_action", "notification_attention_reason_codes": ["account_new_risk_gate"]},
    {"execution_status": "completed", "report_persistence_error": "failed"},
])
def test_urgent_and_unknown_states_still_publish(monkeypatch, evidence):
    monkeypatch.setenv("QSL_ROUTINE_NOTIFICATION_MODE", "daily_digest")
    assert _publish(evidence)


def test_confirmed_routine_orders_go_to_digest_unless_opted_out(monkeypatch):
    monkeypatch.setenv("QSL_ROUTINE_NOTIFICATION_MODE", "daily_digest")
    assert not _publish({"execution_status": "completed"}, orders=({"status": "filled"},))
    monkeypatch.setenv("QSL_DIGEST_ROUTINE_ORDERS", "false")
    assert _publish({"execution_status": "completed"}, orders=({"status": "filled"},))


def test_admission_reason_is_forwarded_to_attention_formatter(monkeypatch):
    import quant_platform_kit.risk.attention_notify as notification

    observed = []
    monkeypatch.setattr(notification, "publish_attention_telegram_transition", lambda **kw: observed.append(kw) or {"sent": 1})
    admission = NewRiskAdmissionResult(
        disposition=NewRiskDisposition.NEW_RISK_PROHIBITED,
        reason_codes=("DAILY_LOSS_LIMIT_EXCEEDED",),
    )
    maybe_publish_attention_for_admission(admission, portfolio={}, log_message=lambda *a, **kw: None)
    assert observed[0]["guard_reason_codes"] == admission.reason_codes
    assert admission.disposition is NewRiskDisposition.NEW_RISK_PROHIBITED
