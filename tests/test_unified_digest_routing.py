"""Offline routing tests: no broker, storage, or Telegram calls."""

import copy

import pytest

from notifications.digest_snapshot.digest_routing import defer_routine_cycle_to_digest


def test_default_off_and_explicit_opt_in(monkeypatch):
    monkeypatch.delenv("QSL_ROUTINE_NOTIFICATION_MODE", raising=False)
    evidence = {"execution_status": "no_action", "orders_pending_count": 0}
    original = copy.deepcopy(evidence)
    assert not defer_routine_cycle_to_digest(evidence, submitted_order_count=0)
    monkeypatch.setenv("QSL_ROUTINE_NOTIFICATION_MODE", "daily_digest")
    assert defer_routine_cycle_to_digest(evidence, submitted_order_count=0)
    assert evidence == original


@pytest.mark.parametrize("status", ["blocked", "unknown", "pending", "pending_reconciliation", "rejected", "", None])
def test_urgent_or_unknown_results_are_never_deferred(status):
    assert not defer_routine_cycle_to_digest(
        {"execution_status": status}, submitted_order_count=0, mode="daily_digest",
    )


@pytest.mark.parametrize("field,value", [
    ("notification_attention_reason_codes", ["DAILY_LOSS_LIMIT_EXCEEDED"]),
    ("errors", ["storage_failed"]), ("orders_pending_count", 1),
    ("reconciliation_required", True), ("broker_submission_done", True),
    ("no_op_reason", "RECONCILE_ONLY"), ("notification_error", "unknown_result"),
])
def test_healthy_label_cannot_hide_guard_or_operational_evidence(field, value):
    assert not defer_routine_cycle_to_digest(
        {"execution_status": "no_action", field: value},
        submitted_order_count=0, mode="daily_digest",
    )


@pytest.mark.parametrize("count", [1, None, False, -1])
def test_any_order_or_unknown_order_count_remains_immediate(count):
    assert not defer_routine_cycle_to_digest(
        {"execution_status": "completed"}, submitted_order_count=count, mode="daily_digest",
    )


def test_invalid_mode_preserves_immediate_delivery():
    assert not defer_routine_cycle_to_digest(
        {"execution_status": "no_action"}, submitted_order_count=0, mode="typo",
    )


def test_below_threshold_is_routine_but_an_unclassified_reason_stays_immediate():
    evidence = {"execution_status": "no_op", "no_op_reason": "rebalance_diff_below_threshold"}
    assert defer_routine_cycle_to_digest(evidence, submitted_order_count=0, mode="daily_digest")
    evidence["no_op_reason"] = "unknown_order_result"
    assert not defer_routine_cycle_to_digest(evidence, submitted_order_count=0, mode="daily_digest")


def test_confirmed_routine_orders_are_configurable_and_do_not_mutate(monkeypatch):
    evidence = {"execution_status": "completed", "broker_submission_done": True}
    orders = [{"status": "filled"}]
    original = copy.deepcopy((evidence, orders))
    assert defer_routine_cycle_to_digest(evidence, submitted_order_count=1,
        submitted_orders=orders, mode="daily_digest")
    monkeypatch.setenv("QSL_DIGEST_ROUTINE_ORDERS", "false")
    assert not defer_routine_cycle_to_digest(evidence, submitted_order_count=1,
        submitted_orders=orders, mode="daily_digest")
    assert (evidence, orders) == original


@pytest.mark.parametrize("status", ["pending", "submitted", "rejected", "unknown", "partially_filled", ""])
def test_unresolved_orders_never_become_routine(status):
    assert not defer_routine_cycle_to_digest({"execution_status": "completed"},
        submitted_order_count=1, submitted_orders=[{"status": status}], mode="daily_digest")
