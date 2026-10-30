"""Conservative notification-only evidence for acknowledged Schwab orders."""

from collections.abc import Mapping


def confirmed_acceptances(execution, orders):
    """Recognize acknowledgements without interpreting them as fills."""
    if not isinstance(execution, Mapping) or not orders:
        return False
    if execution.get("execution_status") != "pending_reconciliation":
        return False
    if execution.get("broker_submission_done") is not True:
        return False
    if type(execution.get("orders_pending_count")) is not int or execution["orders_pending_count"] != len(orders):
        return False
    if any(execution.get(key) for key in (
        "notification_attention_reason_codes", "notification_order_issue", "error", "errors",
        "fail_reason", "execution_blocked_reason", "plugin_error", "persistence_error",
        "report_persistence_error", "notification_error", "reconciliation_required",
        "market_data_retry_required", "risk_flags", "execution_blocked", "auth_expired",
        "outcome_unknown", "submission_halted",
    )):
        return False
    ids = []
    for order in orders:
        if not isinstance(order, Mapping) or str(order.get("status") or "").lower() not in {
            "accepted", "submitted", "broker_acknowledged", "pending", "partially_filled",
        }:
            return False
        broker_id = order.get("broker_order_id")
        if not isinstance(broker_id, str) or not broker_id.strip():
            return False
        ids.append(broker_id)
    return len(ids) == len(set(ids))


def cycle_notification_evidence(execution, orders, *, dry_run=False):
    """Project a completed zero-order cycle without altering its execution."""
    from .digest_snapshot.digest_candidates import cycle_evidence
    from .digest_snapshot.digest_routing import defer_routine_cycle_to_digest
    copy = dict(execution)
    if (not orders and not copy.get("execution_status")
            and copy.get("notification_cycle_completed") is True
            and not copy.get("notification_order_issue")):
        copy["execution_status"] = "no_action"
    facts = cycle_evidence(copy, orders, dry_run=dry_run,
                           known_pending=confirmed_acceptances(execution, orders))
    metadata = execution.get("signal_metadata") or {}
    if not isinstance(metadata, Mapping) or not defer_routine_cycle_to_digest(
        {**metadata, "execution_status": metadata.get("execution_status") or "no_action"},
        submitted_order_count=0, mode="daily_digest", include_orders=True,
    ):
        facts["routine"] = False
    return facts
