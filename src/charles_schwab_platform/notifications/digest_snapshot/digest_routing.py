"""Opt-in routing for routine cycles whose persisted reports feed a daily digest.

This decides notification delivery only. It never changes an execution result,
calls a broker, grants order permissions, or treats deferral as delivery.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence


def daily_digest_enabled(mode: str | None = None) -> bool:
    return (mode if mode is not None else os.getenv("QSL_ROUTINE_NOTIFICATION_MODE", "immediate")) == "daily_digest"


def defer_routine_cycle_to_digest(
    execution: Mapping[str, object],
    *,
    submitted_order_count: int,
    mode: str | None = None,
    submitted_orders: Sequence[Mapping[str, object]] | None = None,
    include_orders: bool | None = None,
    known_pending: bool = False,
) -> bool:
    """Defer healthy cycles and confirmed routine orders; urgent states stay live.

    Default ``QSL_ROUTINE_NOTIFICATION_MODE=immediate`` preserves existing
    callers. An owner can opt a verified producer/recipient into ``daily_digest``
    after validating the existing private report-to-digest replacement path.
    """
    if not daily_digest_enabled(mode) or type(submitted_order_count) is not int or submitted_order_count < 0:
        return False
    if not isinstance(execution, Mapping):
        return False
    pending_proven = known_pending is True and submitted_order_count > 0
    status = str(execution.get("execution_status") or "").lower()
    if status not in {"no_action", "no_op", "completed", "executed"} and not (pending_proven and status == "pending_reconciliation"):
        return False
    if execution.get("no_op_reason") not in (
        None, "", "no_action", "no_signal", "no_rebalance", "below_threshold",
        "rebalance_diff_below_threshold", "threshold_not_met",
    ) and not (pending_proven and execution.get("no_op_reason") == "broker_order_pending_confirmation"):
        return False
    if any(execution.get(key) for key in (
        "error", "errors", "fail_reason", "execution_blocked_reason",
        "plugin_error", "persistence_error", "report_persistence_error", "notification_error",
        "reconciliation_required", "market_data_retry_required",
        "notification_attention_reason_codes",
        "account_new_risk_buy_blocked", "small_account_buy_blocked", "risk_flags",
        "execution_blocked", "execution_blocking_skips", "strategy_run_persistence_error",
        "strategy_plugin_error", "strategy_plugin_alert_error", "auth_expired",
        "strategy_plugin_alert_failed_count", "strategy_plugin_alert_telegram_failed_count",
        "data_error", "blocked_reason", "failed_order_count", "outcome_unknown", "submission_halted",
    )):
        # Independent failure, risk or unclassified evidence remains urgent,
        # including when a producer has established a pending acknowledgement.
        return False
    if not pending_proven and (execution.get("pending_reconciliation") or execution.get("orders_pending_count")):
        return False
    if str(execution.get("risk_gate") or "").upper() in {"BLOCK", "BLOCKED", "REJECT", "REJECTED", "DENY", "DENIED"}:
        return False
    if submitted_order_count:
        orders_enabled = include_orders if include_orders is not None else os.getenv("QSL_DIGEST_ROUTINE_ORDERS", "true") == "true"
        if not orders_enabled or submitted_orders is None or len(submitted_orders) != submitted_order_count:
            return False
        # Only producer-verified acknowledgements may use the pending statuses;
        # rejected and unknown outcomes remain immediate. No fill is inferred.
        routine_statuses = {"filled", "completed", "executed"}
        if pending_proven:
            routine_statuses |= {"accepted", "submitted", "pending", "pendingsubmit", "apipending", "apipendingsubmit", "presubmitted", "partiallyfilled", "partial", "partially_filled", "broker_acknowledged"}
        if any(not isinstance(order, Mapping) or str(order.get("status") or "").lower() not in routine_statuses
               for order in submitted_orders):
            return False
    elif execution.get("broker_submission_done"):
        return False
    return True


def immediate_plugin_signals(signals, *, mode: str | None = None):
    """Filter notification inputs only; callers retain original strategy signals."""
    if not daily_digest_enabled(mode):
        return signals
    return tuple(signal for signal in signals if not (
        str(getattr(signal, "canonical_route", "") or "").lower() in {"watch", "opportunity_watch", "panic_reversal", "taco_rebound"}
        and str(getattr(signal, "suggested_action", "") or "").lower() in {"notify_manual_review", "watch", "observe"}
        and not any(getattr(signal, key, None) for key in ("error", "errors", "risk_halt", "auth_expired"))
    ))


def routine_notification(sender, runtime, report, text, *, routine_order=True):
    """Route only explicitly classified routine call sites, never message text."""
    orders_enabled = os.getenv("QSL_DIGEST_ROUTINE_ORDERS", "true") == "true"
    if daily_digest_enabled() and (not routine_order or orders_enabled):
        report.setdefault("notifications", []).append({
            "delivery_status": "deferred_digest", "transport_acknowledged": False,
            "sink": "telegram", "routine_order": routine_order,
        })
        return False
    return sender(runtime, report, text)
