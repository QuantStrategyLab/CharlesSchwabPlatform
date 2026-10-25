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
    if str(execution.get("execution_status") or "").lower() not in {"no_action", "no_op", "completed", "executed"}:
        return False
    if execution.get("no_op_reason") not in (
        None, "", "no_action", "no_signal", "no_rebalance", "below_threshold",
        "rebalance_diff_below_threshold", "threshold_not_met",
    ):
        return False
    if any(execution.get(key) for key in (
        "error", "errors", "fail_reason", "execution_blocked_reason",
        "plugin_error", "persistence_error", "report_persistence_error", "notification_error",
        "pending_reconciliation", "reconciliation_required", "market_data_retry_required",
        "notification_attention_reason_codes", "orders_pending_count",
        "account_new_risk_buy_blocked", "small_account_buy_blocked", "risk_flags",
        "execution_blocked", "execution_blocking_skips", "strategy_run_persistence_error",
        "strategy_plugin_error", "strategy_plugin_alert_error", "auth_expired",
        "strategy_plugin_alert_failed_count", "strategy_plugin_alert_telegram_failed_count",
        "data_error", "blocked_reason", "failed_order_count",
    )):
        # Only an explicit healthy no-order fact is eligible. Even a failed
        # routine notification or unclassified no-op reason stays immediate.
        return False
    if str(execution.get("risk_gate") or "").upper() in {"BLOCK", "BLOCKED", "REJECT", "REJECTED", "DENY", "DENIED"}:
        return False
    if submitted_order_count:
        orders_enabled = include_orders if include_orders is not None else os.getenv("QSL_DIGEST_ROUTINE_ORDERS", "true") == "true"
        if not orders_enabled or submitted_orders is None or len(submitted_orders) != submitted_order_count:
            return False
        # A submission acknowledgement is not a completed fill. Pending,
        # rejected and unknown outcomes retain the immediate channel.
        if any(not isinstance(order, Mapping) or str(order.get("status") or "").lower() not in {"filled", "completed", "executed"}
               for order in submitted_orders):
            return False
    elif execution.get("broker_submission_done"):
        return False
    return True


def immediate_plugin_signals(signals):
    """Filter notification inputs only; callers retain original strategy signals."""
    if not daily_digest_enabled():
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
