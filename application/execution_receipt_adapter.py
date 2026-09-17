"""Bounded execution-receipt facts derived from Schwab cycle results."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from quant_platform_kit.common.execution_receipts import (
    attach_runtime_execution_receipt,
    resolve_execution_receipt_fact,
)


# Only promote these explicit cycle reasons. Broad no-ops stay ``no_action``
# so existing digests and projections keep digest-compatible outcomes.
_NO_SIGNAL_REASON_HEADS = frozenset({"no_signal"})
_NO_REBALANCE_REASON_HEADS = frozenset({"no_rebalance", "target_diff_below_threshold"})


def attach_cycle_execution_receipt(
    report: dict[str, Any],
    cycle_result: object,
) -> dict[str, Any]:
    """Attach only the actual lifecycle fact carried by a Schwab cycle.

    Schwab records accepted orders as pending reconciliation.  That is stronger
    and safer than a local submission count, but is still not a fill; this
    adapter therefore never derives acknowledgement or fill from the order
    records.
    """

    execution = _as_mapping(getattr(cycle_result, "execution", {}))
    submitted_orders = tuple(getattr(cycle_result, "submitted_orders", ()) or ())
    status = str(execution.get("execution_status") or "").strip().lower()
    risk_blocked = status == "blocked" and str(execution.get("no_op_reason") or "").strip() in {
        "rejected:too_many_positions",
        "strategy_risk_rejected",
    }
    reconciliation_required = status == "pending_reconciliation" or bool(
        execution.get("orders_pending_count")
    )
    outcome, confirmation = resolve_execution_receipt_fact(
        dry_run=bool(report.get("dry_run")),
        submission_attempted=bool(execution.get("broker_submission_done")) or bool(submitted_orders),
        reconciliation_required=reconciliation_required,
        risk_blocked=risk_blocked,
    )
    if outcome == "no_action" and not bool(report.get("dry_run")):
        explicit = _explicit_non_action_outcome(execution)
        if explicit is not None:
            outcome = explicit
            confirmation = "not_applicable"
    return attach_runtime_execution_receipt(
        report,
        outcome=outcome,
        broker_confirmation=confirmation,
    )


def attach_terminal_fallback_execution_receipt(report: dict[str, Any]) -> dict[str, Any]:
    """Preserve uncertainty when a cycle fails before emitting a result."""

    failed = str(report.get("status") or "").strip().lower() == "error"
    outcome, confirmation = resolve_execution_receipt_fact(
        dry_run=bool(report.get("dry_run")),
        submission_attempted=failed,
        failed=failed,
    )
    return attach_runtime_execution_receipt(
        report,
        outcome=outcome,
        broker_confirmation=confirmation,
    )


def _as_mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _explicit_non_action_outcome(execution: Mapping[str, Any]) -> str | None:
    """Map only explicit cycle reasons to no_signal / no_rebalance."""

    reason = str(execution.get("no_op_reason") or "").strip().lower()
    if not reason:
        return None
    head = reason.split(":", 1)[0].strip()
    if head in _NO_SIGNAL_REASON_HEADS:
        return "no_signal"
    if head in _NO_REBALANCE_REASON_HEADS:
        return "no_rebalance"
    return None
