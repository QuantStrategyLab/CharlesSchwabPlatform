"""Notification classification of completed, explicitly approved gate facts."""
from __future__ import annotations

import math
from collections.abc import Mapping

from .block_reason import decision_block_reason


def _number(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def clean_gate_reasons(values):
    if not isinstance(values, list) or not 1 <= len(values) <= 2:
        return []
    clean = []
    for value in values:
        if not isinstance(value, Mapping):
            return []
        code, observed, limit = (value.get(k) for k in ("reason_code", "observed_value", "limit"))
        if code == "consecutive_loss_limit_exceeded":
            if type(observed) is not int or type(limit) is not int or not 5 == limit < observed <= 999_999_999:
                return []
            reason = dict(reason_code=code, observed_value=observed, limit=limit)
        elif code == "daily_loss_limit_exceeded":
            equity = value.get("equity_usd")
            if (not all(_number(v) for v in (observed, limit, equity)) or equity <= 0 or limit <= 0
                    or observed < limit or not math.isclose(limit, equity * (0.01 + 0.04 * 2000 / (2000 + equity)), rel_tol=1e-12)):
                return []
            reason = dict(reason_code=code, observed_value=observed, limit=limit, equity_usd=equity)
        else:
            return []
        if any(item["reason_code"] == code for item in clean):
            return []
        clean.append(reason)
    return clean


def decision_gate_reasons(decision):
    flags = {flag for flag in (getattr(decision, "risk_flags", ()) or ()) if str(flag).startswith("rejected:")}
    if flags != {"rejected:circuit_breaker"}:
        return []
    reason = decision_block_reason(decision)
    return clean_gate_reasons([reason]) if reason else []


def daily_gate_reasons(admission, snapshot, resolution, formula):
    if (not isinstance(formula, Mapping) or formula.get("pct_max") != 0.05 or formula.get("pct_min") != 0.01
            or formula.get("equity_scale_usd", formula.get("E0_usd")) != 2000):
        return []
    if (set(admission.reason_codes or ()) != {"DAILY_LOSS_LIMIT_EXCEEDED"}
            or snapshot is None or resolution.reason != "formula" or resolution.status != "valid"):
        return []
    return clean_gate_reasons([dict(reason_code="daily_loss_limit_exceeded",
        observed_value=snapshot.daily_loss_usd, limit=resolution.limit_usd, equity_usd=snapshot.equity_usd)])


def approved_block_reasons(execution, *, order_count, dry_run=False):
    reasons = clean_gate_reasons(execution.get("notification_approved_gates"))
    if not reasons or dry_run or type(order_count) is not int or order_count != 0:
        return []
    metadata = execution.get("signal_metadata") or {}
    if not isinstance(metadata, Mapping):
        return []
    for source in (execution, metadata):
        if (any(source.get(k) for k in ("errors", "error", "order_errors", "orders_pending_count", "fail_reason",
                "execution_blocked_reason", "plugin_error", "persistence_error", "report_persistence_error",
                "notification_error", "pending_reconciliation", "reconciliation_required", "market_data_retry_required"))
                or source.get("execution_status") in {"error", "failed", "pending", "pending_reconciliation", "reconciliation_required"}):
            return []
    if (metadata.get("execution_status") not in {None, "", "no_action", "no_op", "completed", "executed"}
            or metadata.get("notification_attention_reason_codes")):
        return []
    if execution.get("broker_submission_done", False) is not False:
        return []
    attention = set(execution.get("notification_attention_reason_codes") or ())
    if attention - {"account_new_risk_gate"}:
        return []
    status = execution.get("execution_status")
    if status not in {"blocked", "risk_blocked", "strategy_risk_rejected"} and not (
            status in {"no_action", "no_op"} and attention == {"account_new_risk_gate"}):
        return []
    if execution.get("no_op_reason") not in {None, "", "strategy_risk_rejected", "rejected:circuit_breaker", "account_new_risk_gate"}:
        return []
    return reasons


def render_gate_reasons(reasons, locale):
    zh = locale == "zh"
    lines = []
    for reason in reasons:
        observed, limit = reason["observed_value"], reason["limit"]
        if reason["reason_code"] == "consecutive_loss_limit_exceeded":
            lines.append(f"连亏熔断：连续亏损 {observed}>{limit}" if zh else
                f"Consecutive-loss gate: consecutive losses {observed}>{limit}")
        else:
            lines.append(f"日亏损上限：日亏损 {observed:g}≥{limit:g}" if zh else
                f"Daily-loss gate: daily loss {observed:g}>={limit:g}")
    lines.append("本轮未下单" if zh else "No order this run")
    return "\n".join(lines)


def report_gate_reasons(report, *, observed_at=None):
    """Require the existing receipt validator; never make a report acceptable."""
    summary = report.get("summary")
    if not isinstance(summary, Mapping) or report.get("dry_run") is not False:
        return []
    if (report.get("status") not in {"ok", "success", "completed"}
            or any(report.get(k) for k in ("errors", "error", "error_summary"))
            or type(summary.get("orders_pending_count")) is not int or summary["orders_pending_count"] != 0):
        return []
    reasons = approved_block_reasons(summary, order_count=summary.get("orders_submitted_count"))
    if not reasons:
        return []
    from notifications.receipt_evidence import _run
    from datetime import datetime, timezone
    try:
        evidence = _run(report, observed_at or datetime.now(timezone.utc)).get("evidence", {})
    except (ValueError, TypeError, OverflowError):
        return []
    if (evidence.get("receipt_state") != "valid" or evidence.get("receipt_outcome") != "risk_blocked"
            or evidence.get("receipt_broker_confirmation") != "not_applicable"
            or evidence.get("errors_present") is not False or evidence.get("broker_submission_done") is not False
            or evidence.get("action_done") is True or evidence.get("orders_pending_count") != 0):
        return []
    return reasons
