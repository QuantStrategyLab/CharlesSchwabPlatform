"""Notification-only copies of completed risk decisions and validated receipts."""
from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence


def decision_block_reason(decision) -> dict:
    """Read the gate's emitted values; never duplicate or configure its limits."""
    diagnostics = getattr(decision, "diagnostics", None)
    if not isinstance(diagnostics, Mapping) or diagnostics.get("risk_gate") != "REJECT":
        return {}
    if "rejected:circuit_breaker" not in (getattr(decision, "risk_flags", ()) or ()):
        return {}
    reason = diagnostics.get("reason")
    if not isinstance(reason, str) or len(reason) > 128:
        return {}
    match = re.fullmatch(r"连续亏损 ([0-9]{1,9}) 笔 > ([0-9]{1,9}) 熔断", reason)
    if match is None:
        return {}
    observed, limit = map(int, match.groups())
    if type(diagnostics.get("consecutive_losses")) is not int or diagnostics["consecutive_losses"] != observed:
        return {}
    return clean_block_reason({"reason_code": "consecutive_loss_limit_exceeded",
                               "observed_value": observed, "limit": limit})


def clean_block_reason(value) -> dict:
    """Only bounded numeric facts and a known code may leave private reports."""
    if not isinstance(value, Mapping) or value.get("reason_code") != "consecutive_loss_limit_exceeded":
        return {}
    observed, limit = value.get("observed_value"), value.get("limit")
    if (type(observed) is not int or type(limit) is not int
            or not 0 <= limit < observed <= 999_999_999):
        return {}
    return {"reason_code": "consecutive_loss_limit_exceeded", "observed_value": observed, "limit": limit}


def latest_block_reason(record: Mapping, reports: Sequence[Mapping]) -> dict:
    """Describe only the latest real run, never infer whole-day zero counts.

    Reports must already pass the caller's source/account admission. The daily
    projection supplies the validated receipt and duplicate/conflict checks.
    """
    if record.get("conflicts") != []:
        return {}
    runs = [run for run in record.get("runs", ()) if isinstance(run, Mapping)
            and run.get("execution_lane") == "live" and run.get("activity") != "insufficient"]
    if not runs or any(not isinstance(run.get("started_at"), str) for run in runs):
        return {}
    latest = max(run["started_at"] for run in runs)
    selected = [run for run in runs if run["started_at"] == latest]
    if len(selected) != 1:
        return {}
    run = selected[0]
    evidence = run.get("evidence") or {}
    if (run.get("activity") != "blocked" or evidence.get("receipt_state") != "valid"
            or evidence.get("receipt_outcome") != "risk_blocked"
            or evidence.get("receipt_broker_confirmation") != "not_applicable"
            or evidence.get("broker_submission_done") is not False
            or evidence.get("action_done") is True or evidence.get("errors_present") is not False
            or type(evidence.get("orders_pending_count")) is not int
            or evidence["orders_pending_count"] != 0):
        return {}
    values = []
    for report in reports:
        raw_id = report.get("run_id")
        if not isinstance(raw_id, str) or not raw_id:
            continue
        if "run." + hashlib.sha256(raw_id.encode()).hexdigest()[:32] != run.get("run_id"):
            continue
        summary = report.get("summary")
        summary = summary if isinstance(summary, Mapping) else {}
        # Conflicting copies must not select whichever reason happens to arrive first.
        values.append(clean_block_reason(summary.get("notification_block_reason")))
    if values and any(value != values[0] for value in values):
        return {}
    reason = values[0] if values else {}
    return {**(reason or {"reason_code": "strategy_risk_rejected", "observed_value": None, "limit": None}),
            "order_submitted_this_run": False}
