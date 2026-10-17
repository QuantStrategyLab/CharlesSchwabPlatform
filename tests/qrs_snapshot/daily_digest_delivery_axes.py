#!/usr/bin/env python3
"""B11 delivery-axis projection for daily digest receipts (dual-write only).

Maps existing receipt / run fields onto four mutually non-impersonating axes:

- execution_status
- evidence_persistence_status
- evidence_delivery_status
- notification_delivery_status

Additive: never removes or renames legacy fields (delivered, dry_run,
evidence_status, per-run status, …). Readers that ignore unknown keys stay
compatible. Pure projection — does not invent equity/fills and does not
trigger order retries.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

# --- Allowed enums (document freeze; keep stable for B12 consumers) ---

EXECUTION_STATUSES = frozenset(
    {
        "ok_no_order",
        "ok_filled",
        "ok_rebalanced",
        "execution_alert",
        "execution_unknown",
        "reconciliation_required",
        "not_applicable",
    }
)
PERSISTENCE_STATUSES = frozenset(
    {
        "persisted",
        "persistence_failed",
        "skipped",
        "absent",
        "unknown",
    }
)
EVIDENCE_DELIVERY_STATUSES = frozenset(
    {
        "delivered_to_central",
        "delivered_idle",
        "not_loaded",
        "not_configured",
        "intake_degraded",
        "delivery_failed",
        "pending_central",
        "omitted",
        "unknown",
    }
)
NOTIFICATION_DELIVERY_STATUSES = frozenset(
    {
        "dry_run_not_sent",
        "sent_ok",
        "send_failed",
        "blocked",
        "unknown",
        "not_applicable",
    }
)


def _norm(value: object) -> str:
    return str(value or "").strip()


def _lower(value: object) -> str:
    return _norm(value).lower()


def project_run_execution_status(run: Mapping[str, Any]) -> str:
    """Project one run row → execution_status (§3.1)."""

    explicit = _norm(run.get("execution_status"))
    if explicit:
        return explicit if explicit in EXECUTION_STATUSES else explicit

    if _lower(run.get("status")) == "alert":
        return "execution_alert"

    fill_status = _lower(run.get("fill_count_status"))
    fill_count = run.get("fill_count")
    reason = _lower(run.get("reason_code"))
    kind = _lower(run.get("rebalance_kind"))
    conclusion = _lower(run.get("rebalance_conclusion"))

    if fill_status == "unknown" or "fills_not_" in reason or "counts_unknown" in reason:
        # Still allow verified no-order when fill is known 0 below.
        if not (fill_status == "known" and fill_count == 0):
            if fill_status == "unknown" or "fills_not_" in reason:
                return "execution_unknown"

    if fill_status != "unknown" and isinstance(fill_count, int) and fill_count >= 1:
        return "ok_filled"

    if kind == "rebalance" and (
        "success" in conclusion
        or "filled" in conclusion
        or "成交" in _norm(run.get("rebalance_conclusion"))
    ):
        return "ok_rebalanced"

    if kind in {"no_order", "no_rebalance"} and (
        fill_status == "known" and fill_count == 0
    ):
        return "ok_no_order"

    if "reconciliation" in reason:
        return "reconciliation_required"

    return "execution_unknown"


def project_run_persistence_status(run: Mapping[str, Any]) -> str:
    """Project one run row → evidence_persistence_status (§3.2, weak)."""

    explicit = _norm(run.get("evidence_persistence_status"))
    if explicit:
        return explicit if explicit in PERSISTENCE_STATUSES else explicit

    reason = _lower(run.get("reason_code"))
    note = _lower(run.get("note"))
    provenance = _lower(run.get("evidence_provenance"))
    producer = _lower(run.get("producer_status"))

    if "persistence_failed" in reason or "persistence_failed" in note:
        return "persistence_failed"
    if producer == "skipped" or "skipped" in reason:
        return "skipped"
    if producer == "empty":
        return "absent"
    if provenance.startswith("github"):
        return "unknown"
    if producer == "projected" or "archive" in reason or "archive" in note:
        return "persisted"
    if "facts" in reason or provenance in {"candidates", "archive_projection"}:
        return "persisted"
    if provenance:
        return "unknown"
    return "unknown"


def project_run_evidence_delivery_status(
    run: Mapping[str, Any],
    *,
    in_central_runs: bool,
) -> str:
    """Per-run evidence delivery into central (§3.3 / §4.1)."""

    explicit = _norm(run.get("evidence_delivery_status"))
    if explicit:
        return explicit if explicit in EVIDENCE_DELIVERY_STATUSES else explicit
    if in_central_runs:
        return "delivered_to_central"
    return "pending_central"


def project_evidence_delivery_status(receipt: Mapping[str, Any]) -> str:
    """Receipt-level evidence_delivery_status (§3.3)."""

    coverage = receipt.get("source_coverage") or {}
    if not isinstance(coverage, Mapping):
        coverage = {}
    evidence_status = _lower(receipt.get("evidence_status"))
    configured = bool(coverage.get("candidates_configured"))
    loaded = bool(coverage.get("candidates_loaded"))
    failures = coverage.get("github_failures") or receipt.get("failures") or []
    runs = receipt.get("runs") or []

    if evidence_status == "evidence_unknown" or failures:
        return "intake_degraded"
    if configured and not loaded:
        return "not_loaded"
    if not configured and not loaded and not runs:
        # May still have github-only evidence; if evidence_status has_runs with
        # github stubs, treat as delivered_to_central when runs present.
        if evidence_status == "verified_idle":
            return "delivered_idle"
        if not coverage.get("github_enabled"):
            return "not_configured"
    if loaded and runs:
        return "delivered_to_central"
    if evidence_status == "verified_idle":
        return "delivered_idle"
    if loaded and not runs:
        return "delivered_idle"
    if evidence_status == "has_runs" and runs:
        return "delivered_to_central"
    if configured:
        return "not_loaded"
    return "unknown"


def project_notification_delivery_status(receipt: Mapping[str, Any]) -> str:
    """Receipt-level notification_delivery_status (§3.4)."""

    if "dry_run" not in receipt and "delivered" not in receipt and "status" not in receipt:
        return "not_applicable"

    if receipt.get("dry_run") is True:
        return "dry_run_not_sent"

    status = _lower(receipt.get("status"))
    if receipt.get("delivered") is True and status == "delivered":
        return "sent_ok"
    if status == "send_failed":
        return "send_failed"
    if status == "blocked_missing_secrets":
        return "blocked"
    if status == "route_check":
        return "not_applicable"
    if status == "dry_run":
        return "dry_run_not_sent"
    return "unknown"


def _rollup_execution(statuses: Sequence[str]) -> str:
    if not statuses:
        return "not_applicable"
    if any(s == "execution_alert" for s in statuses):
        return "execution_alert"
    if any(s == "reconciliation_required" for s in statuses):
        return "reconciliation_required"
    if any(s == "execution_unknown" for s in statuses):
        return "execution_unknown"
    if any(s == "ok_filled" for s in statuses):
        return "ok_filled"
    if any(s == "ok_rebalanced" for s in statuses):
        return "ok_rebalanced"
    if all(s == "ok_no_order" for s in statuses):
        return "ok_no_order"
    return "execution_unknown"


def _rollup_persistence(statuses: Sequence[str]) -> str:
    if not statuses:
        return "unknown"
    if any(s == "persistence_failed" for s in statuses):
        return "persistence_failed"
    if any(s == "unknown" for s in statuses):
        return "unknown"
    if all(s == "skipped" for s in statuses):
        return "skipped"
    if all(s == "absent" for s in statuses):
        return "absent"
    if all(s == "persisted" for s in statuses):
        return "persisted"
    return "unknown"


def attach_delivery_axes(receipt: dict[str, Any]) -> dict[str, Any]:
    """Dual-write ``delivery_axes`` (+ per-run axes) onto an existing receipt.

    Mutates and returns the same dict. Legacy fields are preserved.
    """

    runs = receipt.get("runs")
    if not isinstance(runs, list):
        runs = []

    exec_statuses: list[str] = []
    persist_statuses: list[str] = []
    for run in runs:
        if not isinstance(run, dict):
            continue
        execution = project_run_execution_status(run)
        persistence = project_run_persistence_status(run)
        delivery = project_run_evidence_delivery_status(run, in_central_runs=True)
        # Additive per-run dual-write (ignore-unknown for old readers).
        run["execution_status"] = execution
        run["evidence_persistence_status"] = persistence
        run["evidence_delivery_status"] = delivery
        exec_statuses.append(execution)
        persist_statuses.append(persistence)

    receipt["delivery_axes"] = {
        "execution_status": _rollup_execution(exec_statuses),
        "evidence_persistence_status": _rollup_persistence(persist_statuses),
        "evidence_delivery_status": project_evidence_delivery_status(receipt),
        "notification_delivery_status": project_notification_delivery_status(
            receipt
        ),
    }
    return receipt


__all__ = [
    "attach_delivery_axes",
    "project_evidence_delivery_status",
    "project_notification_delivery_status",
    "project_run_evidence_delivery_status",
    "project_run_execution_status",
    "project_run_persistence_status",
]
