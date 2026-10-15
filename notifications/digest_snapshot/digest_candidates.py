"""Notification-only facts projected from existing, privately archived reports.

No broker calls. Missing or contradictory evidence stays unknown. Callers must
supply the exact protected runtime target; public labels never establish it.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def cycle_evidence(execution: Mapping, orders: Sequence[Mapping], *, dry_run: bool = False) -> dict:
    """Keep counts derived from actual order responses, never from log lines."""
    from .digest_routing import defer_routine_cycle_to_digest

    count = len(orders)
    valid = all(isinstance(order, Mapping) and bool(order.get("status")) for order in orders)
    confirmed = valid and all(str(order["status"]).lower() in {"filled", "completed", "executed"} for order in orders)
    healthy = defer_routine_cycle_to_digest(execution, submitted_order_count=count,
        submitted_orders=orders, mode="daily_digest", include_orders=True)
    return {
        "schema_version": "qsl.notification_cycle.v1",
        "order_count": count if valid and not dry_run else None,
        "fill_count": count if confirmed and not dry_run else None,
        "routine": healthy and not dry_run,
        "dry_run": dry_run,
    }


def project_reports(reports: Sequence[Mapping], *, expected_target: Mapping,
                    opaque_account_uid: str, target_id: str, now: datetime) -> dict:
    """Aggregate each unique persisted cycle for the same target and source day."""
    if any(not isinstance(v, str) or not v.strip() for v in (opaque_account_uid, target_id)) or now.tzinfo is None:
        raise ValueError("candidate_identity_or_time_missing")
    required = {"platform_id", "strategy_profile", "service_name", "account_scope", "account_selector", "deployment_selector", "dry_run_only"}
    if not required.issubset(expected_target) or expected_target.get("dry_run_only") is not False:
        raise ValueError("candidate_target_invalid")
    if (any(not isinstance(expected_target[k], str) or not expected_target[k].strip()
            for k in required - {"account_selector", "dry_run_only"})
            or not isinstance(expected_target["account_selector"], list) or not expected_target["account_selector"]
            or any(not isinstance(v, str) or not v.strip() for v in expected_target["account_selector"])):
        raise ValueError("candidate_target_invalid")
    rows, seen = {}, set()
    for report in reports:
        target = report.get("runtime_target") or {}
        if not isinstance(target, Mapping) or any(target.get(k) != expected_target[k] for k in required):
            raise ValueError("candidate_source_identity_mismatch")
        if (report.get("schema_version") != "runtime_report.v1" or report.get("dry_run") is not False
                or report.get("platform") != expected_target["platform_id"]
                or report.get("strategy_profile") != expected_target["strategy_profile"]
                or report.get("service_name") != expected_target["service_name"]):
            raise ValueError("candidate_source_invalid")
        stamp = datetime.fromisoformat(str(report.get("started_at", "")).replace("Z", "+00:00"))
        if stamp.tzinfo is None or not timedelta(0) <= now - stamp <= timedelta(hours=36):
            raise ValueError("candidate_source_time_invalid")
        if report.get("status") not in {"ok", "completed", "success", "error", "aborted", "failed"}:
            raise ValueError("candidate_source_incomplete")
        run_id = report.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("candidate_run_id_missing")
        if run_id in seen:
            raise ValueError("candidate_duplicate_run")
        seen.add(run_id)
        day = (stamp.astimezone(ZoneInfo("Asia/Singapore")) - timedelta(hours=6)).date().isoformat()
        facts = (report.get("summary") or {}).get("notification_digest") or {}
        known = (facts.get("schema_version") == "qsl.notification_cycle.v1" and facts.get("dry_run") is False
                 and all(type(facts.get(k)) is int and facts[k] >= 0 for k in ("order_count", "fill_count"))
                 and facts["fill_count"] <= facts["order_count"])
        error = report.get("status") in {"error", "failed", "aborted"} or bool((report.get("error_summary") or {}).get("errors"))
        alert = error or not known or facts.get("routine") is not True
        row = rows.setdefault(day, {
            "platform_id": expected_target["platform_id"], "strategy_profile": expected_target["strategy_profile"],
            "opaque_account_uid": opaque_account_uid, "target_id": target_id, "business_day": day,
            "actually_ran": True, "as_of": stamp.isoformat(), "status": "ok", "fill_count": 0,
            "order_count": 0, "cycle_count": 0, "field_status": {}, "evidence_provenance": "candidates",
            "persistence_status": "persisted", "delivery_status": "pending_central",
        })
        row["cycle_count"] += 1
        row["as_of"] = max(row["as_of"], stamp.isoformat())
        for key in ("fill_count", "order_count"):
            row[key] = row[key] + facts[key] if known and row[key] is not None else None
            row["field_status"][key] = "known" if row[key] is not None else "counts_unknown"
        if alert:
            row.update(status="alert", reason_code="source_cycle_alert" if error else "execution_evidence_missing_or_attention",
                       execution_status="execution_alert" if error else "execution_unknown")
    return {"schema_version": "qsl.digest_candidates.v1", "producer_status": "projected", "runs": list(rows.values())}


def summary_evidence(summary: Mapping, *, dry_run: bool = False) -> dict:
    """IBKR named order collections are broker facts, unlike formatted logs."""
    if not isinstance(summary.get("orders_submitted"), (list, tuple)):
        return {"schema_version": "qsl.notification_cycle.v1", "order_count": None,
                "fill_count": None, "routine": False, "dry_run": dry_run}
    orders = list(summary.get("orders_submitted") or ()) + list(summary.get("option_orders_submitted") or ())
    filled = list(summary.get("orders_filled") or ()) + list(summary.get("option_orders_filled") or ())
    evidence = dict(summary)
    if (orders and len(orders) == len(filled) and not summary.get("orders_pending")
            and not summary.get("option_orders_pending") and not summary.get("orders_partially_filled")
            and not summary.get("option_orders_partially_filled")):
        # These collections are populated by the existing broker reconciliation.
        facts = [{"status": "filled"} for _ in filled]
    else:
        facts = orders
    return cycle_evidence(evidence, facts, dry_run=dry_run)
