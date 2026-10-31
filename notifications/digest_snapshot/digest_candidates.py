"""Notification-only facts projected from existing, privately archived reports.

No broker calls. Missing or contradictory evidence stays unknown. Callers must
supply the exact protected runtime target; public labels never establish it.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_ROUTINE_ROUTES = frozenset({"watch", "opportunity_watch", "panic_reversal", "taco_rebound"})
_ROUTINE_ACTIONS = frozenset({"watch", "observe", "notify_manual_review"})


def routine_observation_counts(observations) -> dict[str, int]:
    """Count live signals using the same criteria as the immediate filter."""
    counts: dict[str, int] = {}
    for item in observations:
        route = str(getattr(item, "canonical_route", "") or "").lower()
        action = str(getattr(item, "suggested_action", "") or "").lower()
        if route in _ROUTINE_ROUTES and action in _ROUTINE_ACTIONS and not any(getattr(item, k, None) for k in ("error", "errors", "risk_halt", "auth_expired")):
            counts[route] = counts.get(route, 0) + 1
    return counts


def cycle_evidence(execution: Mapping, orders: Sequence[Mapping], *, dry_run: bool = False,
                   known_pending: bool = False) -> dict:
    """Keep counts from broker responses; known_pending requires producer proof."""
    from .digest_routing import defer_routine_cycle_to_digest

    count = len(orders)
    valid = all(isinstance(order, Mapping) and bool(order.get("status")) for order in orders)
    confirmed = valid and all(str(order["status"]).lower() in {"filled", "completed", "executed"} for order in orders)
    healthy = defer_routine_cycle_to_digest(execution, submitted_order_count=count,
        submitted_orders=orders, mode="daily_digest", include_orders=True, known_pending=known_pending)
    fill_ids = []
    fills_known = confirmed and healthy
    for order in orders:
        records = order.get("fills") if isinstance(order, Mapping) else None
        if not isinstance(records, (list, tuple)) or not records:
            fills_known = False
            break
        for record in records:
            identity = next((record.get(k) for k in ("execution_id", "execId", "tradeId") if record.get(k) is not None), None) if isinstance(record, Mapping) else None
            if identity is None:
                fills_known = False
                break
            fill_ids.append(str(identity))
    fills_known = fills_known and len(fill_ids) == len(set(fill_ids))
    return {
        "schema_version": "qsl.notification_cycle.v1",
        "order_count": count if valid and not dry_run else None,
        "fill_count": len(fill_ids) if fills_known and not dry_run else None,
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
    rows, seen, block_reports, not_run_rows = {}, set(), {}, {}
    for report in reports:
        target = report.get("runtime_target") or {}
        if not isinstance(target, Mapping) or any(target.get(k) != expected_target[k] for k in required):
            raise ValueError("candidate_source_identity_mismatch")
        if (report.get("schema_version") != "runtime_report.v1" or report.get("dry_run") is not False
                or report.get("platform") not in ({"schwab", "charles_schwab"} if expected_target["platform_id"] == "schwab" else {expected_target["platform_id"]})
                or report.get("strategy_profile") != expected_target["strategy_profile"]
                or report.get("service_name") != expected_target["service_name"]):
            raise ValueError("candidate_source_invalid")
        stamp = datetime.fromisoformat(str(report.get("started_at", "")).replace("Z", "+00:00"))
        if stamp.tzinfo is None or not timedelta(0) <= now - stamp <= timedelta(hours=36):
            raise ValueError("candidate_source_time_invalid")
        skip_reason = (report.get("diagnostics") or {}).get("skip_reason")
        not_run = (report.get("status") == "skipped"
                   and skip_reason in {"runtime_execution_not_admitted", "market_closed"})
        if not not_run and report.get("status") not in {"ok", "completed", "success", "error", "aborted", "failed"}:
            raise ValueError("candidate_source_incomplete")
        run_id = report.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("candidate_run_id_missing")
        if run_id in seen:
            raise ValueError("candidate_duplicate_run")
        seen.add(run_id)
        day = (stamp.astimezone(ZoneInfo("Asia/Singapore")) - timedelta(hours=6)).date().isoformat()
        if not_run:
            summary = report.get("summary") or {}
            if (summary.get("notification_digest")
                    or any(summary.get(k) for k in ("broker_submission_done", "orders_submitted_count", "orders_pending_count"))
                    or report.get("errors") or (report.get("error_summary") or {}).get("errors")):
                raise ValueError("candidate_non_execution_conflict")
            not_run_rows[day] = {
                "platform_id": expected_target["platform_id"], "strategy_profile": expected_target["strategy_profile"],
                "opaque_account_uid": opaque_account_uid, "target_id": target_id, "business_day": day,
                "actually_ran": False, "as_of": stamp.isoformat(), "status": "ok" if skip_reason == "market_closed" else "alert",
                "fill_count": None, "order_count": None, "cycle_count": 0,
                "field_status": {"fill_count": "unknown", "order_count": "unknown", "cycle_count": "known"},
                "execution_status": "not_run", "reason_code": skip_reason,
                "evidence_provenance": "candidates", "persistence_status": "persisted",
                "delivery_status": "pending_central",
            }
            continue
        if expected_target["platform_id"] == "schwab":
            block_reports.setdefault(day, []).append(report)
        summary = report.get("summary") or {}
        facts = summary.get("notification_digest") or {}
        known = (facts.get("schema_version") == "qsl.notification_cycle.v1" and facts.get("dry_run") is False
                 and type(facts.get("order_count")) is int and facts["order_count"] >= 0
                 and (facts.get("fill_count") is None or (type(facts["fill_count"]) is int and facts["fill_count"] >= 0)))
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
            row[key] = row[key] + facts[key] if known and type(facts.get(key)) is int and row[key] is not None else None
            row["field_status"][key] = "known" if row[key] is not None else "counts_unknown"
        if not alert and row["status"] == "ok":
            pending = str(summary.get("execution_status") or "").lower() == "pending_reconciliation" and facts.get("routine") is True and bool(facts.get("order_count"))
            if pending or row.get("execution_status") == "order_submitted":
                row["execution_status"] = "order_submitted"
            else:
                row["execution_status"] = "executed" if row["order_count"] else "no_action"
        if alert:
            row.update(status="alert", reason_code="source_cycle_alert" if error else "execution_evidence_missing_or_attention",
                       execution_status="execution_alert" if error else "execution_unknown")
    if block_reports:
        from notifications.block_reason import latest_block_reason
        from scripts.runtime_daily_report_projection import _run
        for day, sources in block_reports.items():
            reason = latest_block_reason({"conflicts": [], "runs": [_run(report, now) for report in sources]}, sources)
            if reason:
                rows[day]["block_reason"] = reason
    return {"schema_version": "qsl.digest_candidates.v1", "producer_status": "projected",
            "runs": [*rows.values(), *not_run_rows.values()]}


def summary_evidence(summary: Mapping, *, dry_run: bool = False, known_pending: bool = False) -> dict:
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
        facts = [{**order, "status": "filled"} for order in filled] if all(isinstance(order, Mapping) for order in filled) else orders
    else:
        facts = orders
    return cycle_evidence(evidence, facts, dry_run=dry_run, known_pending=known_pending)
