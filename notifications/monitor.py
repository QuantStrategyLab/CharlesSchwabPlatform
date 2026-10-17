"""Notification-only facade around the unchanged production heartbeat policy."""
from .execution_observation import report_reasons as report_gate_reasons
from .receipt_evidence import _run, _instant
from .receipt_alerts import render_receipt_alert
from .unified import enabled, publish_receipt, publish_approved_receipts
from datetime import datetime, timezone


def _healthy(report):
    evidence = _run(report, datetime.now(timezone.utc))["evidence"]
    summary = report.get("summary") or {}
    return (evidence["receipt_state"] == "valid" and evidence["errors_present"] is False
        and summary.get("execution_status") not in {"blocked", "risk_blocked", "strategy_risk_rejected",
            "unknown", "pending", "pending_reconciliation", "reconciliation_required", "error", "failed"}
        and summary.get("orders_pending_count") == 0)


def _defer_approved(reports, complete):
    if not complete or not reports:
        return False
    approved = [report for report in reports if report_gate_reasons(report)]
    return bool(approved) and all(report in approved or _healthy(report) for report in reports) and publish_approved_receipts(approved)


def publish_alert(legacy, *, sender, locale, reports=(), complete=False, **presentation):
    if not enabled():
        return legacy()
    try:
        if _defer_approved(reports, complete):
            return None
    except Exception:
        # Unknown observation must stay urgent; it never grants acceptance.
        pass
    message = render_receipt_alert(locale=locale, **presentation)[:3900]
    return publish_receipt(lambda: sender(message), kind="receipt_unacceptable" if reports else "receipt_missing",
        reason="execution_receipt_unacceptable")


def publish_health(legacy, reports):
    if not enabled() or not reports:
        return legacy()
    try:
        if _defer_approved(reports, True):
            return 0
        healthy = all(_healthy(report) for report in reports)
    except Exception:
        healthy = False
    if not healthy:
        from scripts import execution_report_heartbeat as heartbeat
        publish_alert(legacy, sender=heartbeat._send_telegram, locale=heartbeat._notification_locale(),
            unconfirmed=True, reports=reports)
        return 0
    results = [publish_receipt(legacy, kind="healthy_heartbeat", reason="receipt_evidence_observed", source_report=report,
        recovered=True, observed_at=_instant(report.get("finished_at")))
        for report in reports]
    return next((value for value in results if type(value) is int and value), 0)
