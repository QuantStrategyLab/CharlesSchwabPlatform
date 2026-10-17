"""Read-only observation of the old executor's actual no-submission gate branch.

This is notification evidence, never an execution status or execution receipt.
The unchanged receipt adapter may truthfully report no_action for this branch.
"""
from __future__ import annotations

import copy
from collections.abc import Mapping
from datetime import datetime, timezone

from .approved_gates import clean_gate_reasons, daily_gate_reasons, report_gate_reasons

FIELD = "notification_gate_completion"
KEYS = ("platform_id", "strategy_profile", "service_name", "account_scope", "account_selector", "deployment_selector", "dry_run_only", "execution_mode")
FAILURES = ("error", "errors", "order_errors", "orders_pending_count", "fail_reason", "execution_blocked_reason",
    "plugin_error", "persistence_error", "report_persistence_error", "notification_error", "pending_reconciliation",
    "reconciliation_required", "market_data_retry_required", "risk_flags", "execution_blocked", "failed_order_count",
    "strategy_plugin_error", "strategy_plugin_alert_error", "auth_expired", "blocked_reason")


def _target(report):
    target = report.get("runtime_target")
    if not isinstance(target, Mapping) or any(key not in target for key in KEYS):
        return None
    value = {key: copy.deepcopy(target[key]) for key in KEYS}
    if value["platform_id"] in {"schwab", "charles_schwab"}:
        value["platform_id"] = "schwab"
    return value


def _scope(report):
    target = _target(report)
    if (not target or report.get("schema_version") != "runtime_report.v1"
            or report.get("platform") not in {"schwab", "charles_schwab"}
            or target["platform_id"] != "schwab" or target["dry_run_only"] is not False
            or target["execution_mode"] != "live" or report.get("dry_run") is not False
            or report.get("strategy_profile") != target["strategy_profile"]
            or report.get("service_name") != target["service_name"]
            or not isinstance(report.get("run_id"), str) or not report["run_id"]
            or not isinstance(report.get("started_at"), str) or not report["started_at"]):
        return None
    return dict(source_run_id=report["run_id"], started_at=report["started_at"], runtime_target=target)


def _clean_execution(execution):
    metadata = execution.get("signal_metadata") or {}
    if not isinstance(metadata, Mapping):
        return False
    for source in (execution, metadata):
        if (any(source.get(key) for key in FAILURES)
                or source.get("execution_status") not in {None, "", "no_action", "no_op", "completed", "executed"}
                or source.get("no_op_reason") not in {None, ""}
                or source.get("broker_submission_done", False) is not False
                or source.get("notification_attention_reason_codes")):
            return False
    return True


class _Observation:
    def __init__(self, report):
        self.scope = _scope(report)
        self.order_paths = 0

    def order_path(self):
        # Any order path disqualifies this proof, including a rejected sell that
        # never appears in submitted_orders. This is not a business order count.
        self.order_paths += 1

    def complete(self, execution, *, admission, snapshot, portfolio, orders, branch,
                 needed_buys, sold, bought, outcome_unknown, dry_run):
        from application.account_new_risk_gate_support import resolve_max_daily_loss_resolution, _runtime_risk_limits_policy
        from quant_platform_kit.risk.account_new_risk_gate import NewRiskAdmissionResult, NewRiskDisposition
        from .unified import _context
        if (not self.scope or _scope(_context.get() or {}) != self.scope or self.order_paths
                or dry_run or orders or sold or bought or outcome_unknown
                or branch != "account_new_risk_gate" or not needed_buys
                or not isinstance(admission, NewRiskAdmissionResult)
                or admission.disposition != NewRiskDisposition.NEW_RISK_PROHIBITED or not _clean_execution(execution)):
            return
        resolution = resolve_max_daily_loss_resolution(portfolio)
        formula = (_runtime_risk_limits_policy() or {}).get("max_daily_loss_equity_formula")
        reasons = daily_gate_reasons(admission, snapshot, resolution, formula)
        if not reasons or clean_gate_reasons(execution.get("notification_approved_gates")) != reasons:
            return
        execution[FIELD] = {"schema_version": "qsl.schwab_gate_completion.v1", **copy.deepcopy(self.scope),
            "admission": {"disposition": admission.disposition.value, "reason_codes": list(admission.reason_codes)},
            "branch": branch, "needed_buy_count": len(needed_buys), "order_path_count": self.order_paths,
            "submitted_order_count": len(orders), "approved_gate_reasons": reasons}


def start_observation():
    from .unified import enabled, _context
    if not enabled():
        return None
    observer = _Observation(_context.get() or {})
    return observer if observer.scope else None


def execution_reasons(execution, *, report, order_count):
    proof = execution.get(FIELD)
    if (not isinstance(proof, Mapping) or proof.get("schema_version") != "qsl.schwab_gate_completion.v1"
            or _scope(report) is None or any(proof.get(key) != value for key, value in _scope(report).items())
            or proof.get("admission") != {"disposition": "NEW_RISK_PROHIBITED", "reason_codes": ["DAILY_LOSS_LIMIT_EXCEEDED"]}
            or proof.get("branch") != "account_new_risk_gate"
            or type(proof.get("needed_buy_count")) is not int or proof["needed_buy_count"] <= 0
            or type(proof.get("order_path_count")) is not int or proof["order_path_count"] != 0
            or type(proof.get("submitted_order_count")) is not int or proof["submitted_order_count"] != 0
            or type(order_count) is not int or order_count != 0 or not _clean_execution(execution)):
        return []
    reasons = clean_gate_reasons(proof.get("approved_gate_reasons"))
    if (len(reasons) != 1 or reasons[0]["reason_code"] != "daily_loss_limit_exceeded"
            or reasons != clean_gate_reasons(execution.get("notification_approved_gates"))):
        return []
    return reasons


def report_reasons(report, *, observed_at=None):
    # Retain the existing blocked/risk_blocked validator byte-for-byte. The new
    # path proves a different actual terminal branch with its original no_action
    # receipt, not a reinterpretation or weakening of that receipt contract.
    existing = report_gate_reasons(report, observed_at=observed_at)
    if existing:
        return existing
    summary = report.get("summary")
    if (not isinstance(summary, Mapping) or report.get("status") not in {"ok", "success", "completed"}
            or any(report.get(key) for key in ("errors", "error", "error_summary"))):
        return []
    reasons = execution_reasons(summary, report=report, order_count=summary.get("orders_submitted_count"))
    if not reasons:
        return []
    from .receipt_evidence import _run
    try:
        facts = _run(report, observed_at or datetime.now(timezone.utc))["evidence"]
    except (ValueError, TypeError, OverflowError):
        return []
    if (facts.get("receipt_state") != "valid" or facts.get("receipt_outcome") != "no_action"
            or facts.get("receipt_broker_confirmation") != "not_applicable" or facts.get("broker_submission_done") is not False
            or facts.get("action_done") is True or facts.get("errors_present") is not False
            or type(facts.get("orders_pending_count")) is not int or facts["orders_pending_count"] != 0):
        return []
    return reasons
