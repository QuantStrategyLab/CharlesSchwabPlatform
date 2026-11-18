"""Shared report-projection and identity helpers for notifications and scripts."""

from __future__ import annotations

import datetime as dt
import hashlib
import re
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

from quant_platform_kit.common.execution_receipts import attach_execution_receipt

POSITIONS_SCOPE = "strategy_symbols_only"
POSITIONS_KEYS = (
    "broker_reported_positions",
    "broker_reported_positions_scope",
    "broker_reported_positions_complete",
)
_POSITION_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9./ -]{0,31}\Z", re.ASCII)
_DECIMAL_TEXT = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$")
MAX_POSITIONS = 64

_ORDERS = {"submitted": 0, "broker_acknowledged": 1, "partially_filled": 2, "filled": 3}
_NON_REAL_LANES = frozenset({"dry_run", "shadow", "validation"})
_GOOD_STATUSES = frozenset({"ok", "skipped", "success", "completed", "no_action"})
_BAD_STATUSES = frozenset(
    {"error", "failed", "failure", "cancelled", "canceled", "timed_out"}
)
_EXECUTION_STATUSES = frozenset(
    {
        "no_op",
        "no_action",
        "no_signal",
        "no_rebalance",
        "skipped",
        "outside_market_hours",
        "submitted",
        "pending_reconciliation",
        "reconciliation_required",
        "unknown",
        "unknown_order",
        "blocked",
        "risk_blocked",
        "error",
        "failed",
        "failure",
        "dry_run",
        "previewed",
        "partial",
        "partially_filled",
        "filled",
        "completed",
    }
)


class ProjectionError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def money_text(value: object) -> str:
    if not isinstance(value, str) or _DECIMAL_TEXT.fullmatch(value) is None:
        raise ProjectionError("invalid_net_assets")
    try:
        amount = Decimal(value)
    except InvalidOperation:
        raise ProjectionError("invalid_net_assets") from None
    if not amount.is_finite():
        raise ProjectionError("invalid_net_assets")
    whole, _, fraction = value.lstrip("-").partition(".")
    if len(whole) > 15 or len(fraction) > 8:
        raise ProjectionError("invalid_net_assets")
    return value


def project_positions(observation: Mapping[str, Any]) -> list[dict[str, Any]] | None:
    """Validate runtime ``broker_reported_positions``; None (omit) on any doubt."""
    raw = observation.get("broker_reported_positions")
    scope = observation.get("broker_reported_positions_scope")
    native_stocks = scope == "stocks_only" and (
        observation.get("broker_reported_positions_complete") is True
        or (
            observation.get("broker_reported_positions_complete") is False
            and observation.get("broker_stock_inventory_status") == "available"
        )
    )
    if scope != POSITIONS_SCOPE and not native_stocks:
        return None
    if not isinstance(raw, list) or (not raw and not native_stocks) or len(raw) > MAX_POSITIONS:
        return None
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            return None
        if set(item) - {"symbol", "quantity", "market_value", "currency", "avg_cost", "currency_source"}:
            return None
        symbol = item.get("symbol")
        if (
            not isinstance(symbol, str)
            or _POSITION_SYMBOL.fullmatch(symbol) is None
            or symbol in seen
        ):
            return None
        seen.add(symbol)
        if item.get("currency") not in (None, "USD"):
            return None
        if "currency_source" in item and item["currency_source"] != "owner_confirmed":
            return None
        try:
            quantity = money_text(item.get("quantity"))
            market_value = (
                None
                if native_stocks and item.get("market_value") is None
                else money_text(item.get("market_value"))
            )
        except ProjectionError:
            return None
        rows.append(
            {
                "symbol": symbol,
                "quantity": quantity,
                "market_value": market_value,
                "currency": "USD",
                "currency_source": "owner_confirmed",
            }
        )
        if item.get("avg_cost") is not None:
            try:
                rows[-1]["avg_cost"] = money_text(item["avg_cost"])
            except ProjectionError:
                pass  # Unknown optional cost; retain the validated holding.
    return rows


def account_hash_matches(expected: object, observed: object) -> bool:
    """Preserve exact IDs; compare Schwab's 64-digit hex digest by its bytes."""
    if not isinstance(expected, str) or not isinstance(observed, str):
        return False
    if expected == observed:
        return True
    if not all(re.fullmatch(r"[0-9A-Fa-f]{64}", value) for value in (expected, observed)):
        return False
    return bytes.fromhex(expected) == bytes.fromhex(observed)


def _instant(value: object) -> dt.datetime | None:
    if not isinstance(value, (str, dt.datetime)):
        return None
    try:
        parsed = (
            value
            if isinstance(value, dt.datetime)
            else dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        )
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(dt.timezone.utc)
    except (ValueError, OverflowError):
        return None


def _iso(value: dt.datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value is not None else None


def _execution_lane(payload: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    runtime = payload["runtime_target"]
    diagnostics = payload.get("diagnostics")
    sources = (
        payload,
        summary,
        runtime,
        diagnostics if isinstance(diagnostics, Mapping) else {},
    )
    non_real = set()
    for source in sources:
        for key in (
            "execution_lane",
            "run_kind",
            "validation_label",
            "mode",
            "run_source",
        ):
            value = source.get(key)
            if isinstance(value, str) and value in _NON_REAL_LANES:
                non_real.add(value)
        if source.get("validation_only") is True:
            non_real.add("validation")
    if payload.get("dry_run") is True or runtime.get("dry_run_only") is True:
        non_real.add("dry_run")
    if non_real:
        return next(iter(non_real)) if len(non_real) == 1 else "insufficient"
    return (
        "live"
        if payload.get("dry_run") is False and runtime.get("execution_mode") == "live"
        else "insufficient"
    )


def project_run_summary(payload: Mapping[str, Any], observed: dt.datetime) -> dict[str, Any]:
    summary = (
        payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    )
    started, finished = (
        _instant(payload.get("started_at")),
        _instant(payload.get("finished_at")),
    )
    raw_id = payload.get("run_id")
    run_id = (
        "run." + hashlib.sha256(raw_id.encode()).hexdigest()[:32]
        if isinstance(raw_id, str) and raw_id
        else None
    )
    receipt: Mapping[str, Any] = {}
    receipt_state = "missing"
    if payload.get("execution_receipt") is not None:
        receipt_state = "invalid"
        try:
            receipt = attach_execution_receipt(
                dict(payload), payload["execution_receipt"]
            )["execution_receipt"]
            instant = _instant(receipt.get("observed_at"))
            if (
                instant is None
                or instant > observed
                or started is None
                or finished is None
                or not (started.replace(microsecond=0) <= instant <= finished)
            ):
                receipt = {}
            else:
                receipt_state = "valid"
        except (ValueError, TypeError, OverflowError):
            receipt = {}
    status = payload.get("status")
    status = (
        status
        if isinstance(status, str) and status in _GOOD_STATUSES | _BAD_STATUSES
        else None
    )
    execution = summary.get("execution_status")
    execution = (
        execution
        if isinstance(execution, str) and execution in _EXECUTION_STATUSES
        else None
    )
    count = summary.get("orders_pending_count")
    count = count if type(count) is int and count >= 0 else None
    broker = summary.get("broker_submission_done")
    broker = broker if type(broker) is bool else None
    action = summary.get("action_done")
    action = action if type(action) is bool else None
    errors = bool(
        payload.get("errors") or payload.get("error") or payload.get("error_summary")
    )
    lane = _execution_lane(payload, summary)
    outcome = receipt.get("outcome")
    activity = "insufficient"
    if (
        execution in {"pending_reconciliation", "reconciliation_required"}
        or (count is not None and count > 0)
        or outcome == "reconciliation_required"
        or receipt.get("broker_confirmation") == "reconciliation_required"
    ):
        activity = "reconciliation_required"
    elif execution in {"unknown", "unknown_order"}:
        activity = "unknown"
    elif execution in {"blocked", "risk_blocked"} or outcome == "risk_blocked":
        activity = "blocked"
    elif (
        errors
        or execution in {"error", "failed", "failure"}
        or status in _BAD_STATUSES
        or outcome == "failed"
    ):
        activity = "failed"
    elif (
        receipt_state == "valid" and status in _GOOD_STATUSES and lane != "insufficient"
    ):
        if lane in _NON_REAL_LANES:
            activity = "previewed"
        elif outcome in _ORDERS:
            activity = outcome
        elif (
            outcome in {"no_signal", "no_rebalance", "no_action"}
            and broker is False
            and action is not True
            and count == 0
            and (
                summary.get("execution_status") is None
                or execution
                in {
                    "no_op",
                    "no_action",
                    "no_signal",
                    "no_rebalance",
                    "skipped",
                    "outside_market_hours",
                    "completed",
                }
            )
        ):
            activity = "no_submission" if outcome == "no_action" else outcome
    return {
        "run_id": run_id,
        "source_object": None,
        "source_objects": [],
        "started_at": _iso(started),
        "finished_at": _iso(finished),
        "object_updated_at": None,
        "report_status": status,
        "execution_lane": lane,
        "activity": activity,
        "run_time_known": started is not None or finished is not None,
        "evidence": {
            "execution_status": execution,
            "broker_submission_done": broker,
            "action_done": action,
            "orders_pending_count": count,
            "errors_present": errors,
            "receipt_outcome": outcome,
            "receipt_broker_confirmation": receipt.get("broker_confirmation"),
            "receipt_state": receipt_state,
            "receipt_id": receipt.get("receipt_id"),
        },
    }
