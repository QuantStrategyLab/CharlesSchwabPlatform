"""Privacy-safe Schwab daily projection of reports a caller has already read.

This is a pure adapter, not a reader, publisher, account binding, or fill source.
The sole target is the deployed live service/profile/scope below. Callers supply
already-confirmed schedule facts in the existing daily ``schedule`` shape and
explicitly declare complete read coverage; neither input authenticates a source.
No cron, calendar, object listing, account lookup, or broker operation occurs.
A future receiver must independently resolve and verify the real account binding.

Business dates use New York run start time (finish time only if start is absent).
Object modification times never establish business dates or execution evidence.
Raw identifiers, URIs, errors, account observations, symbols and orders are not
returned. Run IDs are opaque digests so conflicting copies remain detectable.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from quant_platform_kit.common.execution_receipts import attach_execution_receipt

from application.account_observation import expected_account_hash_from_selector
from scripts.publish_account_facts_from_reports import _report_uri_parts
from scripts.runtime_heartbeat_policy import match_payload_target, target_key

_TARGET = {
    "service": "charles-schwab-quant-service",
    "strategy_profile": "soxl_soxx_trend_income",
    "account_scope": "live",
}
_ZONE = ZoneInfo("America/New_York")
_SCHEDULE_FIELDS = frozenset(
    {
        "state",
        "business_date",
        "timezone",
        "latest_due_at",
        "next_due_at",
        "grace_ends_at",
        "publication_grace_ended",
        "expected_window",
        "reason",
    }
)
_SCHEDULE_STATES = frozenset(
    {"due", "within_grace", "not_due", "market_closed", "outside_window"}
)
_ANOMALIES = {"reconciliation_required": 0, "unknown": 1, "blocked": 2, "failed": 3}
# A completed run cannot hide a different run still awaiting completion.
_ORDERS = {"submitted": 0, "broker_acknowledged": 1, "partially_filled": 2, "filled": 3}
_NON_REAL_LANES = frozenset({"dry_run", "shadow", "validation"})
_QUIET = frozenset({"no_signal", "no_rebalance", "no_submission"})
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


def project_daily_runtime(
    *,
    target: Mapping[str, Any],
    reports: Sequence[Mapping[str, Any]],
    observed_at: dt.datetime,
    business_date: dt.date | None = None,
    schedule_facts: Mapping[str, Any] | None = None,
    coverage_complete: bool = False,
    read_errors: Sequence[str] = (),
    expected_account_hash: str | None = None,
) -> dict[str, Any]:
    """Return the existing records/runs/schedule/completeness/fills shape.

    ``reports`` accepts raw runtime reports or ``{payload, object_uri,
    object_updated_at}`` envelopes. Optional object URIs must use the existing
    archived report path. Their bucket is not authenticated by this adapter.
    Unknown/mismatched schedule fields fail closed. ``coverage_complete=True``
    is a caller assertion, not evidence that a listing or scheduler is genuine.
    The function never derives a receipt, account key, fill record or zero fills.
    Native/explicit-empty selectors require independent expected_account_hash
    context from the source caller. This parameter is not authentication and is
    never inferred from the report; the default retains only old legacy forms.
    """
    if not isinstance(target, Mapping) or dict(target) != _TARGET:
        raise ValueError("invalid_target")
    observed = _instant(observed_at)
    if not isinstance(observed_at, dt.datetime) or observed is None:
        raise ValueError("invalid_observed_at")
    day = (
        business_date
        if business_date is not None
        else observed.astimezone(_ZONE).date()
    )
    if type(day) is not dt.date or day > observed.astimezone(_ZONE).date():
        raise ValueError("invalid_business_date")
    schedule = _schedule(schedule_facts, day, observed)
    read_failed = bool(read_errors) or coverage_complete is not True
    errors = ["report_read_error"] * len(read_errors)
    if coverage_complete is not True:
        errors.append("coverage_unconfirmed")
    runs: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    for item in reports:
        payload = item.get("payload", item) if isinstance(item, Mapping) else None
        reason = _identity_problem(payload, expected_account_hash=expected_account_hash)
        if not reason and not _valid_source(item):
            reason = "invalid_source_object"
        if reason:
            if reason in {"invalid_report", "invalid_source_object"}:
                read_failed = True
                errors.append("report_rejected")
            unmatched.append({"reason": reason})
            continue
        assert isinstance(payload, Mapping)
        run = _run(payload, observed)
        started, finished = (
            _instant(payload.get("started_at")),
            _instant(payload.get("finished_at")),
        )
        problem = _time_problem(payload, started, finished, observed)
        if problem:
            excluded.append({"run_id": run["run_id"], "reason": problem})
            # An undated anomaly remains visible, but can never certify a day.
            if problem == "missing_run_time" and run["activity"] in _ANOMALIES:
                runs.append(run)
            continue
        run_at = started or finished
        assert run_at is not None
        run_day = run_at.astimezone(_ZONE).date()
        if run_day != day:
            if run_day > day or run["activity"] not in {
                "reconciliation_required",
                "unknown",
            }:
                excluded.append(
                    {"run_id": run["run_id"], "reason": "other_business_date"}
                )
                continue
        runs.append(run)
    runs, conflicts = _dedupe(runs)
    invalid_time = any(item["reason"] != "other_business_date" for item in excluded)
    status, kind, completeness = _judge(
        runs, conflicts, schedule, day, read_failed, invalid_time
    )
    lanes = {run["execution_lane"] for run in runs}
    record = {
        "platform": "schwab",
        "target_key": target_key(_TARGET),
        "target": dict(_TARGET),
        "business_date": day.isoformat(),
        "timezone": _ZONE.key,
        "observed_at": _iso(observed),
        "status": status,
        "kind": kind,
        "completeness": completeness,
        "execution_lane": next(iter(lanes)) if len(lanes) == 1 else "insufficient",
        "schedule": schedule,
        "runs": runs,
        "excluded_reports": excluded,
        "conflicts": conflicts,
        "fills": {"source": "not_connected", "records": [], "count": None},
    }
    return {
        "platform": "schwab",
        "observed_at": _iso(observed),
        "completeness": "complete" if completeness == "complete" else "incomplete",
        "read_errors": errors,
        "records": [record],
        "unmatched_reports": unmatched,
    }


def _scope_problem(payload: object) -> str | None:
    """Validate report/target namespaces, not the private account binding."""
    if (
        not isinstance(payload, Mapping)
        or payload.get("schema_version") != "runtime_report.v1"
    ):
        return "invalid_report"
    if (
        payload.get("platform") != "charles_schwab"
        or payload.get("project_id") != "charlesschwabquant"
    ):
        return "wrong_platform"
    runtime = payload.get("runtime_target")
    if not isinstance(runtime, Mapping):
        return "wrong_target"
    identity, _ = match_payload_target(payload, [dict(_TARGET)])
    if identity != target_key(_TARGET):
        return "wrong_target"
    # The shared matcher prefers top-level fields. Reject contradictory aliases
    # rather than letting that preference hide another nested service or scope.
    for source in (payload, runtime):
        for names, expected in (
            (("service_name", "service", "cloud_run_service"), _TARGET["service"]),
            (("strategy_profile", "strategy", "profile"), _TARGET["strategy_profile"]),
            (
                ("account_scope", "account_group", "account_region"),
                _TARGET["account_scope"],
            ),
        ):
            if any(
                source.get(key) is not None and source[key] != expected for key in names
            ):
                return "wrong_target"
    if (
        runtime.get("strategy_profile") != _TARGET["strategy_profile"]
        or runtime.get("account_scope") != "live"
    ):
        return "wrong_target"
    if "account_selector" in runtime:
        selectors = runtime["account_selector"]
        if (
            type(selectors) is not list
            or len(selectors) > 1
            or any(
                not isinstance(value, str) or not value or value != value.strip()
                for value in selectors
            )
        ):
            return "wrong_target"
    if "platform_id" in runtime and runtime["platform_id"] != "schwab":
        return "wrong_platform"
    return None


def _account_identity_problem(
    payload: Mapping[str, Any],
    expected_account_hash: str,
) -> str | None:
    """Compare to caller-provided identity; preserve original container errors."""
    summary = payload.get("summary", {})
    observation = summary.get("account_observation", {})
    report_hash = observation.get("account_hash")
    if report_hash != expected_account_hash:
        if "account_observation" not in summary:
            return "source_observation_missing"
        if "account_hash" not in observation:
            return "source_hash_missing"
        if (
            not isinstance(report_hash, str)
            or not report_hash
            or report_hash != report_hash.strip()
        ):
            return "source_identity_invalid_shape"
        return "source_identity_mismatch"
    selector_hash = expected_account_hash_from_selector(
        payload["runtime_target"].get("account_selector")
    )
    if selector_hash is not None and selector_hash != expected_account_hash:
        return "source_selector_mismatch"
    return None


def _identity_problem(
    payload: object,
    *,
    expected_account_hash: str | None = None,
) -> str | None:
    problem = _scope_problem(payload)
    if problem is not None:
        return problem
    runtime = payload["runtime_target"]
    if expected_account_hash is None:
        # Preserve the old unbound legacy forms only. New native/empty selector
        # forms require independent caller context, never the report's own hash.
        return (
            None
            if "account_selector" not in runtime
            or runtime["account_selector"] == ["live"]
            else "wrong_target"
        )
    if (
        not isinstance(expected_account_hash, str)
        or not expected_account_hash
        or expected_account_hash != expected_account_hash.strip()
        or len(expected_account_hash) > 512
    ):
        return "wrong_target"
    try:
        return (
            "wrong_target"
            if _account_identity_problem(payload, expected_account_hash)
            else None
        )
    except Exception:
        return "wrong_target"


def _valid_source(item: Mapping[str, Any]) -> bool:
    for key in ("object_uri", "source_object"):
        if key not in item or item[key] is None:
            continue
        uri = item[key]
        if not isinstance(uri, str):
            return False
        try:
            parts = urlsplit(uri)
            prefix = f"gs://{parts.netloc}/execution-reports/charles_schwab/soxl_soxx_trend_income/"
            month, stamp = _report_uri_parts(uri, prefix)
            written = dt.datetime.strptime(stamp, "%Y%m%dT%H%M%SZ")
            if month != written.strftime("%Y-%m"):
                return False
        except (ValueError, TypeError):
            return False
    if item.get("object_uri") and item.get("source_object"):
        return item["object_uri"] == item["source_object"]
    return True


def _schedule(facts: object, day: dt.date, observed: dt.datetime) -> dict[str, Any]:
    result = {
        "state": "unevaluable",
        "business_date": day.isoformat(),
        "timezone": _ZONE.key,
        "latest_due_at": None,
        "next_due_at": None,
        "grace_ends_at": None,
        "publication_grace_ended": None,
        "expected_window": "unspecified",
        "reason": "schedule_missing" if facts is None else "schedule_invalid",
    }
    if not isinstance(facts, Mapping) or set(facts) - _SCHEDULE_FIELDS:
        return result
    state, reason = facts.get("state"), facts.get("reason")
    reasons = {
        "due": {"publication_grace_ended"},
        "within_grace": {"publication_grace_open"},
        "not_due": {"before_schedule", "no_cron_on_business_date"},
        "market_closed": {"market_closed"},
        "outside_window": {"outside_expected_window"},
    }
    if (
        not isinstance(state, str)
        or state not in _SCHEDULE_STATES
        or not isinstance(reason, str)
        or reason not in reasons[state]
        or facts.get("business_date") not in (day, day.isoformat())
        or facts.get("timezone") != _ZONE.key
    ):
        return result
    times = {
        name: _instant(facts.get(name))
        for name in ("latest_due_at", "next_due_at", "grace_ends_at")
    }
    if any(
        facts.get(name) is not None and value is None for name, value in times.items()
    ):
        return result
    latest, next_due, grace = (
        times[name] for name in ("latest_due_at", "next_due_at", "grace_ends_at")
    )
    ended, window = facts.get("publication_grace_ended"), facts.get("expected_window")
    if (
        (ended is not None and type(ended) is not bool)
        or not isinstance(window, str)
        or window not in {"unspecified", "inside", "outside"}
    ):
        return result
    if latest is not None and (
        latest > observed or latest.astimezone(_ZONE).date() != day
    ):
        return result
    if next_due is not None and (
        next_due <= observed or next_due.astimezone(_ZONE).date() != day
    ):
        return result
    if grace is not None and (latest is None or grace < latest):
        return result
    if state in {"due", "within_grace"}:
        if (
            latest is None
            or grace is None
            or next_due is not None
            or window == "outside"
        ):
            return result
        if (state == "due" and (ended is not True or grace > observed)) or (
            state == "within_grace" and (ended is not False or grace <= observed)
        ):
            return result
    elif state == "not_due":
        if (
            latest is not None
            or grace is not None
            or ended is not None
            or window == "outside"
        ):
            return result
        if (reason == "before_schedule") != (next_due is not None):
            return result
    elif state == "market_closed":
        if latest is None or grace is not None or ended is not None:
            return result
    elif (
        window != "outside"
        or ended is not None
        or ((latest is None) != (grace is None))
        or (grace is not None and next_due is not None)
    ):
        return result
    result.update(
        {
            "state": state,
            "publication_grace_ended": ended,
            "expected_window": window,
            "reason": reason,
        }
    )
    result.update({name: _iso(value) for name, value in times.items()})
    return result


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


def _run(payload: Mapping[str, Any], observed: dt.datetime) -> dict[str, Any]:
    summary = (
        payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    )
    runtime = payload["runtime_target"]
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
            # Validate only the existing receipt. A shallow copy avoids changing
            # the original report; this API does not synthesize a new receipt.
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


def _time_problem(
    payload: Mapping[str, Any],
    started: dt.datetime | None,
    finished: dt.datetime | None,
    observed: dt.datetime,
) -> str | None:
    if any(
        payload.get(key) is not None and value is None
        for key, value in (("started_at", started), ("finished_at", finished))
    ):
        return "invalid_run_time"
    if started is None and finished is None:
        return "missing_run_time"
    if started is not None and finished is not None and finished < started:
        return "inverted_run_time"
    if any(value is not None and value > observed for value in (started, finished)):
        return "future_run_time"
    return None


def _dedupe(runs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    groups: dict[str, set[str]] = {}
    unique = []
    for run in runs:
        identity = run["run_id"]
        signature = json.dumps(run, sort_keys=True)
        if identity is None:
            unique.append(run)
            continue
        signatures = groups.setdefault(identity, set())
        if signature not in signatures:
            signatures.add(signature)
            unique.append(run)
    return unique, [
        identity for identity, signatures in groups.items() if len(signatures) > 1
    ]


def _judge(
    runs: list[dict[str, Any]],
    conflicts: list[str],
    schedule: Mapping[str, Any],
    day: dt.date,
    read_failed: bool,
    invalid_time: bool,
) -> tuple[str, str, str]:
    incomplete_evidence = (
        invalid_time
        or bool(conflicts)
        or any(
            run["run_id"] is None
            or run["evidence"]["receipt_state"] != "valid"
            or not run["run_time_known"]
            for run in runs
        )
    )
    completeness = (
        "incomplete"
        if read_failed
        else "insufficient"
        if incomplete_evidence or schedule["state"] == "unevaluable"
        else "complete"
    )
    anomalies = [run["activity"] for run in runs if run["activity"] in _ANOMALIES]
    if anomalies:
        return min(anomalies, key=_ANOMALIES.__getitem__), "run", completeness
    if conflicts:
        return "conflict", "incomplete", completeness
    if read_failed:
        return "read_incomplete", "incomplete", "incomplete"
    if (
        incomplete_evidence
        or schedule["state"] == "unevaluable"
        or any(run["activity"] == "insufficient" for run in runs)
    ):
        return "insufficient", "incomplete", "insufficient"
    latest = _instant(schedule["latest_due_at"])
    covering = []
    for run in runs:
        started, finished = _instant(run["started_at"]), _instant(run["finished_at"])
        if (
            latest is not None
            and started is not None
            and finished is not None
            and started >= latest
            and started.astimezone(_ZONE).date() == day
        ):
            covering.append(run)
    if covering:
        lanes = {run["execution_lane"] for run in covering}
        activities = {run["activity"] for run in covering}
        if len(lanes) == 1 and lanes <= _NON_REAL_LANES:
            return next(iter(lanes)), "run", "complete"
        if len(lanes) != 1:
            return "insufficient", "incomplete", "insufficient"
        orders = activities & _ORDERS.keys()
        if orders:
            return min(orders, key=_ORDERS.__getitem__), "run", "complete"
        if (
            len(activities) == 1
            and activities <= _QUIET
            and schedule["state"] in {"due", "within_grace"}
        ):
            return next(iter(activities)), "run", "complete"
        return "insufficient", "incomplete", "insufficient"
    if schedule["state"] == "due":
        return "missing_report", "incomplete", "incomplete"
    return str(schedule["state"]), "schedule", "complete"


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
