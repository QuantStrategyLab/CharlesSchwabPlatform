"""Opt-in notification facts and routing; no broker, transport, or storage IO.

Consumers inject their existing candidate producer and durable marker storage.
This module neither changes receipt acceptance nor replaces the central daily
recipient/report-date guard. Unknown evidence is never converted to zero.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

from .digest_routing import daily_digest_enabled

Route = Literal["immediate", "digest_candidate", "legacy"]
EVENT_KINDS = frozenset({
    "routine_cycle", "healthy_heartbeat", "order_submitted", "fill",
    "receipt_missing", "receipt_stale", "receipt_unacceptable", "auth_failure",
    "order_status_unknown", "risk_escalation", "real_error", "blocked_run",
    "expected_block_observed",
})
ROUTINE_EVENT_KINDS = frozenset({"routine_cycle", "healthy_heartbeat"})
ORDER_EVENT_KINDS = frozenset({"order_submitted", "fill"})
SGT = ZoneInfo("Asia/Singapore")


def unified_notification_enabled(env: Mapping[str, object], key: str) -> bool:
    """Read a release flag: only boolean True or the string 'true' enables it."""
    value = env.get(key)
    return value is True or isinstance(value, str) and value.strip().lower() == "true"


def effective_notification_mode(env: Mapping[str, object], flag_key: str) -> str:
    """Map the release boolean onto the existing single routing mode."""
    if unified_notification_enabled(env, flag_key):
        return "daily_digest"
    return str(env.get("QSL_ROUTINE_NOTIFICATION_MODE") or "immediate")


def select_notification_route(*, event_kind: str, env: Mapping[str, object], flag_key: str) -> Route:
    """OFF delegates untouched to legacy. Unknown/blocked/urgent stays immediate.

    The new contract defaults routine orders to immediate. Legacy routing's
    historical order default is deliberately not changed by this API.
    """
    if not unified_notification_enabled(env, flag_key):
        return "legacy"
    if daily_digest_enabled(effective_notification_mode(env, flag_key)):
        if event_kind in ROUTINE_EVENT_KINDS:
            return "digest_candidate"
        if event_kind in ORDER_EVENT_KINDS and unified_notification_enabled(env, "QSL_DIGEST_ROUTINE_ORDERS"):
            return "digest_candidate"
    return "immediate"


@dataclass(frozen=True)
class BusinessWindow:
    start: datetime
    end: datetime
    business_day: date
    report_date: date
    window_id: str


def sgt_business_window(occurred_at: datetime) -> BusinessWindow:
    """Containing half-open [D 06:00, D+1 06:00) window; not a send guard."""
    if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
        raise ValueError("notification_time_timezone_missing")
    day = (occurred_at.astimezone(SGT) - timedelta(hours=6)).date()
    start = datetime.combine(day, time(6), SGT)
    end = start + timedelta(days=1)
    return BusinessWindow(start, end, day, end.date(), f"sgt-0600:{day.isoformat()}")


def _digest(parts: list[object]) -> str:
    return hashlib.sha256(json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def order_identity_digest(order_or_execution_id: str) -> str:
    """Irreversible, domain-separated identity; never put raw broker IDs in keys."""
    if not isinstance(order_or_execution_id, str) or not order_or_execution_id.strip():
        raise ValueError("notification_order_identity_missing")
    return _digest(["qsl.notification_order.v1", order_or_execution_id])


@dataclass(frozen=True)
class NotificationEvent:
    event_kind: str
    occurred_at: datetime
    platform_id: str | None = None
    opaque_account_uid: str | None = None
    target_id: str | None = None
    strategy_profile: str | None = None
    source_run_id: str | None = None
    order_execution_digest: str | None = None
    incident_reason: str | None = None
    order_count: int | None = None
    fill_count: int | None = None
    expected_block_evidence: str | None = None

    def __post_init__(self) -> None:
        sgt_business_window(self.occurred_at)
        for count in (self.order_count, self.fill_count):
            if count is not None and (type(count) is not int or count < 0):
                raise ValueError("notification_count_invalid")
        digest = self.order_execution_digest
        if digest is not None and (len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)):
            raise ValueError("notification_order_digest_invalid")
        if self.event_kind == "expected_block_observed" and not _identity_present(self.expected_block_evidence):
            raise ValueError("notification_expected_block_evidence_missing")

    @property
    def coverage_status(self) -> str:
        return "known" if routine_dedupe_key(self) is not None else "unknown"

    def to_candidate(self) -> dict[str, object]:
        """Private producer fact, to be projected with existing digest_candidates.

        This event envelope is not a replacement for qsl.digest_candidates.v1;
        the existing archive projector remains responsible for window counts.
        """
        result = asdict(self)
        window = sgt_business_window(self.occurred_at)
        result.update(
            schema_version="qsl.notification_event.v1", occurred_at=self.occurred_at.isoformat(),
            business_day=window.business_day.isoformat(), report_date=window.report_date.isoformat(),
            window_id=window.window_id, dedupe_key=routine_dedupe_key(self),
            coverage_status=self.coverage_status,
        )
        if self.coverage_status == "unknown":
            result.update(order_count=None, fill_count=None)
        return result


def _identity_present(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def routine_dedupe_key(event: NotificationEvent) -> str | None:
    parts = [event.platform_id, event.opaque_account_uid, event.target_id,
             event.strategy_profile, event.source_run_id, event.event_kind]
    if event.event_kind in ORDER_EVENT_KINDS:
        parts.append(event.order_execution_digest)
    return _digest(["qsl.notification_routine.v1", *parts]) if all(map(_identity_present, parts)) else None


class CandidatePersistenceError(RuntimeError):
    pass


@dataclass(frozen=True)
class RoutingResult:
    route: Route
    candidate_persisted: bool
    result: object = None


def route_notification_event(
    event: NotificationEvent, *, env: Mapping[str, object], flag_key: str,
    persist_candidate: Callable[[dict[str, object]], bool],
    legacy: Callable[[], object], immediate: Callable[[], object],
) -> RoutingResult:
    """Persist before deferral. Alert delivery survives candidate-write failure.

    The immediate callback owns durable incident reservation and receipt ACK.
    Callers must inspect candidate_persisted; deferral is never delivery.
    """
    route = select_notification_route(event_kind=event.event_kind, env=env, flag_key=flag_key)
    if route == "legacy":
        return RoutingResult(route, False, legacy())
    if route == "digest_candidate":
        if event.coverage_status != "known":
            raise CandidatePersistenceError("notification_candidate_identity_unknown")
        if persist_candidate(event.to_candidate()) is not True:
            raise CandidatePersistenceError("notification_candidate_write_unconfirmed")
        return RoutingResult(route, True)
    try:
        persisted = persist_candidate(event.to_candidate()) is True
    except Exception:
        # A routine evidence-store failure cannot suppress a real alert.
        persisted = False
    return RoutingResult(route, persisted, immediate())


class IncidentMarkerStore(Protocol):
    """Durable JSON state, with atomic compare-and-set across workers.

    Missing key -> (None, None); read failures MUST raise, not act like missing.
    A version is an opaque storage generation. No non-atomic has/write adapter
    is provided: existing alert_marker has no atomic claim or readable receipt.
    """
    def read(self, key: str) -> tuple[object, Mapping[str, object] | None]: ...

    def compare_and_set(self, key: str, version: object, value: Mapping[str, object]) -> bool: ...


@dataclass(frozen=True)
class IncidentDecision:
    action: Literal["send", "duplicate", "recovered", "coverage_unknown"]
    incident_key: str | None = None
    transition_key: str | None = None


class IncidentTracker:
    """Reserve before sending; pending/uncertain is never blindly resent.

    The caller supplies ordered observations and explicit recovery evidence.
    Merely seeing a healthy label elsewhere is not proof of recovery. Severity
    is a caller-owned nonnegative rank; a rise is a new escalation. Storage
    errors propagate; a process-local set is never a production fallback.
    """
    def __init__(self, store: IncidentMarkerStore):
        self.store = store

    def _read_marker(self, key: str) -> tuple[object, dict[str, object] | None]:
        version, value = self.store.read(key)
        if version is None and value is None:
            return version, None
        required = {"schema_version", "active", "state", "severity", "generation",
                    "transition_key", "delivery_status", "receipt_id"}
        if (version is None or not isinstance(value, Mapping) or not required.issubset(value)
                or value["schema_version"] != "qsl.notification_incident.v1"
                or type(value["active"]) is not bool
                or not _identity_present(value["state"])
                or any(type(value[k]) is not int or value[k] < 0 for k in ("severity", "generation"))
                or value["delivery_status"] not in {"not_attempted", "pending", "uncertain", "delivered"}
                or (value["generation"] > 0 and not _identity_present(value["transition_key"]))):
            raise RuntimeError("notification_marker_history_incomplete")
        return version, dict(value)

    def observe(self, event: NotificationEvent, *, state: str, severity: int = 1,
                recovered: bool = False) -> IncidentDecision:
        parts = [event.platform_id, event.target_id, event.opaque_account_uid,
                 event.strategy_profile, event.incident_reason]
        if not all(map(_identity_present, parts)):
            return IncidentDecision("coverage_unknown")
        if not _identity_present(state) or type(severity) is not int or severity < 0:
            raise ValueError("notification_incident_state_invalid")
        key = _digest(["qsl.notification_incident.v1", *parts])
        for _ in range(8):
            version, previous = self._read_marker(key)
            previous = previous or {
                "schema_version": "qsl.notification_incident.v1", "active": False,
                "state": None, "severity": 0, "generation": 0,
                "transition_key": None, "delivery_status": "not_attempted", "receipt_id": None,
            }
            active = previous.get("active") is True
            generation = int(previous.get("generation", 0))
            transition = previous.get("transition_key")
            if recovered:
                value = {**previous, "active": False, "state": state, "severity": severity}
                action = "recovered"
            elif active and severity <= int(previous["severity"]):
                # A downgrade updates observation state without sending. Later
                # severity-up remains visible, even before full recovery.
                value = {**previous, "state": state, "severity": severity}
                action = "duplicate"
            else:
                generation += 1
                transition = _digest([key, generation, previous.get("state"), state, severity])
                value = {"schema_version": "qsl.notification_incident.v1",
                         "active": True, "state": state, "severity": severity,
                         "generation": generation, "transition_key": transition,
                         "delivery_status": "pending", "receipt_id": None}
                action = "send"
            if value == previous or self.store.compare_and_set(key, version, value):
                return IncidentDecision(action, key, transition)
        raise RuntimeError("notification_marker_concurrent_update")

    def record_delivery(self, decision: IncidentDecision, *, receipt_id: str | None = None) -> None:
        """Only an explicit receipt is delivered; absent receipt stays uncertain."""
        if decision.action != "send" or not decision.incident_key:
            raise ValueError("notification_delivery_without_reservation")
        version, previous = self._read_marker(decision.incident_key)
        if not previous or previous.get("transition_key") != decision.transition_key:
            raise RuntimeError("notification_marker_transition_changed")
        value = {**previous, "delivery_status": "delivered" if _identity_present(receipt_id) else "uncertain",
                 "receipt_id": receipt_id if _identity_present(receipt_id) else None}
        if not self.store.compare_and_set(decision.incident_key, version, value):
            raise RuntimeError("notification_marker_concurrent_update")
