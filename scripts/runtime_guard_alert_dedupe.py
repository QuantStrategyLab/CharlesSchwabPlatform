"""Runtime-guard alert de-duplication (NOTIFY-04 B5).

The guard runs every 30 minutes with a 180-minute log lookback, so one Cloud
Run / Cloud Scheduler failure entry is re-reported by up to six consecutive
runs. This module decides whether an alert is a pure repeat:

* new failure log entries (by event id) → always alert;
* a new issue type (normalized issue text fingerprint) → always alert;
* the same issues with no new entries → repeat; re-alert after
  ``RUNTIME_GUARD_ALERT_DEDUPE_REMINDER_HOURS`` (default 6).

Modes (``RUNTIME_GUARD_ALERT_DEDUPE_MODE``):

* ``off`` (default when unset): no state, behaviour unchanged;
* ``dry_run``: never suppresses; logs ``would_suppress`` and keeps state as if
  ``on`` were active, so the effect can be observed before switching;
* ``on``: suppresses pure repeats.

State is a small JSON file (hashes and timestamps only, no log text, no
account data) persisted between runs by the workflow cache step.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

MODES = ("off", "dry_run", "on")
DEFAULT_STATE_PATH = ".runtime-guard-dedupe/state.json"
DEFAULT_REMINDER_HOURS = 6.0
EVENT_RETENTION_HOURS = 48.0
STATE_VERSION = 1


@dataclass(frozen=True)
class DedupeDecision:
    mode: str
    send: bool
    would_suppress: bool
    new_event_count: int
    new_issue_fingerprint: bool
    reason: str

    def log_line(self) -> str:
        return (
            "runtime_guard_dedupe "
            f"mode={self.mode} send={str(self.send).lower()} "
            f"would_suppress={str(self.would_suppress).lower()} "
            f"new_events={self.new_event_count} "
            f"new_issue_type={str(self.new_issue_fingerprint).lower()} reason={self.reason}"
        )


def resolve_mode(raw: str | None = None) -> str:
    value = str(raw if raw is not None else os.environ.get("RUNTIME_GUARD_ALERT_DEDUPE_MODE") or "").strip().lower()
    return value if value in MODES else "off"


def _state_path(raw: str | None = None) -> Path:
    return Path(raw or os.environ.get("RUNTIME_GUARD_DEDUPE_STATE_PATH") or DEFAULT_STATE_PATH)


def _reminder_hours() -> float:
    try:
        value = float(os.environ.get("RUNTIME_GUARD_ALERT_DEDUPE_REMINDER_HOURS") or DEFAULT_REMINDER_HOURS)
    except ValueError:
        return DEFAULT_REMINDER_HOURS
    return value if value > 0 else DEFAULT_REMINDER_HOURS


def event_id(entry: Mapping[str, Any]) -> str:
    """Stable id for one log entry (Cloud Logging insertId preferred)."""

    insert_id = str(entry.get("insertId") or "").strip()
    if insert_id:
        basis = f"insert:{insert_id}"
    else:
        resource = entry.get("resource") if isinstance(entry.get("resource"), Mapping) else {}
        basis = json.dumps(
            {
                "timestamp": entry.get("timestamp"),
                "logName": entry.get("logName"),
                "resource": resource.get("labels") if isinstance(resource, Mapping) else None,
                "severity": entry.get("severity"),
            },
            sort_keys=True,
            default=str,
        )
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24]


def issue_fingerprint(name: str, issues: Sequence[str]) -> str:
    """Issue-type fingerprint; counts/minutes are normalized so N→N+1 is the same type."""

    normalized = sorted({re.sub(r"\d+", "#", str(item)).strip() for item in issues})
    basis = json.dumps({"name": name, "issues": normalized}, ensure_ascii=False)
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:24]


def _parse(ts: Any) -> dt.datetime | None:
    try:
        value = dt.datetime.fromisoformat(str(ts))
    except (TypeError, ValueError):
        return None
    return value if value.tzinfo else value.replace(tzinfo=dt.timezone.utc)


def _load(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"version": STATE_VERSION, "events": {}, "issues": {}}
    if not isinstance(payload, dict) or payload.get("version") != STATE_VERSION:
        return {"version": STATE_VERSION, "events": {}, "issues": {}}
    payload.setdefault("events", {})
    payload.setdefault("issues", {})
    return payload


def _save(path: Path, state: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def _prune(state: dict[str, Any], now: dt.datetime) -> None:
    horizon = now - dt.timedelta(hours=EVENT_RETENTION_HOURS)
    state["events"] = {
        key: value
        for key, value in dict(state.get("events") or {}).items()
        if (_parse(value) or now) >= horizon
    }


def decide(
    *,
    name: str,
    issues: Sequence[str],
    event_ids: Iterable[str],
    now: dt.datetime | None = None,
    mode: str | None = None,
    state_path: str | None = None,
) -> DedupeDecision:
    """Decide whether to send this alert and record it in the state."""

    resolved = resolve_mode(mode)
    ids = sorted({str(item) for item in event_ids if str(item)})
    if resolved == "off":
        return DedupeDecision(resolved, True, False, len(ids), True, "dedupe_off")
    stamp = now or dt.datetime.now(dt.timezone.utc)
    path = _state_path(state_path)
    state = _load(path)
    _prune(state, stamp)
    events: dict[str, str] = state["events"]
    issues_state: dict[str, str] = state["issues"]

    new_ids = [item for item in ids if item not in events]
    fingerprint = issue_fingerprint(name, issues)
    last_alert = _parse(issues_state.get(fingerprint))
    new_type = last_alert is None
    reminder_due = (
        last_alert is not None
        and stamp - last_alert >= dt.timedelta(hours=_reminder_hours())
    )
    if new_ids:
        repeat, reason = False, "new_failure_events"
    elif new_type:
        repeat, reason = False, "new_issue_type"
    elif reminder_due:
        repeat, reason = False, "reminder_due"
    else:
        repeat, reason = True, "repeat_within_reminder_window"

    if not repeat:
        for item in ids:
            events.setdefault(item, stamp.isoformat())
        issues_state[fingerprint] = stamp.isoformat()
    else:
        for item in ids:
            events.setdefault(item, stamp.isoformat())
    _save(path, state)

    send = not (repeat and resolved == "on")
    return DedupeDecision(resolved, send, repeat, len(new_ids), new_type, reason)


def clear_issues(*, mode: str | None = None, state_path: str | None = None, now: dt.datetime | None = None) -> None:
    """Healthy run: forget issue fingerprints so a recurrence alerts immediately."""

    if resolve_mode(mode) == "off":
        return
    stamp = now or dt.datetime.now(dt.timezone.utc)
    path = _state_path(state_path)
    state = _load(path)
    _prune(state, stamp)
    state["issues"] = {}
    _save(path, state)
