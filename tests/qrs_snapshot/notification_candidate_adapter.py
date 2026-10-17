"""QPK event facts -> existing private digest rows, without IO or alert sends."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import fields
from datetime import datetime
from typing import Any, Mapping, Sequence

import daily_digest_notify as digest
from notification_snapshot.unified_contract import NotificationEvent, sgt_business_window

PLATFORMS = ("ibkr", "schwab", "longbridge", "binance", "firstrade", "qrs")
ROUTINE_KINDS = {"routine_cycle", "healthy_heartbeat"}
IDENTITY = ("platform_id", "opaque_account_uid", "target_id", "strategy_profile", "source_run_id")


def _event(raw: Mapping[str, Any]) -> NotificationEvent:
    if raw.get("schema_version") != "qsl.notification_event.v1":
        raise ValueError("notification_event_schema_invalid")
    try:
        if not isinstance(raw.get("event_kind"), str) or not raw["event_kind"].strip():
            raise ValueError("notification_event_kind_invalid")
        for name in (*IDENTITY, "incident_reason", "expected_block_evidence", "order_execution_digest"):
            if raw.get(name) is not None and not isinstance(raw[name], str):
                raise ValueError("notification_event_identity_invalid")
        values = {field.name: raw[field.name] for field in fields(NotificationEvent) if field.name in raw}
        values["occurred_at"] = datetime.fromisoformat(values["occurred_at"].replace("Z", "+00:00"))
        event = NotificationEvent(**values)
        projected = event.to_candidate()
        for name in ("business_day", "report_date", "window_id", "dedupe_key", "coverage_status"):
            if name in raw and raw[name] != projected[name]:
                raise ValueError("notification_event_projection_mismatch")
        return event
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValueError("notification_event_invalid") from None


def events_for(row: Mapping[str, Any]) -> list[NotificationEvent]:
    if "notification_events" in row:
        raw = row["notification_events"]
        if not isinstance(raw, list) or not all(isinstance(item, Mapping) for item in raw):
            raise ValueError("notification_events_invalid")
    elif "notification_event" in row:
        raw = [row["notification_event"]]
    elif row.get("schema_version") == "qsl.notification_event.v1":
        raw = [row]
    else:
        return []
    if not all(isinstance(item, Mapping) for item in raw):
        raise ValueError("notification_events_invalid")
    return [_event(item) for item in raw]


def _bind_identity(row, events):
    for event in events:
        for name in IDENTITY:
            value = getattr(event, name)
            if row.get(name) and value and row[name] != value:
                raise ValueError("notification_candidate_identity_mismatch")
            if value and not row.get(name):
                row[name] = value


def validate_run_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one business run; never accept a window aggregate as a run."""
    row = dict(raw)
    events = events_for(row)
    _bind_identity(row, events)
    if type(row.get("actually_ran")) is not bool:
        raise ValueError("notification_candidate_run_evidence_required")
    if not row["actually_ran"] and events:
        raise ValueError("notification_candidate_noncounting_events_unsupported")
    if row["actually_ran"]:
        run_id = row.get("source_run_id")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("notification_candidate_source_run_id_required")
        for event in events:
            if event.source_run_id != run_id:
                raise ValueError("notification_candidate_identity_mismatch")
        if row.get("schema_version") != "qsl.notification_event.v1":
            for name in ("order_count", "fill_count", "cycle_count"):
                if name not in row or (row[name] is not None and (type(row[name]) is not int or row[name] < 0)):
                    raise ValueError("notification_candidate_count_invalid")
            cycles, status = digest.resolve_count_field(row, "cycle_count")
            if status == "known" and cycles != 1:
                raise ValueError("notification_candidate_per_run_required")
    if any(name in row for name in (
        "source_runs", "source_run_ids", "events_by_run", "notification_source_runs", "notification_events_by_run"
    )):
        raise ValueError("notification_candidate_per_run_required")
    return row


def partition(rows: Sequence[Mapping[str, Any]], business_day: str):
    """Use event timestamps for new facts; legacy rows retain existing dates."""
    current, stale = [], []
    for raw in rows:
        row = validate_run_row(raw)
        events = events_for(row)
        _bind_identity(row, events)
        if events:
            days = {sgt_business_window(event.occurred_at).business_day.isoformat() for event in events}
            if len(days) != 1:
                raise ValueError("notification_events_span_windows")
            day = days.pop()
            if row.get("business_day") and row["business_day"] != day:
                raise ValueError("notification_candidate_window_mismatch")
            row["business_day"] = day
        else:
            day = str(row.get("business_day") or "")
        if day == business_day:
            current.append(row)
        else:
            row["business_day"] = day
            stale.append(row)
    return current, stale


def aggregate(rows: Sequence[Mapping[str, Any]], expected: Sequence[Mapping[str, Any]]):
    """Count each protected run once, keeping conflicting counts unknown.

    Counts belong to archive-projected run rows. Order/fill event counts are
    never added to those run counts or interpreted as fills. The same run's
    multiple producer/event observations are evidence, not additional runs.
    """
    grouped = {}
    event_ids = set()
    immediate_ids = set()
    unknown_event = False
    observed_targets = set()
    run_counts = defaultdict(int)
    unknown_run_platforms = set()
    allowed = {tuple(str(row[name]) for name in IDENTITY[:4]) for row in expected}
    for index, raw in enumerate(rows):
        row = validate_run_row(raw)
        events = events_for(row)
        _bind_identity(row, events)
        if row.get("schema_version") == "qsl.notification_event.v1":
            # A raw event fact is not an archive-projected run counter.
            # Accept its identity/incident, but require producer projection for
            # whole-run order/fill counts rather than treating event counts as it.
            row.update(order_count=None, fill_count=None, cycle_count=None)
            row["field_status"] = {"order_count": "unknown", "fill_count": "unknown", "cycle_count": "unknown"}
        for event in events:
            if event.coverage_status != "known":
                unknown_event = True
            key = event.to_candidate()["dedupe_key"]
            if key:
                event_ids.add(key)
                if event.event_kind not in ROUTINE_KINDS:
                    immediate_ids.add(key)
            else:
                # Keep unrelated unknown events separate; do not invent identity.
                if event.event_kind not in ROUTINE_KINDS:
                    immediate_ids.add(("unknown", index, len(immediate_ids)))
            if event.event_kind not in ROUTINE_KINDS | {"order_submitted", "fill"}:
                row["status"] = "alert"
            if event.event_kind == "blocked_run":
                row["rebalance_conclusion"] = "blocked"
        if row.get("actually_ran") is not True:
            # An explicit not-due row covers a target but is never a run.
            if row.get("actually_ran") is False and all(row.get(name) for name in IDENTITY[:4]):
                observed_targets.add(tuple(str(row[name]) for name in IDENTITY[:4]))
            continue
        identity = tuple(str(row.get(name) or "") for name in IDENTITY)
        known_identity = all(identity)
        if all(identity[:4]) and identity[:4] not in allowed:
            raise ValueError("notification_candidate_target_unexpected")
        if not known_identity:
            unknown_run_platforms.add(row["platform_id"])
        else:
            observed_targets.add(identity[:4])
        key = identity if known_identity else ("unknown", index)
        blocked = bool(row.get("block_reason")) or row.get("rebalance_conclusion") in {"blocked", "risk_blocked"} or row.get("signal_summary") in {"blocked", "risk_blocked"}
        if (row.get("status") == "alert" or blocked) and not any(
                event.event_kind not in ROUTINE_KINDS for event in events):
            immediate_ids.add(("run", key))
        previous = grouped.get(key)
        if previous is None:
            grouped[key] = row
            run_counts[row["platform_id"]] += 1
            continue
        merged = dict(previous)
        if (previous.get("schema_version") == "qsl.notification_event.v1"
                and row.get("schema_version") != "qsl.notification_event.v1"):
            # The merged result is an archive run row once archive facts arrive.
            merged.pop("schema_version", None)
        for name in ("order_count", "fill_count", "cycle_count"):
            old_value, old_status = digest.resolve_count_field(previous, name)
            value, status = digest.resolve_count_field(row, name)
            conflict_key = "_conflict_" + name
            if previous.get(conflict_key) or (old_status == status == "known" and old_value != value):
                merged[name] = None
                merged[conflict_key] = True
                merged["field_status"] = {**merged.get("field_status", {}), name: "unknown"}
            elif old_status != "known" and status == "known":
                merged[name] = value
                merged["field_status"] = {**merged.get("field_status", {}), name: "known"}
        # Keep only this run's events, including later monitor observations.
        facts = {}
        for event in (*events_for(previous), *events):
            fact = event.to_candidate()
            facts[event] = fact
        if facts:
            merged.pop("notification_event", None)
            merged["notification_events"] = list(facts.values())
        # Never overwrite an earlier alert with a later healthy snapshot.
        if row.get("status") == "alert":
            merged["status"] = "alert"
        for name in ("block_reason", "rebalance_conclusion", "execution_status"):
            if row.get(name) and (not merged.get(name) or name == "block_reason"):
                merged[name] = row[name]
        grouped[key] = merged
    # Raw observations do not permanently taint an archive-authoritative run.
    # Conflicts remain sticky on the final row even after later duplicates.
    count_fields = ("order_count", "fill_count", "cycle_count")
    conflicts = sum(bool(row.get("_conflict_" + name))
                    for row in grouped.values() for name in count_fields)
    unknown = unknown_event or bool(unknown_run_platforms) or any(
        digest.resolve_count_field(row, name)[1] != "known"
        for row in grouped.values() for name in count_fields
    )
    observed_targets &= allowed
    scope_unknown = bool(unknown_run_platforms or conflicts or allowed - observed_targets) or unknown_event
    unknown = unknown or scope_unknown
    inventory = {}
    for platform in PLATFORMS:
        targets = {tuple(str(row.get(name) or "") for name in IDENTITY[:4])
                   for row in expected if row.get("platform_id") == platform}
        inventory[platform] = dict(expected_targets=len(targets), covered_targets=len(targets & observed_targets),
                                   runs=None if platform in unknown_run_platforms else run_counts[platform])
    return list(grouped.values()), dict(source_inventory=inventory, counts_unknown=unknown, counts_scope_unknown=scope_unknown,
                                       run_count_unknown=bool(unknown_run_platforms),
                                       event_count=len(event_ids), immediate_events=len(immediate_ids),
                                       count_conflicts=conflicts)


def validate_expected(expected):
    if not isinstance(expected, list) or not expected:
        raise ValueError("digest_expected_targets_required")
    for row in expected:
        if not isinstance(row, Mapping) or row.get("platform_id") not in PLATFORMS or not all(
                isinstance(row.get(name), str) and row[name].strip() for name in IDENTITY[:4]):
            raise ValueError("digest_expected_targets_invalid")
