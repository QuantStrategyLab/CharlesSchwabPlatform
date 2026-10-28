"""Schwab producer/marker adapter for the notification-only QPK contract.

Private event facts are staged beside the existing archive producer. They are
not a second digest sender or a replacement for protected archive coverage.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import replace
from datetime import datetime, timezone

from .digest_snapshot.digest_candidates import cycle_evidence, project_reports
from .digest_snapshot.digest_routing import defer_routine_cycle_to_digest
from .digest_snapshot.unified_contract import (
    CandidatePersistenceError, IncidentTracker, NotificationEvent,
    effective_notification_mode, order_identity_digest, route_notification_event,
    select_notification_route, sgt_business_window, unified_notification_enabled,
)

FLAG = "SCHWAB_UNIFIED_NOTIFICATION_ENABLED"
_context = ContextVar("schwab_notification_report", default=None)


def enabled(env=None):
    return unified_notification_enabled(os.environ if env is None else env, FLAG)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _stamp(value):
    stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("notification_time_timezone_missing")
    return stamp


@contextmanager
def notification_context(report):
    if not enabled():
        yield
        return
    token = _context.set(report)
    try:
        yield
    finally:
        _context.reset(token)


class GCSNotificationStore:
    """Generation-conditional JSON writes; no local/in-process fallback."""
    def __init__(self, env=None, client=None):
        self.env = os.environ if env is None else env
        prefix = str(self.env.get("DIGEST_CANDIDATES_GCS_URI") or "").rstrip("/")
        if not prefix.startswith("gs://") or not prefix[5:].partition("/")[2] or any(c in prefix for c in "*?#\n "):
            raise ValueError("notification_store_configuration_missing")
        self.prefix = prefix + "/schwab"
        if client is None:
            from google.cloud import storage
            client = storage.Client()
        self.client = client

    def _blob(self, suffix):
        from google.cloud import storage
        return storage.Blob.from_string(self.prefix + "/" + suffix, client=self.client)

    def read(self, key):
        from google.api_core.exceptions import NotFound
        blob = self._blob("notification-markers/" + key + ".json")
        try:
            blob.reload(retry=None, timeout=30)
        except NotFound:
            return None, None
        generation = int(blob.generation)
        # A disappearing/changing object or an incomplete read is an error,
        # never permission to pretend the previous attempt did not exist.
        value = json.loads(blob.download_as_text(if_generation_match=generation, retry=None, timeout=30))
        if not isinstance(value, dict):
            raise ValueError("notification_marker_invalid")
        return generation, value

    def compare_and_set(self, key, version, value):
        from google.api_core.exceptions import PreconditionFailed
        blob = self._blob("notification-markers/" + key + ".json")
        try:
            blob.upload_from_string(json.dumps(value, allow_nan=False), content_type="application/json",
                if_generation_match=0 if version is None else version, retry=None, timeout=30)
            return True
        except PreconditionFailed:
            return False

    def persist(self, fact, projection=None):
        from google.api_core.exceptions import PreconditionFailed
        key = fact.get("dedupe_key")
        if not key or fact.get("coverage_status") != "known":
            return False
        # Staging is outside <platform>/<day>/<target>.json, so a partial cycle
        # can never replace the completed-window aggregate or inflate its runs.
        blob = self._blob(f"notification-markers/events/{fact['business_day']}/{key}.json")
        payload = {"schema_version": "qsl.notification_candidate_fact.v1", "event": fact, "projection": projection}
        body = json.dumps(payload, sort_keys=True, allow_nan=False)
        try:
            blob.upload_from_string(body, content_type="application/json", if_generation_match=0, retry=None, timeout=30)
        except PreconditionFailed:
            # A duplicate must prove identical evidence, not overwrite a conflict.
            if json.loads(blob.download_as_text(retry=None, timeout=30)) != payload:
                raise ValueError("notification_candidate_conflict")
        return True


class StaleObservation(RuntimeError):
    pass


class _OrderedStore:
    def __init__(self, store, event):
        self.store, self.event = store, event

    def read(self, key):
        version, value = self.store.read(key)
        if value and value.get("observed_at") and _stamp(value["observed_at"]) > self.event.occurred_at:
            raise StaleObservation("notification_observation_out_of_order")
        return version, value

    def compare_and_set(self, key, version, value):
        return self.store.compare_and_set(key, version, {**value, "observed_at": self.event.occurred_at.isoformat()})


def event_for(kind, *, profile=None, reason=None, occurred_at=None, source_run_id=None,
              order_count=None, fill_count=None, order_digest=None, env=None, expected_block_evidence=None):
    env = os.environ if env is None else env
    report = _context.get() or {}
    try:
        target = json.loads(str(env.get("RUNTIME_TARGET_JSON") or "{}"))
    except ValueError:
        target = {}
    if not isinstance(target, dict):
        target = {}
    stamp = occurred_at or (_stamp(report["started_at"]) if report.get("started_at") else datetime.now(timezone.utc))
    return NotificationEvent(kind, stamp, platform_id="schwab",
        opaque_account_uid=env.get("DIGEST_OPAQUE_ACCOUNT_UID") or None,
        target_id=env.get("DIGEST_TARGET_ID") or None,
        strategy_profile=profile or report.get("strategy_profile") or target.get("strategy_profile") or env.get("STRATEGY_PROFILE") or None,
        source_run_id=source_run_id or report.get("run_id") or env.get("GITHUB_RUN_ID") or None,
        incident_reason=reason, order_count=order_count, fill_count=fill_count, order_execution_digest=order_digest,
        expected_block_evidence=expected_block_evidence)


def _log_failure(reason):
    # Machine log only: no exception text, cloud paths or runtime identities.
    print(f"unified_notification_failed reason={reason} coverage=unknown", flush=True)


def _incident(event, *, stream, state, severity, recovered, sender, store):
    if not all((event.platform_id, event.target_id, event.opaque_account_uid, event.strategy_profile, event.incident_reason)):
        raise ValueError("notification_identity_unknown")
    tracker = IncidentTracker(_OrderedStore(store, event))
    scope_key = "observation-" + _hash([event.platform_id, event.target_id, event.opaque_account_uid, event.strategy_profile, stream])
    version, prior = store.read(scope_key)
    if version is not None and (not isinstance(prior, dict) or
        prior.get("schema_version") != "qsl.notification_observation.v1" or
        not prior.get("observed_at") or not isinstance(prior.get("incident_reasons"), list) or
        any(not isinstance(reason, str) or not reason for reason in prior["incident_reasons"])):
        raise RuntimeError("notification_observation_history_incomplete")
    prior = prior or {}
    if prior.get("observed_at") and _stamp(prior["observed_at"]) > event.occurred_at:
        return False
    prior_reasons = list(prior.get("incident_reasons") or ())
    # Only an explicit current observation of this same stream can recover its
    # prior incident. A no-due check must never call this as recovery evidence.
    if recovered:
        for prior_reason in prior_reasons:
            tracker.observe(replace(event, incident_reason=prior_reason), state="recovered", severity=0, recovered=True)
    if recovered:
        decision = None
    else:
        decision = tracker.observe(event, state=state, severity=severity)
    current = {"schema_version": "qsl.notification_observation.v1", "observed_at": event.occurred_at.isoformat(),
        "incident_reasons": [] if recovered else list(dict.fromkeys([*prior_reasons, event.incident_reason]))}
    if not store.compare_and_set(scope_key, version, current):
        # The incident itself has already been reserved. Never send on a lost
        # observation race; a pending marker requires reconciliation.
        raise RuntimeError("notification_observation_concurrent_update")
    if decision is None or decision.action != "send":
        return False
    try:
        response = sender()
        acknowledged = response is True or isinstance(response, dict) and response.get("acknowledged") is True
        receipt = response.get("receipt_id") if isinstance(response, dict) else None
        if (not acknowledged or not isinstance(receipt, str) or not receipt.strip()
                or receipt.startswith("transport-ack:")
                or receipt in {decision.transition_key, event.to_candidate().get("dedupe_key")}):
            receipt = None
    except Exception:
        tracker.record_delivery(decision)
        raise
    # The existing transport exposes a boolean ACK, not a Telegram message ID.
    tracker.record_delivery(decision, receipt_id=receipt)
    version, marker = store.read(decision.incident_key)
    if not store.compare_and_set(decision.incident_key, version,
            {**marker, "transport_acknowledged": acknowledged}):
        raise RuntimeError("notification_marker_concurrent_update")
    return acknowledged


def route(event, *, legacy, immediate=None, stream="cycle", state="alert", severity=1,
          recovered=False, projection=None, store=None, env=None, candidate_details=None):
    env = os.environ if env is None else env
    if not enabled(env):
        return legacy()
    routine = select_notification_route(event_kind=event.event_kind, env=env, flag_key=FLAG) == "digest_candidate"
    persisted = False
    try:
        store = store or GCSNotificationStore(env)

        def persist(fact):
            nonlocal persisted
            persisted = store.persist({**fact, **(candidate_details or {})}, projection) is True
            return persisted

        def deliver():
            return _incident(event, stream=stream, state=state, severity=severity,
                recovered=recovered, sender=immediate or legacy, store=store)

        outcome = route_notification_event(event, env=env, flag_key=FLAG,
            persist_candidate=persist, legacy=legacy, immediate=deliver)
        if outcome.route == "digest_candidate" and recovered:
            _incident(event, stream=stream, state="healthy", severity=0, recovered=True, sender=lambda: False, store=store)
        if not outcome.candidate_persisted:
            _log_failure("candidate_write_unconfirmed")
        return outcome.result
    except CandidatePersistenceError:
        # Never turn a failed routine candidate write into silent success.
        _log_failure("candidate_write_unconfirmed")
        return legacy()
    except StaleObservation:
        _log_failure("stale_observation")
        return False
    except Exception:
        # Incomplete marker history cannot authorize an untracked alert retry.
        _log_failure("marker_or_producer_unavailable")
        if routine and not persisted:
            return legacy()
        return False


def _cycle_projection(result, fact, config):
    report = copy.deepcopy(_context.get() or {})
    target = report.get("runtime_target")
    if not isinstance(target, dict) or target.get("platform_id") not in {"schwab", "charles_schwab"}:
        raise CandidatePersistenceError("notification_target_unknown")
    # Normalize only the notification copy of the established Schwab aliases.
    target["platform_id"] = "schwab"
    report.update(platform="schwab", status="ok", dry_run=bool(config.dry_run_only))
    report["summary"] = {"notification_digest": cycle_evidence(result.execution, result.submitted_orders,
        dry_run=bool(config.dry_run_only))}
    if report["dry_run"]:
        # Existing live archive acceptance is unchanged; previews are facts only.
        return {"schema_version": "qsl.digest_candidates.v1", "runs": [], "notification_events": [fact]}
    payload = _project_runs([report], expected_target=target, opaque_account_uid=fact["opaque_account_uid"],
        target_id=fact["target_id"], now=datetime.now(timezone.utc))
    for row in payload["runs"]:
        row["notification_events"] = [fact]
    return payload


def publish_cycle(result, *, config, legacy):
    """Called after execution; only the notification callback is moved."""
    if not enabled():
        return legacy()
    execution, orders = result.execution, tuple(result.submitted_orders or ())
    from .approved_gates import approved_block_reasons, render_gate_reasons
    approved = approved_block_reasons(execution, order_count=len(orders), dry_run=config.dry_run_only)
    if set(getattr(config, "notification_attention_reason_codes", ()) or ()) - {"account_new_risk_gate"}:
        approved = []
    healthy = defer_routine_cycle_to_digest(execution, submitted_order_count=len(orders),
        submitted_orders=orders, mode="daily_digest", include_orders=True)
    metadata = execution.get("signal_metadata") or {}
    metadata_healthy = isinstance(metadata, dict) and defer_routine_cycle_to_digest(
        {**metadata, "execution_status": metadata.get("execution_status") or "no_action"},
        submitted_order_count=0, mode="daily_digest", include_orders=True)
    healthy = healthy and metadata_healthy
    if getattr(config, "notification_attention_reason_codes", ()):
        healthy = False
    status = str(execution.get("execution_status") or "unknown")
    reason = str(execution.get("no_op_reason") or status)
    dry_run_healthy = config.dry_run_only and not getattr(config, "notification_attention_reason_codes", ()) and status in {"dry_run", "dry_run_completed"} and metadata_healthy and defer_routine_cycle_to_digest(
        {**execution, "execution_status": "no_action"}, submitted_order_count=0, mode="daily_digest", include_orders=True)
    if orders and not config.dry_run_only and all(isinstance(order, dict) and str(order.get("status") or "").lower() in {"filled", "completed", "executed"} for order in orders):
        kind = "order_submitted"
    elif dry_run_healthy or approved:
        kind = "routine_cycle"
    elif status == "blocked" or status.endswith("_blocked"):
        kind = "blocked_run"
    elif orders and not healthy:
        kind = "order_status_unknown"
    elif status in {"unknown", "pending", "pending_reconciliation", "reconciliation_required"}:
        kind = "order_status_unknown"
    elif healthy and orders:
        kind = "order_submitted"
    elif healthy:
        kind = "routine_cycle"
    else:
        kind = "real_error"
    identities = [str(order.get("broker_order_id") or order.get("order_id") or "") if isinstance(order, dict) else "" for order in orders]
    digest = order_identity_digest(json.dumps(sorted(identities))) if identities and all(identities) else None
    if kind == "order_status_unknown" and digest:
        reason += ":" + digest
    event = event_for(kind, profile=config.strategy_profile, reason=reason,
        order_count=None if config.dry_run_only else len(orders),
        fill_count=cycle_evidence(execution, orders, dry_run=config.dry_run_only)["fill_count"], order_digest=digest)
    fact = event.to_candidate()
    if approved:
        fact.update(approved_gate_reasons=approved,
            reason_text_zh=render_gate_reasons(approved, "zh"), reason_text_en=render_gate_reasons(approved, "en"))
    execution.setdefault("notification_events", []).append(fact)
    try:
        projection = _cycle_projection(result, fact, config)
    except Exception:
        _log_failure("candidate_projection_failed")
        # Urgent events still require the durable marker; routine falls back to
        # the original direct path when its producer has not proved coverage.
        if kind == "routine_cycle":
            return legacy()
        projection = None
    if kind == "order_submitted":
        # Real orders stay immediate by default. Count staging failures must not
        # suppress or delay the existing order/fill notification.
        try:
            store = GCSNotificationStore()
            store.persist(fact, projection)
            if healthy:
                _incident(event, stream="cycle", state="healthy", severity=0,
                    recovered=True, sender=lambda: False, store=store)
        except Exception:
            _log_failure("order_candidate_or_recovery_unconfirmed")
        return legacy()
    return route(event, legacy=legacy, projection=projection, state=status, recovered=healthy,
        candidate_details={k: fact[k] for k in ("approved_gate_reasons", "reason_text_zh", "reason_text_en") if k in fact})


def publish_incident(legacy, *, reason, profile=None, kind="real_error"):
    if not enabled():
        return legacy()
    return route(event_for(kind, profile=profile, reason=reason), legacy=legacy, state=kind)


def _notification_report(report):
    # Composer identity is preserved in the archive. Only this notification
    # projection copy uses the contract's canonical platform spelling.
    value = copy.deepcopy(report)
    if value.get("platform") in {"charles_schwab", "schwab"}:
        value["platform"] = "schwab"
    target = value.get("runtime_target")
    if isinstance(target, dict) and target.get("platform_id") in {"charles_schwab", "schwab"}:
        target["platform_id"] = "schwab"
    return value


def _project_runs(reports, **kwargs):
    # The archive remains authoritative. Project each business run separately;
    # window totals must never be relabelled as one run or copied to every run.
    unique = {}
    for report in reports:
        key = report.get("run_id")
        if key in unique and unique[key] != report:
            raise ValueError("notification_source_run_conflict")
        unique[key] = report
    rows = []
    for run_id, report in unique.items():
        payload = project_reports([_notification_report(report)], **kwargs)
        for row in payload["runs"]:
            row["source_run_id"] = run_id
            events = (report.get("summary") or {}).get("notification_events") or []
            if not isinstance(events, list):
                raise ValueError("notification_event_identity_unknown")
            row["notification_events"] = copy.deepcopy(events)
            row["field_status"] = {name: "known" if row.get(name) is not None else "unknown"
                for name in ("order_count", "fill_count", "cycle_count")}
            from .approved_gates import report_gate_reasons, render_gate_reasons
            approved = report_gate_reasons(report, observed_at=kwargs.get("now"))
            if approved:
                row.update(approved_gate_reasons=approved,
                    reason_text_zh=render_gate_reasons(approved, "zh"), reason_text_en=render_gate_reasons(approved, "en"))
            rows.append(row)
    return {"schema_version": "qsl.digest_candidates.v1", "producer_status": "projected", "runs": rows}


def _run_identity(value):
    return tuple(value.get(name) for name in
        ("platform_id", "opaque_account_uid", "target_id", "strategy_profile", "source_run_id"))


def persist_projected_candidate(payload, *, env=None, store=None):
    """Merge archive-authoritative runs into one exact object, with one CAS."""
    import re
    from google.api_core.exceptions import NotFound
    env = os.environ if env is None else env
    slot = env.get("DIGEST_STORE_TARGET", "")
    rows = payload.get("runs")
    if (not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,40}", slot)
            or payload.get("schema_version") != "qsl.digest_candidates.v1"
            or not isinstance(rows, list) or not rows or len(rows) > 10000):
        raise ValueError("notification_candidate_invalid")
    scope = ("platform_id", "opaque_account_uid", "target_id", "strategy_profile", "business_day")
    identity = tuple(rows[0].get(k) for k in scope)
    if not all(isinstance(v, str) and v for v in identity) or identity[0] != "schwab":
        raise ValueError("notification_candidate_identity_unknown")
    from datetime import date
    date.fromisoformat(identity[-1])
    store = store or GCSNotificationStore(env)
    blob = store._blob(f"{identity[-1]}/{slot}.json")
    try:
        blob.reload(retry=None, timeout=30)
        generation = int(blob.generation)
        if getattr(blob, "size", 0) and int(blob.size) > 2 * 1024**2:
            raise ValueError("notification_candidate_budget_exceeded")
        previous = json.loads(blob.download_as_text(if_generation_match=generation, retry=None, timeout=30))
    except NotFound:
        generation, previous = 0, None
    if previous is not None and (not isinstance(previous, dict)
            or previous.get("schema_version") != "qsl.digest_candidates.v1"
            or not isinstance(previous.get("runs"), list)):
        raise ValueError("notification_candidate_history_unknown")
    merged = {}
    for row in [*((previous or {}).get("runs") or []), *rows]:
        executed = row.get("actually_ran") is True and row.get("cycle_count") == 1 if isinstance(row, dict) else False
        not_run = (isinstance(row, dict) and row.get("actually_ran") is False and row.get("cycle_count") == 0
                   and row.get("execution_status") == "not_run" and row.get("reason_code") == "runtime_execution_not_admitted"
                   and row.get("order_count") is None and row.get("fill_count") is None)
        if (not isinstance(row, dict) or tuple(row.get(k) for k in scope) != identity
                or not (executed or not_run)
                or not isinstance(row.get("source_run_id"), str) or not row["source_run_id"]):
            raise ValueError("notification_candidate_scope_conflict")
        key = row["source_run_id"]
        current = dict(row)
        events = current.pop("notification_events", [])
        prior = dict(merged.get(key) or {})
        prior_events = prior.pop("notification_events", [])
        if prior and prior != current:
            raise ValueError("notification_candidate_source_conflict")
        facts = {}
        for event in [*prior_events, *events]:
            if (not isinstance(event, dict) or not event.get("dedupe_key")
                    or tuple(event.get(k) for k in (*scope, "source_run_id")) != (*identity, key)):
                raise ValueError("notification_event_identity_unknown")
            fact = dict(event)
            for nullable in ("incident_reason", "order_execution_digest", "expected_block_evidence", "order_count", "fill_count"):
                fact.setdefault(nullable, None)
            event_key = fact["dedupe_key"]
            if event_key in facts and facts[event_key] != fact:
                raise ValueError("notification_event_conflict")
            facts[event_key] = fact
        merged[key] = {**current, "notification_events": [facts[k] for k in sorted(facts)]}
    final = {"schema_version": "qsl.digest_candidates.v1", "producer_status": "projected",
             "runs": [merged[key] for key in sorted(merged)]}
    if final == previous:
        return True
    body = json.dumps(final, sort_keys=True, allow_nan=False)
    if len(body.encode()) > 2 * 1024**2 or len(merged) > 10000:
        raise ValueError("notification_candidate_budget_exceeded")
    blob.upload_from_string(body, content_type="application/json", if_generation_match=generation,
                            retry=None, timeout=30)
    return True


def persist_completed_report(report, *, env=None, store=None):
    """Called only after the existing report persistence succeeded."""
    env = os.environ if env is None else env
    if not enabled(env):
        return False
    try:
        if env.get("RUNTIME_TARGET_ENABLED") != "true":
            raise ValueError("notification_target_not_enabled")
        target = json.loads(env.get("RUNTIME_TARGET_JSON") or "{}")
        if target.get("platform_id") == "charles_schwab":
            target["platform_id"] = "schwab"
        payload = _project_runs([report], expected_target=target,
            opaque_account_uid=env.get("DIGEST_OPAQUE_ACCOUNT_UID", ""),
            target_id=env.get("DIGEST_TARGET_ID", ""), now=datetime.now(timezone.utc))
        return persist_projected_candidate(payload, env=env, store=store)
    except Exception:
        _log_failure("completed_report_candidate_unconfirmed")
        return False


def observe_attention_recovery(event):
    try:
        _incident(event, stream="attention", state="healthy", severity=0, recovered=True,
            sender=lambda: False, store=GCSNotificationStore())
    except Exception:
        _log_failure("attention_recovery_unconfirmed")


def stage_approved_gate_observation(*, profile, reasons):
    from .approved_gates import clean_gate_reasons
    reasons = clean_gate_reasons(reasons)
    if not enabled() or not reasons:
        return False
    try:
        event = event_for("expected_block_observed", profile=profile, reason="approved_gate_block",
            expected_block_evidence="approved-gate:" + _hash(reasons))
        # This is a completed gate observation, not a forecast of this run's
        # orders. Only the cycle/archive may establish the eventual counts.
        return GCSNotificationStore().persist({**event.to_candidate(), "approved_gate_reasons": reasons}) is True
    except Exception:
        _log_failure("approved_gate_observation_unconfirmed")
        return False


def emit_completed_candidates(env, *, now=None, read=None, upload=None, required_runs=(), observed_events=()):
    """Export one archive-authoritative row per business run, with own facts."""
    from .digest_snapshot import digest_archive
    if not enabled(env):
        return digest_archive.emit(env, now=now, **({"read": read} if read else {}), **({"upload": upload} if upload else {}))
    injected_read = read
    read = read or digest_archive._read
    configured = {**env, "QSL_ROUTINE_NOTIFICATION_MODE": effective_notification_mode(env, FLAG)}
    published_runs = set()

    def write(uri, body):
        payload = json.loads(body)
        rows = payload.get("runs", [])
        for row in rows:
            facts = {}
            for fact in [*row.get("notification_events", []), *(e for e in observed_events if e.get("source_run_id") == row.get("source_run_id"))]:
                identity = ("platform_id", "opaque_account_uid", "target_id", "strategy_profile", "business_day", "source_run_id")
                if (not isinstance(fact, dict) or fact.get("schema_version") != "qsl.notification_event.v1"
                        or any(fact.get(k) != row.get(k) for k in identity)
                        or fact.get("coverage_status") != "known" or not fact.get("dedupe_key")):
                    raise ValueError("notification_event_identity_unknown")
                key = fact["dedupe_key"]
                if key in facts and facts[key] != fact:
                    raise ValueError("notification_event_conflict")
                facts[key] = fact
            row["notification_events"] = list(facts.values())
        identities = {_run_identity(row) for row in rows if row.get("actually_ran") is True}
        for required in required_runs:
            if any(value[:4] == required[:4] for value in identities) and required not in identities:
                raise ValueError("notification_observed_run_not_covered")
        projected = json.dumps(payload, allow_nan=False)
        if upload:
            upload(uri, projected)
        else:
            persist_projected_candidate(payload, env=configured)
        published_runs.update(identities)

    def notification_read(args):
        value = read(args)
        if args[0] == "cat" and isinstance(value, dict) and value.get("schema_version") == "runtime_report.v1":
            return _notification_report(value)
        return value

    result = digest_archive.emit(configured, now=now, read=notification_read, upload=write, projector=_project_runs)
    if not set(required_runs) <= published_runs:
        raise ValueError("notification_observed_run_not_covered")
    return result


def _receipt_event(report, kind, reason):
    # A monitor run ID never identifies a business run. Keep the receipt's
    # start time too, so checks after 06:00 still attach to its source window.
    if not isinstance(report, dict) or not report.get("run_id") or not report.get("started_at"):
        raise ValueError("notification_receipt_source_unknown")
    raw = json.loads(os.environ.get("DIGEST_TARGET_BINDINGS_JSON") or "[]")
    bindings = [{**os.environ, **value} for value in raw] if raw else [os.environ]
    keys = ("platform_id", "strategy_profile", "service_name", "account_scope", "account_selector", "deployment_selector", "dry_run_only")
    source_target = dict(report.get("runtime_target") or {})
    if source_target.get("platform_id") == "charles_schwab":
        source_target["platform_id"] = "schwab"
    matches = []
    for binding in bindings:
        target = json.loads(str(binding.get("RUNTIME_TARGET_JSON") or "{}"))
        if all(key in target and source_target.get(key) == target[key] for key in keys):
            matches.append(binding)
    if len(matches) != 1:
        raise ValueError("notification_receipt_binding_unknown")
    return event_for(kind, profile=report.get("strategy_profile"), reason=reason, env=matches[0],
        occurred_at=_stamp(report["started_at"]), source_run_id=report["run_id"])


def publish_approved_receipts(reports):
    from .approved_gates import report_gate_reasons
    try:
        required = []
        for report in reports:
            if not report_gate_reasons(report):
                return False
            event = _receipt_event(report, "routine_cycle", "approved_gate_block")
            required.append(_run_identity(event.to_candidate()))
        # The real archive export carries this run's validated gate reasons.
        # Do not write a second routine_cycle fact over the cycle's immutable key.
        return emit_completed_candidates(os.environ, required_runs=required).get("uploaded") is True
    except Exception:
        _log_failure("approved_receipt_candidate_unconfirmed")
        return False


def publish_receipt(legacy, *, kind, reason, recovered=False, observed_at=None, source_report=None):
    """Receipt acceptance and workflow disposition remain authoritative."""
    if not enabled():
        return legacy()
    if kind == "healthy_heartbeat":
        try:
            event = _receipt_event(source_report, kind, reason)
            store = GCSNotificationStore()
            if store.persist(event.to_candidate()) is not True:
                raise CandidatePersistenceError("notification_receipt_fact_unconfirmed")
            # Publish after staging the observation, so the very same export
            # includes this receipt-linked fact rather than a monitor-only row.
            if emit_completed_candidates(os.environ, required_runs=[_run_identity(event.to_candidate())], observed_events=[event.to_candidate()]).get("uploaded") is not True:
                raise CandidatePersistenceError("notification_archive_write_unconfirmed")
            if recovered:
                _incident(replace(event, occurred_at=observed_at or event.occurred_at), stream="receipt",
                    state="healthy", severity=0, recovered=True, sender=lambda: False, store=store)
            return None
        except Exception:
            _log_failure("receipt_candidate_projection_failed")
            return legacy()
    event = event_for(kind, reason=reason, occurred_at=observed_at)
    try:
        # Receipt incidents without a business run belong only to the durable
        # immediate incident path. A monitor job is never an archive run fact.
        return _incident(event, stream="receipt", state=kind, severity=1, recovered=recovered,
            sender=legacy, store=GCSNotificationStore())
    except StaleObservation:
        _log_failure("stale_observation")
    except Exception:
        _log_failure("receipt_marker_unavailable")
    return False
