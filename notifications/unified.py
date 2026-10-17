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
        blob = self._blob(f"notification-events/{fact['business_day']}/{key}.json")
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
              order_count=None, fill_count=None, order_digest=None, env=None):
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
        incident_reason=reason, order_count=order_count, fill_count=fill_count, order_execution_digest=order_digest)


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
        acknowledged = sender() is True
    except Exception:
        tracker.record_delivery(decision)
        raise
    # The existing transport exposes a boolean ACK, not a Telegram message ID.
    tracker.record_delivery(decision, receipt_id=("transport-ack:" + decision.transition_key) if acknowledged else None)
    return acknowledged


def route(event, *, legacy, immediate=None, stream="cycle", state="alert", severity=1,
          recovered=False, projection=None, store=None, env=None):
    env = os.environ if env is None else env
    if not enabled(env):
        return legacy()
    routine = select_notification_route(event_kind=event.event_kind, env=env, flag_key=FLAG) == "digest_candidate"
    persisted = False
    try:
        store = store or GCSNotificationStore(env)

        def persist(fact):
            nonlocal persisted
            persisted = store.persist(fact, projection) is True
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
    payload = project_reports([report], expected_target=target, opaque_account_uid=fact["opaque_account_uid"],
        target_id=fact["target_id"], now=datetime.now(timezone.utc))
    payload["notification_events"] = [fact]
    for row in payload["runs"]:
        row["source_run_ids"] = [report["run_id"]]
    return payload


def publish_cycle(result, *, config, legacy):
    """Called after execution; only the notification callback is moved."""
    if not enabled():
        return legacy()
    execution, orders = result.execution, tuple(result.submitted_orders or ())
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
    elif dry_run_healthy:
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
    return route(event, legacy=legacy, projection=projection, state=status, recovered=healthy)


def publish_incident(legacy, *, reason, profile=None, kind="real_error"):
    if not enabled():
        return legacy()
    return route(event_for(kind, profile=profile, reason=reason), legacy=legacy, state=kind)


def emit_completed_candidates(env, *, now=None, read=None, upload=None):
    """Existing completed-window archive projection, with non-counting facts."""
    from .digest_snapshot import digest_archive
    if not enabled(env):
        return digest_archive.emit(env, now=now, **({"read": read} if read else {}), **({"upload": upload} if upload else {}))
    injected_read = read
    read = read or digest_archive._read
    configured = {**env, "QSL_ROUTINE_NOTIFICATION_MODE": effective_notification_mode(env, FLAG)}
    archived = []

    def archive_read(args):
        payload = read(args)
        if args[0] == "cat" and isinstance(payload, dict) and payload.get("schema_version") == "runtime_report.v1":
            archived.append(payload)
        return payload

    def project_unique(reports, **kwargs):
        unique = {}
        for report in reports:
            key = report.get("run_id")
            if key in unique and unique[key] != report:
                raise ValueError("notification_source_run_conflict")
            unique[key] = report
        # The original projector still validates identity, run IDs, source
        # times, acceptance facts and unknown counts. Only identical evidence
        # copies are collapsed; conflicting revisions stop publication.
        return project_reports(list(unique.values()), **kwargs)

    def write(uri, body):
        payload = json.loads(body)
        for row in payload.get("runs", []):
            raw_bindings = json.loads(str(env.get("DIGEST_TARGET_BINDINGS_JSON") or "[]"))
            bindings = [{**env, **value} for value in raw_bindings] if raw_bindings else [env]
            binding = next((value for value in bindings if
                value.get("DIGEST_OPAQUE_ACCOUNT_UID") == row.get("opaque_account_uid") and
                value.get("DIGEST_TARGET_ID") == row.get("target_id")), None)
            if binding is None:
                raise ValueError("notification_archive_binding_unknown")
            target = json.loads(str(binding.get("RUNTIME_TARGET_JSON") or "{}"))
            source_ids = {source["run_id"] for source in archived if source.get("runtime_target") == target
                and source.get("run_id") and source.get("dry_run") is False
                and sgt_business_window(_stamp(source["started_at"])).business_day.isoformat() == row["business_day"]}
            row["source_run_ids"] = sorted(source_ids)
            prefix, separator, _ = uri.rpartition("/" + row["platform_id"] + "/")
            if not separator:
                raise ValueError("notification_event_store_mismatch")
            event_prefix = f"{prefix}/schwab/notification-events/{row['business_day']}/"
            if injected_read:
                entries = read(["ls", "--json", event_prefix + "*.json"])
            else:
                # SDK prefix listing distinguishes a confirmed empty prefix
                # from a listing failure.
                from google.cloud import storage
                bucket, _, path = event_prefix[5:].partition("/")
                entries = [{"url": f"gs://{bucket}/{blob.name}"} for blob in
                    storage.Client().list_blobs(bucket, prefix=path, max_results=10001, retry=None, timeout=30)]
            if len(entries) > 10000:
                raise ValueError("notification_event_budget_exceeded")
            facts = {}
            for entry in entries:
                locator = str(entry.get("url") or "").split("#", 1)[0]
                expected_prefix = event_prefix
                if not locator.startswith(expected_prefix):
                    raise ValueError("notification_event_locator_mismatch")
                fact = read(["cat", locator]).get("event", {})
                identity = ("opaque_account_uid", "target_id", "strategy_profile", "business_day")
                if fact.get("schema_version") != "qsl.notification_event.v1" or any(fact.get(k) != row.get(k) for k in identity):
                    continue
                key = fact.get("dedupe_key")
                if not key or fact.get("coverage_status") != "known":
                    raise ValueError("notification_event_identity_unknown")
                if key in facts and facts[key] != fact:
                    raise ValueError("notification_event_conflict")
                facts[key] = fact
            row["notification_events"] = list(facts.values())
            if any(fact.get("event_kind") != "healthy_heartbeat" and fact.get("source_run_id") not in source_ids for fact in facts.values()):
                # A durable fact cannot be counted as another run; a missing
                # source receipt also cannot become a healthy zero-trade day.
                row.update(status="alert", reason_code="notification_source_report_missing",
                    order_count=None, fill_count=None, coverage_status="unknown")
                row.setdefault("field_status", {}).update(order_count="counts_unknown", fill_count="counts_unknown")
        projected = json.dumps(payload, allow_nan=False)
        if upload:
            upload(uri, projected)
        else:
            from google.cloud import storage
            storage.Blob.from_string(uri, client=storage.Client()).upload_from_string(
                projected, content_type="application/json", retry=None, timeout=30)

    return digest_archive.emit(configured, now=now, read=archive_read, upload=write, projector=project_unique)


def publish_receipt(legacy, *, kind, reason, recovered=False, observed_at=None):
    """Receipt checks remain authoritative; only their send callback is routed."""
    if not enabled():
        return legacy()
    event = event_for(kind, reason=reason, occurred_at=observed_at)
    if kind == "healthy_heartbeat":
        try:
            # A healthy send is covered only after the real completed-window
            # producer has written; no event-only success can silence it.
            outcome = emit_completed_candidates(os.environ)
            if outcome.get("uploaded") is not True:
                raise CandidatePersistenceError("notification_archive_write_unconfirmed")
        except Exception:
            _log_failure("receipt_candidate_projection_failed")
            return legacy()
    return route(event, legacy=legacy, stream="receipt", state=kind, recovered=recovered)
