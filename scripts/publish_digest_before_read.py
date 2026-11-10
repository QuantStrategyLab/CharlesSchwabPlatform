"""Publish one completed-window candidate before the central read, without broker calls."""
from __future__ import annotations

import json
import os
import signal
from datetime import datetime, time, timedelta, timezone
from time import monotonic, sleep
from zoneinfo import ZoneInfo

from google.api_core.exceptions import NotFound

from notifications.digest_snapshot.digest_archive import ArchiveReader

SHANGHAI = ZoneInfo("Asia/Shanghai")
PLATFORM = "schwab"
WAIT_SECONDS = 480
POLL_SECONDS = 30


class DeadlineExceeded(BaseException):
    """Escape producer catch-all handlers; an interrupted write stays unknown."""


class PublicationWaitTimeout(ValueError):
    """A candidate remained absent for the bounded readiness wait."""

    def __init__(self):
        super().__init__("candidate_publication_wait_timeout")


class PublicationDeadlineMissed(ValueError):
    """No attempt or success is allowed after the central read deadline."""

    def __init__(self):
        super().__init__("candidate_publication_deadline_missed")


def publication_day(now):
    if now.tzinfo is None:
        raise ValueError("candidate_timezone_required")
    local = now.astimezone(SHANGHAI)
    if not time(6) <= local.time() < time(6, 40):
        raise ValueError("candidate_publication_window_closed")
    return (local.date() - timedelta(days=1)).isoformat()


def _emit(env, *, now, read):
    from notifications.unified import emit_completed_candidates
    return emit_completed_candidates(env, now=now, read=read)


def publish(env, *, now=None, client=None, emit=None):
    clock = now or (lambda: datetime.now(timezone.utc))
    started = clock()
    wait_deadline = monotonic() + WAIT_SECONDS
    day = publication_day(started)
    if (env.get("DIGEST_PUBLISH_ENABLED") != "true"
            or env.get("QSL_ROUTINE_NOTIFICATION_MODE") != "daily_digest"
            or env.get("RUNTIME_TARGET_ENABLED") != "true"):
        raise ValueError("candidate_publication_not_enabled")
    if str(env.get("DIGEST_TARGET_BINDINGS_JSON") or "").strip():
        raise ValueError("candidate_publication_single_target_required")
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    required = ("strategy_profile", "service_name", "account_scope", "deployment_selector")
    if (not isinstance(target, dict) or target.get("platform_id") != PLATFORM
            or any(not isinstance(target.get(key), str) or not target[key].strip() for key in required)
            or type(target.get("dry_run_only")) is not bool
            or not isinstance(target.get("account_selector"), list) or not target["account_selector"]
            or any(not isinstance(value, str) or not value.strip() for value in target["account_selector"])):
        raise ValueError("candidate_publication_target_invalid")
    keys = ("DIGEST_OPAQUE_ACCOUNT_UID", "DIGEST_TARGET_ID", "DIGEST_STORE_TARGET",
            "DIGEST_CANDIDATES_GCS_URI", "EXECUTION_REPORT_GCS_URI")
    if any(not isinstance(env.get(key), str) or not env[key].strip() for key in keys):
        raise ValueError("candidate_publication_binding_missing")
    if client is None:
        from google.cloud import storage
        client = storage.Client()
    reader = ArchiveReader(client)
    identity = {"platform_id": target["platform_id"],
                "strategy_profile": target["strategy_profile"],
                "opaque_account_uid": env["DIGEST_OPAQUE_ACCOUNT_UID"],
                "target_id": env["DIGEST_TARGET_ID"], "business_day": day}
    uri = (f"{env['DIGEST_CANDIDATES_GCS_URI'].rstrip('/')}/{identity['platform_id']}/"
           f"{day}/{env['DIGEST_STORE_TARGET']}.json")

    def check_deadline(current):
        local = current.astimezone(SHANGHAI)
        if local.date() != started.astimezone(SHANGHAI).date() or local.time() >= time(6, 55):
            raise PublicationDeadlineMissed()

    attempt_time = started
    projected = False
    while True:
        if projected:
            # Fresh generation-bound read on every attempt; never replay a write.
            try:
                body = reader.read(["cat", uri])
                break
            except NotFound:
                pass
        else:
            try:
                result = (emit or _emit)(env, now=attempt_time, read=reader.read)
            except ValueError as error:
                # These exact producer errors occur before its upload callback.
                # Storage errors, conflicts and unknown writes must escape.
                if type(error) is not ValueError or str(error) not in {
                    "candidate_source_absent", "candidate_due_source_absent",
                }:
                    raise
            else:
                if (not isinstance(result, dict) or result.get("status") != "projected"
                        or type(result.get("uploaded")) is not bool):
                    raise ValueError("candidate_publication_unconfirmed")
                # Even an explicit negative acknowledgement needs an exact-object
                # readback. It does not permit another write or imply success.
                projected = True
                continue
        current = clock()
        check_deadline(current)
        remaining = wait_deadline - monotonic()
        if remaining <= 0:
            raise PublicationWaitTimeout()
        sleep(min(POLL_SECONDS, remaining))
        attempt_time = clock()
        check_deadline(attempt_time)
        if monotonic() >= wait_deadline:
            raise PublicationWaitTimeout()

    rows = body.get("runs")
    if (body.get("schema_version") != "qsl.digest_candidates.v1"
            or not isinstance(rows, list) or not rows
            or any(not isinstance(row, dict)
                   or any(row.get(key) != value for key, value in identity.items())
                   or type(row.get("actually_ran")) is not bool for row in rows)):
        raise ValueError("candidate_publication_readback_invalid")
    check_deadline(clock())
    if monotonic() > wait_deadline:
        raise PublicationWaitTimeout()
    return {"status": "published", "business_day": day, "readback_verified": True}


def main():
    def timeout(signum, frame):
        raise DeadlineExceeded()

    previous = signal.signal(signal.SIGALRM, timeout)
    signal.alarm(600)
    try:
        print(json.dumps(publish(os.environ), sort_keys=True))
        return 0
    except PublicationWaitTimeout:
        print(json.dumps({"status": "failed", "reason_code": "candidate_publication_wait_timeout"}))
        return 1
    except PublicationDeadlineMissed:
        print(json.dumps({"status": "failed", "reason_code": "candidate_publication_deadline_missed"}))
        return 1
    except DeadlineExceeded:
        print(json.dumps({"status": "failed", "reason_code": "candidate_publication_watchdog_timeout"}))
        return 1
    except Exception as error:
        # Never expose exception messages, private locators or credentials.
        reason_code = "candidate_publication_unconfirmed"
        if type(error) is ValueError and str(error) == "notification_candidate_source_conflict":
            reason_code = "notification_candidate_source_conflict"
        print(json.dumps({"status": "failed", "reason_code": reason_code}))
        return 1
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


if __name__ == "__main__":
    raise SystemExit(main())
