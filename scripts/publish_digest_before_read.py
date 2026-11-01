"""Publish one completed-window candidate before the central read, without broker calls."""
from __future__ import annotations

import json
import os
import signal
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")
PLATFORM = "schwab"


class DeadlineExceeded(BaseException):
    """Escape producer catch-all handlers; an interrupted write stays unknown."""


def publication_day(now):
    if now.tzinfo is None:
        raise ValueError("candidate_timezone_required")
    local = now.astimezone(SHANGHAI)
    if not time(6) <= local.time() < time(6, 40):
        raise ValueError("candidate_publication_window_closed")
    return (local.date() - timedelta(days=1)).isoformat()


class ArchiveReader:
    """Native Storage reads with the job identity; no gcloud or secret access."""

    def __init__(self, client):
        self.client = client

    def read(self, args):
        uri = args[-1]
        if not uri.startswith("gs://"):
            raise ValueError("candidate_archive_uri_invalid")
        bucket, separator, key = uri[5:].partition("/")
        if not bucket or not separator or not key:
            raise ValueError("candidate_archive_uri_invalid")
        if args[0] == "ls":
            blobs = self.client.list_blobs(
                bucket, prefix=key.split("*", 1)[0], match_glob=key,
                max_results=10001, retry=None, timeout=30,
            )
            rows = []
            for blob in blobs:
                rows.append({"url": f"gs://{bucket}/{blob.name}",
                             "metadata": {"updated": blob.updated.isoformat()}})
                if len(rows) > 10000:
                    raise ValueError("candidate_archive_budget_exceeded")
            return rows
        if args[0] != "cat":
            raise ValueError("candidate_archive_operation_invalid")
        blob = self.client.bucket(bucket).blob(key)
        blob.reload(retry=None, timeout=30)
        if blob.size is None or blob.size > 2 * 1024**2:
            raise ValueError("candidate_archive_object_too_large")
        return json.loads(blob.download_as_bytes(
            if_generation_match=int(blob.generation), retry=None, timeout=30,
        ))


def _emit(env, *, now, read):
    from notifications.unified import emit_completed_candidates
    return emit_completed_candidates(env, now=now, read=read)


def publish(env, *, now=None, client=None, emit=None):
    clock = now or (lambda: datetime.now(timezone.utc))
    started = clock()
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
    result = (emit or _emit)(env, now=started, read=reader.read)
    if result.get("status") != "projected" or result.get("uploaded") is not True:
        raise ValueError("candidate_publication_unconfirmed")
    identity = {"platform_id": target["platform_id"],
                "strategy_profile": target["strategy_profile"],
                "opaque_account_uid": env["DIGEST_OPAQUE_ACCOUNT_UID"],
                "target_id": env["DIGEST_TARGET_ID"], "business_day": day}
    uri = (f"{env['DIGEST_CANDIDATES_GCS_URI'].rstrip('/')}/{identity['platform_id']}/"
           f"{day}/{env['DIGEST_STORE_TARGET']}.json")
    body = reader.read(["cat", uri])
    rows = body.get("runs")
    if (body.get("schema_version") != "qsl.digest_candidates.v1"
            or not isinstance(rows, list) or not rows
            or any(not isinstance(row, dict)
                   or any(row.get(key) != value for key, value in identity.items())
                   or type(row.get("actually_ran")) is not bool for row in rows)):
        raise ValueError("candidate_publication_readback_invalid")
    finished = clock().astimezone(SHANGHAI)
    if finished.date() != started.astimezone(SHANGHAI).date() or finished.time() >= time(6, 55):
        raise ValueError("candidate_publication_deadline_missed")
    return {"status": "published", "business_day": day, "readback_verified": True}


def main():
    def timeout(signum, frame):
        raise DeadlineExceeded()

    previous = signal.signal(signal.SIGALRM, timeout)
    signal.alarm(600)
    try:
        print(json.dumps(publish(os.environ), sort_keys=True))
        return 0
    except (Exception, DeadlineExceeded):
        # Never expose exception messages, private locators or credentials.
        print(json.dumps({"status": "failed", "reason_code": "candidate_publication_unconfirmed"}))
        return 1
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


if __name__ == "__main__":
    raise SystemExit(main())
