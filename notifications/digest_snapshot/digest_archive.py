"""Publish notification candidates from existing archives; disabled by default.

Runs only inside an existing cloud monitor, with its existing identity. It never
invokes a platform service or broker, reads secrets, or enables a runtime target.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .digest_candidates import project_reports

_SAFE = re.compile(r"[a-z0-9][a-z0-9_-]{0,40}\Z")


def _read(args):
    result = subprocess.run(["gcloud", "storage", *args], capture_output=True, text=True, check=False)
    if result.returncode:
        raise ValueError("candidate_archive_read_failed")
    return json.loads(result.stdout)


def emit(env, *, now=None, read=_read, upload=None):
    if env.get("QSL_ROUTINE_NOTIFICATION_MODE", "immediate") != "daily_digest":
        return {"status": "disabled", "uploaded": False}
    bindings = env.get("DIGEST_TARGET_BINDINGS_JSON", "").strip()
    if bindings:
        values = json.loads(bindings)
        if not isinstance(values, list) or not values or len(values) > 100 or not all(isinstance(v, dict) for v in values):
            raise ValueError("candidate_bindings_invalid")
        results = []
        for value in values:
            results.append(emit({**env, **value, "DIGEST_TARGET_BINDINGS_JSON": ""}, now=now, read=read, upload=upload))
        return {"status": "projected", "uploaded": any(v["uploaded"] for v in results), "targets": len(results)}
    if env.get("RUNTIME_TARGET_ENABLED") == "false":
        return {"status": "target_disabled", "uploaded": False}
    if env.get("RUNTIME_TARGET_ENABLED") != "true":
        raise ValueError("candidate_target_enablement_unknown")
    target = json.loads(env.get("RUNTIME_TARGET_JSON", ""))
    platform = target.get("platform_id", "")
    slot = env.get("DIGEST_STORE_TARGET", "")
    if not _SAFE.fullmatch(platform) or not _SAFE.fullmatch(slot):
        raise ValueError("candidate_store_identity_missing")
    uid, tid = env.get("DIGEST_OPAQUE_ACCOUNT_UID", ""), env.get("DIGEST_TARGET_ID", "")
    if not uid or not tid:
        raise ValueError("candidate_identity_missing")
    prefix = env.get("EXECUTION_REPORT_GCS_URI", "").rstrip("/")
    destination = env.get("DIGEST_CANDIDATES_GCS_URI", "").rstrip("/")
    if any(not re.fullmatch(r"gs://[a-z0-9][a-z0-9._-]+/[^\s?#*]+", value) for value in (prefix, destination)):
        raise ValueError("candidate_store_configuration_missing")
    current = now or datetime.now(timezone.utc)
    if current.astimezone(ZoneInfo("Asia/Singapore")).hour < 6:
        return {"status": "before_cutoff", "uploaded": False}
    day = (current.astimezone(ZoneInfo("Asia/Singapore")) - timedelta(hours=6)).date() - timedelta(days=1)
    # Monitor runs after the fixed 06:00 SGT cut-off. Reject early runs rather
    # than overwriting the previous day's complete candidate with partial facts.
    end = datetime(day.year, day.month, day.day, 6, tzinfo=ZoneInfo("Asia/Singapore")) + timedelta(days=1)
    start = end - timedelta(days=1)
    months = {start.astimezone(timezone.utc).strftime("%Y-%m"), end.astimezone(timezone.utc).strftime("%Y-%m")}
    entries = []
    for month in sorted(months):
        entries.extend(read(["ls", "--json", f"{prefix}/{platform}/**/{month}/*.json"]))
    if len(entries) > 10000:
        raise ValueError("candidate_archive_budget_exceeded")
    reports, uris = [], set()
    for entry in entries:
        uri = str(entry.get("url", "")).split("#", 1)[0]
        if uri in uris:
            continue
        if not uri.startswith(f"{prefix}/{platform}/"):
            raise ValueError("candidate_source_locator_mismatch")
        uris.add(uri)
        raw_stamp = (entry.get("metadata") or {}).get("updated") or (entry.get("metadata") or {}).get("timeCreated")
        stamp = datetime.fromisoformat(str(raw_stamp).replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("candidate_archive_time_missing")
        if stamp < start or stamp > current:
            continue
        source = read(["cat", uri])
        source_target = source.get("runtime_target") or {}
        keys = ("platform_id", "strategy_profile", "service_name", "account_scope", "account_selector", "deployment_selector", "dry_run_only")
        if not isinstance(source_target, dict) or any(source_target.get(key) != target.get(key) for key in keys):
            continue
        source_stamp = datetime.fromisoformat(str(source.get("started_at", "")).replace("Z", "+00:00"))
        if source_stamp.tzinfo is None:
            raise ValueError("candidate_source_time_invalid")
        if start <= source_stamp < end:
            reports.append(source)
    if not reports:
        schedule_raw = env.get("DIGEST_NOTIFICATION_SCHEDULE_JSON", "").strip()
        if not schedule_raw:
            raise ValueError("candidate_source_absent")
        from quant_platform_kit.common.runtime_heartbeat_policy import filter_due_targets
        schedule = json.loads(schedule_raw)
        due, evaluated = filter_due_targets([schedule], since=start, now=end - timedelta(microseconds=1),
            publication_grace=timedelta(0), warning_logger=lambda message: None)
        if not evaluated or due:
            raise ValueError("candidate_due_source_absent")
        payload = {"schema_version": "qsl.digest_candidates.v1", "runs": [{
            "platform_id": platform, "strategy_profile": target["strategy_profile"],
            "opaque_account_uid": uid, "target_id": tid, "business_day": day.isoformat(),
            "actually_ran": False, "status": "ok", "scheduled_idle": True,
            "evidence_provenance": "candidates",
        }]}
    else:
        payload = project_reports(reports, expected_target=target, opaque_account_uid=uid, target_id=tid, now=current)
    if upload is None:
        from google.cloud import storage
        def upload(uri, body):
            storage.Blob.from_string(uri, client=storage.Client()).upload_from_string(
                body, content_type="application/json", retry=None, timeout=30)
    # Never retry an unknown write result. Public output contains no identities.
    upload(f"{destination}/{platform}/{day.isoformat()}/{slot}.json", json.dumps(payload, allow_nan=False))
    return {"status": "projected", "uploaded": True, "runs": len(reports)}


def main():
    try:
        print(json.dumps(emit(os.environ), sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"status": "blocked", "uploaded": False, "reason": "candidate_projection_failed"}))
        return 1
