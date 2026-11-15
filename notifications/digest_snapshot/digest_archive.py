"""Publish notification candidates from existing archives; disabled by default.

Runs only inside an existing cloud monitor, with its existing identity. It never
invokes a platform service or broker, reads secrets, or enables a runtime target.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from .digest_candidates import project_reports

_SAFE = re.compile(r"[a-z0-9][a-z0-9_-]{0,40}\Z")


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


def _read(args):
    from google.cloud import storage
    try:
        return ArchiveReader(storage.Client()).read(args)
    except Exception as error:
        # Empty SDK listings return []; all failed reads remain failures.
        raise ValueError("candidate_archive_read_failed") from error


def emit(env, *, now=None, read=_read, upload=None, projector=project_reports):
    if env.get("QSL_ROUTINE_NOTIFICATION_MODE", "immediate") != "daily_digest":
        return {"status": "disabled", "uploaded": False}
    bindings = env.get("DIGEST_TARGET_BINDINGS_JSON", "").strip()
    if bindings:
        values = json.loads(bindings)
        if not isinstance(values, list) or not values or len(values) > 100 or not all(isinstance(v, dict) for v in values):
            raise ValueError("candidate_bindings_invalid")
        results = []
        for value in values:
            results.append(emit({**env, **value, "DIGEST_TARGET_BINDINGS_JSON": ""}, now=now, read=read, upload=upload, projector=projector))
        return {"status": "projected", "uploaded": any(v["uploaded"] for v in results), "targets": len(results)}
    if env.get("RUNTIME_TARGET_ENABLED") == "false":
        return {"status": "target_disabled", "uploaded": False}
    if env.get("RUNTIME_TARGET_ENABLED") != "true":
        raise ValueError("candidate_target_enablement_unknown")
    target = json.loads(env.get("RUNTIME_TARGET_JSON", ""))
    platform = target.get("platform_id", "")
    source_platform = {"schwab": "charles_schwab"}.get(platform, platform)
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
    lookback_start = start - timedelta(days=7)
    months = {start.astimezone(timezone.utc).strftime("%Y-%m"), end.astimezone(timezone.utc).strftime("%Y-%m")}
    entries = []
    for month in sorted(months):
        entries.extend(read(["ls", "--json", f"{prefix}/{source_platform}/**/{month}/*.json"]))
    total_listed = len(entries)
    if total_listed > 10000:
        raise ValueError("candidate_archive_budget_exceeded")
    keys = ("platform_id", "strategy_profile", "service_name", "account_scope", "account_selector", "deployment_selector", "dry_run_only")
    reports, lookback_entries, uris = [], [], set()

    def _entry_stamp(entry):
        raw_stamp = (entry.get("metadata") or {}).get("updated") or (entry.get("metadata") or {}).get("timeCreated")
        stamp = datetime.fromisoformat(str(raw_stamp).replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("candidate_archive_time_missing")
        return stamp

    def _matches_target(source_target, *, allow_platform_alias=False):
        if not isinstance(source_target, dict):
            return False
        for key in keys:
            if key == "platform_id" and allow_platform_alias:
                aliases = {"schwab", "charles_schwab"}
                if target.get(key) in aliases and source_target.get(key) in aliases:
                    continue
            if source_target.get(key) != target.get(key):
                return False
        return True

    for entry in entries:
        uri = str(entry.get("url", "")).split("#", 1)[0]
        if uri in uris:
            continue
        if not uri.startswith(f"{prefix}/{source_platform}/"):
            raise ValueError("candidate_source_locator_mismatch")
        uris.add(uri)
        stamp = _entry_stamp(entry)
        if stamp < lookback_start or stamp > current:
            continue
        if stamp < start:
            lookback_entries.append((stamp, uri))
            continue
        source = read(["cat", uri])
        source_target = source.get("runtime_target") or {}
        if not _matches_target(source_target):
            continue
        source_stamp = datetime.fromisoformat(str(source.get("started_at", "")).replace("Z", "+00:00"))
        if source_stamp.tzinfo is None:
            raise ValueError("candidate_source_time_invalid")
        if start <= source_stamp < end:
            reports.append(source)
        elif lookback_start <= source_stamp < start:
            lookback_entries.append((source_stamp, uri))
    payload = projector(reports, expected_target=target, opaque_account_uid=uid, target_id=tid, now=current) if reports else {"runs": []}
    if not payload["runs"]:
        schedule_raw = env.get("DIGEST_NOTIFICATION_SCHEDULE_JSON", "").strip()
        if not schedule_raw:
            raise ValueError("candidate_source_absent")
        from quant_platform_kit.common.runtime_heartbeat_policy import filter_due_targets
        schedule = json.loads(schedule_raw)
        due, evaluated = filter_due_targets([schedule], since=start, now=end - timedelta(microseconds=1),
            publication_grace=timedelta(0), warning_logger=lambda message: None)
        if not evaluated or due:
            raise ValueError("candidate_due_source_absent")
        carryover_snapshots = []
        if target.get("dry_run_only") is False:
            from notifications.unified import _account_snapshots, _notification_report
            norm_target = ({**target, "platform_id": "schwab"}
                           if target.get("platform_id") in {"charles_schwab", "schwab"} else target)
            allow_probe = not target.get("dry_run_only")
            for source in sorted(reports, key=lambda item: str(item.get("started_at", "")), reverse=True):
                carryover_snapshots = _account_snapshots(
                    _notification_report(copy_source := json.loads(json.dumps(source))),
                    norm_target, current, allow_carryover=True, allow_live_probe=allow_probe,
                )
                if carryover_snapshots:
                    break
            if not carryover_snapshots:
                def _scan_carryover(candidates):
                    for _stamp, item_uri in sorted(candidates, key=lambda item: item[0], reverse=True):
                        source = read(["cat", item_uri])
                        if not _matches_target(source.get("runtime_target"), allow_platform_alias=True):
                            continue
                        source_stamp = datetime.fromisoformat(str(source.get("started_at", "")).replace("Z", "+00:00"))
                        if source_stamp.tzinfo is None:
                            raise ValueError("candidate_source_time_invalid")
                        if not (lookback_start <= source_stamp < start):
                            continue
                        snaps = _account_snapshots(
                            _notification_report(source), norm_target, current,
                            allow_carryover=True, allow_live_probe=allow_probe,
                        )
                        if snaps:
                            return snaps
                    return []

                carryover_snapshots = _scan_carryover(lookback_entries)
                if not carryover_snapshots:
                    extra_months = {(lookback_start + timedelta(days=offset)).astimezone(timezone.utc).strftime("%Y-%m")
                                    for offset in (0, 4, 7)} - months
                    extra_candidates = []
                    for month in sorted(extra_months):
                        month_entries = read(["ls", "--json", f"{prefix}/{source_platform}/**/{month}/*.json"])
                        total_listed += len(month_entries)
                        if total_listed > 10000:
                            raise ValueError("candidate_archive_budget_exceeded")
                        for entry in month_entries:
                            uri = str(entry.get("url", "")).split("#", 1)[0]
                            if uri in uris:
                                continue
                            if not uri.startswith(f"{prefix}/{source_platform}/"):
                                raise ValueError("candidate_source_locator_mismatch")
                            uris.add(uri)
                            stamp = _entry_stamp(entry)
                            if lookback_start <= stamp < start:
                                extra_candidates.append((stamp, uri))
                    carryover_snapshots = _scan_carryover(extra_candidates)

        idle_row = {
            "platform_id": platform, "strategy_profile": target["strategy_profile"],
            "opaque_account_uid": uid, "target_id": tid, "business_day": day.isoformat(),
            "actually_ran": False, "status": "ok", "scheduled_idle": True,
            "execution_status": "not_run", "reason_code": "not_due", "cycle_count": 0,
            "order_count": None, "fill_count": None,
            "field_status": {"order_count": "unknown", "fill_count": "unknown", "cycle_count": "known"},
            "evidence_provenance": "candidates",
        }
        if carryover_snapshots:
            idle_row["account_daily_facts"] = {"snapshots": carryover_snapshots}
        payload = {"schema_version": "qsl.digest_candidates.v1", "runs": [idle_row]}
    if upload is None:
        from google.cloud import storage
        def upload(uri, body):
            storage.Blob.from_string(uri, client=storage.Client()).upload_from_string(
                body, content_type="application/json", retry=None, timeout=30)
    # Never retry an unknown write result. Public output contains no identities.
    upload(f"{destination}/{platform}/{day.isoformat()}/{slot}.json", json.dumps(payload, allow_nan=False))
    return {"status": "projected", "uploaded": True,
            "runs": sum(row.get("cycle_count") or 0 for row in payload["runs"] if row.get("actually_ran") is True)}


def main():
    try:
        print(json.dumps(emit(os.environ), sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"status": "blocked", "uploaded": False, "reason": "candidate_projection_failed"}))
        return 1
