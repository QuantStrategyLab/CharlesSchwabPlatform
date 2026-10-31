"""Observe one already-rejected outcome object's metadata; no recovery actions."""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
from typing import Any
from urllib.parse import urlsplit


def projection(*, enabled: bool = False) -> dict[str, Any]:
    return {
        "enabled": enabled, "reason": "not_observed" if enabled else "not_requested",
        "object_metadata_status": "unknown", "object_checksum_matches_scan": None,
        "generation_present": None, "creation_time_present": None,
        "update_time_present": None, "declared_source_sha": None,
        "declared_run_id": None, "writer_declaration_verified": False,
        "audit_read_attempted": False, "audit_status": "unknown",
        "audit_event_limit": 20, "audit_window_seconds": 600,
        "audit_window_basis": "object_time_created",
        "audit_matching_object_events": None, "audit_principal_present": None,
        "application_writer_verified": False, "business_outcome_verified": False,
        "recovery_qualified": False,
    }


def locator(uri: str, content: str) -> dict[str, Any]:
    raw = content.encode("utf-8")
    return {"uri": uri, "size": len(raw),
            "md5": base64.b64encode(hashlib.md5(raw, usedforsecurity=False).digest()).decode("ascii")}


def inspect_origin(*, outcomes: list[dict], state_uri: str, project: str,
                   client: Any = None, run: Any = subprocess.run) -> dict[str, Any]:
    result = projection(enabled=True)
    if len(outcomes) != 1:
        result["reason"] = "rejected_outcome_not_unique"
        return result
    try:
        item = outcomes[0]
        parts, root = urlsplit(item["uri"]), urlsplit(state_uri)
        if (parts.scheme != "gs" or parts.netloc != root.netloc or not parts.path
            or not parts.path.startswith(root.path.rstrip("/") + "/")
            or parts.query or parts.fragment):
            result["reason"] = "object_binding_invalid"
            return result
        if client is None:
            from google.cloud import storage

            client = storage.Client(project=project)
        blob = client.bucket(parts.netloc).blob(parts.path.lstrip("/"))
        blob.reload(timeout=15, retry=None)  # Metadata GET only, never download a body.
        result.update(object_metadata_status="observed", generation_present=blob.generation is not None,
                      creation_time_present=isinstance(blob.time_created, dt.datetime),
                      update_time_present=isinstance(blob.updated, dt.datetime))
        matches = (isinstance(blob.md5_hash, str) and blob.md5_hash == item["md5"]
                   and type(blob.size) is int and blob.size == item["size"])
        result["object_checksum_matches_scan"] = matches if blob.md5_hash is not None else None
        if not matches or blob.generation is None:
            result["reason"] = "object_generation_or_checksum_unverified"
            return result
        custom = blob.metadata if isinstance(blob.metadata, dict) else {}
        sources = [custom[key] for key in ("source_revision", "source_commit", "source_sha", "git_sha") if key in custom]
        runs = [custom[key] for key in ("github_run_id", "run_id") if key in custom]
        if sources and len(set(sources)) == 1 and isinstance(sources[0], str) and re.fullmatch(r"[0-9a-f]{40}", sources[0]):
            result["declared_source_sha"] = sources[0]
        if runs and len(set(runs)) == 1 and isinstance(runs[0], str) and re.fullmatch(r"[1-9][0-9]{0,19}", runs[0]):
            result["declared_run_id"] = runs[0]
        observed = blob.time_created
        if not isinstance(observed, dt.datetime) or observed.tzinfo is None:
            result["reason"] = "object_audit_time_unavailable"
            return result
        resource = "projects/_/buckets/" + parts.netloc + "/objects/" + parts.path.lstrip("/")
        start, end = observed - dt.timedelta(minutes=5), observed + dt.timedelta(minutes=5)
        query = ("resource.type=\"gcs_bucket\" AND protoPayload.resourceName=" + json.dumps(resource)
                 + " AND (protoPayload.methodName=\"storage.objects.create\" OR protoPayload.methodName=\"storage.objects.update\")"
                 + " AND timestamp>=" + json.dumps(start.isoformat()) + " AND timestamp<=" + json.dumps(end.isoformat()))
        result["audit_read_attempted"] = True
        process = run(("gcloud", "logging", "read", query, "--project", project, "--format=json", "--limit=21", "--order=asc"),
                      capture_output=True, text=True, check=False, timeout=30,
                      env={**os.environ, "CLOUDSDK_CORE_DISABLE_FILE_LOGGING": "true", "CLOUDSDK_CORE_LOG_HTTP": "false"})
        if process.returncode:
            denied = "PERMISSION_DENIED" in process.stderr or "does not have permission" in process.stderr
            result.update(reason="audit_read_unavailable", audit_status="permission_denied" if denied else "read_failed")
            return result
        if len(process.stdout.encode("utf-8")) > 2 * 1024 * 1024:
            result.update(reason="audit_read_unavailable", audit_status="response_limit")
            return result
        events = json.loads(process.stdout)
        if not isinstance(events, list) or len(events) > 20:
            result.update(reason="audit_read_unavailable", audit_status="truncated_or_invalid")
            return result
        principals = []
        for event in events:
            payload = event.get("protoPayload") if isinstance(event, dict) else None
            if (not isinstance(payload, dict) or payload.get("resourceName") != resource
                or payload.get("methodName") not in {"storage.objects.create", "storage.objects.update"}):
                result.update(reason="audit_read_unavailable", audit_status="invalid_scope")
                return result
            auth = payload.get("authenticationInfo")
            principals.append(isinstance(auth, dict) and bool(auth.get("principalEmail")))
        result.update(reason="object_origin_metadata_observed", audit_status="matching_events" if events else "no_matching_events",
                      audit_matching_object_events=len(events), audit_principal_present=any(principals))
    except Exception as exc:
        result["reason"] = "object_metadata_permission_denied" if type(exc).__name__ in {"Forbidden", "PermissionDenied"} else "origin_read_unavailable"
    return result
