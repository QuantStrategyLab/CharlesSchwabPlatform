"""Bounded archive clues for one rejected outcome; never an order reconciliation."""

from __future__ import annotations

import datetime as dt
from typing import Any
from urllib.parse import urlsplit

from scripts import diagnose_pilot_metadata as metadata
from scripts.publish_account_facts_from_reports import _report_uri_parts, _valid_report_prefix
from scripts.schwab_native_identity import _account_hash_matches


_MAX_REPORTS = 20
_MAX_BYTES = 1024 * 1024


def projection(*, enabled: bool = False) -> dict[str, Any]:
    return {
        "enabled": enabled, "status": "unknown" if enabled else "disabled",
        "reason": "ledger_not_available" if enabled else "not_requested",
        "report_read_attempted": False, "window_days": 3, "body_limit": _MAX_REPORTS,
        "window_exhausted": None, "window_reports_count": None,
        "date_candidates_count": None,
        "identity_date_candidates_count": None, "candidates_with_revision_count": None,
        "exact_execution_link_supported": False, "writer_source_verified": False,
        "broker_order_mapping_verified": False, "recovery_qualified": False,
    }


def inspect_history(*, outcomes: list[dict], env: dict, state_uri: str,
                    target: dict, account: str, project: str, client: Any = None) -> dict[str, Any]:
    result = projection(enabled=True)
    if target.get("live_continuity", {}).get("state") != "PAUSED":
        result["reason"] = "continuity_not_paused"
        return result
    if len(outcomes) != 1:
        result["reason"] = "rejected_outcome_not_unique"
        return result
    record = outcomes[0]["metadata"]
    try:
        day = dt.date.fromisoformat(record["effective_date"])
        if day.isoformat() != record["effective_date"]:
            raise ValueError
        start = day - dt.timedelta(days=1)
        end = day + dt.timedelta(days=2)
    except (ValueError, TypeError, KeyError, OverflowError):
        result["reason"] = "outcome_date_window_unavailable"
        return result
    # Reuse actual-serving sources, without a dispatch-supplied bucket or URI.
    uri = None
    execution_source = "EXECUTION_REPORT_CLOUD_URI" if "EXECUTION_REPORT_CLOUD_URI" in env else "EXECUTION_REPORT_GCS_URI"
    for name in ("QSL_EXECUTION_REPORT_CLOUD_URI", "QSL_EXECUTION_REPORT_GCS_URI", execution_source):
        entry = env.get(name)
        if entry is not None:
            if "value" not in entry:
                result["reason"] = "report_source_unavailable"
                return result
            if entry["value"]:
                uri = entry["value"]
                break
    if not _valid_report_prefix(uri) or urlsplit(uri).netloc != urlsplit(state_uri).netloc:
        result["reason"] = "report_source_unavailable"
        return result
    parts = urlsplit(uri)
    prefix = parts.path.lstrip("/")
    def boundary(date: dt.date) -> str:
        day = date.isoformat()
        return prefix + day[:7] + "/" + day.replace("-", "") + "T000000Z.json"
    result["report_read_attempted"] = True
    try:
        if client is None:
            from google.cloud import storage

            client = storage.Client(project=project)
        objects = iter(client.list_blobs(
            parts.netloc, prefix=prefix, start_offset=boundary(start), end_offset=boundary(end),
            max_results=_MAX_REPORTS + 1, page_size=_MAX_REPORTS + 1, timeout=15, retry=None,
        ))
        count = candidates = revisions = 0
        result.update(window_exhausted=False, window_reports_count=0,
                      date_candidates_count=0, identity_date_candidates_count=0, candidates_with_revision_count=0)
        for blob in objects:
            if count == _MAX_REPORTS:
                result["reason"] = "report_window_truncated"
                return result
            object_uri = f"gs://{parts.netloc}/{blob.name}"
            month, stamp = _report_uri_parts(object_uri, uri)
            observed_day = dt.datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").date()
            if observed_day.isoformat()[:7] != month:
                raise ValueError
            if not boundary(start) <= blob.name < boundary(end):
                raise ValueError
            if (type(blob.size) is not int or not 0 <= blob.size <= _MAX_BYTES
                    or blob.content_encoding or blob.generation is None):
                raise ValueError
            content = blob.download_as_bytes(start=0, end=_MAX_BYTES,
                if_generation_match=blob.generation, timeout=15, retry=None)
            if not isinstance(content, bytes) or len(content) > _MAX_BYTES:
                raise ValueError
            report = metadata._json(content.decode("utf-8"))
            count += 1
            result["window_reports_count"] = count
            if not isinstance(report, dict):
                raise ValueError
            declared = report.get("runtime_target")
            summary = report.get("summary")
            if not isinstance(declared, dict) or not isinstance(summary, dict):
                continue
            if (report.get("schema_version") != "runtime_report.v1"
                    or report.get("run_id") != stamp or report.get("project_id") != project
                    or report.get("service_name") != target["service_name"]
                    or report.get("platform") != "charles_schwab" or report.get("dry_run") is not False
                    or report.get("strategy_profile") != target["strategy_profile"]
                    or any(summary.get(field) != record[field] for field in ("signal_date", "effective_date"))):
                continue
            result["date_candidates_count"] += 1
            observation = summary.get("account_observation")
            if (any(declared.get(field) != target[field] for field in metadata._IDENTITY_FIELDS)
                    or not isinstance(observation, dict)
                    or not _account_hash_matches(account, observation.get("account_hash"))):
                continue
            candidates += 1
            result["identity_date_candidates_count"] = candidates
            diagnostics = report.get("diagnostics")
            revision = diagnostics.get("runtime_revision") if isinstance(diagnostics, dict) else None
            if isinstance(revision, str) and metadata._NAME.fullmatch(revision):
                revisions += 1
                result["candidates_with_revision_count"] = revisions
        result["window_exhausted"] = True
        # Existing runtime reports carry dates/counts/revision, but no contract
        # binding their execution to the ledger marker or broker order IDs.
        # Even one candidate with zero submitted orders cannot repair an outcome.
        result["reason"] = "exact_execution_link_unavailable" if candidates else "qualified_report_not_found"
    except Exception:
        result["reason"] = "report_read_failed"
        result["window_exhausted"] = None
        for field in ("window_reports_count", "date_candidates_count", "identity_date_candidates_count", "candidates_with_revision_count"):
            result[field] = None
    return result
