"""Explicit manual daily preparation; --publish additionally attempts one POST.

Unlike the pure caller's offline CLI, this entry reads cloud metadata and reports
when explicitly invoked. It prints classifications only, never source payloads.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from collections.abc import Callable, Mapping
from typing import Any

if __package__ in {None, ""}:
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import publish_runtime_daily_from_reports as caller
from scripts.publish_account_facts_from_reports import REGION
from scripts.runtime_daily_source_facts import (
    SAFE_REASONS,
    effective_schedule,
    read_source_facts,
)

_PUBLICATION_REASONS = frozenset(
    {
        "projection_unavailable",
        "publish_configuration_unavailable",
        "source_identity_changed",
        "projection_budget_exceeded",
        "projection_changed",
        "projection_invalid",
        "publish_failed",
        "ack_invalid",
    }
)


_DAILY_SUMMARY_STATUSES = frozenset(
    {
        "reconciliation_required",
        "unknown",
        "blocked",
        "failed",
        "conflict",
        "read_incomplete",
        "insufficient",
    }
)


def _sealed_projection_record(
    prepared: caller.PreparedDaily,
) -> dict[str, Any] | None:
    """Read the existing sealed projection; do not create admission metadata."""
    try:
        if prepared.reason != "prepared" or not caller._within_budget(
            prepared.projection
        ):
            return None
        body = caller._canonical_body(prepared.projection)
        if prepared._preparation_digest != caller._preparation_digest(
            body, prepared._source_binding_id
        ):
            return None
        projection = json.loads(body)
        records = projection.get("records")
        if (
            type(records) is not list
            or len(records) != 1
            or type(records[0]) is not dict
        ):
            return None
        status, runs = records[0].get("status"), records[0].get("runs")
        if (
            type(status) is not str
            or status not in _DAILY_SUMMARY_STATUSES
            or type(runs) is not list
            or len(runs) > caller.MAX_ITEMS
        ):
            return None
        return records[0]
    except Exception:
        return None


def _prepared_projection_summary(
    prepared: caller.PreparedDaily,
) -> dict[str, str | int]:
    record = _sealed_projection_record(prepared)
    if record is None:
        return {}
    return {
        "daily_status": record["status"],
        "projected_run_count": len(record["runs"]),
    }


def _zero_run_diagnostics(
    prepared: caller.PreparedDaily,
    *,
    batch: caller.ReadBatch,
    report_prefix: str,
    expected_runtime_revision: str,
    observed_at: dt.datetime,
) -> dict[str, int | bool]:
    """Supplement only sealed empty runs with bounded, fixed memory facts."""
    try:
        record = _sealed_projection_record(prepared)
        if record is None or record["runs"]:
            return {}
        counts = caller.diagnose_report_prefilter(
            batch=batch,
            report_prefix=report_prefix,
            expected_runtime_revision=expected_runtime_revision,
            observed_at=observed_at,
        )
        categories = {
            "zero_run_" + key for key in caller.PREFILTER_DIAGNOSTIC_CATEGORIES
        }
        scope_fields = {
            "zero_run_scope_" + key for key in caller.SCOPE_DIAGNOSTIC_FIELDS
        }
        integers = categories | scope_fields | {"zero_run_entries"}
        flags = {"zero_run_read_failed", "zero_run_truncated"}
        if (
            type(counts) is not dict
            or set(counts) != integers | flags
            or any(
                type(counts[key]) is not int or not 0 <= counts[key] <= caller.MAX_ITEMS
                for key in integers
            )
            or any(type(counts[key]) is not bool for key in flags)
            or type(batch) is not caller.ReadBatch
            or type(batch.entries) not in (list, tuple)
            or counts["zero_run_entries"] != len(batch.entries)
            or counts["zero_run_read_failed"] is not batch.read_failed
            or counts["zero_run_truncated"] is not batch.truncated
            or sum(counts[key] for key in categories) != counts["zero_run_entries"]
            or sum(counts[key] for key in scope_fields)
            != counts["zero_run_scope_invalid"]
        ):
            return {}
        excluded = record.get("excluded_reports")
        if type(excluded) is not list or any(
            type(item) is not dict
            or item.get("reason")
            not in {
                "other_business_date",
                "invalid_run_time",
                "missing_run_time",
                "inverted_run_time",
                "future_run_time",
            }
            for item in excluded
        ):
            return {}
        other_day = sum(item["reason"] == "other_business_date" for item in excluded)
        if other_day > counts["zero_run_provenance_passed"]:
            return {}
        return {**counts, "zero_run_other_business_date": other_day}
    except Exception:
        return {}


def _valid_mismatch_counts(counts: object) -> bool:
    if type(counts) is not dict or set(counts) != caller.IDENTITY_DIAGNOSTIC_KEYS:
        return False
    if any(
        type(value) is not int or not 0 <= value <= caller.MAX_ITEMS
        for value in counts.values()
    ):
        return False
    total = sum(
        counts[key]
        for key in (
            "mismatch_provenance_passed",
            "mismatch_provenance_failed",
            "mismatch_provenance_unknown",
        )
    )
    return (
        0 < total <= caller.MAX_ITEMS
        and counts["mismatch_passed_ascii_case_only"]
        <= counts["mismatch_provenance_passed"]
    )


def run_daily(
    environ: Mapping[str, str],
    *,
    publish: bool = False,
    observed_at: dt.datetime | None = None,
    fact_reader: Callable[..., Any] = read_source_facts,
    archive_reader: Callable[..., Any] = caller.read_archive,
    publisher: Callable[..., Any] = caller.publish_prepared,
    session_dates_loader: Callable[..., Any] | None = None,
) -> dict[str, str | int]:
    try:
        if type(publish) is not bool:
            return {"status": "skipped", "reason": "invalid_mode"}
        now = (
            observed_at if observed_at is not None else dt.datetime.now(dt.timezone.utc)
        )
        if (
            not isinstance(now, dt.datetime)
            or now.tzinfo is None
            or now.utcoffset() is None
        ):
            return {"status": "skipped", "reason": "observation_time_invalid"}
        prefix = environ.get("SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX")
        if (
            not caller._valid_report_prefix(prefix)
            or environ.get("GCP_PROJECT_ID") != caller.PROJECT_ID
            or environ.get("GCP_REGION") != REGION
        ):
            return {"status": "skipped", "reason": "source_configuration_invalid"}
        try:
            caller._select_identity(environ)
        except Exception:
            return {"status": "skipped", "reason": "source_identity_unavailable"}
        facts = fact_reader(environ)
        if (
            not isinstance(facts.runtime_revision, str)
            or caller._REVISION.fullmatch(facts.runtime_revision) is None
        ):
            return {"status": "skipped", "reason": "service_revision_unavailable"}
        expected = environ.get("EXPECTED_RUNTIME_REVISION")
        if expected and expected != facts.runtime_revision:
            return {"status": "skipped", "reason": "service_revision_mismatch"}
        batch = archive_reader(report_prefix=prefix, observed_at=now)
        prepared = caller.prepare_daily(
            environ=environ,
            batch=batch,
            report_prefix=prefix,
            expected_runtime_revision=facts.runtime_revision,
            observed_at=now,
            session_dates_loader=session_dates_loader,
            schedule_provider=lambda policy, **kwargs: effective_schedule(
                policy, facts=facts, **kwargs
            ),
        )
        if prepared.reason != "prepared" or prepared.projection is None:
            skipped: dict[str, str | int] = {
                "status": "skipped",
                "reason": prepared.reason,
            }
            if prepared.reason == "source_identity_mismatch":
                # Diagnose only after the original failure. No source reread,
                # admission change, projection mutation or publication follows.
                try:
                    counts = caller.diagnose_identity_mismatch(
                        environ=environ,
                        batch=batch,
                        report_prefix=prefix,
                        expected_runtime_revision=facts.runtime_revision,
                        observed_at=now,
                    )
                    if _valid_mismatch_counts(counts):
                        skipped.update(counts)
                except Exception:
                    pass
            return skipped
        schedule = prepared.projection["records"][0]["schedule"]["state"]
        result = {
            "status": "prepared",
            "reason": "coverage_unconfirmed",
            "completeness": "incomplete",
            "schedule": schedule,
            "source_facts": facts.reason
            if facts.reason in SAFE_REASONS
            else "scheduler_unavailable",
        }
        result.update(_prepared_projection_summary(prepared))
        if result.get("projected_run_count") == 0:
            result.update(
                _zero_run_diagnostics(
                    prepared,
                    batch=batch,
                    report_prefix=prefix,
                    expected_runtime_revision=facts.runtime_revision,
                    observed_at=now,
                )
            )
        if publish:
            publication_env = dict(environ)
            # A step-local daily route, not a change to legacy execution evidence.
            publication_env["EXECUTION_EVIDENCE_SYNC_URL"] = caller.SYNC_URL
            outcome = publisher(prepared, environ=publication_env, publish=True)
            if outcome.get("status") == "stored_acknowledged":
                result["status"] = "stored_acknowledged"
                result["account_attribution"] = "receiver_reported_account"
            else:
                result["status"] = (
                    "skipped" if outcome.get("status") == "skipped" else "unconfirmed"
                )
                reason = outcome.get("reason")
                result["reason"] = (
                    reason
                    if reason in _PUBLICATION_REASONS
                    else "publication_unconfirmed"
                )
        return result
    except Exception:
        return {"status": "skipped", "reason": "operation_failed"}


def main(
    argv: list[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    operation: Callable[..., dict[str, str | int]] = run_daily,
) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args not in ([], ["--publish"]):
        print("skipped:unsupported_arguments")
        return 2
    try:
        result = operation(
            os.environ if environ is None else environ, publish=args == ["--publish"]
        )
        print(json.dumps(result, sort_keys=True))
        return 0 if result.get("status") in {"prepared", "stored_acknowledged"} else 2
    except Exception:
        print("skipped:operation_failed")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
