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


def run_daily(
    environ: Mapping[str, str],
    *,
    publish: bool = False,
    observed_at: dt.datetime | None = None,
    fact_reader: Callable[..., Any] = read_source_facts,
    archive_reader: Callable[..., Any] = caller.read_archive,
    publisher: Callable[..., Any] = caller.publish_prepared,
    session_dates_loader: Callable[..., Any] | None = None,
) -> dict[str, str]:
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
            return {"status": "skipped", "reason": prepared.reason}
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
    operation: Callable[..., dict[str, str]] = run_daily,
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
