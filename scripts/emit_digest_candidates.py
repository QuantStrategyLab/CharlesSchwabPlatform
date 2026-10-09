"""Prepare one Schwab daily projection and write ephemeral DIGEST_CANDIDATES.

Uses the same prepare path as ``run_runtime_daily_from_reports`` (read-only
archive + metadata). Does not publish to QRS, does not upload artifacts, and
never prints equity / opaque uid / target_id values.

Identity for candidates comes from protected env:
  - SCHWAB_DIGEST_OPAQUE_ACCOUNT_UID (preferred) or the configured account_hash
    from RUNTIME_TARGET_JSON binding (already opaque)
  - SCHWAB_DIGEST_TARGET_ID or SCHWAB_ACCOUNT_FACTS_TARGET_ID

Output path: SCHWAB_DIGEST_CANDIDATES_OUTPUT_PATH (required).
Optional equity: SCHWAB_DIGEST_ACCOUNT_FACTS_PATH pointing at a local facts JSON.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import publish_runtime_daily_from_reports as caller
from scripts.project_digest_candidates import (
    load_json_object,
    project_digest_candidates,
)
from scripts.publish_account_facts_from_reports import REGION
from scripts.runtime_daily_source_facts import effective_schedule, read_source_facts


def _safe_identity(environ: Mapping[str, str], account_hash: str) -> tuple[str, str]:
    uid = environ.get("SCHWAB_DIGEST_OPAQUE_ACCOUNT_UID") or account_hash or ""
    tid = (
        environ.get("SCHWAB_DIGEST_TARGET_ID")
        or environ.get("SCHWAB_ACCOUNT_FACTS_TARGET_ID")
        or ""
    )
    if not isinstance(uid, str):
        uid = ""
    if not isinstance(tid, str):
        tid = ""
    return uid.strip(), tid.strip()


def _resealed_projection(prepared: caller.PreparedDaily) -> dict[str, Any] | None:
    """Return the canonical projection body when the preparation digest matches.

    Unlike ``_sealed_projection_record`` (anomaly-status summary only), this
    accepts any prepared daily status so quiet ``no_signal`` days still emit.
    """
    try:
        if prepared.reason != "prepared" or prepared.projection is None:
            return None
        if not caller._within_budget(prepared.projection):
            return None
        body = caller._canonical_body(prepared.projection)
        if prepared._preparation_digest != caller._preparation_digest(
            body, prepared._source_binding_id or ""
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
        runs = records[0].get("runs")
        if type(runs) is not list or len(runs) > caller.MAX_ITEMS:
            return None
        return projection
    except Exception:
        return None


def emit_digest_candidates(
    environ: Mapping[str, str],
    *,
    observed_at: dt.datetime | None = None,
    fact_reader: Callable[..., Any] = read_source_facts,
    archive_reader: Callable[..., Any] = caller.read_archive,
    session_dates_loader: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    output_raw = environ.get("SCHWAB_DIGEST_CANDIDATES_OUTPUT_PATH")
    if not isinstance(output_raw, str) or not output_raw.strip():
        return {"status": "skipped", "reason": "output_path_missing"}
    output_path = Path(output_raw.strip())

    now = observed_at if observed_at is not None else dt.datetime.now(dt.timezone.utc)
    if (
        not isinstance(now, dt.datetime)
        or now.tzinfo is None
        or now.utcoffset() is None
    ):
        return {"status": "skipped", "reason": "observation_time_invalid"}

    prefix = environ.get("SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX")
    if environ.get("GCP_PROJECT_ID") != caller.PROJECT_ID:
        return {"status": "skipped", "reason": "source_configuration_invalid"}
    if not caller._valid_report_prefix(prefix) or environ.get("GCP_REGION") != REGION:
        return {"status": "skipped", "reason": "source_configuration_invalid"}

    try:
        account_hash, _policy = caller._select_identity(environ)
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
        schedule_provider=lambda policy_arg, **kwargs: effective_schedule(
            policy_arg, facts=facts, **kwargs
        ),
    )
    if prepared.reason != "prepared" or prepared.projection is None:
        return {
            "status": "skipped",
            "reason": prepared.reason or "projection_unavailable",
        }

    sealed = _resealed_projection(prepared)
    if sealed is None:
        return {"status": "skipped", "reason": "projection_seal_failed"}

    uid, tid = _safe_identity(environ, account_hash)
    account_facts = None
    facts_path_raw = environ.get("SCHWAB_DIGEST_ACCOUNT_FACTS_PATH")
    if isinstance(facts_path_raw, str) and facts_path_raw.strip():
        try:
            account_facts = load_json_object(Path(facts_path_raw.strip()))
        except (OSError, ValueError, json.JSONDecodeError):
            return {"status": "skipped", "reason": "account_facts_unreadable"}

    payload = project_digest_candidates(
        daily_projection=sealed,
        opaque_account_uid=uid,
        target_id=tid,
        account_facts=account_facts,
    )
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except OSError:
        return {"status": "skipped", "reason": "output_write_failed"}

    record = sealed["records"][0]
    return {
        "status": "candidates_written",
        "reason": payload.get("producer_reason") or "ok",
        "producer_status": payload.get("producer_status"),
        "runs": len(payload.get("runs") or []),
        "projected_run_count": len(record.get("runs") or []),
        "daily_status": record.get("status"),
        "identity_uid_present": bool(uid),
        "identity_target_present": bool(tid),
        "equity_present": any(
            isinstance(row, Mapping) and row.get("equity") is not None
            for row in (payload.get("runs") or [])
        ),
    }


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args:
        print(json.dumps({"status": "skipped", "reason": "unsupported_arguments"}))
        return 2
    try:
        result = emit_digest_candidates(os.environ)
    except Exception:
        print(json.dumps({"status": "skipped", "reason": "operation_failed"}))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("status") == "candidates_written" else 2


if __name__ == "__main__":
    raise SystemExit(main())
