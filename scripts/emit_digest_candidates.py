"""Prepare one Schwab daily projection and write ephemeral DIGEST_CANDIDATES.

Uses the same prepare path as ``run_runtime_daily_from_reports`` (read-only
archive + metadata). Does not publish to QRS, does not upload artifacts, and
never prints equity / opaque uid / target_id values.

Identity for candidates comes from protected env:
  - SCHWAB_DIGEST_OPAQUE_ACCOUNT_UID (preferred) or the configured account_hash
    from RUNTIME_TARGET_JSON binding (already opaque)
  - SCHWAB_DIGEST_TARGET_ID or SCHWAB_ACCOUNT_FACTS_TARGET_ID

Output path: SCHWAB_DIGEST_CANDIDATES_OUTPUT_PATH (required).

Optional equity (read-only; never POST to broker or QRS account-facts sync):
  - SCHWAB_DIGEST_ACCOUNT_FACTS_PATH: explicit local facts JSON, or
  - archive projection via ``project_schwab_account_facts_history`` when
    SCHWAB_ACCOUNT_FACTS_SERVICE_NAME is set and SCHWAB_NET_ASSETS_CURRENCY=USD.
  Ephemeral facts (when projected from archive) may be written to
  SCHWAB_DIGEST_ACCOUNT_FACTS_OUTPUT_PATH (default: beside candidates file).
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import publish_runtime_daily_from_reports as caller
from scripts.project_digest_candidates import (
    load_json_object,
    project_digest_candidates,
)
from scripts.publish_account_facts_from_reports import (
    REGION,
    project_schwab_account_facts_history,
)
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


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _project_account_facts_from_archive(
    *,
    batch: caller.ReadBatch,
    report_prefix: str,
    expected_runtime_revision: str,
    service_name: str,
    target_id: str,
    cash_currency: str | None,
    observed_at: dt.datetime,
) -> tuple[dict[str, Any] | None, str, dict[str, int]]:
    """Read-only facts projection from archive entries. Never POSTs.

    Returns (facts|None, source_or_reason, skip_reason_counts). Counts are
    amount-free reason tallies for diagnosis when every entry is skipped.
    """
    skip_counts: dict[str, int] = {}
    entries: Sequence[Mapping[str, Any]] = batch.entries or ()
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        payload = entry.get("payload")
        uri = entry.get("object_uri")
        if not isinstance(payload, Mapping) or not isinstance(uri, str) or not uri:
            skip_counts["entry_shape_invalid"] = skip_counts.get("entry_shape_invalid", 0) + 1
            continue
        projected = project_schwab_account_facts_history(
            payload,
            source_report_uri=uri,
            report_prefix=report_prefix,
            expected_service_name=service_name,
            expected_runtime_revision=expected_runtime_revision,
            expected_target_id=target_id,
            expected_cash_currency=cash_currency,
            now=observed_at,
        )
        if projected.get("status") == "skipped":
            reason = str(projected.get("reason") or "account_facts_skipped")
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            continue
        # Success body has no status key; never call _publish_once.
        return dict(projected), "archive_projection", skip_counts
    if not skip_counts:
        return None, "account_facts_absent", skip_counts
    # Prefer the most frequent skip reason (stable tie-break by name).
    top_reason = sorted(skip_counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    return None, top_reason, skip_counts


def _resolve_account_facts(
    environ: Mapping[str, str],
    *,
    batch: caller.ReadBatch,
    report_prefix: str,
    expected_runtime_revision: str,
    target_id: str,
    observed_at: dt.datetime,
    candidates_output: Path,
) -> tuple[dict[str, Any] | None, str, bool]:
    """Return (facts, source_tag, facts_file_written).

    Explicit ``SCHWAB_DIGEST_ACCOUNT_FACTS_PATH`` wins. Otherwise attempt a
    read-only archive projection. Never POSTs to QRS / broker.
    """
    facts_path_raw = environ.get("SCHWAB_DIGEST_ACCOUNT_FACTS_PATH")
    if isinstance(facts_path_raw, str) and facts_path_raw.strip():
        try:
            facts = load_json_object(Path(facts_path_raw.strip()))
        except (OSError, ValueError, json.JSONDecodeError):
            return None, "account_facts_unreadable", False
        return facts, "explicit_path", False

    if environ.get("SCHWAB_NET_ASSETS_CURRENCY") != "USD":
        return None, "net_assets_currency_unconfirmed", False

    service_raw = environ.get("SCHWAB_ACCOUNT_FACTS_SERVICE_NAME")
    if not isinstance(service_raw, str) or not service_raw.strip():
        return None, "account_facts_service_absent", False

    if not target_id:
        return None, "target_id_absent_for_facts", False

    cash_raw = environ.get("SCHWAB_CASH_CURRENCY")
    cash_currency = cash_raw.strip() if isinstance(cash_raw, str) and cash_raw.strip() else None

    facts, source, skip_counts = _project_account_facts_from_archive(
        batch=batch,
        report_prefix=report_prefix,
        expected_runtime_revision=expected_runtime_revision,
        service_name=service_raw.strip(),
        target_id=target_id,
        cash_currency=cash_currency,
        observed_at=observed_at,
    )
    if facts is None:
        # Encode amount-free skip histogram into source tag for logs
        # (e.g. observation_out_of_window:18+runtime_target_scope_mismatch:2).
        if skip_counts:
            hist = "+".join(
                f"{name}:{count}"
                for name, count in sorted(skip_counts.items(), key=lambda kv: (-kv[1], kv[0]))
            )
            return None, f"{source}|{hist}", False
        return None, source, False

    out_raw = environ.get("SCHWAB_DIGEST_ACCOUNT_FACTS_OUTPUT_PATH")
    if isinstance(out_raw, str) and out_raw.strip():
        facts_path = Path(out_raw.strip())
    else:
        facts_path = candidates_output.with_name("schwab-digest-account-facts.json")
    try:
        _write_json(facts_path, facts)
    except OSError:
        # Candidates can still proceed with in-memory facts.
        return facts, source, False
    return facts, source, True


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
    account_facts, facts_source, facts_written = _resolve_account_facts(
        environ,
        batch=batch,
        report_prefix=prefix,
        expected_runtime_revision=facts.runtime_revision,
        target_id=tid,
        observed_at=now,
        candidates_output=output_path,
    )
    if facts_source == "account_facts_unreadable":
        return {"status": "skipped", "reason": "account_facts_unreadable"}

    payload = project_digest_candidates(
        daily_projection=sealed,
        opaque_account_uid=uid,
        target_id=tid,
        account_facts=account_facts,
    )
    try:
        _write_json(output_path, payload)
    except OSError:
        return {"status": "skipped", "reason": "output_write_failed"}

    record = sealed["records"][0]
    runs = payload.get("runs") or []
    equity_present = any(
        isinstance(row, Mapping) and row.get("equity") is not None for row in runs
    )
    fill_count_null = all(
        isinstance(row, Mapping) and row.get("fill_count") is None for row in runs
    )
    return {
        "status": "candidates_written",
        "reason": payload.get("producer_reason") or "ok",
        "producer_status": payload.get("producer_status"),
        "runs": len(runs),
        "projected_run_count": len(record.get("runs") or []),
        "daily_status": record.get("status"),
        "identity_uid_present": bool(uid),
        "identity_target_present": bool(tid),
        "equity_present": equity_present,
        "fill_count_null": fill_count_null,
        "account_facts_source": facts_source,
        "account_facts_ephemeral_written": facts_written,
        # Holdings come only from archive facts broker_reported_positions.
        "holdings_omitted": not any(
            isinstance(row, Mapping) and row.get("holdings") for row in runs
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
