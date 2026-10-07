"""Prepare one fixed Schwab daily target; publication is opt-in and not wired.

All cloud observations are explicit inputs. The CLI is offline preparation only.
The initial reader cannot prove historical retention/unresolved completeness.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from zoneinfo import ZoneInfo

# Support direct offline script invocation without importing either publisher main.
if __package__ in {None, ""}:
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.publish_account_facts_from_reports import (
    PROJECT_ID,
    _REVISION,
    _binding_id,
    _instant,
    _report_uri_parts,
    _valid_report_prefix,
)
from scripts.runtime_daily_report_projection import (
    _account_identity_problem,
    _scope_problem,
    project_daily_runtime,
)
from scripts.runtime_heartbeat_policy import (
    filter_due_targets,
    load_runtime_targets,
    target_latest_due_at,
)

TARGET = {
    "service": "charles-schwab-quant-service",
    "strategy_profile": "soxl_soxx_trend_income",
    "account_scope": "live",
}
ZONE = ZoneInfo("America/New_York")
SYNC_URL = (
    "https://qsl-strategy-switch-console.pigbibi.workers.dev/api/runtime-daily/sync"
)
MAX_ITEMS = 20
MAX_BODY_BYTES = 64 * 1024
MAX_REPORT_BYTES = 1024 * 1024
MAX_ACK_BYTES = 4096
IDENTITY_DIAGNOSTIC_KEYS = frozenset(
    {
        "mismatch_provenance_passed",
        "mismatch_provenance_failed",
        "mismatch_provenance_unknown",
        "mismatch_passed_ascii_case_only",
    }
)
PREFILTER_DIAGNOSTIC_CATEGORIES = frozenset(
    {
        "uri_invalid",
        "time_invalid",
        "schema_invalid",
        "scope_invalid",
        "revision_mismatch",
        "path_mismatch",
        "time_order_invalid",
        "size_invalid",
        "unevaluable",
        "provenance_passed",
    }
)


@dataclass(repr=False)
class ReadBatch:
    """Private reader observations; there is deliberately no completeness switch."""

    entries: Sequence[Mapping[str, Any]] = ()
    read_failed: bool = False
    truncated: bool = False


@dataclass(frozen=True)
class PreparedDaily:
    reason: str
    projection: dict[str, Any] | None = field(default=None, repr=False)
    _source_binding_id: str | None = field(default=None, repr=False)
    _preparation_digest: str | None = field(default=None, repr=False)


def _canonical_body(projection: Mapping[str, Any]) -> bytes:
    return json.dumps(
        projection,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _preparation_digest(body: bytes, binding_id: str) -> str:
    # In-process accidental-mutation guard, not authentication of hostile Python.
    return hashlib.sha256(body + b"\0" + binding_id.encode("ascii")).hexdigest()


def _json_object(raw: object) -> dict[str, Any]:
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_BODY_BYTES:
        raise ValueError
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise ValueError
    return result


def _select_identity(environ: Mapping[str, str]) -> tuple[str, dict[str, Any]]:
    """Use existing policy normalization, but inspect each item before deduping."""
    independent = _json_object(environ.get("RUNTIME_TARGET_JSON"))
    independent_env = dict(environ)
    independent_env.pop("CLOUD_RUN_SERVICE_TARGETS_JSON", None)
    independent_targets = load_runtime_targets(independent_env, include_disabled=True)
    if (
        len(independent_targets) != 1
        or any(
            independent_targets[0].get(key) != value for key, value in TARGET.items()
        )
        or independent_targets[0].get("enabled") is not True
    ):
        raise ValueError
    binding = independent.get("runtime_risk_limits", {}).get("binding", {})
    account_hash = binding.get("account_hash")
    if (
        not isinstance(account_hash, str)
        or not account_hash
        or account_hash != account_hash.strip()
        or len(account_hash) > 512
    ):
        raise ValueError
    raw = environ.get("CLOUD_RUN_SERVICE_TARGETS_JSON")
    policies = independent_targets
    if raw:
        if len(raw.encode("utf-8")) > MAX_BODY_BYTES:
            raise ValueError
        configured = json.loads(raw)
        items = (
            configured.get("targets") if isinstance(configured, dict) else configured
        )
        defaults = (
            configured.get("defaults", {}) if isinstance(configured, dict) else {}
        )
        if not isinstance(items, list) or not all(isinstance(x, dict) for x in items):
            raise ValueError
        policies = []
        for item in items:
            single = dict(environ)
            single["CLOUD_RUN_SERVICE_TARGETS_JSON"] = json.dumps(
                {"targets": [item], "defaults": defaults}
            )
            policies.extend(load_runtime_targets(single, include_disabled=True))
    candidates = [
        p for p in policies if p.get("service", "").lower() == TARGET["service"]
    ]
    if (
        len(candidates) != 1
        or any(candidates[0].get(key) != value for key, value in TARGET.items())
        or candidates[0].get("enabled") is not True
    ):
        raise ValueError
    # An explicit absent calendar is not repaired with an inferred default here.
    policy = dict(candidates[0])
    if independent.get("market_calendar") == "":
        policy["market_calendar"] = ""
    return account_hash, policy


def matured_schedule(
    policy: Mapping[str, Any],
    *,
    observed_at: dt.datetime,
    session_dates_loader: Callable[..., set[dt.date]] | None = None,
    publication_grace: dt.timedelta = dt.timedelta(minutes=30),
) -> dict[str, Any] | None:
    """Only map an established current-day matured due fact; never infer not-due."""
    if (
        not policy.get("market_calendar")
        or policy.get("market_timezone") != ZONE.key
        or policy.get("enabled") is not True
    ):
        return None
    day = observed_at.astimezone(ZONE).date()
    start = dt.datetime.combine(day, dt.time(), ZONE)
    warnings: list[bool] = []
    grace = publication_grace
    options: dict[str, Any] = {}
    if session_dates_loader is not None:
        options["session_dates_loader"] = session_dates_loader
    try:
        due, evaluated = filter_due_targets(
            [dict(policy)],
            since=start,
            now=observed_at,
            market_aware=True,
            publication_grace=grace,
            warning_logger=lambda _message: warnings.append(True),
            **options,
        )
        latest = target_latest_due_at(due[0]) if len(due) == 1 else None
        if (
            warnings
            or not evaluated
            or latest is None
            or latest.tzinfo is None
            or latest.astimezone(ZONE).date() != day
            or latest + grace > observed_at
        ):
            return None
    except Exception:
        return None
    return {
        "state": "due",
        "business_date": day.isoformat(),
        "timezone": ZONE.key,
        "latest_due_at": latest.isoformat(),
        "next_due_at": None,
        "grace_ends_at": (latest + grace).isoformat(),
        "publication_grace_ended": True,
        "expected_window": "unspecified",
        "reason": "publication_grace_ended",
    }


def read_archive(
    *,
    report_prefix: str,
    observed_at: dt.datetime,
    client: Any = None,
) -> ReadBatch:
    """Read at most 20 archived objects, with current-day objects considered first.

    Two bounded prefix ranges cover current/later and earlier archives. Exhaustion
    is observed, not assumed from an object count. It still does not prove report
    retention or the complete earlier-unresolved history, so no complete flag is
    emitted. No shared temporary reports or raw error logs are created.
    """
    result = ReadBatch(entries=[])
    try:
        if not _valid_report_prefix(report_prefix):
            raise ValueError
        now = _instant(observed_at.isoformat())
        parts = urlsplit(report_prefix)
        prefix = parts.path.lstrip("/")
        start = dt.datetime.combine(now.astimezone(ZONE).date(), dt.time(), ZONE)
        boundary = prefix + start.astimezone(dt.timezone.utc).strftime(
            "%Y-%m/%Y%m%dT%H%M%SZ.json"
        )
        if client is None:
            from google.cloud import storage

            client = storage.Client(project=PROJECT_ID)
        attempted = 0
        for offset in ({"start_offset": boundary}, {"end_offset": boundary}):
            remaining = MAX_ITEMS - attempted
            objects = iter(
                client.list_blobs(
                    parts.netloc,
                    prefix=prefix,
                    max_results=remaining + 1,
                    page_size=remaining + 1,
                    timeout=15,
                    retry=None,
                    **offset,
                )
            )
            for _ in range(remaining + 1):
                blob = next(objects, None)
                if blob is None:
                    break
                if attempted == MAX_ITEMS:
                    result.truncated = True
                    return result
                attempted += 1
                try:
                    uri = f"gs://{parts.netloc}/{blob.name}"
                    _report_uri_parts(uri, report_prefix)
                    if (
                        type(blob.size) is not int
                        or not 0 <= blob.size <= MAX_REPORT_BYTES
                        or blob.content_encoding
                        or blob.generation is None
                    ):
                        raise ValueError
                    content = blob.download_as_bytes(
                        start=0,
                        end=MAX_REPORT_BYTES,
                        if_generation_match=blob.generation,
                        timeout=15,
                        retry=None,
                    )
                    if (
                        not isinstance(content, bytes)
                        or len(content) > MAX_REPORT_BYTES
                    ):
                        raise ValueError
                    payload = json.loads(content)
                    if not isinstance(payload, dict):
                        raise ValueError
                    result.entries.append({"payload": payload, "object_uri": uri})
                except Exception:
                    result.read_failed = True
    except Exception:
        result.read_failed = True
    return result


def _report_provenance_passes(
    payload: Mapping[str, Any],
    *,
    object_uri: str,
    report_prefix: str,
    expected_runtime_revision: str,
    observed_at: dt.datetime,
) -> bool:
    """The original non-account checks, in their original evaluation order."""
    return (
        _report_provenance_problem(
            payload,
            object_uri=object_uri,
            report_prefix=report_prefix,
            expected_runtime_revision=expected_runtime_revision,
            observed_at=observed_at,
        )
        is None
    )


def _report_provenance_problem(
    payload: Mapping[str, Any],
    *,
    object_uri: str,
    report_prefix: str,
    expected_runtime_revision: str,
    observed_at: dt.datetime,
) -> str | None:
    """Decompose the same predicate, preserving short-circuit and exceptions."""
    month, stamp = _report_uri_parts(object_uri, report_prefix)
    started = _instant(payload.get("started_at"))
    finished = _instant(payload.get("finished_at"))
    scope_problem = _scope_problem(payload)
    if scope_problem is not None:
        return (
            "schema_invalid" if scope_problem == "invalid_report" else "scope_invalid"
        )
    if (
        payload.get("diagnostics", {}).get("runtime_revision")
        != expected_runtime_revision
    ):
        return "revision_mismatch"
    if (
        stamp != payload.get("run_id")
        or stamp != started.strftime("%Y%m%dT%H%M%SZ")
        or month != started.strftime("%Y-%m")
    ):
        return "path_mismatch"
    if not started <= finished <= observed_at:
        return "time_order_invalid"
    if len(json.dumps(payload).encode("utf-8")) > MAX_REPORT_BYTES:
        return "size_invalid"
    return None


def diagnose_report_prefilter(
    *,
    batch: ReadBatch,
    report_prefix: str,
    expected_runtime_revision: str,
    observed_at: dt.datetime,
) -> dict[str, int | bool] | None:
    """Classify this memory batch without identity comparison or source reads."""
    try:
        if (
            type(batch) is not ReadBatch
            or type(batch.entries) not in (list, tuple)
            or len(batch.entries) > MAX_ITEMS
            or type(batch.read_failed) is not bool
            or type(batch.truncated) is not bool
            or not _valid_report_prefix(report_prefix)
            or not isinstance(expected_runtime_revision, str)
            or _REVISION.fullmatch(expected_runtime_revision) is None
            or not isinstance(observed_at, dt.datetime)
        ):
            return None
        now = _instant(observed_at.isoformat())
        counts = {"zero_run_" + key: 0 for key in PREFILTER_DIAGNOSTIC_CATEGORIES}
        for entry in batch.entries:
            reason = "unevaluable"
            if type(entry) is dict and type(entry.get("payload")) is dict:
                payload = entry["payload"]
                try:
                    _report_uri_parts(entry["object_uri"], report_prefix)
                except Exception:
                    reason = "uri_invalid"
                else:
                    try:
                        _instant(payload.get("started_at"))
                        _instant(payload.get("finished_at"))
                    except Exception:
                        reason = "time_invalid"
                    else:
                        try:
                            reason = (
                                _report_provenance_problem(
                                    payload,
                                    object_uri=entry["object_uri"],
                                    report_prefix=report_prefix,
                                    expected_runtime_revision=expected_runtime_revision,
                                    observed_at=now,
                                )
                                or "provenance_passed"
                            )
                        except Exception:
                            pass
            if reason not in PREFILTER_DIAGNOSTIC_CATEGORIES:
                return None
            counts["zero_run_" + reason] += 1
        return {
            "zero_run_entries": len(batch.entries),
            "zero_run_read_failed": batch.read_failed,
            "zero_run_truncated": batch.truncated,
            **counts,
        }
    except Exception:
        return None


def diagnose_identity_mismatch(
    *,
    environ: Mapping[str, str],
    batch: ReadBatch,
    report_prefix: str,
    expected_runtime_revision: str,
    observed_at: dt.datetime,
) -> dict[str, int] | None:
    """Count only the supplied first 20 memory entries, never read more data.

    This cannot admit a report, repair a hash, change coverage, or publish.
    Missing context is unavailable (None), not a fabricated zero-count result.
    Source truncation/read errors are deliberately neither cleared nor inferred.
    """
    try:
        account_hash, _policy = _select_identity(environ)
        if (
            not _valid_report_prefix(report_prefix)
            or not isinstance(expected_runtime_revision, str)
            or _REVISION.fullmatch(expected_runtime_revision) is None
            or not isinstance(observed_at, dt.datetime)
            or type(batch.entries) not in (list, tuple)
        ):
            return None
        now = _instant(observed_at.isoformat())
        counts = dict.fromkeys(IDENTITY_DIAGNOSTIC_KEYS, 0)
        # Exact built-in containers avoid iterator callbacks or hidden readers.
        for entry in batch.entries[:MAX_ITEMS]:
            if type(entry) is not dict:
                continue
            payload = entry.get("payload")
            if type(payload) is not dict:
                continue
            summary = payload.get("summary")
            if type(summary) is not dict:
                continue
            observation = summary.get("account_observation")
            if type(observation) is not dict:
                continue
            report_hash = observation.get("account_hash")
            if (
                type(report_hash) is not str
                or not report_hash
                or report_hash != report_hash.strip()
                or report_hash == account_hash
            ):
                continue
            try:
                passed = _report_provenance_passes(
                    payload,
                    object_uri=entry["object_uri"],
                    report_prefix=report_prefix,
                    expected_runtime_revision=expected_runtime_revision,
                    observed_at=now,
                )
            except Exception:
                counts["mismatch_provenance_unknown"] += 1
                continue
            if not passed:
                counts["mismatch_provenance_failed"] += 1
                continue
            counts["mismatch_provenance_passed"] += 1
            if (
                report_hash.isascii()
                and account_hash.isascii()
                and report_hash.lower() == account_hash.lower()
            ):
                # Syntactic relation only. Never use it for identity or binding.
                counts["mismatch_passed_ascii_case_only"] += 1
        return counts
    except Exception:
        return None


def prepare_daily(
    *,
    environ: Mapping[str, str],
    batch: ReadBatch,
    report_prefix: str,
    expected_runtime_revision: str,
    observed_at: dt.datetime,
    session_dates_loader: Callable[..., set[dt.date]] | None = None,
    schedule_provider: Callable[..., dict[str, Any] | None] | None = None,
) -> PreparedDaily:
    """Compute a privacy-safe body from independent policy and private reads."""
    try:
        account_hash, policy = _select_identity(environ)
    except Exception:
        return PreparedDaily("source_identity_unavailable")
    if (
        not isinstance(expected_runtime_revision, str)
        or _REVISION.fullmatch(expected_runtime_revision) is None
        or not _valid_report_prefix(report_prefix)
    ):
        return PreparedDaily("source_configuration_invalid")
    try:
        now = _instant(observed_at.isoformat())
        if not isinstance(observed_at, dt.datetime):
            raise ValueError
    except Exception:
        return PreparedDaily("observation_time_invalid")
    admitted = []
    failed = batch.read_failed or batch.truncated
    try:
        for index, entry in enumerate(batch.entries):
            if index >= MAX_ITEMS:
                failed = True
                break
            try:
                payload = entry["payload"]
                uri = entry["object_uri"]
                if not _report_provenance_passes(
                    payload,
                    object_uri=uri,
                    report_prefix=report_prefix,
                    expected_runtime_revision=expected_runtime_revision,
                    observed_at=now,
                ):
                    raise ValueError
                # Preserve the original exception path for malformed containers:
                # they remain excluded bad reports, not new whole-batch skips.
                identity_problem = _account_identity_problem(payload, account_hash)
                if identity_problem is not None:
                    return PreparedDaily(identity_problem)
                admitted.append({"payload": payload, "object_uri": uri})
            except Exception:
                failed = True
    except Exception:
        failed = True
    schedule = None
    try:
        minutes = float(
            environ.get("RUNTIME_HEARTBEAT_PUBLICATION_GRACE_MINUTES") or "30"
        )
        if not math.isfinite(minutes) or minutes < 0:
            raise ValueError
        # The manual runner supplies a provider gated on actual cloud facts.
        # Missing facts return None before sealing; declared cron is no fallback.
        provider = matured_schedule if schedule_provider is None else schedule_provider
        schedule = provider(
            policy,
            observed_at=now,
            session_dates_loader=session_dates_loader,
            publication_grace=dt.timedelta(minutes=minutes),
        )
    except Exception:
        pass
    projection = project_daily_runtime(
        target=TARGET,
        reports=admitted,
        observed_at=now,
        schedule_facts=schedule,
        # No current reader proves retention and all earlier unresolved runs.
        coverage_complete=False,
        read_errors=["report_read_error"] if failed else [],
        expected_account_hash=account_hash,
    )
    if not _within_budget(projection):
        return PreparedDaily("projection_budget_exceeded")
    binding_id = _binding_id(account_hash, TARGET["service"])
    return PreparedDaily(
        "prepared",
        projection,
        binding_id,
        _preparation_digest(_canonical_body(projection), binding_id),
    )


def _within_budget(projection: Mapping[str, Any]) -> bool:
    try:
        if (
            len(
                json.dumps(
                    projection, separators=(",", ":"), ensure_ascii=True
                ).encode()
            )
            > MAX_BODY_BYTES
        ):
            return False
        record = projection["records"][0]
        return all(
            len(values) <= MAX_ITEMS
            for values in (
                projection["read_errors"],
                projection["unmatched_reports"],
                record["runs"],
                record["excluded_reports"],
                record["conflicts"],
                record["fills"]["records"],
            )
        )
    except Exception:
        return False


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(
        self, request: Request, *_args: object, **_kwargs: object
    ) -> None:
        return None


def publish_prepared(
    prepared: PreparedDaily,
    *,
    environ: Mapping[str, str],
    publish: bool = False,
    expected_account_key: str | None = None,
    opener_factory: Callable[..., Any] = build_opener,
) -> dict[str, str]:
    """Single explicitly requested POST; no retries or independently-verified UI claim.

    The receiver compares the canonical source ID to its current protected binding
    before storage, then alone resolves the UI account key. An optional already
    trusted expected key can be checked, but is not a new required configuration.
    """
    if not publish:
        return {"status": "prepared", "reason": "publication_not_requested"}
    if prepared.reason != "prepared" or prepared.projection is None:
        return {"status": "skipped", "reason": "projection_unavailable"}
    token = environ.get("EXECUTION_EVIDENCE_SYNC_TOKEN")
    if (
        not isinstance(token, str)
        or not token
        or token != token.strip()
        or "\r" in token
        or "\n" in token
        or environ.get("EXECUTION_EVIDENCE_SYNC_URL") != SYNC_URL
    ):
        return {"status": "skipped", "reason": "publish_configuration_unavailable"}
    try:
        account_hash, _policy = _select_identity(environ)
        if prepared._source_binding_id != _binding_id(account_hash, TARGET["service"]):
            raise ValueError
    except Exception:
        return {"status": "skipped", "reason": "source_identity_changed"}
    if not _within_budget(prepared.projection):
        return {"status": "skipped", "reason": "projection_budget_exceeded"}
    try:
        body_bytes = _canonical_body(prepared.projection)
        if prepared._preparation_digest != _preparation_digest(
            body_bytes, prepared._source_binding_id
        ):
            raise ValueError
        # Use this exact verified snapshot for both request and ACK expectations;
        # subsequent mutations of the exposed projection cannot change either.
        body = json.loads(body_bytes)
    except Exception:
        return {"status": "skipped", "reason": "projection_changed"}
    record = body["records"][0]
    if (
        body.get("platform") != "schwab"
        or len(body["records"]) != 1
        or record.get("platform") != "schwab"
        or record.get("target") != TARGET
        or record.get("target_key") != "|".join(TARGET.values())
    ):
        return {"status": "skipped", "reason": "projection_invalid"}
    try:
        request = Request(
            SYNC_URL,
            method="POST",
            data=body_bytes,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "User-Agent": "QSL-Schwab-RuntimeDaily/1.0",
                "X-QSL-Source-Binding-ID": prepared._source_binding_id,
            },
        )
        with opener_factory(_NoRedirect()).open(request, timeout=15) as response:
            if response.status != 200:
                return {"status": "unconfirmed", "reason": "publish_failed"}
            content = response.read(MAX_ACK_BYTES + 1)
            if len(content) > MAX_ACK_BYTES:
                return {"status": "unconfirmed", "reason": "ack_invalid"}
            try:
                result = json.loads(content)
                account = result.get("account_key")
                if (
                    set(result)
                    != {
                        "ok",
                        "stored",
                        "platform",
                        "target_key",
                        "business_date",
                        "account_key",
                    }
                    or result["ok"] is not True
                    or result["stored"] is not True
                    or result["platform"] != "schwab"
                    or result["target_key"] != record["target_key"]
                    or result["business_date"] != record["business_date"]
                    or not isinstance(account, str)
                    or not account
                    or account != account.strip()
                    or len(account) > 256
                    or any(ord(c) < 32 for c in account)
                    or (
                        expected_account_key is not None
                        and account != expected_account_key
                    )
                ):
                    raise ValueError
            except Exception:
                return {"status": "unconfirmed", "reason": "ack_invalid"}
    except Exception:
        return {"status": "unconfirmed", "reason": "publish_failed"}
    # Never emit the receiver's private account alias or claim a logged-in readback.
    return {
        "status": "stored_acknowledged",
        "account_attribution": "receiver_reported_account",
    }


def main(argv: list[str] | None = None) -> int:
    # No cloud access, publication switch, raw report output or coverage override.
    if argv if argv is not None else sys.argv[1:]:
        print("skipped:unsupported_arguments")
        return 2
    project_daily_runtime(
        target=TARGET, reports=(), observed_at=dt.datetime.now(dt.timezone.utc)
    )
    print("prepared:coverage_unconfirmed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
