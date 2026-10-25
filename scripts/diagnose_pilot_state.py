"""Read the existing serving account's owner fence and bounded ledger digest.

This is storage evidence, not broker reconciliation or authority to resume. It
never creates a client, reads a token/report, writes state or clears a claim.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import logging
import os
import re
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

from scripts import diagnose_pilot_metadata as metadata
from scripts.diagnose_pilot_native_identity import _account_hash, _pause_projection


SCHEMA = "schwab.pilot-state-readback.v1"
_LIMIT = 256  # Original QPK bound, per claims/outcomes namespace, not all history.
_LOCAL_REASONS = frozenset({
    "serving_account_binding_unavailable", "serving_account_binding_mismatch",
    "serving_changed_before_storage", "serving_changed_after_storage",
    "storage_source_unavailable", "storage_source_secret_reference",
    "storage_source_invalid", "storage_provider_unavailable",
    "owner_record_invalid", "ledger_result_invalid",
    "ledger_window_changed", "ledger_classification_incomplete",
})


def _classification_projection() -> dict[str, Any]:
    return {
        "read_status": "unavailable", "window_stable": None, "complete": False,
        "claim_records_count": None, "outcome_records_count": None,
        "paired_count": None, "claims_without_outcome_in_window_count": None,
        "outcomes_without_claim_in_window_count": None,
        "explicit_unknown_outcomes_count": None, "pending_reconciliation_outcomes_count": None,
        "recorded_no_pending_outcomes_count": None, "unclassified_outcomes_count": None,
        "invalid_records_count": None,
    }


def _entries_digest(entries: Mapping[str, list[tuple[str, str]]]) -> str:
    # Exact representation in the frozen QPK 2d6626f2 ledger digest contract.
    from quant_platform_kit.common.execution_state import EXECUTION_LEDGER_DIGEST_SCHEMA_VERSION

    records = [
        {"namespace": namespace, "object_sha256": hashlib.sha256(uri.encode()).hexdigest(),
         "content_sha256": hashlib.sha256(content.encode()).hexdigest()}
        for namespace, values in entries.items() for uri, content in values
    ]
    encoded = json.dumps(
        {"schema_version": EXECUTION_LEDGER_DIGEST_SCHEMA_VERSION,
         "records": sorted(records, key=lambda value: (value["namespace"], value["object_sha256"], value["content_sha256"]))},
        ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    return hashlib.sha256(encoded.encode()).hexdigest()


def _record_classification(store: Any, *, namespace: str, uri: str, content: str,
                           prefix: str, account: str, profile: str, outcome: bool) -> tuple[str, str]:
    from quant_platform_kit.common.execution_state import _clean_key_part, _clean_relative_key

    payload = metadata._json(content)
    record = payload.get("metadata") if isinstance(payload, dict) else None
    key = payload.get("marker_key") if isinstance(payload, dict) else None
    if (
        not isinstance(record, dict) or not isinstance(key, str) or not key.startswith(prefix + "/")
        or len(key.split("/")) != 8 or _clean_relative_key(key) != key
        or store._cloud_uri_for_namespace(key, namespace) != uri
        or payload.get("schema_version") != ("execution_outcome.v1" if outcome else "execution_claim.v1")
        or record.get("strategy_profile") != profile or record.get("account_scope") != account
        or record.get("dry_run_only") is not False
    ):
        raise ValueError
    for index, field in ((5, "signal_date"), (6, "effective_date")):
        value = record.get(field)
        fallback = "signal" if index == 5 else "effective"
        if not isinstance(value, str) or key.split("/")[index] != _clean_key_part(value or f"no-{fallback}-date", fallback=fallback):
            raise ValueError
    if not outcome:
        if payload.get("state") != "claimed":
            raise ValueError
        return key, "claim"
    count, orders, status = record.get("orders_pending_count"), record.get("submitted_orders"), record.get("execution_status")
    if (type(count) is not int or count < 0 or not isinstance(orders, list)
            or not all(isinstance(order, dict) for order in orders) or not isinstance(status, str)
            or type(record.get("broker_submission_done")) is not bool):
        raise ValueError
    # Keep the original _recorded_execution_replay semantics. These are recorded
    # intent states, never an inference that the broker has finished an order.
    if status == "pending_reconciliation":
        if count < sum(order.get("status") in {"accepted", "unknown"} for order in orders):
            raise ValueError
        return key, "explicit_unknown" if any(order.get("status") == "unknown" for order in orders) else "pending_reconciliation"
    if status in {"completed", "no_action", "no_op"} and count == 0 and not orders:
        return key, "recorded_no_pending"
    return key, "unclassified"


def _classify_ledger(store: Any, *, account: str, profile: str, expected_digest: str,
                     expected_count: int, projection: dict[str, Any]) -> None:
    from quant_platform_kit.common.execution_state import _clean_key_part

    prefix = "/".join(("v1", "schwab", _clean_key_part(account, fallback="account"),
                       _clean_key_part(profile, fallback="strategy"), "live"))
    entries = {}
    projection["read_status"] = "partial"
    for namespace, field in ((store.namespace, "claim_records_count"), (store.outcome_namespace, "outcome_records_count")):
        # Private reader behavior is intentionally tied to the existing frozen
        # QPK pin: newest by key, bounded bodies, potentially unbounded listing.
        values = store._recent_ledger_entries(namespace=namespace, relative_prefix=prefix, record_limit=_LIMIT)
        if not isinstance(values, list) or len(values) > _LIMIT or any(
            not isinstance(value, tuple) or len(value) != 2 or not all(isinstance(part, str) for part in value)
            for value in values
        ):
            raise metadata._Unavailable("ledger_result_invalid")
        entries[namespace] = values
        projection[field] = len(values)
    after_digest, after_count = store.calculate_recent_ledger_digest(
        platform="schwab", strategy_profile=profile, account_scope=account,
        execution_mode="live", record_limit=_LIMIT,
    )
    projection["window_stable"] = (
        after_digest == expected_digest == _entries_digest(entries)
        and type(after_count) is int and after_count == expected_count == sum(map(len, entries.values()))
    )
    if not projection["window_stable"]:
        raise metadata._Unavailable("ledger_window_changed")
    keys: dict[str, set[str]] = {store.namespace: set(), store.outcome_namespace: set()}
    counts = {"explicit_unknown": 0, "pending_reconciliation": 0, "recorded_no_pending": 0, "unclassified": 0}
    invalid = 0
    for namespace, values in entries.items():
        for uri, content in values:
            try:
                key, category = _record_classification(
                    store, namespace=namespace, uri=uri, content=content, prefix=prefix,
                    account=account, profile=profile, outcome=namespace == store.outcome_namespace,
                )
                if key in keys[namespace]:
                    raise ValueError
                keys[namespace].add(key)
                if category != "claim":
                    counts[category] += 1
            except (ValueError, TypeError, KeyError):
                invalid += 1
    projection["invalid_records_count"] = invalid
    if invalid:
        # Invalid records may contain a counterpart or an UNKNOWN outcome;
        # do not report exact pairing/semantic totals from a valid subset.
        return
    claims, outcomes = keys[store.namespace], keys[store.outcome_namespace]
    projection.update(
        paired_count=len(claims & outcomes),
        claims_without_outcome_in_window_count=len(claims - outcomes),
        outcomes_without_claim_in_window_count=len(outcomes - claims),
        **{category + "_outcomes_count": count for category, count in counts.items()},
    )
    projection["complete"] = counts["unclassified"] == 0
    projection["read_status"] = "observed" if projection["complete"] else "partial"


@contextlib.contextmanager
def _quiet_dependencies():
    previous = logging.root.manager.disable
    try:
        logging.disable(sys.maxsize)
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            yield
    finally:
        logging.disable(previous)


def _storage_source(env: Mapping[str, dict[str, Any]]) -> tuple[str, str]:
    def value(name: str) -> str | None:
        state, raw = metadata._plain(env, name)
        if state == "secret_reference":
            raise metadata._Unavailable("storage_source_secret_reference")
        if state == "invalid":
            raise metadata._Unavailable("storage_source_invalid")
        return "" if state == "blank" else raw

    def current_or_legacy(current: str, legacy: str) -> tuple[str, str | None]:
        # QPK falls back to legacy only when the new key is absent, not blank.
        selected = current if current in env else legacy
        return selected, value(selected)

    selected, uri = current_or_legacy("SCHWAB_EXECUTION_STATE_CLOUD_URI", "SCHWAB_EXECUTION_STATE_GCS_URI")
    if not uri:
        for name in ("QSL_EXECUTION_REPORT_CLOUD_URI", "QSL_EXECUTION_REPORT_GCS_URI"):
            uri = value(name)
            if uri:
                selected = name
                break
        else:
            selected, uri = current_or_legacy("EXECUTION_REPORT_CLOUD_URI", "EXECUTION_REPORT_GCS_URI")
    if not uri:
        raise metadata._Unavailable("storage_source_unavailable")
    if uri != uri.strip() or len(uri) > 2048 or not re.fullmatch(r"gs://[a-z0-9][a-z0-9._-]+(?:/[^?#\r\n]*)?", uri):
        raise metadata._Unavailable("storage_source_invalid")
    state, provider = metadata._plain(env, "QSL_CLOUD_PROVIDER")
    if state not in {"absent", "explicit"} or (provider is not None and provider.strip().lower() != "gcp"):
        raise metadata._Unavailable("storage_provider_unavailable")
    return selected, uri


def _existing_store(*, source: str, uri: str, project: str):
    from quant_platform_kit.cloud import _resolve_provider
    from quant_platform_kit.common.execution_state import build_execution_marker_store_from_env

    # The fixed GCP serving source must not be redirected by runner/local config.
    if _resolve_provider() != "gcp":
        raise metadata._Unavailable("storage_provider_unavailable")
    store = build_execution_marker_store_from_env(
        platform_env_prefix="SCHWAB", project_id=project,
        env_reader=lambda name, default=None: uri if name == source else default,
    )
    return replace(store, local_dir=None)


def _read_owner(store: Any, *, account: str, profile: str) -> dict[str, Any]:
    from quant_platform_kit.common.execution_state import build_account_owner_marker_key

    key = build_account_owner_marker_key(broker="schwab", account_id=account)
    present = store.has_marker(key)
    if type(present) is not bool:
        raise metadata._Unavailable("owner_record_invalid")
    if not present:
        return {"read_status": "absent", "present": False, "record_valid": None, "owner_matches_strategy_profile": None}
    payload = store.read_marker(key)
    record = payload.get("metadata") if isinstance(payload, dict) else None
    owner = record.get("owner_id") if isinstance(record, dict) else None
    if (
        not isinstance(payload, dict) or payload.get("schema_version") != "execution_claim.v1"
        or payload.get("marker_key") != key or payload.get("state") != "claimed"
        or not isinstance(owner, str) or not owner or owner != owner.strip()
    ):
        # An object that vanished or became unreadable is unknown, never absent.
        raise metadata._Unavailable("owner_record_invalid")
    return {"read_status": "observed", "present": True, "record_valid": True, "owner_matches_strategy_profile": owner == profile}


def diagnose_state(
    environ: Mapping[str, str], *, describe: Callable[..., dict[str, Any]] | None = None,
    store_factory: Callable[..., Any] = _existing_store,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema_version": SCHEMA, "status": "unavailable", "reason": "source_unavailable",
        "account_source": "actual_serving_runtime_risk_binding",
        "serving_revision_sha256": None, "identity_matches": {},
        "binding_account_matches_expected": None,
        "serving_stable_before_storage": None, "serving_stable_after_storage": None,
        "storage_source": None, "storage_read_attempted": False,
        "continuity": {},
        "owner": {"read_status": "unavailable", "present": None, "record_valid": None, "owner_matches_strategy_profile": None},
        "ledger": {"read_status": "unavailable", "sha256": None, "records_count": None,
                   "body_record_limit_per_namespace": _LIMIT, "body_read_passes": 3,
                   "full_history_verified": False,
                   "unresolved_claims_verified": False, "broker_complete": False,
                   "classification": _classification_projection()},
        "broker_requested": False, "token_read": False, "reports_read": False,
        "cloud_write_attempted": False, "claims_changed": False, "breaker_reset": False,
        "trading_state_changed": False, "grant_exclusivity_verified": False,
        "recovery_qualified": False, "execution_enabled_by_check": False,
    }
    read = metadata._describe if describe is None else describe
    stage = "metadata"
    try:
        with _quiet_dependencies():
            service, project, region, expected, _declared = metadata._expected(environ)
        metadata._mask_target(expected)
        revision = metadata._serving(read("services", service, project, region), service)
        metadata._mask(revision)
        result["serving_revision_sha256"] = hashlib.sha256(revision.encode()).hexdigest()
        env = metadata._revision_env(read("revisions", revision, project, region), revision, service)
        target = metadata._runtime_target(env, service)
        metadata._mask_target(target)
        result["continuity"] = _pause_projection(target)
        result["identity_matches"] = {field: target[field] == expected[field] for field in metadata._IDENTITY_FIELDS}
        if not all(result["identity_matches"].values()):
            raise metadata._Unavailable("runtime_identity_mismatch")
        account = _account_hash(target)
        result["binding_account_matches_expected"] = account == _account_hash(expected)
        if not result["binding_account_matches_expected"]:
            raise metadata._Unavailable("serving_account_binding_mismatch")
        source, uri = _storage_source(env)
        metadata._mask(uri)
        result["storage_source"] = source
        result["serving_stable_before_storage"] = metadata._serving(read("services", service, project, region), service) == revision
        if not result["serving_stable_before_storage"]:
            raise metadata._Unavailable("serving_changed_before_storage")
        stage = "owner"
        with _quiet_dependencies():
            store = store_factory(source=source, uri=uri, project=project)
            result["storage_read_attempted"] = True
            result["owner"] = _read_owner(store, account=account, profile=target["strategy_profile"])
            stage = "ledger"
            digest, count = store.calculate_recent_ledger_digest(
                platform="schwab", strategy_profile=target["strategy_profile"],
                account_scope=account, execution_mode="live", record_limit=_LIMIT,
            )
            if not isinstance(digest, str) or not re.fullmatch("[0-9a-f]{64}", digest) or type(count) is not int or not 0 <= count <= 2 * _LIMIT:
                raise metadata._Unavailable("ledger_result_invalid")
            result["ledger"].update(read_status="partial", sha256=digest, records_count=count)
            _classify_ledger(store, account=account, profile=target["strategy_profile"],
                             expected_digest=digest, expected_count=count,
                             projection=result["ledger"]["classification"])
            if result["ledger"]["classification"]["complete"]:
                result["ledger"]["read_status"] = "observed"
        stage = "metadata"
        result["serving_stable_after_storage"] = metadata._serving(read("services", service, project, region), service) == revision
        if not result["serving_stable_after_storage"]:
            raise metadata._Unavailable("serving_changed_after_storage")
        if result["ledger"]["classification"]["complete"]:
            result.update(status="observed_state", reason="observed")
        else:
            result["reason"] = "ledger_classification_incomplete"
    except metadata._Unavailable as exc:
        reason = str(exc)
        result["reason"] = reason if reason in metadata._REASONS | _LOCAL_REASONS else "state_read_unavailable"
    except (subprocess.TimeoutExpired, TimeoutError):
        result["reason"] = "metadata_timeout" if stage == "metadata" else stage + "_read_failed"
    except Exception:
        result["reason"] = "metadata_unavailable" if stage == "metadata" else stage + "_read_failed"
    if result["status"] != "observed_state" and result["owner"]["read_status"] != "unavailable":
        result["status"] = "partial_state"
    return result


def main() -> int:
    result = diagnose_state(os.environ) if len(sys.argv) == 1 else {
        "schema_version": SCHEMA, "status": "unavailable", "reason": "arguments_not_supported",
    }
    encoded = json.dumps(result, sort_keys=True, allow_nan=False)
    output = os.environ.get("PILOT_STATE_RESULT_PATH")
    if output:
        try:
            descriptor = os.open(Path(output), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(encoded + "\n")
        except OSError:
            print(json.dumps({"schema_version": SCHEMA, "status": "unavailable", "reason": "result_write_failed"}))
            return 1
    print(encoded)
    return 0 if result["status"] == "observed_state" else 1


if __name__ == "__main__":
    raise SystemExit(main())
