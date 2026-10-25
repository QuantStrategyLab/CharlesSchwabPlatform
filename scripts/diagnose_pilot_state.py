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
})


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
                   "body_record_limit_per_namespace": _LIMIT, "full_history_verified": False,
                   "unresolved_claims_verified": False, "broker_complete": False},
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
            result["ledger"].update(read_status="observed", sha256=digest, records_count=count)
        stage = "metadata"
        result["serving_stable_after_storage"] = metadata._serving(read("services", service, project, region), service) == revision
        if not result["serving_stable_after_storage"]:
            raise metadata._Unavailable("serving_changed_after_storage")
        result.update(status="observed_state", reason="observed")
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
