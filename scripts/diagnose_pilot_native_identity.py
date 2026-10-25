"""Verify the actual serving Schwab account through one existing native reader.

This diagnostic cannot change continuity, refresh credentials, submit orders or
read financial reports. It neither consumes nor trusts the separate daily target.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import io
import json
import os
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from scripts import diagnose_pilot_metadata as metadata


SCHEMA = "schwab.pilot-native-identity.v1"
_NATIVE_STATUSES = frozenset({
    "configuration_invalid", "token_dependency_unavailable", "token_adc_unavailable",
    "token_permission_denied", "token_not_found", "token_version_unavailable",
    "token_network_unavailable", "token_load_failed", "token_invalid", "token_expired",
    "token_rejected", "native_account_access_denied", "native_identity_unavailable",
    "native_identity_response_invalid", "native_identity_ambiguous", "native_identity_mismatch",
})
_LOCAL_REASONS = frozenset({
    "serving_account_binding_unavailable", "serving_account_binding_mismatch",
    "serving_changed_before_native", "serving_changed_after_native",
    "runtime_identity_mismatch", "observation_time_invalid",
})


def _account_hash(target: Mapping[str, Any]) -> str:
    risk = target.get("runtime_risk_limits")
    binding = risk.get("binding") if isinstance(risk, dict) else None
    value = binding.get("account_hash") if isinstance(binding, dict) else None
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 512:
        raise metadata._Unavailable("serving_account_binding_unavailable")
    return value


def _pause_projection(target: Mapping[str, Any]) -> dict[str, Any]:
    continuity = target.get("live_continuity")
    record = continuity if isinstance(continuity, dict) else {}
    state = record.get("state")
    # Presence is not a validated cause. The existing continuity contract does
    # not attest an incident reason, and no free text escapes this projection.
    structured = (
        isinstance(record.get("reason_code"), str) and bool(record["reason_code"].strip())
    ) or (
        isinstance(record.get("reason_codes"), list)
        and bool(record["reason_codes"])
        and all(isinstance(value, str) and bool(value.strip()) for value in record["reason_codes"])
    ) or (
        isinstance(record.get("reason"), dict) and bool(record["reason"])
    )
    return {
        "state": state if isinstance(state, str) and state in metadata._STATES else None,
        "paused": state == "PAUSED", "structured_reason_field_present": structured,
        "reason_qualified": False,
    }


def _existing_native_reader(**kwargs: Any) -> str:
    from scripts.schwab_native_identity import verify_native_identity

    return verify_native_identity(**kwargs)


def diagnose_native_identity(
    environ: Mapping[str, str], *, describe: Callable[..., dict[str, Any]] | None = None,
    identity_reader: Callable[..., str] = _existing_native_reader,
    observed_at: dt.datetime | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema_version": SCHEMA, "status": "unavailable", "reason": "source_unavailable",
        "account_source": "actual_serving_runtime_risk_binding",
        "serving_revision_sha256": None, "identity_matches": {},
        "binding_account_matches_expected": None,
        "serving_stable_before_native": None, "serving_stable_after_native": None,
        "native_check_attempted": False, "native_status": None,
        "native_identity_matches": None, "serving_bound_identity_verified": False,
        "continuity": {}, "reports_read": False, "token_refresh_attempted": False,
        "secret_write_attempted": False, "orders_submitted": False,
        "notifications_sent": False, "cloud_write_attempted": False,
        "trading_state_changed": False, "breaker_reset": False,
        "account_policy_qualified": False, "spendable_cash_verified": False,
        "execution_enabled_by_check": False,
    }
    read = metadata._describe if describe is None else describe
    try:
        now = dt.datetime.now(dt.timezone.utc) if observed_at is None else observed_at
        if not isinstance(now, dt.datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise metadata._Unavailable("observation_time_invalid")
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            service, project, region, expected, _declared = metadata._expected(environ)
        metadata._mask_target(expected)
        revision = metadata._serving(read("services", service, project, region), service)
        metadata._mask(revision)
        result["serving_revision_sha256"] = hashlib.sha256(revision.encode()).hexdigest()
        revision_payload = read("revisions", revision, project, region)
        target = metadata._runtime_target(metadata._revision_env(revision_payload, revision, service), service)
        metadata._mask_target(target)
        result["continuity"] = _pause_projection(target)
        result["identity_matches"] = {field: target[field] == expected[field] for field in metadata._IDENTITY_FIELDS}
        if not all(result["identity_matches"].values()):
            raise metadata._Unavailable("runtime_identity_mismatch")
        actual_hash = _account_hash(target)
        result["binding_account_matches_expected"] = actual_hash == _account_hash(expected)
        if not result["binding_account_matches_expected"]:
            raise metadata._Unavailable("serving_account_binding_mismatch")
        result["serving_stable_before_native"] = metadata._serving(read("services", service, project, region), service) == revision
        if not result["serving_stable_before_native"]:
            raise metadata._Unavailable("serving_changed_before_native")
        result["native_check_attempted"] = True
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            native_status = identity_reader(environ={"GCP_PROJECT_ID": project}, expected_account_hash=actual_hash, observed_at=now)
        if native_status != "verified":
            result["native_status"] = native_status if native_status in _NATIVE_STATUSES else "native_identity_unavailable"
            result["native_identity_matches"] = False if native_status in {"native_identity_mismatch", "native_identity_ambiguous"} else None
            result["reason"] = result["native_status"]
            return result
        result["native_status"] = "verified"
        result["native_identity_matches"] = True
        result["serving_stable_after_native"] = metadata._serving(read("services", service, project, region), service) == revision
        if not result["serving_stable_after_native"]:
            raise metadata._Unavailable("serving_changed_after_native")
        result.update(status="verified_native_identity", reason="observed", serving_bound_identity_verified=True)
    except metadata._Unavailable as exc:
        reason = str(exc)
        result["reason"] = reason if reason in metadata._REASONS | _LOCAL_REASONS else "native_identity_unavailable"
    except (subprocess.TimeoutExpired, TimeoutError):
        result["reason"] = "metadata_timeout" if not result["native_check_attempted"] or result["native_status"] == "verified" else "native_identity_unavailable"
    except Exception:
        result["reason"] = "native_identity_unavailable" if result["native_check_attempted"] and result["native_status"] != "verified" else "metadata_unavailable"
    return result


def main() -> int:
    if len(sys.argv) != 1:
        result = {"schema_version": SCHEMA, "status": "unavailable", "reason": "arguments_not_supported"}
    else:
        result = diagnose_native_identity(os.environ)
    encoded = json.dumps(result, sort_keys=True, allow_nan=False)
    output = os.environ.get("PILOT_NATIVE_RESULT_PATH")
    if output:
        try:
            descriptor = os.open(Path(output), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(encoded + "\n")
        except OSError:
            print(json.dumps({"schema_version": SCHEMA, "status": "unavailable", "reason": "result_write_failed"}))
            return 1
    print(encoded)
    return 0 if result["status"] == "verified_native_identity" else 1


if __name__ == "__main__":
    raise SystemExit(main())
