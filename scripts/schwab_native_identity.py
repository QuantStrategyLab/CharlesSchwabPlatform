"""Verify the configured Schwab account using the existing token, without refresh."""

from __future__ import annotations

import datetime as dt
import importlib
import json
import math
import time
from collections.abc import Mapping
from typing import Any, Callable

from scripts.publish_account_facts_from_reports import PROJECT_ID

SECRET_ID = "schwab_token"
API_URL = "https://api.schwabapi.com/trader/v1/accounts/accountNumbers"
MAX_TOKEN_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 64 * 1024
MAX_ACCOUNTS = 100
REQUEST_TIMEOUT = (5, 15)
SECRET_READ_TIMEOUT = 15
_CLOCK_SKEW_SECONDS = 30


def _load_existing_token(project_id: str, secret_id: str) -> str:
    """Read one fixed Secret version without retries or local token writes."""
    from google.cloud import secretmanager_v1

    client = secretmanager_v1.SecretManagerServiceClient()
    response = client.access_secret_version(
        request={"name": f"projects/{project_id}/secrets/{secret_id}/versions/latest"},
        retry=None,
        timeout=SECRET_READ_TIMEOUT,
    )
    payload = response.payload.data
    if not isinstance(payload, bytes) or len(payload) > MAX_TOKEN_BYTES:
        return ""
    return payload.decode("utf-8")


def _is_instance_from(error: Exception, module_name: str, class_names: tuple[str, ...]) -> bool:
    try:
        module = importlib.import_module(module_name)
    except Exception:
        return False
    types = tuple(
        candidate
        for name in class_names
        if isinstance((candidate := getattr(module, name, None)), type)
    )
    return bool(types) and isinstance(error, types)


def _classify_token_load_error(error: Exception) -> str:
    """Map SDK failures to fixed safe categories without inspecting messages."""
    if isinstance(error, ImportError):
        return "token_dependency_unavailable"
    if isinstance(error, UnicodeDecodeError):
        return "token_invalid"
    if _is_instance_from(
        error,
        "google.api_core.exceptions",
        ("PermissionDenied", "Forbidden"),
    ):
        return "token_permission_denied"
    if _is_instance_from(
        error, "google.api_core.exceptions", ("NotFound",)
    ):
        return "token_not_found"
    if _is_instance_from(
        error, "google.api_core.exceptions", ("FailedPrecondition",)
    ):
        return "token_version_unavailable"
    if _is_instance_from(
        error,
        "google.auth.exceptions",
        ("DefaultCredentialsError", "RefreshError"),
    ) or _is_instance_from(
        error, "google.api_core.exceptions", ("Unauthenticated", "Unauthorized")
    ):
        return "token_adc_unavailable"
    if _is_instance_from(error, "google.auth.exceptions", ("TransportError",)) or (
        _is_instance_from(
            error,
            "google.api_core.exceptions",
            (
                "DeadlineExceeded",
                "GatewayTimeout",
                "InternalServerError",
                "RetryError",
                "ServiceUnavailable",
                "TooManyRequests",
            ),
        )
    ):
        return "token_network_unavailable"
    return "token_load_failed"


def check_token_load_status(
    *,
    environ: Mapping[str, str],
    observed_at: dt.datetime,
    secret_loader: Callable[[str, str], str] | None = None,
) -> str:
    """Read and validate the existing token without contacting Schwab."""
    if (
        environ.get("GCP_PROJECT_ID") != PROJECT_ID
        or not isinstance(observed_at, dt.datetime)
        or observed_at.tzinfo is None
        or observed_at.utcoffset() is None
    ):
        return "configuration_invalid"
    try:
        now = observed_at.timestamp()
    except (OverflowError, OSError, ValueError):
        return "configuration_invalid"
    try:
        loader = _load_existing_token if secret_loader is None else secret_loader
        payload = loader(PROJECT_ID, SECRET_ID)
    except Exception as error:
        return _classify_token_load_error(error)
    try:
        access_token, status = _access_token(payload, now=now)
    except Exception:
        return "token_invalid"
    return "token_payload_valid" if access_token is not None else status


def _finite_timestamp(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and (isinstance(value, int) or math.isfinite(value))
    )


def _access_token(payload: object, *, now: float) -> tuple[str | None, str]:
    if not isinstance(payload, str) or len(payload.encode("utf-8")) > MAX_TOKEN_BYTES:
        return None, "token_invalid"
    try:
        wrapper = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, "token_invalid"
    if not isinstance(wrapper, Mapping) or "creation_timestamp" not in wrapper:
        return None, "token_invalid"
    creation_timestamp = wrapper.get("creation_timestamp")
    token = wrapper.get("token")
    if (
        not _finite_timestamp(creation_timestamp)
        or not isinstance(token, Mapping)
    ):
        return None, "token_invalid"
    access_token = token.get("access_token")
    expires_at = token.get("expires_at")
    if (
        not isinstance(access_token, str)
        or not access_token
        or len(access_token) > 8192
        or access_token != access_token.strip()
        or not access_token.isascii()
        or any(ord(char) < 33 or ord(char) > 126 for char in access_token)
        or not _finite_timestamp(expires_at)
    ):
        return None, "token_invalid"
    if expires_at <= now + _CLOCK_SKEW_SECONDS:
        return None, "token_expired"
    return access_token, "verified"


def _read_response_json(response: Any, *, deadline: float) -> object | None:
    if getattr(response, "status_code", None) != 200:
        return None
    body = bytearray()
    for chunk in response.iter_content(chunk_size=8192):
        if time.monotonic() > deadline or not isinstance(chunk, bytes):
            return None
        if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
            return None
        body.extend(chunk)
    try:
        return json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def verify_native_identity(
    *,
    environ: Mapping[str, str],
    expected_account_hash: str,
    observed_at: dt.datetime,
    secret_loader: Callable[[str, str], str] | None = None,
    session: Any = None,
) -> str:
    """Return a fixed status; never return native account IDs or token material."""
    if (
        environ.get("GCP_PROJECT_ID") != PROJECT_ID
        or not isinstance(expected_account_hash, str)
        or not expected_account_hash
        or expected_account_hash != expected_account_hash.strip()
        or len(expected_account_hash) > 512
        or not isinstance(observed_at, dt.datetime)
        or observed_at.tzinfo is None
        or observed_at.utcoffset() is None
    ):
        return "configuration_invalid"
    try:
        now = observed_at.timestamp()
    except (OverflowError, OSError, ValueError):
        return "configuration_invalid"
    try:
        loader = _load_existing_token if secret_loader is None else secret_loader
        payload = loader(PROJECT_ID, SECRET_ID)
    except Exception as error:
        return _classify_token_load_error(error)
    try:
        access_token, token_status = _access_token(payload, now=now)
    except Exception:
        return "token_invalid"
    if access_token is None:
        return token_status

    owned = session is None
    try:
        if session is None:
            import requests

            session = requests.Session()
            session.trust_env = False
        deadline = time.monotonic() + REQUEST_TIMEOUT[1]
        try:
            response_context = session.get(
                API_URL,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=REQUEST_TIMEOUT,
                stream=True,
                allow_redirects=False,
            )
            with response_context as response:
                status = getattr(response, "status_code", None)
                if status == 401:
                    return "token_rejected"
                if status == 403:
                    return "native_account_access_denied"
                if status != 200:
                    return "native_identity_unavailable"
                data = _read_response_json(response, deadline=deadline)
        except Exception:
            return "native_identity_unavailable"
        if not isinstance(data, list) or not data or len(data) > MAX_ACCOUNTS:
            return "native_identity_response_invalid"
        hashes: list[str] = []
        for item in data:
            if (
                not isinstance(item, Mapping)
                or not isinstance(item.get("hashValue"), str)
                or not item["hashValue"]
                or item["hashValue"] != item["hashValue"].strip()
                or len(item["hashValue"]) > 512
            ):
                return "native_identity_response_invalid"
            hashes.append(item["hashValue"])
        matches = sum(value == expected_account_hash for value in hashes)
        if matches == 1:
            return "verified"
        if matches > 1:
            return "native_identity_ambiguous"
        return "native_identity_mismatch"
    finally:
        if owned and session is not None:
            try:
                session.close()
            except Exception:
                pass
