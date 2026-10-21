"""Read-only Schwab checks and a separate, explicitly enabled one-shot renewal."""

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


def renew_access_token_once(*, environ, observed_at, secret_client=None, http_transport=None):
    """Separate opt-in credential mutation, never part of identity/check/capture.

    One token POST and one append per invocation, not a distributed
    exactly-once transaction. Unknown POST/write outcomes require operator
    reconciliation; never redispatch automatically or fall back to login.
    """
    import contextlib
    import io
    import logging

    class Discard(io.TextIOBase):
        def write(self, value):
            return len(value)

    result = dict(status="blocked", reason="renewal_not_enabled", refresh_attempts=0,
                  secret_write_attempts=0, source_version=None, new_version=None)
    previous = logging.root.manager.disable
    with contextlib.redirect_stdout(Discard()), contextlib.redirect_stderr(Discard()):
        logging.disable(max(previous, 2**31 - 1))
        try:
            if environ.get("SCHWAB_RENEW_ACCESS_TOKEN_ONCE") != "true":
                return result
            result["reason"] = "configuration_invalid"
            if (environ.get("GCP_PROJECT_ID") != PROJECT_ID
                    or not isinstance(observed_at, dt.datetime) or observed_at.tzinfo is None
                    or observed_at.utcoffset() is None
                    or not _finite_timestamp(observed_at.timestamp())):
                result["reason"] = "configuration_invalid"
                return result
            result["reason"] = "renewal_dependency_unavailable"
            _renew_access_token(result, observed_at, secret_client, http_transport)
        except BaseException:
            if result["reason"] == "refresh_outcome_unknown" and not result["refresh_attempts"]:
                result["reason"] = "refresh_not_attempted"
        finally:
            logging.disable(previous)
    return result


def _renew_access_token(result, observed_at, secret_client, http_transport):
    import re

    import httpx
    from authlib.integrations.base_client.errors import OAuthError
    from authlib.integrations.httpx_client import OAuth2Client
    from google.cloud import secretmanager_v1
    from schwab.auth import TOKEN_ENDPOINT, TokenMetadata

    prefix = "projects/401309731911/secrets/"
    parent = prefix + SECRET_ID
    owned = secret_client is None
    client = secretmanager_v1.SecretManagerServiceClient() if owned else secret_client
    started = time.monotonic()
    observed = observed_at.timestamp()

    def now():
        if time.monotonic() - started >= 60:
            raise TimeoutError
        return observed + time.monotonic() - started

    def version_number(name):
        match = re.fullmatch(re.escape(parent) + r"/versions/([1-9][0-9]{0,18})", name or "")
        if match is None:
            raise ValueError
        return int(match[1])

    def read(name):
        now()
        response = client.access_secret_version(request={"name": name}, retry=None, timeout=15)
        data = response.payload.data
        if not isinstance(data, bytes) or len(data) > MAX_TOKEN_BYTES:
            raise ValueError
        return response.name, data.decode("utf-8")

    def unique(items):
        values = {}
        for key, value in items:
            if key in values:
                raise ValueError
            values[key] = value
        return values

    try:
        result["reason"] = "secret_read_failed"
        source, payload = read(parent + "/versions/latest")
        result["source_version"] = version_number(source)
        result["reason"] = "token_invalid"
        wrapper = json.loads(payload, object_pairs_hook=unique)
        token, status = _access_token(payload, now=now())
        if token is not None:
            result.update(status="not_needed", reason="token_payload_valid")
            return
        if status != "token_expired":
            return
        refresh = wrapper["token"].get("refresh_token")
        if (not isinstance(refresh, str) or not refresh or len(refresh) > 8192
                or not refresh.isascii() or any(ord(c) < 33 or ord(c) > 126 for c in refresh)):
            result["reason"] = "refresh_credential_missing"
            return
        result["reason"] = "persistence_not_permitted"
        now()
        permission = client.test_iam_permissions(
            request={"resource": parent, "permissions": ["secretmanager.versions.add"]},
            retry=None, timeout=15)
        if "secretmanager.versions.add" not in permission.permissions:
            return
        result["reason"] = "secret_read_failed"
        credentials = []
        for name in ("charles-schwab-api-key", "charles-schwab-app-secret"):
            ref = prefix + name + "/versions/1"
            actual, value = read(ref)
            if (actual != ref or not value or len(value) > 8192 or not value.isascii()
                    or any(ord(c) < 33 or ord(c) > 126 for c in value)):
                raise ValueError
            credentials.append(value)

        def persist(updated, **_metadata):
            result["reason"] = "refresh_response_invalid"
            data = json.dumps(updated, allow_nan=False).encode("utf-8")
            if len(data) > MAX_TOKEN_BYTES or _access_token(data.decode(), now=now())[0] is None:
                raise ValueError
            new_refresh = updated["token"].get("refresh_token")
            if (updated["creation_timestamp"] != wrapper["creation_timestamp"]
                    or not isinstance(new_refresh, str) or not new_refresh
                    or len(new_refresh) > 8192 or not new_refresh.isascii()
                    or any(ord(c) < 33 or ord(c) > 126 for c in new_refresh)):
                raise ValueError
            if result["secret_write_attempts"]:
                raise ValueError
            now()
            result.update(reason="persistence_outcome_unknown", secret_write_attempts=1)
            saved = client.add_secret_version(
                request={"parent": parent, "payload": {"data": data}}, retry=None, timeout=15)
            number = version_number(saved.name)
            if number <= result["source_version"]:
                raise ValueError
            result.update(status="renewed", reason="token_persisted", new_version=number)

        metadata = TokenMetadata.from_loaded_token(wrapper, persist)
        downstream = http_transport if http_transport is not None else httpx.HTTPTransport(
            retries=0, trust_env=False)

        class Once(httpx.BaseTransport):
            def handle_request(self, request):
                now()
                if (result["refresh_attempts"] or request.method != "POST"
                        or str(request.url) != TOKEN_ENDPOINT):
                    raise ValueError
                result["refresh_attempts"] = 1
                response = downstream.handle_request(request)
                try:
                    if response.headers.get("content-encoding", "identity").lower() not in ("", "identity"):
                        raise ValueError
                    body = bytearray()
                    chunks = [response.content] if response.is_stream_consumed else response.iter_raw()
                    for chunk in chunks:
                        now()
                        if not isinstance(chunk, bytes) or len(body) + len(chunk) > MAX_TOKEN_BYTES:
                            raise ValueError
                        body.extend(chunk)
                    # Authlib can accept token-shaped JSON on non-success HTTP
                    # statuses. Only an explicit 200 may reach its token writer.
                    if response.status_code != 200:
                        if response.status_code in (400, 401):
                            rejected = json.loads(bytes(body), object_pairs_hook=unique)
                            if isinstance(rejected, dict) and rejected.get("error") in ("invalid_grant", "invalid_client"):
                                raise OAuthError(error=rejected["error"])
                        raise ValueError
                    return httpx.Response(200, content=bytes(body), headers={"content-type": "application/json"})
                finally:
                    response.close()

            def close(self):
                downstream.close()

        result["reason"] = "refresh_outcome_unknown"
        try:
            with OAuth2Client(credentials[0], client_secret=credentials[1], token=metadata.token,
                              token_endpoint=TOKEN_ENDPOINT, update_token=metadata.wrapped_token_write_func(),
                              transport=Once(), trust_env=False, follow_redirects=False,
                              timeout=15, headers={"Accept-Encoding": "identity"}) as oauth:
                oauth.refresh_token(TOKEN_ENDPOINT)
        except OAuthError as error:
            if error.error in ("invalid_grant", "invalid_client") and not result["secret_write_attempts"]:
                result["reason"] = "refresh_rejected" if error.error == "invalid_grant" else "client_rejected"
            raise
    finally:
        if owned:
            try:
                client.transport.close()
            except BaseException:
                pass


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


def main(argv=None):
    """Only the explicit renewal CLI mutates credentials; existing APIs stay read-only."""
    import os
    import sys

    args = sys.argv[1:] if argv is None else argv
    if args != ["--renew-access-token-once"]:
        result = dict(status="blocked", reason="arguments_invalid", refresh_attempts=0,
                      secret_write_attempts=0, source_version=None, new_version=None)
    else:
        result = renew_access_token_once(environ=os.environ,
                                        observed_at=dt.datetime.now(dt.timezone.utc))
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in ("renewed", "not_needed") else 2


if __name__ == "__main__":
    raise SystemExit(main())
