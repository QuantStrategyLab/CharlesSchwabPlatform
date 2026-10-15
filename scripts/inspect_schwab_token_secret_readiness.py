"""Check Schwab token Secret metadata and ADC access permission only.

This diagnostic never reads a Secret payload or calls Schwab. It reports only
fixed status values and booleans so operators can decide whether a separately
approved native identity check is even possible.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.publish_account_facts_from_reports import PROJECT_ID

API_HOST = "secretmanager.googleapis.com"
SECRET_ID = "schwab_token"
API_ROOT = f"https://{API_HOST}/v1/projects/{PROJECT_ID}/secrets/{SECRET_ID}"
MAX_RESPONSE_BYTES = 64 * 1024
PERMISSION = "secretmanager.versions.access"
_SECRET_NAME = re.compile(
    rf"projects/({re.escape(PROJECT_ID)}|[1-9][0-9]{{5,19}})/secrets/{re.escape(SECRET_ID)}\Z"
)
_VERSION_NAME = re.compile(
    rf"projects/({re.escape(PROJECT_ID)}|[1-9][0-9]{{5,19}})/secrets/{re.escape(SECRET_ID)}/versions/([1-9][0-9]*)\Z"
)


class _ReadFailure(Exception):
    def __init__(self, reason: str) -> None:
        self.reason = reason


def _authorized_session() -> Any:
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return AuthorizedSession(credentials, max_refresh_attempts=0, refresh_timeout=15)


def _safe_json_response(response: Any) -> Mapping[str, Any]:
    status = getattr(response, "status_code", None)
    if status == 404:
        raise _ReadFailure("not_found")
    if status == 401:
        raise _ReadFailure("authentication_unavailable")
    if status == 403:
        raise _ReadFailure("permission_denied")
    if status != 200:
        raise _ReadFailure("metadata_unavailable")
    deadline = time.monotonic() + 15
    body = bytearray()
    for chunk in response.iter_content(chunk_size=8192):
        if time.monotonic() > deadline or not isinstance(chunk, bytes):
            raise _ReadFailure("metadata_unavailable")
        if len(body) + len(chunk) > MAX_RESPONSE_BYTES:
            raise _ReadFailure("metadata_unavailable")
        body.extend(chunk)
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise _ReadFailure("metadata_unavailable") from None
    if not isinstance(value, Mapping):
        raise _ReadFailure("metadata_unavailable")
    return value


def _request_json(
    session: Any,
    *,
    method: str,
    url: str,
    body: Mapping[str, Any] | None = None,
    params: Mapping[str, str] | None = None,
    canonical_secret_name: str | None = None,
) -> Mapping[str, Any]:
    parsed = urllib.parse.urlsplit(url)
    metadata_path = f"/v1/projects/{PROJECT_ID}/secrets/{SECRET_ID}"
    if canonical_secret_name is not None and not _SECRET_NAME.fullmatch(
        canonical_secret_name
    ):
        raise ValueError("request_target_rejected")
    latest_path = (
        f"/v1/{canonical_secret_name}/versions/latest"
        if canonical_secret_name is not None
        else None
    )
    permission_path = parsed.path.removeprefix("/v1/").removesuffix(":testIamPermissions")
    is_permission_path = bool(
        parsed.path.endswith(":testIamPermissions")
        and canonical_secret_name is not None
        and permission_path == canonical_secret_name
    )
    if (
        parsed.scheme != "https"
        or parsed.netloc != API_HOST
        or parsed.query
        or parsed.fragment
        or not (parsed.path == metadata_path or parsed.path == latest_path or is_permission_path)
    ):
        raise ValueError("request_target_rejected")
    if (
        (
            (parsed.path == metadata_path or parsed.path == latest_path)
            and (method != "GET" or body is not None)
        )
        or (
            is_permission_path
            and (
                method != "POST"
                or body != {"permissions": [PERMISSION]}
            )
        )
    ):
        raise ValueError("request_method_rejected")
    request = session.get if method == "GET" else session.post
    with request(
        url,
        json=body,
        params=params,
        timeout=(5, 15),
        stream=True,
        allow_redirects=False,
    ) as response:
        return _safe_json_response(response)


def inspect_secret_readiness(
    environ: Mapping[str, str], *, session: Any = None
) -> dict[str, bool | None | str]:
    """Check one fixed Secret, latest-version state and access permission."""
    if environ.get("GCP_PROJECT_ID") != PROJECT_ID:
        return {
            "status": "configuration_invalid",
            "secret_exists": None,
            "latest_version_enabled": None,
            "permission_reported": None,
        }

    owned = session is None
    try:
        if session is None:
            session = _authorized_session()
        try:
            secret = _request_json(
                session,
                method="GET",
                url=API_ROOT,
                params={"fields": "name"},
            )
        except _ReadFailure as exc:
            if exc.reason == "not_found":
                return {
                "status": "secret_not_found",
                "secret_exists": False,
                "latest_version_enabled": None,
                "permission_reported": None,
                }
            if exc.reason == "authentication_unavailable":
                return {
                    "status": "metadata_authentication_unavailable",
                    "secret_exists": None,
                    "latest_version_enabled": None,
                    "permission_reported": None,
                }
            if exc.reason == "permission_denied":
                return {
                    "status": "metadata_permission_denied",
                    "secret_exists": None,
                    "latest_version_enabled": None,
                    "permission_reported": None,
                }
            return {
                "status": "metadata_unavailable",
                "secret_exists": None,
                "latest_version_enabled": None,
                "permission_reported": None,
            }
        except Exception:
            return {
                "status": "metadata_unavailable",
                "secret_exists": None,
                "latest_version_enabled": None,
                "permission_reported": None,
            }
        secret_name = secret.get("name")
        secret_match = (
            _SECRET_NAME.fullmatch(secret_name)
            if isinstance(secret_name, str)
            else None
        )
        if secret_match is None:
            return {
                "status": "metadata_unavailable",
                "secret_exists": None,
                "latest_version_enabled": None,
                "permission_reported": None,
            }

        try:
            version = _request_json(
                session,
                method="GET",
                url=f"https://{API_HOST}/v1/{secret_name}/versions/latest",
                params={"fields": "name,state"},
                canonical_secret_name=secret_name,
            )
        except Exception:
            return {
                "status": "version_unavailable",
                "secret_exists": True,
                "latest_version_enabled": None,
                "permission_reported": None,
            }
        version_name = version.get("name")
        match = (
            _VERSION_NAME.fullmatch(version_name)
            if isinstance(version_name, str)
            else None
        )
        if (
            match is None
            or match.group(1) not in {PROJECT_ID, secret_match.group(1)}
            or version.get("state") not in {"ENABLED", "DISABLED", "DESTROYED"}
        ):
            return {
                "status": "version_unavailable",
                "secret_exists": True,
                "latest_version_enabled": None,
                "permission_reported": None,
            }
        enabled = version["state"] == "ENABLED"
        try:
            permission = _request_json(
                session,
                method="POST",
                url=f"https://{API_HOST}/v1/{secret_name}:testIamPermissions",
                body={"permissions": [PERMISSION]},
                canonical_secret_name=secret_name,
            )
        except Exception:
            return {
                "status": "permission_check_unavailable",
                "secret_exists": True,
                "latest_version_enabled": enabled,
                "permission_reported": None,
            }
        permissions = permission.get("permissions", [])
        if not isinstance(permissions, list) or any(
            not isinstance(item, str) for item in permissions
        ):
            return {
                "status": "permission_check_unavailable",
                "secret_exists": True,
                "latest_version_enabled": enabled,
                "permission_reported": None,
            }
        return {
            "status": "checked",
            "secret_exists": True,
            "latest_version_enabled": enabled,
            "permission_reported": PERMISSION in permissions,
        }
    except Exception:
        return {
            "status": "diagnostic_unavailable",
            "secret_exists": None,
            "latest_version_enabled": None,
            "permission_reported": None,
        }
    finally:
        if owned and session is not None:
            try:
                session.close()
            except Exception:
                pass


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args != ["--metadata-only"]:
        print(
            json.dumps(
                {
                    "status": "unsupported_arguments",
                    "secret_exists": None,
                    "latest_version_enabled": None,
                    "permission_reported": None,
                },
                sort_keys=True,
            )
        )
        return 2
    result = inspect_secret_readiness(os.environ)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "checked" else 2


if __name__ == "__main__":
    raise SystemExit(main())
