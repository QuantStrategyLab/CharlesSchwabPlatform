"""Project and optionally publish one archived Schwab runtime observation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


HISTORY_SCHEMA = "schwab_account_snapshot_history.v1"
SNAPSHOT_SCHEMA = "schwab_account_snapshot.v1"
SOURCE_BINDING_KIND = "deployment_runtime_account"
SYNC_URL = "https://qsl-strategy-switch-console.pigbibi.workers.dev/api/account-facts/sync"
USER_AGENT = "QSL-Schwab-AccountFacts/1.0"
PROJECT_ID = "charlesschwabquant"
REGION = "us-central1"
SERVICE_NAME = "charles-schwab-quant-service"
STRATEGY_PROFILE = "soxl_soxx_trend_income"
ACCOUNT_SCOPE = "live"
ACCOUNT_SELECTOR = ("live",)
PLATFORM = "charles_schwab"
REPORT_PREFIX = (
    "gs://qsl-runtime-logs-shared/execution-reports/charles_schwab/"
    "soxl_soxx_trend_income/"
)
MAX_AGE = timedelta(hours=36)
FUTURE_SKEW = timedelta(minutes=5)
_REPORT_PARTS = re.compile(r"^(\d{4}-\d{2})/(\d{8}T\d{6}Z)\.json$")
_DECIMAL_TEXT = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$")
_REVISION = re.compile(r"^[a-z][a-z0-9-]{0,62}$")


class _ProjectionError(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _instant(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise _ProjectionError("invalid_observation_time")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        raise _ProjectionError("invalid_observation_time") from None


def _money_text(value: object) -> str:
    if not isinstance(value, str) or _DECIMAL_TEXT.fullmatch(value) is None:
        raise _ProjectionError("invalid_net_assets")
    try:
        amount = Decimal(value)
    except InvalidOperation:
        raise _ProjectionError("invalid_net_assets") from None
    if not amount.is_finite():
        raise _ProjectionError("invalid_net_assets")
    whole, _, fraction = value.lstrip("-").partition(".")
    if len(whole) > 15 or len(fraction) > 8:
        raise _ProjectionError("invalid_net_assets")
    return value


def _report_uri_parts(uri: str) -> tuple[str, str]:
    parsed = urlsplit(uri)
    expected = urlsplit(REPORT_PREFIX)
    if (
        parsed.scheme != "gs"
        or parsed.netloc != "qsl-runtime-logs-shared"
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
        or expected.scheme != "gs"
    ):
        raise _ProjectionError("report_uri_invalid")
    path = parsed.path.lstrip("/")
    prefix_path = expected.path.lstrip("/")
    if not path.startswith(prefix_path):
        raise _ProjectionError("report_uri_invalid")
    match = _REPORT_PARTS.fullmatch(path[len(prefix_path):])
    if not match:
        raise _ProjectionError("report_uri_invalid")
    return match.group(1), match.group(2)


def _binding_id(account_hash: str) -> str:
    binding = {
        "account_hash": account_hash,
        "account_scope": ACCOUNT_SCOPE,
        "account_selector": ACCOUNT_SCOPE,
        "currency_source": "owner_confirmed",
        "net_assets_currency": "USD",
        "platform": "schwab",
        "project_id": PROJECT_ID,
        "service_name": SERVICE_NAME,
    }
    canonical = json.dumps(
        binding,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def project_schwab_account_facts_history(
    report: Mapping[str, Any],
    *,
    source_report_uri: str,
    expected_runtime_revision: str,
    expected_target_id: str,
    now: datetime,
) -> dict[str, Any]:
    """Return the strict Schwab history body or an amount-free skip reason."""
    try:
        if not isinstance(report, Mapping) or report.get("schema_version") != "runtime_report.v1":
            raise _ProjectionError("report_invalid")
        if (
            report.get("platform") != PLATFORM
            or report.get("project_id") != PROJECT_ID
            or report.get("service_name") != SERVICE_NAME
            or report.get("strategy_profile") != STRATEGY_PROFILE
            or report.get("account_scope") not in (None, ACCOUNT_SCOPE)
            or report.get("status") != "ok"
            or report.get("errors") != []
        ):
            raise _ProjectionError("report_identity_mismatch")
        runtime_target = report.get("runtime_target")
        if not isinstance(runtime_target, Mapping) or (
            runtime_target.get("strategy_profile") != STRATEGY_PROFILE
            or runtime_target.get("account_scope") != ACCOUNT_SCOPE
            or runtime_target.get("account_selector") != list(ACCOUNT_SELECTOR)
        ):
            raise _ProjectionError("runtime_target_mismatch")
        diagnostics = report.get("diagnostics")
        if not isinstance(diagnostics, Mapping) or (
            not expected_runtime_revision
            or diagnostics.get("runtime_revision") != expected_runtime_revision
        ):
            raise _ProjectionError("runtime_revision_mismatch")
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise _ProjectionError("invalid_observation_time")
        current = now.astimezone(timezone.utc)

        summary = report.get("summary")
        observation = summary.get("account_observation") if isinstance(summary, Mapping) else None
        if not isinstance(observation, Mapping):
            raise _ProjectionError("account_observation_missing")
        account_hash = observation.get("account_hash")
        if not isinstance(account_hash, str) or not account_hash or account_hash != account_hash.strip():
            raise _ProjectionError("account_identity_invalid")
        if (
            observation.get("currency") is not None
            or observation.get("net_assets_currency") != "USD"
            or observation.get("net_assets_currency_source") != "owner_confirmed"
            or observation.get("net_assets_source") != "liquidationValue"
        ):
            raise _ProjectionError("net_assets_currency_unconfirmed")
        net_assets = _money_text(observation.get("net_assets"))
        observed_at = _instant(observation.get("observed_at"))
        started_at = _instant(report.get("started_at"))
        finished_at = _instant(report.get("finished_at"))
        if finished_at < started_at or observed_at < started_at or observed_at > finished_at:
            raise _ProjectionError("report_observation_mismatch")
        if observed_at < current - MAX_AGE or observed_at > current + FUTURE_SKEW:
            raise _ProjectionError("observation_out_of_window")

        report_month, report_run_id = _report_uri_parts(source_report_uri)
        if (
            report_month != started_at.strftime("%Y-%m")
            or report_run_id != report.get("run_id")
        ):
            raise _ProjectionError("report_provenance_mismatch")

        if not isinstance(expected_target_id, str) or not expected_target_id.strip():
            raise _ProjectionError("target_identity_unavailable")
        return {
            "schema_version": HISTORY_SCHEMA,
            "snapshot_schema_version": SNAPSHOT_SCHEMA,
            "account_scope": ACCOUNT_SCOPE,
            "target_id": expected_target_id,
            "source_binding": {
                "kind": SOURCE_BINDING_KIND,
                "status": "bound",
                "id": _binding_id(account_hash),
            },
            "observed_started_at": observation["observed_at"],
            "observed_finished_at": observation["observed_at"],
            "snapshot_atomic": False,
            "observation_date": observed_at.date().isoformat(),
            "broker_reported_balances": [
                {
                    "currency": "USD",
                    "net_assets": net_assets,
                    "source_tag": "liquidationValue",
                    "currency_source": "owner_confirmed",
                }
            ],
            "cash": [],
            "account_hash": account_hash,
        }
    except _ProjectionError as exc:
        return {"status": "skipped", "reason": exc.reason}


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request: Request, *_args: object, **_kwargs: object) -> None:
        return None


def _publish_once(body: Mapping[str, Any], *, sync_url: str, token: str) -> dict[str, Any]:
    if sync_url != SYNC_URL or not token:
        return {"status": "skipped", "reason": "publish_configuration_unavailable"}
    request = Request(
        SYNC_URL,
        data=json.dumps(body, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with build_opener(_NoRedirect()).open(request, timeout=15) as response:
            status = getattr(response, "status", None)
            if isinstance(status, int) and 200 <= status < 300:
                return {"status": "published"}
            return {"status": "skipped", "reason": "publish_failed", "http_status": status}
    except HTTPError as exc:
        return {"status": "skipped", "reason": "publish_failed", "http_status": exc.code}
    except TimeoutError:
        return {"status": "skipped", "reason": "publish_failed", "category": "timeout"}
    except URLError:
        return {"status": "skipped", "reason": "publish_failed", "category": "url_error"}
    except OSError:
        return {"status": "skipped", "reason": "publish_failed", "category": "transport_error"}
    except Exception:
        return {"status": "skipped", "reason": "publish_failed", "category": "unknown"}


def _gcloud_json(*args: str) -> dict[str, Any] | None:
    try:
        result = subprocess.run(args, capture_output=True, check=False)
    except OSError:
        return None
    if result.returncode != 0:
        return None
    try:
        value = json.loads(result.stdout)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _service_revision_matches(expected_revision: str) -> bool:
    service = _gcloud_json(
        "gcloud", "run", "services", "describe", SERVICE_NAME,
        "--project", PROJECT_ID, "--region", REGION, "--format=json",
    )
    if service is None:
        return False
    status = service.get("status")
    if not isinstance(status, Mapping):
        return False
    traffic = status.get("traffic")
    if not isinstance(traffic, list):
        return False
    expected_percent = 0
    total_percent = 0
    for item in traffic:
        if not isinstance(item, Mapping):
            return False
        revision_name = item.get("revisionName")
        percent = item.get("percent")
        if "percent" not in item:
            tag = item.get("tag")
            if (
                not isinstance(tag, str)
                or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", tag) is None
                or not isinstance(revision_name, str)
                or not revision_name.strip()
            ):
                return False
            percent = 0
        if isinstance(percent, bool) or not isinstance(percent, int):
            return False
        if not 0 <= percent <= 100:
            return False
        total_percent += percent
        if revision_name == expected_revision:
            expected_percent += percent
    return total_percent == 100 and expected_percent == 100


def _read_report(uri: str) -> Mapping[str, Any] | None:
    try:
        result = subprocess.run(
            ("gcloud", "storage", "cat", uri, "--project", PROJECT_ID),
            capture_output=True,
            check=False,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    try:
        value = json.loads(result.stdout)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, Mapping) else None


def _safe_result_text(result: Mapping[str, Any]) -> str:
    status = result.get("status")
    reason = result.get("reason")
    if status == "published":
        return "published"
    if status == "skipped" and isinstance(reason, str) and re.fullmatch(r"[a-z_]+", reason):
        http_status = result.get("http_status")
        if isinstance(http_status, int) and not isinstance(http_status, bool) and 100 <= http_status <= 599:
            return f"skipped:{reason}:http_status={http_status}"
        category = result.get("category")
        if isinstance(category, str) and category in {"timeout", "url_error", "transport_error", "unknown"}:
            return f"skipped:{reason}:category={category}"
        return f"skipped:{reason}"
    return "skipped:unknown"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-uri", required=True)
    parser.add_argument("--expected-runtime-revision", required=True)
    args = parser.parse_args(argv)

    if os.getenv("GITHUB_ACTIONS") != "true":
        print("skipped:workflow_context_required")
        return 2
    token = os.getenv("SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN", "")
    sync_url = os.getenv("ACCOUNT_FACTS_SYNC_URL", "")
    if not token or sync_url != SYNC_URL:
        print("skipped:publish_configuration_unavailable")
        return 2
    if os.getenv("SCHWAB_NET_ASSETS_CURRENCY") != "USD":
        print("skipped:net_assets_currency_unconfirmed")
        return 2
    target_id = os.getenv("SCHWAB_ACCOUNT_FACTS_TARGET_ID", "")
    if not target_id.strip():
        print("skipped:target_identity_unavailable")
        return 2
    try:
        _report_uri_parts(args.report_uri)
    except _ProjectionError as exc:
        print(f"skipped:{exc.reason}")
        return 2
    if not _REVISION.fullmatch(args.expected_runtime_revision):
        print("skipped:runtime_revision_invalid")
        return 2
    if not _service_revision_matches(args.expected_runtime_revision):
        print("skipped:runtime_revision_readback_mismatch")
        return 2
    report = _read_report(args.report_uri)
    if report is None:
        print("skipped:report_unavailable")
        return 2
    projected = project_schwab_account_facts_history(
        report,
        source_report_uri=args.report_uri,
        expected_runtime_revision=args.expected_runtime_revision,
        expected_target_id=target_id,
        now=datetime.now(timezone.utc),
    )
    if projected.get("status") == "skipped":
        print(_safe_result_text(projected))
        return 2
    history = {key: value for key, value in projected.items() if key != "status"}
    result = _publish_once(
        history,
        sync_url=sync_url,
        token=token,
    )
    print(_safe_result_text(result))
    return 0 if result.get("status") == "published" else 2


if __name__ == "__main__":
    raise SystemExit(main())
