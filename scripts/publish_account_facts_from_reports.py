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
STRATEGY_PROFILE = "soxl_soxx_trend_income"
ACCOUNT_SCOPE = "live"
ACCOUNT_SELECTOR = ("live",)
_MARKET_SCOPE_TOKENS = frozenset({"US", "HK", "CN", "SG"})


def _runtime_account_scope_kind(
    value: object,
    *,
    expected_target_id: str | None = None,
) -> str:
    """Amount-free classification of runtime_target.account_scope (no raw value)."""
    if value is None:
        return "absent"
    if not isinstance(value, str):
        return "non_string"
    text = value.strip()
    if not text:
        return "blank"
    if text.lower() == ACCOUNT_SCOPE:
        return "live"
    if text.upper() in _MARKET_SCOPE_TOKENS:
        return "market_code"
    if (
        isinstance(expected_target_id, str)
        and expected_target_id.strip()
        and text == expected_target_id.strip()
    ):
        return "looks_like_target_id"
    # Opaque account hashes used elsewhere are long case-sensitive tokens.
    if len(text) >= 32 and all(ch.isalnum() or ch in "-_" for ch in text):
        return "hash_shaped"
    if text.lower() in {"paper", "dry_run", "dry-run", "prod", "production"}:
        return "mode_token"
    return "other_token"

PLATFORM = "charles_schwab"
MAX_AGE = timedelta(hours=36)
FUTURE_SKEW = timedelta(minutes=5)
_REPORT_PARTS = re.compile(r"^(\d{4}-\d{2})/(\d{8}T\d{6}Z)\.json$")
_DECIMAL_TEXT = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$")
_ACCOUNT_TYPE_TOKEN = re.compile(r"[A-Za-z_]{1,32}\Z", re.ASCII)
_REVISION = re.compile(r"^[a-z][a-z0-9-]{0,62}$")


POSITIONS_SCOPE = "strategy_symbols_only"
# Archive-projection-only keys: the console /api/account-facts/sync contract is
# exact-key, so these are carried for offline consumers (digest emit) and are
# stripped before any publish.
ARCHIVE_ONLY_KEYS = ("broker_reported_positions", "broker_reported_positions_scope")
_POSITION_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9./ -]{0,31}\Z", re.ASCII)
_MAX_POSITIONS = 64


def _project_positions(observation: Mapping[str, Any]) -> list[dict[str, str]] | None:
    """Validate runtime ``broker_reported_positions``; None (omit) on any doubt."""
    raw = observation.get("broker_reported_positions")
    if observation.get("broker_reported_positions_scope") != POSITIONS_SCOPE:
        return None
    if not isinstance(raw, list) or not raw or len(raw) > _MAX_POSITIONS:
        return None
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            return None
        if set(item) - {"symbol", "quantity", "market_value", "currency"}:
            return None
        symbol = item.get("symbol")
        if (
            not isinstance(symbol, str)
            or _POSITION_SYMBOL.fullmatch(symbol) is None
            or symbol in seen
        ):
            return None
        seen.add(symbol)
        if item.get("currency") not in (None, "USD"):
            return None
        try:
            quantity = _money_text(item.get("quantity"))
            market_value = _money_text(item.get("market_value"))
        except _ProjectionError:
            return None
        rows.append(
            {
                "symbol": symbol,
                "quantity": quantity,
                "market_value": market_value,
                "currency": "USD",
                "currency_source": "owner_confirmed",
            }
        )
    return rows


def strip_archive_only_keys(body: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in body.items() if key not in ARCHIVE_ONLY_KEYS}


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


def _report_uri_parts(uri: str, report_prefix: str) -> tuple[str, str]:
    parsed = urlsplit(uri)
    expected = urlsplit(report_prefix)
    if (
        not _valid_report_prefix(report_prefix)
        or parsed.scheme != "gs"
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
        or parsed.netloc != expected.netloc
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


def _valid_report_prefix(report_prefix: str) -> bool:
    if (
        not isinstance(report_prefix, str)
        or not report_prefix
        or report_prefix.strip() != report_prefix
    ):
        return False
    expected = urlsplit(report_prefix)
    return not (
        expected.scheme != "gs"
        or not expected.netloc
        or expected.query
        or expected.fragment
        or expected.username is not None
        or expected.password is not None
        or not expected.path.endswith("/")
        or expected.path.lstrip("/")
        != "execution-reports/charles_schwab/soxl_soxx_trend_income/"
        or any(char in expected.path for char in "*?[]")
    )


def _binding_id(account_hash: str, service_name: str) -> str:
    binding = {
        "account_hash": account_hash,
        "account_scope": ACCOUNT_SCOPE,
        "account_selector": ACCOUNT_SCOPE,
        "currency_source": "owner_confirmed",
        "net_assets_currency": "USD",
        "platform": "schwab",
        "project_id": PROJECT_ID,
        "service_name": service_name,
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
    report_prefix: str,
    expected_service_name: str,
    expected_runtime_revision: str,
    expected_target_id: str,
    expected_cash_currency: str | None = None,
    now: datetime,
) -> dict[str, Any]:
    """Return the strict Schwab history body or an amount-free skip reason."""
    try:
        if not isinstance(report, Mapping) or report.get("schema_version") != "runtime_report.v1":
            raise _ProjectionError("report_invalid")
        if (
            report.get("platform") != PLATFORM
            or report.get("project_id") != PROJECT_ID
            or not expected_service_name
            or report.get("service_name") != expected_service_name
            or report.get("strategy_profile") != STRATEGY_PROFILE
            or report.get("account_scope") not in (None, ACCOUNT_SCOPE)
            or report.get("status") != "ok"
            or report.get("errors") != []
        ):
            raise _ProjectionError("report_identity_mismatch")
        runtime_target = report.get("runtime_target")
        if not isinstance(runtime_target, Mapping):
            raise _ProjectionError("runtime_target_mismatch")
        if runtime_target.get("strategy_profile") != STRATEGY_PROFILE:
            raise _ProjectionError("runtime_target_profile_mismatch")
        # Archive reports may omit runtime_target.account_scope (null/absent).
        # Accept None / blank / case-insensitive "live"; reject any other token.
        scope_kind = _runtime_account_scope_kind(
            runtime_target.get("account_scope"),
            expected_target_id=expected_target_id,
        )
        if scope_kind not in {"absent", "blank", "live"}:
            raise _ProjectionError(f"runtime_target_scope_mismatch:{scope_kind}")
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
        # Legacy ["live"] or native single-hash selector matching observation.
        selector = runtime_target.get("account_selector")
        if selector != list(ACCOUNT_SELECTOR):
            if (
                not isinstance(selector, list)
                or len(selector) != 1
                or not isinstance(selector[0], str)
                or not selector[0]
                or selector[0] != selector[0].strip()
                or selector[0] != account_hash
            ):
                raise _ProjectionError("runtime_target_selector_mismatch")
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

        cash: list[dict[str, str]] = []
        if (
            observation.get("cash_balance_source") == "cashBalance"
            and observation.get("cash_currency") == "USD"
            and observation.get("cash_currency_source") == "owner_confirmed"
            and expected_cash_currency == "USD"
        ):
            try:
                cash_balance = _money_text(observation.get("cash_balance"))
            except _ProjectionError:
                cash_balance = None
            if cash_balance is not None:
                cash = [
                    {
                        "currency": "USD",
                        "cash_balance": cash_balance,
                        "source_tag": "cashBalance",
                        "currency_source": "owner_confirmed",
                    }
                ]

        broker_account_type = None
        raw_account_type = observation.get("broker_account_type")
        if (
            isinstance(raw_account_type, str)
            and _ACCOUNT_TYPE_TOKEN.fullmatch(raw_account_type) is not None
            and observation.get("broker_account_type_source") == "securitiesAccount.type"
        ):
            broker_account_type = {
                "value": raw_account_type,
                "source_tag": "securitiesAccount.type",
            }

        report_month, report_run_id = _report_uri_parts(source_report_uri, report_prefix)
        if (
            report_month != started_at.strftime("%Y-%m")
            or report_run_id != report.get("run_id")
        ):
            raise _ProjectionError("report_provenance_mismatch")

        if not isinstance(expected_target_id, str) or not expected_target_id.strip():
            raise _ProjectionError("target_identity_unavailable")
        return_body = {
            "schema_version": HISTORY_SCHEMA,
            "snapshot_schema_version": SNAPSHOT_SCHEMA,
            "account_scope": ACCOUNT_SCOPE,
            "target_id": expected_target_id,
            "source_binding": {
                "kind": SOURCE_BINDING_KIND,
                "status": "bound",
                "id": _binding_id(account_hash, expected_service_name),
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
            "cash": cash,
            "account_hash": account_hash,
        }
        if broker_account_type is not None:
            return_body["broker_account_type"] = broker_account_type
        positions = _project_positions(observation)
        if positions is not None:
            return_body["broker_reported_positions"] = positions
            return_body["broker_reported_positions_scope"] = POSITIONS_SCOPE
        return return_body
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
                try:
                    payload = json.loads(response.read())
                except (AttributeError, TypeError, ValueError):
                    return {
                        "status": "skipped",
                        "reason": "publish_failed",
                        "category": "response_invalid",
                        "http_status": status,
                    }
                if (
                    not isinstance(payload, Mapping)
                    or payload.get("ok") is not True
                    or payload.get("stored") is not True
                    or not isinstance(payload.get("unchanged"), bool)
                ):
                    return {
                        "status": "skipped",
                        "reason": "publish_failed",
                        "category": "response_invalid",
                        "http_status": status,
                    }
                if payload["unchanged"]:
                    return {"status": "unchanged", "reason": "observation_unchanged"}
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


def _service_traffic(service_name: str) -> dict[str, int] | None:
    service = _gcloud_json(
        "gcloud", "run", "services", "describe", service_name,
        "--project", PROJECT_ID, "--region", REGION, "--format=json",
    )
    if service is None:
        return None
    status = service.get("status")
    if not isinstance(status, Mapping):
        return None
    traffic = status.get("traffic")
    if not isinstance(traffic, list):
        return None
    total_percent = 0
    serving: dict[str, int] = {}
    for item in traffic:
        if not isinstance(item, Mapping):
            return None
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
                return None
            percent = 0
        if isinstance(percent, bool) or not isinstance(percent, int):
            return None
        if not 0 <= percent <= 100:
            return None
        total_percent += percent
        if percent:
            if not isinstance(revision_name, str) or not revision_name.strip():
                return None
            serving[revision_name] = serving.get(revision_name, 0) + percent
    if total_percent != 100 or len(serving) != 1:
        return None
    revision_name, percent = next(iter(serving.items()))
    return {revision_name: percent}


def _service_revision_matches(expected_revision: str, service_name: str) -> bool:
    traffic = _service_traffic(service_name)
    if traffic is None:
        return False
    return traffic.get(expected_revision) == 100


def _current_serving_revision(service_name: str) -> str | None:
    traffic = _service_traffic(service_name)
    if traffic is None:
        return None
    return next(iter(traffic))


def _listed_uri(item: object) -> str | None:
    if isinstance(item, str):
        return item.split("#", 1)[0]
    if isinstance(item, Mapping):
        uri = item.get("url")
        if isinstance(uri, str):
            return uri.split("#", 1)[0]
    return None


def _latest_recent_report_uri(
    *,
    report_prefix: str,
    now: datetime,
) -> str | None:
    if (
        not _valid_report_prefix(report_prefix)
        or not isinstance(now, datetime)
        or now.tzinfo is None
        or now.utcoffset() is None
    ):
        return None
    current = now.astimezone(timezone.utc)
    lower_bound = current - MAX_AGE
    upper_bound = current + FUTURE_SKEW
    cursor = lower_bound.date()
    last_date = upper_bound.date()
    candidates: list[tuple[datetime, str]] = []
    while cursor <= last_date:
        month = cursor.strftime("%Y-%m")
        glob = f"{report_prefix}{month}/{cursor:%Y%m%d}T*.json"
        try:
            result = subprocess.run(
                ("gcloud", "storage", "ls", "--json", glob, "--project", PROJECT_ID),
                capture_output=True,
                text=True,
                check=False,
            )
        except OSError:
            return None
        if result.returncode != 0:
            failure_text = (result.stderr or result.stdout or "").lower()
            if "matched no objects" in failure_text or "no urls matched" in failure_text:
                items = []
            else:
                return None
        else:
            try:
                items = json.loads(result.stdout)
            except (TypeError, ValueError):
                return None
        if not isinstance(items, list):
            return None
        for item in items:
            uri = _listed_uri(item)
            if uri is None:
                continue
            try:
                report_month, run_id = _report_uri_parts(uri, report_prefix)
                archived_at = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=timezone.utc
                )
            except (ValueError, _ProjectionError):
                continue
            if (
                report_month != month
                or archived_at.date() != cursor
                or not lower_bound <= archived_at <= upper_bound
            ):
                continue
            candidates.append((archived_at, uri))
        cursor += timedelta(days=1)
    if not candidates:
        return None
    newest_at = max(item[0] for item in candidates)
    newest_uris = {uri for timestamp, uri in candidates if timestamp == newest_at}
    if len(newest_uris) != 1:
        return None
    return next(iter(newest_uris))


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
    if status == "unchanged" and reason == "observation_unchanged":
        return "unchanged:observation_unchanged"
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
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--latest-scheduled-report", action="store_true")
    mode.add_argument("--report-uri")
    parser.add_argument("--expected-runtime-revision")
    args = parser.parse_args(argv)

    if os.getenv("GITHUB_ACTIONS") != "true":
        print("skipped:workflow_context_required")
        return 2
    if args.latest_scheduled_report and os.getenv("GITHUB_EVENT_NAME") != "schedule":
        print("skipped:schedule_context_required")
        return 2
    if (
        (args.latest_scheduled_report and args.expected_runtime_revision is not None)
        or (not args.latest_scheduled_report and not args.expected_runtime_revision)
    ):
        print("skipped:runtime_revision_invalid")
        return 2
    token = os.getenv("SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN", "")
    sync_url = os.getenv("ACCOUNT_FACTS_SYNC_URL", "")
    report_prefix = os.getenv("SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX", "")
    service_name = os.getenv("SCHWAB_ACCOUNT_FACTS_SERVICE_NAME", "")
    if not token or sync_url != SYNC_URL:
        print("skipped:publish_configuration_unavailable")
        return 2
    if not report_prefix or not service_name:
        print("skipped:source_configuration_unavailable")
        return 2
    if not _valid_report_prefix(report_prefix):
        print("skipped:source_configuration_unavailable")
        return 2
    if os.getenv("SCHWAB_NET_ASSETS_CURRENCY") != "USD":
        print("skipped:net_assets_currency_unconfirmed")
        return 2
    target_id = os.getenv("SCHWAB_ACCOUNT_FACTS_TARGET_ID", "")
    cash_currency = os.getenv("SCHWAB_CASH_CURRENCY")
    if not target_id.strip():
        print("skipped:target_identity_unavailable")
        return 2
    now = datetime.now(timezone.utc)
    report_uri = args.report_uri
    expected_revision = args.expected_runtime_revision
    if args.latest_scheduled_report:
        expected_revision = _current_serving_revision(service_name)
        if not isinstance(expected_revision, str) or _REVISION.fullmatch(expected_revision) is None:
            print("skipped:runtime_revision_readback_mismatch")
            return 2
        report_uri = _latest_recent_report_uri(report_prefix=report_prefix, now=now)
        if report_uri is None:
            print("skipped:report_unavailable")
            return 2
    if not isinstance(report_uri, str):
        print("skipped:report_uri_invalid")
        return 2
    try:
        _report_uri_parts(report_uri, report_prefix)
    except _ProjectionError as exc:
        print(f"skipped:{exc.reason}")
        return 2
    if not isinstance(expected_revision, str) or not _REVISION.fullmatch(expected_revision):
        print("skipped:runtime_revision_invalid")
        return 2
    if not _service_revision_matches(expected_revision, service_name):
        print("skipped:runtime_revision_readback_mismatch")
        return 2
    report = _read_report(report_uri)
    if report is None:
        print("skipped:report_unavailable")
        return 2
    projected = project_schwab_account_facts_history(
        report,
        source_report_uri=report_uri,
        report_prefix=report_prefix,
        expected_service_name=service_name,
        expected_runtime_revision=expected_revision,
        expected_target_id=target_id,
        expected_cash_currency=cash_currency,
        now=now,
    )
    if projected.get("status") == "skipped":
        print(_safe_result_text(projected))
        return 2
    history = strip_archive_only_keys(
        {key: value for key, value in projected.items() if key != "status"}
    )
    result = _publish_once(
        history,
        sync_url=sync_url,
        token=token,
    )
    print(_safe_result_text(result))
    return 0 if result.get("status") in {"published", "unchanged"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
