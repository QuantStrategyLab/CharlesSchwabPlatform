"""Dispatch shared monitor checks to platform Cloud Run services."""

from __future__ import annotations

import datetime as dt
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import requests
from google.auth.transport.requests import Request
from google.oauth2 import id_token


MONITOR_TARGET_ENV_NAMES = (
    "MONITOR_DISPATCH_TARGETS_JSON",
    "LONGBRIDGE_MONITOR_DISPATCH_TARGETS_JSON",
    "SCHWAB_MONITOR_DISPATCH_TARGETS_JSON",
    "FIRSTRADE_MONITOR_DISPATCH_TARGETS_JSON",
)
DEFAULT_LOOKBACK_MINUTES = 4
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_MAX_WORKERS = 4


@dataclass(frozen=True)
class MonitorWindow:
    name: str
    path: str
    scheduler_key: str


MONITOR_WINDOWS = (
    MonitorWindow("probe", "/probe", "probe_time"),
    MonitorWindow("precheck", "/dry-run", "precheck_time"),
)


def load_monitor_targets(env: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
    env = env or os.environ
    raw = ""
    for name in MONITOR_TARGET_ENV_NAMES:
        raw = str(env.get(name) or "").strip()
        if raw:
            break
    if not raw:
        return []
    payload = json.loads(raw)
    if isinstance(payload, dict):
        targets = payload.get("targets")
    else:
        targets = payload
    if not isinstance(targets, list):
        raise ValueError("monitor dispatch targets must be a JSON array or an object with targets")
    return [dict(target) for target in targets if isinstance(target, Mapping)]


def dispatch_due_monitors(
    targets: Sequence[Mapping[str, Any]],
    *,
    now: dt.datetime | None = None,
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_workers: int = DEFAULT_MAX_WORKERS,
    token_fetcher: Callable[[str], str] | None = None,
    post_fn: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    now_utc = _as_utc(now or dt.datetime.now(dt.timezone.utc))
    due_dispatches = list(_iter_due_dispatches(targets, now_utc=now_utc, lookback_minutes=lookback_minutes))
    token_fetcher = token_fetcher or _fetch_id_token
    post_fn = post_fn or requests.post
    if not due_dispatches:
        return {
            "ok": True,
            "total_targets": len(targets),
            "dispatches_due": 0,
            "results": [],
        }

    workers = max(1, min(int(max_workers or 1), len(due_dispatches)))
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _send_dispatch,
                dispatch,
                timeout_seconds=timeout_seconds,
                token_fetcher=token_fetcher,
                post_fn=post_fn,
            )
            for dispatch in due_dispatches
        ]
        for future in as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda item: (str(item.get("service_name") or ""), str(item.get("window") or "")))
    return {
        "ok": all(bool(result.get("ok")) for result in results),
        "total_targets": len(targets),
        "dispatches_due": len(due_dispatches),
        "results": results,
    }


def _iter_due_dispatches(
    targets: Sequence[Mapping[str, Any]],
    *,
    now_utc: dt.datetime,
    lookback_minutes: int,
) -> Iterable[dict[str, Any]]:
    for target in targets:
        if not _target_enabled(target):
            continue
        service_url = str(target.get("service_url") or "").rstrip("/")
        if not service_url:
            continue
        scheduler = target.get("scheduler") if isinstance(target.get("scheduler"), Mapping) else {}
        timezone = _target_timezone(scheduler)
        local_now = now_utc.astimezone(timezone)
        for window in MONITOR_WINDOWS:
            schedule = str(scheduler.get(window.scheduler_key) or "").strip()
            if not schedule:
                continue
            if _cron_due_within_window(schedule, local_now=local_now, lookback_minutes=lookback_minutes):
                yield {
                    "service_name": str(target.get("service_name") or ""),
                    "strategy_profile": str(target.get("strategy_profile") or ""),
                    "account_scope": str(target.get("account_scope") or target.get("account_group") or ""),
                    "window": window.name,
                    "url": f"{service_url}{window.path}",
                    "audience": service_url,
                }


def _send_dispatch(
    dispatch: Mapping[str, Any],
    *,
    timeout_seconds: int,
    token_fetcher: Callable[[str], str],
    post_fn: Callable[..., Any],
) -> dict[str, Any]:
    url = str(dispatch.get("url") or "")
    audience = str(dispatch.get("audience") or "")
    base_result = {
        "service_name": dispatch.get("service_name"),
        "strategy_profile": dispatch.get("strategy_profile"),
        "account_scope": dispatch.get("account_scope"),
        "window": dispatch.get("window"),
        "url": url,
    }
    try:
        token = token_fetcher(audience)
        response = post_fn(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "User-Agent": "platform-monitor-dispatcher",
            },
            timeout=timeout_seconds,
        )
        status_code = int(getattr(response, "status_code", 0) or 0)
        return {
            **base_result,
            "status_code": status_code,
            "ok": 200 <= status_code < 300,
        }
    except Exception as exc:
        return {
            **base_result,
            "status_code": 0,
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _target_enabled(target: Mapping[str, Any]) -> bool:
    value = target.get("runtime_target_enabled")
    if value is None:
        value = target.get("enabled")
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off", "disabled"}


def _target_timezone(scheduler: Mapping[str, Any]) -> dt.tzinfo:
    try:
        return ZoneInfo(str(scheduler.get("timezone") or "UTC"))
    except Exception:
        return dt.timezone.utc


def _cron_due_within_window(schedule: str, *, local_now: dt.datetime, lookback_minutes: int) -> bool:
    fields = schedule.split()
    if len(fields) != 5:
        return False
    lookback = max(0, int(lookback_minutes or 0))
    floor_now = local_now.replace(second=0, microsecond=0)
    for minute_offset in range(lookback + 1):
        candidate = floor_now - dt.timedelta(minutes=minute_offset)
        if _cron_matches(fields, candidate):
            return True
    return False


def _cron_matches(fields: Sequence[str], value: dt.datetime) -> bool:
    minute, hour, day_of_month, month, day_of_week = fields
    cron_weekday = (value.weekday() + 1) % 7
    return (
        _field_matches(minute, value.minute, 0, 59)
        and _field_matches(hour, value.hour, 0, 23)
        and _field_matches(day_of_month, value.day, 1, 31)
        and _field_matches(month, value.month, 1, 12)
        and _field_matches(day_of_week, cron_weekday, 0, 7, sunday_alias=True)
    )


def _field_matches(field: str, value: int, min_value: int, max_value: int, *, sunday_alias: bool = False) -> bool:
    for part in field.split(","):
        part = part.strip()
        if not part:
            continue
        if _part_matches(part, value, min_value, max_value, sunday_alias=sunday_alias):
            return True
    return False


def _part_matches(part: str, value: int, min_value: int, max_value: int, *, sunday_alias: bool = False) -> bool:
    if "/" in part:
        base, step_text = part.split("/", 1)
        try:
            step = int(step_text)
        except ValueError:
            return False
        if step <= 0:
            return False
    else:
        base = part
        step = 1
    if base == "*":
        start, end = min_value, max_value
    elif "-" in base:
        start_text, end_text = base.split("-", 1)
        try:
            start, end = int(start_text), int(end_text)
        except ValueError:
            return False
    else:
        try:
            start = end = int(base)
        except ValueError:
            return False
    if sunday_alias and value == 0 and start == end == 7:
        return True
    if value < start or value > end:
        return False
    return (value - start) % step == 0


def _as_utc(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def _fetch_id_token(audience: str) -> str:
    return id_token.fetch_id_token(Request(), audience)
