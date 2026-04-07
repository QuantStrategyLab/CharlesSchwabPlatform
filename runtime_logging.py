from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Callable, Mapping


LogPrinter = Callable[..., Any]


def build_run_id(now: datetime | None = None) -> str:
    current = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    return current.strftime("%Y%m%dT%H%M%SZ")


def extract_cloud_trace(project_id: str | None, header_value: str | None) -> str | None:
    if not project_id or not header_value:
        return None
    trace_id = str(header_value).split("/", 1)[0].strip()
    if not trace_id:
        return None
    return f"projects/{project_id}/traces/{trace_id}"


@dataclass(frozen=True)
class RuntimeLogContext:
    platform: str
    deploy_target: str
    service_name: str
    strategy_profile: str
    run_id: str = ""
    account_scope: str | None = None
    account_group: str | None = None
    account_region: str | None = None
    project_id: str | None = None
    instance_name: str | None = None
    trace: str | None = None
    extra_fields: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for field_name in ("platform", "deploy_target", "service_name", "strategy_profile"):
            if not str(getattr(self, field_name, "") or "").strip():
                raise ValueError(f"{field_name} must not be empty")

    def with_run(
        self,
        run_id: str | None = None,
        *,
        trace: str | None = None,
        extra_fields: Mapping[str, Any] | None = None,
    ) -> "RuntimeLogContext":
        merged_extra = dict(self.extra_fields)
        if extra_fields:
            merged_extra.update(dict(extra_fields))
        return replace(
            self,
            run_id=str(run_id or self.run_id or ""),
            trace=self.trace if trace is None else trace,
            extra_fields=merged_extra,
        )


def emit_runtime_log(
    context: RuntimeLogContext,
    event: str,
    *,
    message: str | None = None,
    severity: str = "INFO",
    printer: LogPrinter = print,
    now: datetime | None = None,
    **fields: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "timestamp": _format_timestamp(now),
        "severity": str(severity or "INFO").upper(),
        "event": str(event),
        "message": str(message or event),
        "platform": context.platform,
        "deploy_target": context.deploy_target,
        "service_name": context.service_name,
        "strategy_profile": context.strategy_profile,
        "run_id": context.run_id or None,
        "account_scope": context.account_scope,
        "account_group": context.account_group,
        "account_region": context.account_region,
        "project_id": context.project_id,
        "instance_name": context.instance_name,
    }
    payload.update(_normalize_mapping(context.extra_fields))
    payload.update(_normalize_mapping(fields))
    if context.trace:
        payload["logging.googleapis.com/trace"] = context.trace

    cleaned_payload = _drop_empty(payload)
    encoded = json.dumps(cleaned_payload, ensure_ascii=False, sort_keys=True, default=_json_default)
    _write_log_line(printer, encoded)
    return cleaned_payload



def _format_timestamp(now: datetime | None) -> str:
    current = now.astimezone(timezone.utc) if now is not None else datetime.now(timezone.utc)
    return current.isoformat().replace("+00:00", "Z")



def _normalize_mapping(mapping: Mapping[str, Any] | None) -> dict[str, Any]:
    if not mapping:
        return {}
    return {str(key): _normalize_value(value) for key, value in mapping.items()}



def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Mapping):
        return _drop_empty({str(key): _normalize_value(item) for key, item in value.items()})
    if isinstance(value, tuple):
        return [_normalize_value(item) for item in value]
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    return value



def _drop_empty(payload: Mapping[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, tuple, dict)) and len(value) == 0:
            continue
        cleaned[str(key)] = value
    return cleaned



def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)



def _write_log_line(printer: LogPrinter, line: str) -> None:
    try:
        printer(line, flush=True)
    except TypeError:
        printer(line)
