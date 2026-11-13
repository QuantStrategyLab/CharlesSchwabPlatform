"""Telegram and unified notification helpers for runtime routes."""

from __future__ import annotations

import os

import requests

from runtime_wiring import (  # noqa: F401
    NOTIFY_LANG,
    PROJECT_ID,
    RUNTIME_SETTINGS,
    SERVICE_NAME,
    STRATEGY_PROFILE,
    TG_CHAT_ID,
    TG_TOKEN,
    build_composer,
    strategy_display_name,
    t,
)
from runtime_wiring import build_composer as _build_composer

def send_tg_message(message):
    return build_composer().send_tg_message(message)


def publish_notification(*, detailed_text, compact_text):
    return build_composer().build_notification_adapters().publish_cycle_notification(
        detailed_text=detailed_text,
        compact_text=compact_text,
    )


def _split_env_list(value: str | None) -> tuple[str, ...]:
    return tuple(
        item.strip()
        for item in str(value or "").replace(";", ",").split(",")
        if item.strip()
    )


def _runtime_error_notification_targets() -> tuple[tuple[str, str], ...]:
    targets: list[tuple[str, str]] = []
    if TG_TOKEN and TG_CHAT_ID:
        targets.append((TG_TOKEN, TG_CHAT_ID))

    seen: set[tuple[str, str]] = set()
    unique_targets: list[tuple[str, str]] = []
    for target in targets:
        if target in seen:
            continue
        seen.add(target)
        unique_targets.append(target)
    return tuple(unique_targets)


def _runtime_error_notification_message(exc: Exception, *, route_label: str) -> str:
    title = "runtime_failure_title"
    return "\n".join((
        t(title),
        t("strategy_label", name=strategy_display_name or STRATEGY_PROFILE),
        t("runtime_failure_context", context=SERVICE_NAME),
        t("runtime_failure_result"),
        t("runtime_failure_action"),
    ))


def _notify_runtime_error(exc: Exception, *, route_label: str) -> bool:
    targets = _runtime_error_notification_targets()
    if not targets:
        print(t("runtime_notification_missing_target"), flush=True)
        return False
    message = _runtime_error_notification_message(exc, route_label=route_label)
    outcomes = []
    for token, chat_id in targets:
        try:
            response = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message},
                timeout=15,
            )
            status_code = int(getattr(response, "status_code", 200) or 200)
            acknowledged = 200 <= status_code < 300
            load_payload = getattr(response, "json", None)
            payload = load_payload() if acknowledged and callable(load_payload) else None
            if isinstance(payload, dict) and payload.get("ok") is False:
                acknowledged = False
            outcomes.append(acknowledged)
        except Exception:
            print(
                t("runtime_notification_delivery_failed"),
                flush=True,
            )
            outcomes.append(False)
    return bool(outcomes) and all(outcomes)


def _notify_digest_coverage_failure() -> bool:
    from quant_platform_kit.common.operational_notification_localization import resolve_operational_notification_locale
    zh = resolve_operational_notification_locale(NOTIFY_LANG) == "zh"
    message = ("⚠️ 每日摘要记录未确认；请检查运行记录和摘要存储。" if zh else
               "⚠️ Daily digest coverage is unconfirmed; check the run and digest storage.")
    try:
        if publish_notification(detailed_text=message, compact_text=message):
            return True
    except Exception:
        pass
    # Reuse the independent existing error sender without exposing trade content.
    return _notify_runtime_error(RuntimeError("digest_coverage_unconfirmed"), route_label="digest_coverage")


def _report_requires_digest_coverage(report) -> bool:
    summary = report.get("summary") or {}
    if (summary.get("notification_digest") or {}).get("routine") is True:
        return True
    return bool(summary.get("routine_observation_counts"))


def _publish_runtime_failure_notification_legacy(*, detailed_text: str, compact_text: str, exc: Exception) -> bool:
    try:
        if publish_notification(detailed_text=detailed_text, compact_text=compact_text):
            return True
        return _notify_runtime_error(exc, route_label="strategy_cycle")
    except Exception:
        print(t("runtime_notification_delivery_failed"), flush=True)
        return _notify_runtime_error(exc, route_label="strategy_cycle")


def _publish_runtime_failure_notification(*, detailed_text: str, compact_text: str, exc: Exception) -> bool:
    from notifications.unified import enabled as unified_enabled, publish_incident

    if not unified_enabled():
        return _publish_runtime_failure_notification_legacy(detailed_text=detailed_text, compact_text=compact_text, exc=exc)

    return publish_incident(
        lambda: publish_notification(detailed_text=detailed_text, compact_text=compact_text),
        reason="runtime_failure:" + type(exc).__name__, profile=STRATEGY_PROFILE,
        kind="auth_failure" if "auth" in type(exc).__name__.lower() else "real_error",
    )


def _handle_route_runtime_error(exc: Exception, *, route_label: str):
    from notifications.unified import publish_incident

    print(t("runtime_failure_log"), flush=True)
    publish_incident(lambda: _notify_runtime_error(exc, route_label=route_label),
        reason="route_runtime_failure:" + route_label + ":" + type(exc).__name__, profile=STRATEGY_PROFILE,
        kind="auth_failure" if "auth" in type(exc).__name__.lower() else "real_error")
    return "Error", 500


def _route_with_runtime_error_fallback(handler, *args, route_label: str, **kwargs):
    try:
        return handler(*args, **kwargs)
    except Exception as exc:
        return _handle_route_runtime_error(exc, route_label=route_label)


