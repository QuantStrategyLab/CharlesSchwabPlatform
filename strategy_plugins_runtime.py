"""Strategy plugin signal loading, report attachment, and alert dispatch."""

from quant_platform_kit.notifications.strategy_plugin_alerts import (
    StrategyPluginAlertStateSettings,
    build_strategy_plugin_alert_context_label as build_alert_context_label,
    publish_strategy_plugin_alerts as dispatch_strategy_plugin_alerts,
)

from runtime_wiring import (
    PROJECT_ID,
    RUNTIME_SETTINGS,
    SERVICE_NAME,
    STRATEGY_PROFILE,
    build_composer,
    build_strategy_adapters,
    t,
)

def load_strategy_plugin_signals():
    return build_composer().load_strategy_plugin_signals(
        getattr(RUNTIME_SETTINGS, "strategy_plugin_mounts_json", None)
    )


def attach_strategy_plugin_report(report, *, signals, error: str | None = None):
    build_composer().attach_strategy_plugin_report(
        report,
        signals=signals,
        error=error,
    )


def translate_strategy_plugin_value(category: str, raw_value: str | None) -> str:
    return build_strategy_adapters().translate_strategy_plugin_value(category, raw_value)


def build_strategy_plugin_notification_lines(signals) -> tuple[str, ...]:
    return build_strategy_adapters().build_strategy_plugin_notification_lines(signals)


def build_strategy_plugin_alert_messages(signals):
    return build_strategy_adapters().build_strategy_plugin_alert_messages(signals)


def build_strategy_plugin_alert_state_settings():
    return StrategyPluginAlertStateSettings.from_env(
        project_id=PROJECT_ID,
    )


def build_strategy_plugin_alert_context_label() -> str:
    return build_alert_context_label(
        platform_id="schwab",
        strategy_profile=STRATEGY_PROFILE,
        service_name=SERVICE_NAME,
        runtime_target=RUNTIME_SETTINGS.runtime_target,
    )


def publish_strategy_plugin_alerts(signals, *, report=None):
    from notifications.digest_snapshot.digest_routing import immediate_plugin_signals
    from notifications.digest_snapshot.digest_candidates import routine_observation_counts
    signals = tuple(signals or ())
    if report is not None:
        report.setdefault("summary", {})["routine_observation_counts"] = routine_observation_counts(signals)
    result = dispatch_strategy_plugin_alerts(
        immediate_plugin_signals(signals, mode="daily_digest"),
        notification_settings=RUNTIME_SETTINGS,
        translator=t,
        strategy_label=STRATEGY_PROFILE,
        context_label=build_strategy_plugin_alert_context_label(),
        state_settings=build_strategy_plugin_alert_state_settings(),
        log_message=print,
    )
    if report is not None:
        result.attach_to_report(report)
    return result

