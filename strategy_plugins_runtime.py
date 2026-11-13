"""Strategy plugin signal loading, report attachment, and alert dispatch."""

from __future__ import annotations

from platform_entry import entry
from quant_platform_kit.notifications.strategy_plugin_alerts import (
    StrategyPluginAlertStateSettings,
    build_strategy_plugin_alert_context_label as build_alert_context_label,
    publish_strategy_plugin_alerts as dispatch_strategy_plugin_alerts,
)

def load_strategy_plugin_signals():
    return entry().build_composer().load_strategy_plugin_signals(getattr(entry().RUNTIME_SETTINGS, 'strategy_plugin_mounts_json', None))

def attach_strategy_plugin_report(report, *, signals, error: str | None=None):
    entry().build_composer().attach_strategy_plugin_report(report, signals=signals, error=error)

def translate_strategy_plugin_value(category: str, raw_value: str | None) -> str:
    return entry().build_strategy_adapters().translate_strategy_plugin_value(category, raw_value)

def build_strategy_plugin_notification_lines(signals) -> tuple[str, ...]:
    return entry().build_strategy_adapters().build_strategy_plugin_notification_lines(signals)

def build_strategy_plugin_alert_messages(signals):
    return entry().build_strategy_adapters().build_strategy_plugin_alert_messages(signals)

def build_strategy_plugin_alert_state_settings():
    return StrategyPluginAlertStateSettings.from_env(project_id=entry().PROJECT_ID)

def build_strategy_plugin_alert_context_label() -> str:
    return build_alert_context_label(platform_id='schwab', strategy_profile=entry().STRATEGY_PROFILE, service_name=entry().SERVICE_NAME, runtime_target=entry().RUNTIME_SETTINGS.runtime_target)

def publish_strategy_plugin_alerts(signals, *, report=None):
    from notifications.digest_snapshot.digest_routing import immediate_plugin_signals
    from notifications.digest_snapshot.digest_candidates import routine_observation_counts
    signals = tuple(signals or ())
    if report is not None:
        report.setdefault('summary', {})['routine_observation_counts'] = routine_observation_counts(signals)
    result = entry().dispatch_strategy_plugin_alerts(immediate_plugin_signals(signals, mode='daily_digest'), notification_settings=entry().RUNTIME_SETTINGS, translator=entry().t, strategy_label=entry().STRATEGY_PROFILE, context_label=entry().build_strategy_plugin_alert_context_label(), state_settings=entry().build_strategy_plugin_alert_state_settings(), log_message=print)
    if report is not None:
        result.attach_to_report(report)
    return result
