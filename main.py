"""Charles Schwab platform HTTP entrypoint."""

from __future__ import annotations

import json
import importlib
import os
import tempfile
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Flask
import requests

from quant_platform_kit.common.platform_runner import dispatch_due_monitors, load_monitor_targets
from application.execution_receipt_adapter import (
    attach_cycle_execution_receipt,
    attach_terminal_fallback_execution_receipt,
)
from application.account_observation import (
    build_account_observation,
    declare_cash_balance_currency,
    declare_net_assets_currency,
    expected_account_hash_from_selector,
)
from application.stock_inventory import collect_stock_inventory
from application.broker_reconciliation import (
    SchwabReconciliationReadError,
    build_reconciliation_candidate,
    collect_read_only_reconciliation_observations,
    validate_reconciliation_candidate,
    validate_reconciliation_preconditions,
)
from application import signal_provenance
from application.external_flow_snapshot import collect_external_flow_snapshot, external_flow_enabled
from application.runtime_broker_adapters import build_runtime_broker_adapters
from application.runtime_report_summary import summarize_execution_cycle_result
from application.runtime_composer import build_runtime_composer
from application.runtime_strategy_adapters import build_runtime_strategy_adapters
from application.rebalance_service import run_strategy_core as run_rebalance_cycle
from application.paper_execution_command_consumer import (
    consume_due_paper_execution_commands,
)
from application.signal_snapshot import build_signal_snapshot
from decision_mapper import map_strategy_decision_to_plan
from entrypoints.cloud_run import is_market_open_now
from runtime_execution_policy import dca_execution_unsupported_reason
from notifications.telegram import (
    build_signal_text,
    build_strategy_display_name,
    build_translator,
)
from quant_platform_kit.notifications.strategy_plugin_alerts import (
    StrategyPluginAlertStateSettings,
    build_strategy_plugin_alert_context_label as build_alert_context_label,
    publish_strategy_plugin_alerts as dispatch_strategy_plugin_alerts,
)
from quant_platform_kit.schwab import (
    fetch_account_snapshot,
    fetch_default_daily_price_history_candles,
    fetch_order_status,
    fetch_quotes,
    get_client_from_secret,
    submit_equity_order,
)
from quant_platform_kit.common.runtime_reports import (
    append_runtime_report_error,
    build_runtime_report_base,
    finalize_runtime_report,
    persist_runtime_report,
)
from quant_platform_kit.common.execution_commands import build_execution_command_store_from_env
from quant_platform_kit.common.strategy_release import build_runtime_loaded_receipt
from quant_platform_kit.common.strategy_plugins import (
    build_strategy_plugin_report_payload,
    load_configured_strategy_plugin_signals,
    parse_strategy_plugin_mounts,
)
from quant_platform_kit.common.runtime_inputs import build_strategy_evaluation_inputs
from runtime_config_support import load_platform_runtime_settings
from runtime_logging import build_run_id, emit_runtime_log
from strategy_runtime import load_strategy_runtime

from runtime_wiring import (
    get_project_id,
    PROJECT_ID,
    SERVICE_NAME,
    APP_KEY,
    APP_SECRET,
    TG_TOKEN,
    TG_CHAT_ID,
    _NOTIFICATION_CHANNEL,
    _NOTIFICATION_WEBHOOK_URL,
    SECRET_ID,
    TOKEN_PATH,
    _optional_float_env,
    _optional_symbol_env,
    INCOME_THRESHOLD_USD,
    QQQI_INCOME_RATIO,
    DUAL_DRIVE_UNLEVERED_SYMBOL,
    LIMIT_BUY_PREMIUM,
    DEFAULT_LIMIT_BUY_PREMIUM_BY_SYMBOL,
    SELL_SETTLE_DELAY_SEC,
    POST_SELL_REFRESH_ATTEMPTS,
    POST_SELL_REFRESH_INTERVAL_SEC,
    DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD,
    DCA_PROFILES,
    _load_limit_buy_premium_by_symbol,
    LIMIT_BUY_PREMIUM_BY_SYMBOL,
    RUNTIME_SETTINGS,
    STRATEGY_PROFILE,
    STRATEGY_DISPLAY_NAME,
    NOTIFY_LANG,
    CASH_ONLY_EXECUTION,
    t,
    signal_text,
    strategy_display_name,
    _normalize_plugin_mounts_strategy,
    _normalize_monitor_targets_strategy,
    build_tqqq_managed_symbols,
    build_strategy_runtime_overrides,
    STRATEGY_RUNTIME,
    STRATEGY_RUNTIME_CONFIG,
    MANAGED_SYMBOLS,
    BENCHMARK_SYMBOL,
    SIGNAL_EFFECTIVE_AFTER_TRADING_DAYS,
    AVAILABLE_INPUTS,
    validate_config,
    build_broker_adapters,
    _runtime_revision_diagnostics,
    build_strategy_adapters,
    _safe_haven_cash_substitute_threshold_usd,
    build_composer,
)
from notifications_runtime import (
    send_tg_message,
    publish_notification,
    _split_env_list,
    _runtime_error_notification_targets,
    _runtime_error_notification_message,
    _notify_runtime_error,
    _notify_digest_coverage_failure,
    _report_requires_digest_coverage,
    _publish_runtime_failure_notification_legacy,
    _publish_runtime_failure_notification,
    _handle_route_runtime_error,
    _route_with_runtime_error_fallback,
)
from strategy_plugins_runtime import (
    load_strategy_plugin_signals,
    attach_strategy_plugin_report,
    translate_strategy_plugin_value,
    build_strategy_plugin_notification_lines,
    build_strategy_plugin_alert_messages,
    build_strategy_plugin_alert_state_settings,
    build_strategy_plugin_alert_context_label,
    publish_strategy_plugin_alerts,
)
from http_routes import (
    app,
    READ_ONLY_BROKER_RECONCILIATION_COLLECTOR,
    FINAL_RISK_ASSESSMENT_PROVIDER,
    log_runtime_event,
    build_execution_report,
    persist_execution_report,
    fetch_reference_history,
    build_price_history,
    build_market_history_loader,
    fetch_managed_snapshot,
    build_market_data_port,
    build_semiconductor_indicators,
    build_account_state_from_snapshot,
    resolve_rebalance_plan,
    run_strategy_core,
    _signal_diagnostics_from_result,
    _has_signal_snapshot_details,
    _summarize_cycle_result_for_report,
    _schwab_force_run_env,
    _schwab_market_open_now,
    attach_signal_data_provenance,
    _handle_schwab_cycle,
    _paper_command_consumer_runtime_is_isolated,
    _paper_command_consumer_session_date,
    _paper_command_consumer_binding,
    _handle_paper_execution_command_consumer,
    _handle_schwab_probe,
    _build_optional_c4_attachment,
    _handle_reconciliation,
    handle_schwab,
    handle_schwab_dry_run,
    handle_paper_execution_command_consumer,
    handle_schwab_probe,
    handle_reconciliation,
    handle_monitor_dispatch,
    request_method,
    health,
)

import platform_entry as _platform_entry
_platform_entry.bind(__import__("sys").modules[__name__])


__all__ = [
    "get_project_id",
    "PROJECT_ID",
    "SERVICE_NAME",
    "APP_KEY",
    "APP_SECRET",
    "TG_TOKEN",
    "TG_CHAT_ID",
    "_NOTIFICATION_CHANNEL",
    "_NOTIFICATION_WEBHOOK_URL",
    "SECRET_ID",
    "TOKEN_PATH",
    "_optional_float_env",
    "_optional_symbol_env",
    "INCOME_THRESHOLD_USD",
    "QQQI_INCOME_RATIO",
    "DUAL_DRIVE_UNLEVERED_SYMBOL",
    "LIMIT_BUY_PREMIUM",
    "DEFAULT_LIMIT_BUY_PREMIUM_BY_SYMBOL",
    "SELL_SETTLE_DELAY_SEC",
    "POST_SELL_REFRESH_ATTEMPTS",
    "POST_SELL_REFRESH_INTERVAL_SEC",
    "DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD",
    "DCA_PROFILES",
    "_load_limit_buy_premium_by_symbol",
    "LIMIT_BUY_PREMIUM_BY_SYMBOL",
    "RUNTIME_SETTINGS",
    "STRATEGY_PROFILE",
    "STRATEGY_DISPLAY_NAME",
    "NOTIFY_LANG",
    "CASH_ONLY_EXECUTION",
    "t",
    "signal_text",
    "strategy_display_name",
    "_normalize_plugin_mounts_strategy",
    "_normalize_monitor_targets_strategy",
    "build_tqqq_managed_symbols",
    "build_strategy_runtime_overrides",
    "STRATEGY_RUNTIME",
    "STRATEGY_RUNTIME_CONFIG",
    "MANAGED_SYMBOLS",
    "BENCHMARK_SYMBOL",
    "SIGNAL_EFFECTIVE_AFTER_TRADING_DAYS",
    "AVAILABLE_INPUTS",
    "validate_config",
    "build_broker_adapters",
    "_runtime_revision_diagnostics",
    "build_strategy_adapters",
    "_safe_haven_cash_substitute_threshold_usd",
    "build_composer",
    "send_tg_message",
    "publish_notification",
    "_split_env_list",
    "_runtime_error_notification_targets",
    "_runtime_error_notification_message",
    "_notify_runtime_error",
    "_notify_digest_coverage_failure",
    "_report_requires_digest_coverage",
    "_publish_runtime_failure_notification_legacy",
    "_publish_runtime_failure_notification",
    "_handle_route_runtime_error",
    "_route_with_runtime_error_fallback",
    "load_strategy_plugin_signals",
    "attach_strategy_plugin_report",
    "translate_strategy_plugin_value",
    "build_strategy_plugin_notification_lines",
    "build_strategy_plugin_alert_messages",
    "build_strategy_plugin_alert_state_settings",
    "build_strategy_plugin_alert_context_label",
    "publish_strategy_plugin_alerts",
    "app",
    "READ_ONLY_BROKER_RECONCILIATION_COLLECTOR",
    "FINAL_RISK_ASSESSMENT_PROVIDER",
    "log_runtime_event",
    "build_execution_report",
    "persist_execution_report",
    "fetch_reference_history",
    "build_price_history",
    "build_market_history_loader",
    "fetch_managed_snapshot",
    "build_market_data_port",
    "build_semiconductor_indicators",
    "build_account_state_from_snapshot",
    "resolve_rebalance_plan",
    "run_strategy_core",
    "_signal_diagnostics_from_result",
    "_has_signal_snapshot_details",
    "_summarize_cycle_result_for_report",
    "_schwab_force_run_env",
    "_schwab_market_open_now",
    "attach_signal_data_provenance",
    "_handle_schwab_cycle",
    "_paper_command_consumer_runtime_is_isolated",
    "_paper_command_consumer_session_date",
    "_paper_command_consumer_binding",
    "_handle_paper_execution_command_consumer",
    "_handle_schwab_probe",
    "_build_optional_c4_attachment",
    "_handle_reconciliation",
    "handle_schwab",
    "handle_schwab_dry_run",
    "handle_paper_execution_command_consumer",
    "handle_schwab_probe",
    "handle_reconciliation",
    "handle_monitor_dispatch",
    "request_method",
    "health",
]


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
