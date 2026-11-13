"""Runtime configuration, composer, and adapter wiring."""

from __future__ import annotations

from platform_entry import entry as platform_entry

import json
import importlib
import os
import tempfile
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests
from quant_platform_kit.common.platform_runner import dispatch_due_monitors, load_monitor_targets
from application.execution_receipt_adapter import attach_cycle_execution_receipt, attach_terminal_fallback_execution_receipt
from application.account_observation import build_account_observation, declare_cash_balance_currency, declare_net_assets_currency, expected_account_hash_from_selector
from application.stock_inventory import collect_stock_inventory
from application.broker_reconciliation import SchwabReconciliationReadError, build_reconciliation_candidate, collect_read_only_reconciliation_observations, validate_reconciliation_candidate, validate_reconciliation_preconditions
from application import signal_provenance
from application.external_flow_snapshot import collect_external_flow_snapshot, external_flow_enabled
from application.runtime_broker_adapters import build_runtime_broker_adapters
from application.runtime_report_summary import summarize_execution_cycle_result
from application.runtime_composer import build_runtime_composer
from application.runtime_strategy_adapters import build_runtime_strategy_adapters
from application.rebalance_service import run_strategy_core as run_rebalance_cycle
from application.paper_execution_command_consumer import consume_due_paper_execution_commands
from application.signal_snapshot import build_signal_snapshot
from decision_mapper import map_strategy_decision_to_plan
from entrypoints.cloud_run import is_market_open_now
from runtime_execution_policy import dca_execution_unsupported_reason
from notifications.telegram import build_signal_text, build_strategy_display_name, build_translator
from quant_platform_kit.notifications.strategy_plugin_alerts import StrategyPluginAlertStateSettings, build_strategy_plugin_alert_context_label as build_alert_context_label, publish_strategy_plugin_alerts as dispatch_strategy_plugin_alerts
from quant_platform_kit.schwab import fetch_account_snapshot, fetch_default_daily_price_history_candles, fetch_order_status, fetch_quotes, get_client_from_secret, submit_equity_order
from quant_platform_kit.common.runtime_reports import append_runtime_report_error, build_runtime_report_base, finalize_runtime_report, persist_runtime_report
from quant_platform_kit.common.execution_commands import build_execution_command_store_from_env
from quant_platform_kit.common.strategy_release import build_runtime_loaded_receipt
from quant_platform_kit.common.strategy_plugins import build_strategy_plugin_report_payload, load_configured_strategy_plugin_signals, parse_strategy_plugin_mounts
from quant_platform_kit.common.runtime_inputs import build_strategy_evaluation_inputs
from runtime_config_support import load_platform_runtime_settings
from runtime_logging import build_run_id, emit_runtime_log
from strategy_runtime import load_strategy_runtime

from quant_platform_kit.notifications.cycle_channel import resolve_cycle_channel_and_url

def get_project_id():
    try:
        from quant_platform_kit.cloud import get_deployment_context
        return get_deployment_context().project_id
    except Exception:
        return os.getenv('GOOGLE_CLOUD_PROJECT')
PROJECT_ID = get_project_id()
SERVICE_NAME = os.getenv('SERVICE_NAME') or os.getenv('K_SERVICE') or 'charles-schwab-platform'
APP_KEY = os.getenv('SCHWAB_API_KEY')
APP_SECRET = os.getenv('SCHWAB_APP_SECRET')
TG_TOKEN = os.getenv('TELEGRAM_TOKEN')
TG_CHAT_ID = os.getenv('QSL_GLOBAL_TELEGRAM_CHAT_ID') or os.getenv('GLOBAL_TELEGRAM_CHAT_ID')
_NOTIFICATION_CHANNEL, _NOTIFICATION_WEBHOOK_URL = resolve_cycle_channel_and_url(explicit_channel=os.getenv('NOTIFICATION_CHANNEL', 'telegram'), telegram_token=TG_TOKEN, telegram_chat_id=TG_CHAT_ID, wecom_url=os.getenv('NOTIFICATION_WECOM_WEBHOOK_URL'), dingtalk_url=os.getenv('NOTIFICATION_DINGTALK_WEBHOOK_URL'), feishu_url=os.getenv('NOTIFICATION_FEISHU_WEBHOOK_URL'), serverchan_url=os.getenv('NOTIFICATION_SERVERCHAN_WEBHOOK_URL'))
SECRET_ID = 'schwab_token'
TOKEN_PATH = os.path.join(tempfile.gettempdir(), 'qsl-schwab-token.json')

def _optional_float_env(name: str) -> float | None:
    value = os.getenv(name)
    if value is None or value.strip() == '':
        return None
    return float(value)

def _optional_symbol_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None or value.strip() == '':
        return None
    return value.strip().upper()
INCOME_THRESHOLD_USD = _optional_float_env('INCOME_THRESHOLD_USD')
QQQI_INCOME_RATIO = _optional_float_env('QQQI_INCOME_RATIO')
DUAL_DRIVE_UNLEVERED_SYMBOL = _optional_symbol_env('DUAL_DRIVE_UNLEVERED_SYMBOL')
LIMIT_BUY_PREMIUM = 1.005
DEFAULT_LIMIT_BUY_PREMIUM_BY_SYMBOL = {'SOXL': 1.015, 'TQQQ': 1.01}
SELL_SETTLE_DELAY_SEC = 3
POST_SELL_REFRESH_ATTEMPTS = 5
POST_SELL_REFRESH_INTERVAL_SEC = 1
DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD = 1000.0
DCA_PROFILES = frozenset({'nasdaq_sp500_smart_dca', 'ibit_smart_dca'})

def _load_limit_buy_premium_by_symbol(*env_names: str) -> dict[str, float]:
    raw_value = ''
    for env_name in env_names:
        value = os.getenv(env_name)
        if value and value.strip():
            raw_value = value.strip()
            break
    if not raw_value:
        return dict(DEFAULT_LIMIT_BUY_PREMIUM_BY_SYMBOL)
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise ValueError(f'Invalid limit buy premium map JSON: {raw_value!r}') from exc
    if not isinstance(payload, dict):
        raise ValueError('Limit buy premium map must be a JSON object keyed by symbol.')
    parsed: dict[str, float] = {}
    for symbol, premium in payload.items():
        symbol_text = str(symbol or '').strip().upper()
        if not symbol_text:
            continue
        premium_value = float(premium)
        if premium_value <= 0.0:
            raise ValueError(f'Limit buy premium for {symbol_text} must be positive.')
        parsed[symbol_text] = premium_value
    return parsed
LIMIT_BUY_PREMIUM_BY_SYMBOL = _load_limit_buy_premium_by_symbol('SCHWAB_LIMIT_BUY_PREMIUM_BY_SYMBOL_JSON', 'LIMIT_BUY_PREMIUM_BY_SYMBOL_JSON')
RUNTIME_SETTINGS = load_platform_runtime_settings()
STRATEGY_PROFILE = RUNTIME_SETTINGS.strategy_profile
STRATEGY_DISPLAY_NAME = RUNTIME_SETTINGS.strategy_display_name
NOTIFY_LANG = RUNTIME_SETTINGS.notify_lang
CASH_ONLY_EXECUTION = getattr(RUNTIME_SETTINGS, 'cash_only_execution', True)
t = build_translator(NOTIFY_LANG)
signal_text = build_signal_text(t)
strategy_display_name = build_strategy_display_name(t)(STRATEGY_PROFILE, fallback_name=STRATEGY_DISPLAY_NAME, metadata=RUNTIME_SETTINGS.strategy_metadata)

def _normalize_plugin_mounts_strategy(raw_mounts_json: str | None) -> str | None:
    """Auto-correct plugin mount strategy fields to match STRATEGY_PROFILE.

    When the user changes the strategy profile, the plugin mount JSON may still
    reference the old strategy name.  This function rewrites the ``strategy``
    field inside every mount entry so it always matches the runtime strategy.
    A warning is printed for each corrected entry.
    """
    if not raw_mounts_json:
        return raw_mounts_json
    try:
        mounts = json.loads(raw_mounts_json)
    except json.JSONDecodeError:
        return raw_mounts_json
    if not isinstance(mounts, dict):
        return raw_mounts_json
    plugins = mounts.get('strategy_plugins')
    if not isinstance(plugins, list):
        return raw_mounts_json
    changed = False
    for item in plugins:
        if not isinstance(item, dict):
            continue
        old = item.get('strategy')
        if old and old != STRATEGY_PROFILE:
            entry['strategy'] = STRATEGY_PROFILE
            changed = True
            print(f'[config-sync] Plugin mount strategy corrected: {old} → {STRATEGY_PROFILE}', flush=True)
    return json.dumps(mounts, ensure_ascii=False) if changed else raw_mounts_json

def _normalize_monitor_targets_strategy(raw_targets_json: str | None) -> str | None:
    """Auto-correct monitor dispatch targets to match STRATEGY_PROFILE."""
    if not raw_targets_json:
        return raw_targets_json
    try:
        targets = json.loads(raw_targets_json)
    except json.JSONDecodeError:
        return raw_targets_json
    if not isinstance(targets, dict):
        return raw_targets_json
    entries = targets.get('targets')
    if not isinstance(entries, list):
        return raw_targets_json
    changed = False
    for entry in entries:
        if not isinstance(item, dict):
            continue
        old = item.get('strategy_profile')
        if old and old != STRATEGY_PROFILE:
            entry['strategy_profile'] = STRATEGY_PROFILE
            changed = True
            print(f'[config-sync] Monitor target strategy corrected: {old} → {STRATEGY_PROFILE}', flush=True)
    return json.dumps(targets, ensure_ascii=False) if changed else raw_targets_json
if hasattr(RUNTIME_SETTINGS, 'strategy_plugin_mounts_json'):
    normalized = _normalize_plugin_mounts_strategy(RUNTIME_SETTINGS.strategy_plugin_mounts_json)
    if normalized != RUNTIME_SETTINGS.strategy_plugin_mounts_json:
        object.__setattr__(RUNTIME_SETTINGS, 'strategy_plugin_mounts_json', normalized)

def build_tqqq_managed_symbols(unlevered_symbol: str) -> tuple[str, ...]:
    symbol = str(unlevered_symbol or 'QQQ').strip().upper()
    if not symbol:
        raise ValueError('DUAL_DRIVE_UNLEVERED_SYMBOL must be a non-empty ticker')
    if symbol in {'TQQQ', 'BOXX', 'SPYI', 'QQQI'}:
        raise ValueError('DUAL_DRIVE_UNLEVERED_SYMBOL must not overlap another TQQQ profile sleeve')
    return ('TQQQ', symbol, 'BOXX', 'SPYI', 'QQQI')

def build_strategy_runtime_overrides(profile: str, runtime_settings=RUNTIME_SETTINGS) -> dict[str, object]:
    overrides: dict[str, object] = {}
    income_layer_enabled = getattr(runtime_settings, 'income_layer_enabled', None)
    income_layer_start_usd = getattr(runtime_settings, 'income_layer_start_usd', None)
    income_layer_max_ratio = getattr(runtime_settings, 'income_layer_max_ratio', None)
    if income_layer_enabled is not None:
        overrides['income_layer_enabled'] = income_layer_enabled
    if income_layer_start_usd is not None:
        overrides['income_layer_start_usd'] = income_layer_start_usd
    if income_layer_max_ratio is not None:
        overrides['income_layer_max_ratio'] = income_layer_max_ratio
    if profile in DCA_PROFILES:
        dca_mode = getattr(runtime_settings, 'dca_mode', None)
        dca_base_investment_usd = getattr(runtime_settings, 'dca_base_investment_usd', None)
        if dca_mode is not None:
            overrides['investment_amount_mode'] = 'fixed'
            overrides['smart_multiplier_enabled'] = dca_mode == 'smart'
        if dca_base_investment_usd is not None:
            overrides['base_investment_usd'] = dca_base_investment_usd
    if profile == 'ibit_smart_dca':
        for setting_name in ('ibit_zscore_exit_enabled', 'ibit_zscore_exit_mode', 'ibit_zscore_exit_parking_symbol', 'ibit_zscore_exit_risk_reduced_exposure', 'ibit_zscore_exit_risk_off_exposure', 'ibit_zscore_exit_allow_outside_execution_window'):
            value = getattr(runtime_settings, setting_name, None)
            if value is not None:
                overrides[setting_name] = value
    if profile == 'tqqq_growth_income':
        if INCOME_THRESHOLD_USD is not None:
            overrides['income_threshold_usd'] = INCOME_THRESHOLD_USD
        if QQQI_INCOME_RATIO is not None:
            overrides['qqqi_income_ratio'] = QQQI_INCOME_RATIO
        if DUAL_DRIVE_UNLEVERED_SYMBOL is not None:
            overrides['dual_drive_unlevered_symbol'] = DUAL_DRIVE_UNLEVERED_SYMBOL
            overrides['managed_symbols'] = build_tqqq_managed_symbols(DUAL_DRIVE_UNLEVERED_SYMBOL)
    return overrides
STRATEGY_RUNTIME = load_strategy_runtime(STRATEGY_PROFILE, runtime_settings=RUNTIME_SETTINGS, runtime_overrides=build_strategy_runtime_overrides(STRATEGY_PROFILE), logger=lambda message: print(message, flush=True))
STRATEGY_RUNTIME_CONFIG = dict(STRATEGY_RUNTIME.merged_runtime_config)
MANAGED_SYMBOLS = STRATEGY_RUNTIME.managed_symbols
BENCHMARK_SYMBOL = STRATEGY_RUNTIME.benchmark_symbol
SIGNAL_EFFECTIVE_AFTER_TRADING_DAYS = getattr(getattr(STRATEGY_RUNTIME.runtime_adapter, 'runtime_policy', None), 'signal_effective_after_trading_days', None)
AVAILABLE_INPUTS = frozenset(STRATEGY_RUNTIME.runtime_adapter.available_inputs)

def validate_config():
    missing = [v for v in ('SCHWAB_API_KEY', 'SCHWAB_APP_SECRET') if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")
    if QQQI_INCOME_RATIO is not None and (not 0.0 <= QQQI_INCOME_RATIO <= 1.0):
        raise ValueError(f'QQQI_INCOME_RATIO must be in [0,1], got {QQQI_INCOME_RATIO}')
validate_config()

def build_broker_adapters():
    return platform_entry().build_runtime_broker_adapters(managed_symbols=MANAGED_SYMBOLS, fetch_account_snapshot_fn=platform_entry().fetch_account_snapshot, fetch_quotes_fn=platform_entry().fetch_quotes, fetch_daily_price_history_fn=platform_entry().fetch_default_daily_price_history_candles, submit_equity_order_fn=platform_entry().submit_equity_order, fetch_order_status_fn=platform_entry().fetch_order_status, cash_only_execution=CASH_ONLY_EXECUTION, expected_account_hash=platform_entry().expected_account_hash_from_selector(getattr(RUNTIME_SETTINGS.runtime_target, 'account_selector', ())))

def _runtime_revision_diagnostics(**fields):
    diagnostics = dict(fields)
    runtime_revision = (os.getenv('K_REVISION') or '').strip()
    if runtime_revision:
        diagnostics['runtime_revision'] = runtime_revision
    return diagnostics

def build_strategy_adapters():
    return platform_entry().build_runtime_strategy_adapters(strategy_runtime=STRATEGY_RUNTIME, strategy_profile=STRATEGY_PROFILE, strategy_runtime_config=STRATEGY_RUNTIME_CONFIG, available_inputs=AVAILABLE_INPUTS, benchmark_symbol=BENCHMARK_SYMBOL, managed_symbols=MANAGED_SYMBOLS, signal_text_fn=signal_text, translator=t, broker_adapters=build_broker_adapters(), build_strategy_evaluation_inputs_fn=platform_entry().build_strategy_evaluation_inputs, map_strategy_decision_to_plan_fn=platform_entry().map_strategy_decision_to_plan, build_strategy_plugin_report_payload_fn=platform_entry().build_strategy_plugin_report_payload, load_configured_strategy_plugin_signals_fn=platform_entry().load_configured_strategy_plugin_signals, parse_strategy_plugin_mounts_fn=platform_entry().parse_strategy_plugin_mounts, reserved_cash_floor_usd=RUNTIME_SETTINGS.reserved_cash_floor_usd, reserved_cash_ratio=RUNTIME_SETTINGS.reserved_cash_ratio, cash_only_execution=CASH_ONLY_EXECUTION)

def _safe_haven_cash_substitute_threshold_usd() -> float:
    return float(getattr(RUNTIME_SETTINGS, 'safe_haven_cash_substitute_threshold_usd', DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD))

def build_composer(*, dry_run_only_override: bool | None=None):
    effective_dry_run_only = RUNTIME_SETTINGS.dry_run_only if dry_run_only_override is None else bool(dry_run_only_override)
    return platform_entry().build_runtime_composer(project_id=PROJECT_ID, service_name=SERVICE_NAME, secret_id=SECRET_ID, app_key=APP_KEY, app_secret=APP_SECRET, token_path=TOKEN_PATH, strategy_profile=STRATEGY_PROFILE, strategy_domain=RUNTIME_SETTINGS.strategy_domain, strategy_display_name=STRATEGY_DISPLAY_NAME, strategy_display_name_localized=strategy_display_name, notify_lang=NOTIFY_LANG, tg_token=TG_TOKEN, tg_chat_id=TG_CHAT_ID, notification_channel=_NOTIFICATION_CHANNEL, webhook_url=_NOTIFICATION_WEBHOOK_URL, managed_symbols=MANAGED_SYMBOLS, benchmark_symbol=BENCHMARK_SYMBOL, signal_effective_after_trading_days=SIGNAL_EFFECTIVE_AFTER_TRADING_DAYS, dry_run_only=effective_dry_run_only, limit_buy_premium=LIMIT_BUY_PREMIUM, limit_buy_premium_by_symbol=LIMIT_BUY_PREMIUM_BY_SYMBOL, sell_settle_delay_sec=SELL_SETTLE_DELAY_SEC, post_sell_refresh_attempts=POST_SELL_REFRESH_ATTEMPTS, post_sell_refresh_interval_sec=POST_SELL_REFRESH_INTERVAL_SEC, safe_haven_cash_substitute_threshold_usd=_safe_haven_cash_substitute_threshold_usd(), broker_adapters=build_broker_adapters(), strategy_adapters=build_strategy_adapters(), client_builder=platform_entry().get_client_from_secret, run_id_builder=platform_entry().build_run_id, event_logger=platform_entry().emit_runtime_log, report_builder=platform_entry().build_runtime_report_base, report_persister=platform_entry().persist_runtime_report, env_reader=os.getenv, sleeper=time.sleep, printer=print, runtime_target=RUNTIME_SETTINGS.runtime_target)
