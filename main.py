import os
import time
import traceback

from flask import Flask
import google.auth
import requests

from application.runtime_broker_adapters import build_runtime_broker_adapters
from application.runtime_composer import build_runtime_composer
from application.runtime_strategy_adapters import build_runtime_strategy_adapters
from application.rebalance_service import run_strategy_core as run_rebalance_cycle
from application.signal_snapshot import build_signal_snapshot
from decision_mapper import map_strategy_decision_to_plan
from entrypoints.cloud_run import is_market_open_today
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
from quant_platform_kit.common.strategy_plugins import (
    build_strategy_plugin_report_payload,
    load_configured_strategy_plugin_signals,
    parse_strategy_plugin_mounts,
)
from quant_platform_kit.strategy_contracts import build_strategy_evaluation_inputs
from runtime_config_support import load_platform_runtime_settings
from runtime_logging import build_run_id, emit_runtime_log
from strategy_runtime import load_strategy_runtime

app = Flask(__name__)


def get_project_id():
    try:
        _, project_id = google.auth.default()
        return project_id if project_id else os.getenv("GOOGLE_CLOUD_PROJECT")
    except Exception:
        return os.getenv("GOOGLE_CLOUD_PROJECT")


PROJECT_ID = get_project_id()
SERVICE_NAME = os.getenv("SERVICE_NAME") or os.getenv("K_SERVICE") or "charles-schwab-platform"
APP_KEY = os.getenv("SCHWAB_API_KEY")
APP_SECRET = os.getenv("SCHWAB_APP_SECRET")
TG_TOKEN = os.getenv("TELEGRAM_TOKEN")
TG_CHAT_ID = os.getenv("GLOBAL_TELEGRAM_CHAT_ID")
SECRET_ID = "schwab_token"
TOKEN_PATH = "/tmp/token.json"


def _optional_float_env(name: str) -> float | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return float(value)


def _optional_symbol_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return value.strip().upper()


INCOME_THRESHOLD_USD = _optional_float_env("INCOME_THRESHOLD_USD")
QQQI_INCOME_RATIO = _optional_float_env("QQQI_INCOME_RATIO")
DUAL_DRIVE_UNLEVERED_SYMBOL = _optional_symbol_env("DUAL_DRIVE_UNLEVERED_SYMBOL")

LIMIT_BUY_PREMIUM = 1.005
SELL_SETTLE_DELAY_SEC = 3
POST_SELL_REFRESH_ATTEMPTS = 5
POST_SELL_REFRESH_INTERVAL_SEC = 1
DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD = 1000.0

RUNTIME_SETTINGS = load_platform_runtime_settings()
STRATEGY_PROFILE = RUNTIME_SETTINGS.strategy_profile
STRATEGY_DISPLAY_NAME = RUNTIME_SETTINGS.strategy_display_name
NOTIFY_LANG = RUNTIME_SETTINGS.notify_lang
t = build_translator(NOTIFY_LANG)
signal_text = build_signal_text(t)
strategy_display_name = build_strategy_display_name(t)(
    STRATEGY_PROFILE,
    fallback_name=STRATEGY_DISPLAY_NAME,
)


def build_tqqq_managed_symbols(unlevered_symbol: str) -> tuple[str, ...]:
    symbol = str(unlevered_symbol or "QQQ").strip().upper()
    if not symbol:
        raise ValueError("DUAL_DRIVE_UNLEVERED_SYMBOL must be a non-empty ticker")
    if symbol in {"TQQQ", "BOXX", "SPYI", "QQQI"}:
        raise ValueError("DUAL_DRIVE_UNLEVERED_SYMBOL must not overlap another TQQQ profile sleeve")
    return ("TQQQ", symbol, "BOXX", "SPYI", "QQQI")


def build_strategy_runtime_overrides(profile: str) -> dict[str, object]:
    overrides: dict[str, object] = {}
    if profile == "tqqq_growth_income":
        if INCOME_THRESHOLD_USD is not None:
            overrides["income_threshold_usd"] = INCOME_THRESHOLD_USD
        if QQQI_INCOME_RATIO is not None:
            overrides["qqqi_income_ratio"] = QQQI_INCOME_RATIO
        if DUAL_DRIVE_UNLEVERED_SYMBOL is not None:
            overrides["dual_drive_unlevered_symbol"] = DUAL_DRIVE_UNLEVERED_SYMBOL
            overrides["managed_symbols"] = build_tqqq_managed_symbols(DUAL_DRIVE_UNLEVERED_SYMBOL)
    return overrides


STRATEGY_RUNTIME = load_strategy_runtime(
    STRATEGY_PROFILE,
    runtime_settings=RUNTIME_SETTINGS,
    runtime_overrides=build_strategy_runtime_overrides(STRATEGY_PROFILE),
    logger=lambda message: print(message, flush=True),
)
STRATEGY_RUNTIME_CONFIG = dict(STRATEGY_RUNTIME.merged_runtime_config)
MANAGED_SYMBOLS = STRATEGY_RUNTIME.managed_symbols
BENCHMARK_SYMBOL = STRATEGY_RUNTIME.benchmark_symbol
SIGNAL_EFFECTIVE_AFTER_TRADING_DAYS = getattr(
    getattr(STRATEGY_RUNTIME.runtime_adapter, "runtime_policy", None),
    "signal_effective_after_trading_days",
    None,
)
AVAILABLE_INPUTS = frozenset(STRATEGY_RUNTIME.runtime_adapter.available_inputs)


def validate_config():
    missing = [v for v in ("SCHWAB_API_KEY", "SCHWAB_APP_SECRET") if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"Missing required env vars: {', '.join(missing)}")
    if QQQI_INCOME_RATIO is not None and not (0.0 <= QQQI_INCOME_RATIO <= 1.0):
        raise ValueError(f"QQQI_INCOME_RATIO must be in [0,1], got {QQQI_INCOME_RATIO}")


validate_config()


def build_broker_adapters():
    return build_runtime_broker_adapters(
        managed_symbols=MANAGED_SYMBOLS,
        fetch_account_snapshot_fn=fetch_account_snapshot,
        fetch_quotes_fn=fetch_quotes,
        fetch_daily_price_history_fn=fetch_default_daily_price_history_candles,
        submit_equity_order_fn=submit_equity_order,
    )


def build_strategy_adapters():
    return build_runtime_strategy_adapters(
        strategy_runtime=STRATEGY_RUNTIME,
        strategy_profile=STRATEGY_PROFILE,
        strategy_runtime_config=STRATEGY_RUNTIME_CONFIG,
        available_inputs=AVAILABLE_INPUTS,
        benchmark_symbol=BENCHMARK_SYMBOL,
        managed_symbols=MANAGED_SYMBOLS,
        signal_text_fn=signal_text,
        translator=t,
        broker_adapters=build_broker_adapters(),
        build_strategy_evaluation_inputs_fn=build_strategy_evaluation_inputs,
        map_strategy_decision_to_plan_fn=map_strategy_decision_to_plan,
        build_strategy_plugin_report_payload_fn=build_strategy_plugin_report_payload,
        load_configured_strategy_plugin_signals_fn=load_configured_strategy_plugin_signals,
        parse_strategy_plugin_mounts_fn=parse_strategy_plugin_mounts,
        reserved_cash_floor_usd=RUNTIME_SETTINGS.reserved_cash_floor_usd,
        reserved_cash_ratio=RUNTIME_SETTINGS.reserved_cash_ratio,
    )


def _safe_haven_cash_substitute_threshold_usd() -> float:
    return float(
        getattr(
            RUNTIME_SETTINGS,
            "safe_haven_cash_substitute_threshold_usd",
            DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD,
        )
    )


def build_composer(*, dry_run_only_override: bool | None = None):
    effective_dry_run_only = RUNTIME_SETTINGS.dry_run_only if dry_run_only_override is None else bool(dry_run_only_override)
    return build_runtime_composer(
        project_id=PROJECT_ID,
        service_name=SERVICE_NAME,
        secret_id=SECRET_ID,
        app_key=APP_KEY,
        app_secret=APP_SECRET,
        token_path=TOKEN_PATH,
        strategy_profile=STRATEGY_PROFILE,
        strategy_domain=RUNTIME_SETTINGS.strategy_domain,
        strategy_display_name=STRATEGY_DISPLAY_NAME,
        strategy_display_name_localized=strategy_display_name,
        notify_lang=NOTIFY_LANG,
        tg_token=TG_TOKEN,
        tg_chat_id=TG_CHAT_ID,
        managed_symbols=MANAGED_SYMBOLS,
        benchmark_symbol=BENCHMARK_SYMBOL,
        signal_effective_after_trading_days=SIGNAL_EFFECTIVE_AFTER_TRADING_DAYS,
        dry_run_only=effective_dry_run_only,
        limit_buy_premium=LIMIT_BUY_PREMIUM,
        sell_settle_delay_sec=SELL_SETTLE_DELAY_SEC,
        post_sell_refresh_attempts=POST_SELL_REFRESH_ATTEMPTS,
        post_sell_refresh_interval_sec=POST_SELL_REFRESH_INTERVAL_SEC,
        safe_haven_cash_substitute_threshold_usd=_safe_haven_cash_substitute_threshold_usd(),
        broker_adapters=build_broker_adapters(),
        strategy_adapters=build_strategy_adapters(),
        client_builder=get_client_from_secret,
        run_id_builder=build_run_id,
        event_logger=emit_runtime_log,
        report_builder=build_runtime_report_base,
        report_persister=persist_runtime_report,
        env_reader=os.getenv,
        sleeper=time.sleep,
        printer=print,
        runtime_target=RUNTIME_SETTINGS.runtime_target,
    )


def send_tg_message(message):
    return build_composer().send_tg_message(message)


def publish_notification(*, detailed_text, compact_text):
    build_composer().build_notification_adapters().publish_cycle_notification(
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
    error_text = f"{type(exc).__name__}: {exc}"
    if len(error_text) > 1200:
        error_text = error_text[:1197] + "..."
    if str(NOTIFY_LANG or "").strip().lower().startswith("zh"):
        return "\n".join(
            (
                "Schwab 策略运行失败",
                f"服务: {SERVICE_NAME}",
                f"版本: {os.getenv('K_REVISION') or '<unknown>'}",
                f"路由: {route_label}",
                f"策略: {STRATEGY_PROFILE}",
                f"错误: {error_text}",
            )
        )
    return "\n".join(
        (
            "Schwab strategy run failed",
            f"service: {SERVICE_NAME}",
            f"revision: {os.getenv('K_REVISION') or '<unknown>'}",
            f"route: {route_label}",
            f"strategy: {STRATEGY_PROFILE}",
            f"error: {error_text}",
        )
    )


def _notify_runtime_error(exc: Exception, *, route_label: str) -> bool:
    targets = _runtime_error_notification_targets()
    if not targets:
        print("Schwab runtime error notification skipped: no Telegram target configured.", flush=True)
        return False
    message = _runtime_error_notification_message(exc, route_label=route_label)
    for token, chat_id in targets:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message},
                timeout=15,
            )
        except Exception as send_exc:
            print(f"Schwab runtime error Telegram send failed: {send_exc}", flush=True)
    return True


def _publish_runtime_failure_notification(*, detailed_text: str, compact_text: str, exc: Exception) -> bool:
    try:
        publish_notification(detailed_text=detailed_text, compact_text=compact_text)
        return True
    except Exception as notification_exc:
        print(f"Schwab runtime error notification fallback: {notification_exc}", flush=True)
        return _notify_runtime_error(exc, route_label="strategy_cycle")


def _handle_route_runtime_error(exc: Exception, *, route_label: str):
    print(f"Schwab route failed before strategy-cycle handling: {type(exc).__name__}: {exc}", flush=True)
    traceback.print_exc()
    _notify_runtime_error(exc, route_label=route_label)
    return "Error", 500


def _route_with_runtime_error_fallback(handler, *args, route_label: str, **kwargs):
    try:
        return handler(*args, **kwargs)
    except Exception as exc:
        return _handle_route_runtime_error(exc, route_label=route_label)


def log_runtime_event(log_context, event, **fields):
    return build_composer().build_reporting_adapters().log_event(log_context, event, **fields)


def build_execution_report(log_context, *, dry_run_only_override: bool | None = None):
    return build_composer(dry_run_only_override=dry_run_only_override).build_reporting_adapters().build_report(log_context)


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
        gcp_project_id=PROJECT_ID,
    )


def build_strategy_plugin_alert_context_label() -> str:
    return build_alert_context_label(
        platform_id="schwab",
        strategy_profile=STRATEGY_PROFILE,
        service_name=SERVICE_NAME,
        runtime_target=RUNTIME_SETTINGS.runtime_target,
    )


def publish_strategy_plugin_alerts(signals, *, report=None):
    result = dispatch_strategy_plugin_alerts(
        signals,
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


def _signal_diagnostics_from_result(result) -> dict[str, object]:
    execution = dict(getattr(result, "execution", {}) or {})
    allocation = dict(getattr(result, "allocation", {}) or {})
    diagnostics: dict[str, object] = {}
    for field_name in (
        "signal_display",
        "status_display",
        "benchmark_symbol",
        "benchmark_price",
        "long_trend_value",
        "exit_line",
        "active_risk_asset",
        "allocation_mode",
        "trend_entry_buffer",
        "trend_mid_buffer",
        "trend_exit_buffer",
        "blend_tier",
        "base_blend_tier",
        "overlay_trigger_count",
        "overlay_trigger_reasons",
        "trend_symbol",
        "trend_price",
        "trend_ma",
        "trend_ma20",
        "trend_ma20_slope",
        "trend_rsi14",
        "trend_rsi14_dynamic_threshold",
        "trend_rsi14_effective_threshold",
        "trend_bb_upper",
        "blend_gate_volatility_delever_symbol",
        "blend_gate_volatility_delever_window",
        "blend_gate_volatility_delever_threshold_mode",
        "blend_gate_volatility_delever_threshold",
        "blend_gate_volatility_delever_dynamic_threshold",
        "blend_gate_volatility_delever_dynamic_sample_count",
        "blend_gate_volatility_delever_dynamic_lookback",
        "blend_gate_volatility_delever_dynamic_percentile",
        "blend_gate_volatility_delever_dynamic_min_periods",
        "blend_gate_volatility_delever_dynamic_floor",
        "blend_gate_volatility_delever_dynamic_cap",
        "blend_gate_volatility_delever_metric",
        "blend_gate_volatility_delever_triggered",
    ):
        value = execution.get(field_name)
        if value is None or value == "":
            continue
        diagnostics[field_name] = value
    if allocation.get("targets"):
        diagnostics["targets"] = dict(allocation["targets"])
    return diagnostics


def _has_signal_snapshot_details(snapshot: dict[str, object]) -> bool:
    return any(
        snapshot.get(field_name)
        for field_name in (
            "signal_as_of",
            "market_date",
            "latest_price_source",
            "target_weights",
            "target_values",
            "indicators",
            "signal",
            "status",
        )
    )


def _summarize_cycle_result_for_report(result, *, dry_run: bool) -> dict[str, object]:
    trade_logs = tuple(getattr(result, "trade_logs", ()) or ())
    order_events_count = len(trade_logs)
    orders_previewed_count = order_events_count if dry_run else 0
    return {
        "action_done": bool(order_events_count),
        "order_events_count": order_events_count,
        "orders_previewed_count": orders_previewed_count,
        "orders_skipped_count": 0,
        "notes_count": 0,
        "dry_run_order_preview_available": bool(dry_run and orders_previewed_count > 0),
        "execution_status": "executed" if order_events_count else "no_action",
    }


def persist_execution_report(report, *, dry_run_only_override: bool | None = None):
    return build_composer(dry_run_only_override=dry_run_only_override).build_reporting_adapters().persist_execution_report(report)


def fetch_reference_history(market_data_port):
    return build_strategy_adapters().fetch_reference_history(market_data_port)


def build_price_history(market_data_port, symbol: str):
    return build_broker_adapters().build_price_history(market_data_port, symbol)


def build_market_history_loader(market_data_port):
    return build_broker_adapters().build_market_history_loader(market_data_port)


def fetch_managed_snapshot(client):
    return build_broker_adapters().fetch_managed_snapshot(client)


def build_market_data_port(client):
    return build_broker_adapters().build_market_data_port(client)


def build_semiconductor_indicators(market_data_source, *, trend_window: int) -> dict[str, dict[str, float]]:
    return build_strategy_adapters().build_semiconductor_indicators(
        market_data_source,
        trend_window=trend_window,
    )


def build_account_state_from_snapshot(snapshot) -> dict[str, object]:
    return build_strategy_adapters().build_account_state_from_snapshot(snapshot)


def resolve_rebalance_plan(*, qqq_history, snapshot):
    return build_strategy_adapters().resolve_rebalance_plan(
        qqq_history=qqq_history,
        snapshot=snapshot,
    )


def run_strategy_core(
    c,
    now_ny,
    *,
    strategy_plugin_signals=(),
    strategy_plugin_error: str | None = None,
    dry_run_only_override: bool | None = None,
):
    composer = build_composer(dry_run_only_override=dry_run_only_override)
    return run_rebalance_cycle(
        c,
        now_ny,
        runtime=composer.build_rebalance_runtime(
            c,
            silent_cycle_notifications=bool(dry_run_only_override),
        ),
        config=composer.build_rebalance_config(
            strategy_plugin_signals=strategy_plugin_signals,
            strategy_plugin_error=strategy_plugin_error,
        ),
    )


def _handle_schwab_cycle(*, dry_run_only_override: bool | None = None, response_body: str = "OK"):
    composer = build_composer(dry_run_only_override=dry_run_only_override)
    reporting_adapters = composer.build_reporting_adapters()
    log_context = reporting_adapters.build_log_context()
    report = build_execution_report(log_context, dry_run_only_override=dry_run_only_override)
    strategy_plugin_signals, strategy_plugin_error = load_strategy_plugin_signals()
    attach_strategy_plugin_report(
        report,
        signals=strategy_plugin_signals,
        error=strategy_plugin_error,
    )
    try:
        log_runtime_event(
            log_context,
            "strategy_cycle_received",
            message="Received strategy precheck request" if dry_run_only_override else "Received strategy execution request",
            execution_window="precheck" if dry_run_only_override else "execution",
        )
        client = composer.build_client()
        if not is_market_open_today():
            log_runtime_event(
                log_context,
                "market_closed",
                message="Market closed; skip strategy execution",
                execution_window="precheck" if dry_run_only_override else "execution",
            )
            finalize_runtime_report(
                report,
                status="skipped",
                diagnostics={"skip_reason": "market_closed"},
            )
            return "Market Closed", 200
        log_runtime_event(
            log_context,
            "strategy_cycle_started",
            message="Starting strategy precheck" if dry_run_only_override else "Starting strategy execution",
            execution_window="precheck" if dry_run_only_override else "execution",
        )
        if dry_run_only_override is None:
            publish_strategy_plugin_alerts(strategy_plugin_signals, report=report)
        execution_result = run_strategy_core(
            client,
            None,
            strategy_plugin_signals=strategy_plugin_signals,
            strategy_plugin_error=strategy_plugin_error,
            dry_run_only_override=dry_run_only_override,
        )
        signal_diagnostics = _signal_diagnostics_from_result(execution_result)
        execution_payload = getattr(execution_result, "execution", None)
        signal_snapshot = (
            dict(execution_payload.get("signal_snapshot") or {})
            if isinstance(execution_payload, dict)
            else {}
        )
        if not signal_snapshot:
            signal_snapshot = build_signal_snapshot(
                platform="schwab",
                strategy_profile=STRATEGY_PROFILE,
                diagnostics=signal_diagnostics,
                execution=execution_payload,
                allocation=getattr(execution_result, "allocation", None),
            )
        if signal_diagnostics:
            log_runtime_event(
                log_context,
                "strategy_signal_diagnostics",
                message="Strategy signal diagnostics",
                execution_window="precheck" if dry_run_only_override else "execution",
                **signal_diagnostics,
            )
        has_signal_snapshot = _has_signal_snapshot_details(signal_snapshot)
        if has_signal_snapshot:
            log_runtime_event(
                log_context,
                "strategy_signal_snapshot",
                message="Strategy signal snapshot",
                execution_window="precheck" if dry_run_only_override else "execution",
                **signal_snapshot,
            )
        execution_summary = _summarize_cycle_result_for_report(
            execution_result,
            dry_run=bool(report.get("dry_run")),
        )
        finalize_runtime_report(
            report,
            status="ok",
            summary=execution_summary,
            diagnostics={
                "signal": signal_diagnostics,
                **({"signal_snapshot": signal_snapshot} if has_signal_snapshot else {}),
            },
        )
        log_runtime_event(
            log_context,
            "strategy_cycle_completed",
            message="Strategy precheck completed" if dry_run_only_override else "Strategy execution completed",
            execution_window="precheck" if dry_run_only_override else "execution",
        )
        return response_body, 200
    except Exception as exc:
        append_runtime_report_error(
            report,
            stage="strategy_cycle",
            message=str(exc),
            error_type=type(exc).__name__,
        )
        finalize_runtime_report(report, status="error")
        log_runtime_event(
            log_context,
            "strategy_cycle_failed",
            message="Strategy execution failed",
            severity="ERROR",
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        error_message = f"{t('error_header')}\n{traceback.format_exc()}"
        _publish_runtime_failure_notification(
            detailed_text=error_message,
            compact_text=error_message,
            exc=exc,
        )
        return "Error", 500
    finally:
        try:
            if dry_run_only_override is None:
                report_path = persist_execution_report(report)
            else:
                report_path = persist_execution_report(report, dry_run_only_override=dry_run_only_override)
            print(f"execution_report {report_path}", flush=True)
        except Exception as persist_exc:
            print(f"failed to persist execution report: {persist_exc}", flush=True)


def _handle_schwab_probe(*, response_body: str = "Probe OK"):
    composer = None
    log_context = None
    report = None
    try:
        composer = build_composer()
        reporting_adapters = composer.build_reporting_adapters()
        log_context = reporting_adapters.build_log_context()
        report = build_execution_report(log_context)
        strategy_plugin_signals, strategy_plugin_error = load_strategy_plugin_signals()
        attach_strategy_plugin_report(
            report,
            signals=strategy_plugin_signals,
            error=strategy_plugin_error,
        )
        log_runtime_event(
            log_context,
            "health_probe_received",
            message="Received health probe request",
            execution_window="probe",
        )
        client = composer.build_client()
        snapshot = fetch_account_snapshot(client, strategy_symbols=MANAGED_SYMBOLS)
        finalize_runtime_report(
            report,
            status="ok",
            summary={
                "buying_power": float(snapshot.buying_power or 0.0),
                "total_equity": float(snapshot.total_equity or 0.0),
            },
        )
        log_runtime_event(
            log_context,
            "health_probe_completed",
            message="Health probe completed",
            execution_window="probe",
            buying_power=float(snapshot.buying_power or 0.0),
            total_equity=float(snapshot.total_equity or 0.0),
        )
        return response_body, 200
    except Exception as exc:
        if report is not None:
            append_runtime_report_error(
                report,
                stage="health_probe",
                message=str(exc),
                error_type=type(exc).__name__,
            )
            finalize_runtime_report(report, status="error")
        if log_context is not None:
            log_runtime_event(
                log_context,
                "health_probe_failed",
                message="Health probe failed",
                severity="ERROR",
                execution_window="probe",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
        error_message = f"{t('health_probe_title')}\n{t('health_probe_error_prefix')}{traceback.format_exc()}"
        if composer is not None:
            composer.build_notification_adapters().publish_cycle_notification(
                detailed_text=error_message,
                compact_text=error_message,
            )
        else:
            print(error_message, flush=True)
        return "Error", 500
    finally:
        try:
            if report is not None:
                report_path = persist_execution_report(report)
                print(f"execution_report {report_path}", flush=True)
        except Exception as persist_exc:
            print(f"failed to persist execution report: {persist_exc}", flush=True)


@app.route("/", methods=["POST", "GET"])
def handle_schwab():
    return _route_with_runtime_error_fallback(
        _handle_schwab_cycle,
        route_label="POST /",
    )


@app.route("/precheck", methods=["POST", "GET"])
def handle_schwab_precheck():
    return _route_with_runtime_error_fallback(
        _handle_schwab_cycle,
        dry_run_only_override=True,
        response_body="Precheck OK",
        route_label="POST /precheck",
    )


@app.route("/probe", methods=["POST", "GET"])
def handle_schwab_probe():
    return _route_with_runtime_error_fallback(
        _handle_schwab_probe,
        route_label="POST /probe",
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
