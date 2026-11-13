"""Flask HTTP application and route handlers."""


# ruff: noqa: F403, F405
from __future__ import annotations

import importlib
import json
import os

from flask import Flask

import notifications_runtime as nr
import runtime_wiring as rw
import strategy_plugins_runtime as spr
from application.execution_receipt_adapter import (
    attach_cycle_execution_receipt,
    attach_terminal_fallback_execution_receipt,
)
from application import signal_provenance
from application.broker_reconciliation import collect_read_only_reconciliation_observations
from application.paper_execution_command_consumer import consume_due_paper_execution_commands
from application.rebalance_service import run_strategy_core as run_rebalance_cycle
from application.runtime_report_summary import execution_report_status, summarize_execution_cycle_result
from entrypoints.cloud_run import is_market_open_now
from quant_platform_kit.common.platform_runner import dispatch_due_monitors, load_monitor_targets
from quant_platform_kit.common.runtime_reports import (
    append_runtime_report_error,
    finalize_runtime_report,
)
from quant_platform_kit.schwab import fetch_account_snapshot

app = Flask(__name__)

READ_ONLY_BROKER_RECONCILIATION_COLLECTOR = collect_read_only_reconciliation_observations
# Optional injectable final RiskEngine assessment provider for C4 shadow.
# Default None → explicit PARKED with "assessment missing"; never forge APPROVE.
FINAL_RISK_ASSESSMENT_PROVIDER = None

from runtime_wiring import *  # noqa: F403, F401


def _log_runtime_event(log_context, event, **fields):
    return rw.build_composer().build_reporting_adapters().log_event(log_context, event, **fields)


def _build_execution_report(log_context, *, dry_run_only_override: bool | None = None):
    return rw.build_composer(dry_run_only_override=dry_run_only_override).build_reporting_adapters().build_report(log_context)


def _persist_execution_report(report, *, dry_run_only_override: bool | None = None):
    return rw.build_composer(dry_run_only_override=dry_run_only_override).build_reporting_adapters().persist_report(report)


def _fetch_reference_history(market_data_port):
    return rw.build_strategy_adapters().fetch_reference_history(market_data_port)


def _build_price_history(market_data_port, symbol: str):
    return rw.build_strategy_adapters().build_price_history(market_data_port, symbol)


def _build_market_history_loader(market_data_port):
    return rw.build_strategy_adapters().build_market_history_loader(market_data_port)


def _fetch_managed_snapshot(client):
    return rw.build_broker_adapters().fetch_managed_snapshot(client)


def _build_market_data_port(client):
    return rw.build_broker_adapters().build_market_data_port(client)


def _build_semiconductor_indicators(market_data_source, *, trend_window: int):
    return rw.build_strategy_adapters().build_semiconductor_indicators(market_data_source, trend_window=trend_window)


def _build_account_state_from_snapshot(snapshot):
    return rw.build_strategy_adapters().build_account_state_from_snapshot(snapshot)


def _resolve_rebalance_plan(*, qqq_history, snapshot):
    return rw.build_strategy_adapters().resolve_rebalance_plan(qqq_history=qqq_history, snapshot=snapshot)


def _run_strategy_core(c, now_ny, **kwargs):
    composer = rw.build_composer(dry_run_only_override=kwargs.get("dry_run_only_override"))
    return run_rebalance_cycle(
        c,
        now_ny,
        runtime=composer.build_rebalance_runtime(c, silent_cycle_notifications=bool(kwargs.get("dry_run_only_override"))),
        config=composer.build_rebalance_config(
            strategy_plugin_signals=kwargs.get("strategy_plugin_signals", ()),
            strategy_plugin_error=kwargs.get("strategy_plugin_error"),
            cash_only_execution=rw.CASH_ONLY_EXECUTION,
        ),
    )


def _log_runtime_event(log_context, event, **fields):
    return rw.build_composer().build_reporting_adapters().log_event(log_context, event, **fields)


def _build_execution_report(log_context, *, dry_run_only_override: bool | None = None):
    return rw.build_composer(dry_run_only_override=dry_run_only_override).build_reporting_adapters().build_report(log_context)


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


def _summarize_cycle_result_for_report(result, *, dry_run: bool, existing_summary=None) -> dict[str, object]:
    return summarize_execution_cycle_result(result, dry_run=dry_run, existing_summary=existing_summary)


def _persist_execution_report(report, *, dry_run_only_override: bool | None = None):
    return rw.build_composer(dry_run_only_override=dry_run_only_override).build_reporting_adapters()._persist_execution_report(report)


def _fetch_reference_history(market_data_port):
    return rw.build_strategy_adapters()._fetch_reference_history(market_data_port)


def _build_price_history(market_data_port, symbol: str):
    return rw.build_broker_adapters()._build_price_history(market_data_port, symbol)


def _build_market_history_loader(market_data_port):
    return rw.build_broker_adapters()._build_market_history_loader(market_data_port)


def _fetch_managed_snapshot(client):
    return rw.build_broker_adapters()._fetch_managed_snapshot(client)


def _build_market_data_port(client):
    return rw.build_broker_adapters()._build_market_data_port(client)


def _build_semiconductor_indicators(market_data_source, *, trend_window: int) -> dict[str, dict[str, float]]:
    return rw.build_strategy_adapters()._build_semiconductor_indicators(
        market_data_source,
        trend_window=trend_window,
    )


def _build_account_state_from_snapshot(snapshot) -> dict[str, object]:
    return rw.build_strategy_adapters()._build_account_state_from_snapshot(snapshot)


def _resolve_rebalance_plan(*, qqq_history, snapshot):
    return rw.build_strategy_adapters()._resolve_rebalance_plan(
        qqq_history=qqq_history,
        snapshot=snapshot,
    )


def _run_strategy_core(
    c,
    now_ny,
    *,
    strategy_plugin_signals=(),
    strategy_plugin_error: str | None = None,
    dry_run_only_override: bool | None = None,
):
    composer = rw.build_composer(dry_run_only_override=dry_run_only_override)
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
            cash_only_execution=CASH_ONLY_EXECUTION,
        ),
    )


def _schwab_force_run_env() -> bool:
    return (os.getenv("SCHWAB_FORCE_RUN") or "").strip().lower() == "true"


def _schwab_market_open_now():
    market_open = is_market_open_now(calendar_name="NASDAQ", timezone_name="America/New_York")
    if isinstance(market_open, tuple):
        return market_open
    return market_open, None


def attach_signal_data_provenance(report) -> None:
    """Attach this run's non-sensitive signal data provenance; never raises."""
    try:
        provenance = signal_provenance.snapshot()
        if provenance and isinstance(report, dict):
            diagnostics = report.get("diagnostics")
            if not isinstance(diagnostics, dict):
                diagnostics = {}
                report["diagnostics"] = diagnostics
            diagnostics["signal_data_provenance"] = provenance
    except Exception:
        return


def _handle_schwab_cycle(*, dry_run_only_override: bool | None = None, response_body: str = "OK"):
    if dry_run_only_override is None and not getattr(RUNTIME_SETTINGS, "runtime_target_enabled", True):
        from notifications import unified
        if unified.enabled():
            try:
                reporting = rw.build_composer().build_reporting_adapters()
                report = _build_execution_report(reporting.build_log_context())
                finalize_runtime_report(
                    report, status="skipped",
                    diagnostics=rw._runtime_revision_diagnostics(skip_reason="runtime_execution_not_admitted"),
                )
                if _persist_execution_report(report):
                    unified.persist_completed_report(report)
            except Exception:
                print("unified_notification_failed reason=non_execution_report_unconfirmed coverage=unknown", flush=True)
        return "Runtime Target Disabled", 200
    composer = rw.build_composer(dry_run_only_override=dry_run_only_override)
    reporting_adapters = composer.build_reporting_adapters()
    log_context = reporting_adapters.build_log_context()
    report = _build_execution_report(log_context, dry_run_only_override=dry_run_only_override)
    signal_provenance.reset()
    strategy_plugin_signals, strategy_plugin_error = spr.load_strategy_plugin_signals()
    spr.attach_strategy_plugin_report(
        report,
        signals=strategy_plugin_signals,
        error=strategy_plugin_error,
    )
    execution_window = "dry_run" if dry_run_only_override else "execution"
    try:
        _log_runtime_event(
            log_context,
            "strategy_cycle_received",
            message="Received strategy dry-run request" if dry_run_only_override else "Received strategy execution request",
            execution_window=execution_window,
        )
        client = composer.build_client()
        market_open, market_hours_error = _schwab_market_open_now()
        if market_hours_error is not None:
            _log_runtime_event(
                log_context,
                "market_hours_check_failed",
                message="Market hours check failed",
                execution_window=execution_window,
                error_message=str(market_hours_error),
            )
        if not market_open and not _schwab_force_run_env():
            _log_runtime_event(
                log_context,
                "market_closed",
                message="Market closed; skip strategy execution",
                execution_window=execution_window,
            )
            finalize_runtime_report(
                report,
                status="skipped",
                diagnostics=rw._runtime_revision_diagnostics(skip_reason="market_closed"),
            )
            return "Market Closed", 200
        if _schwab_force_run_env() and not market_open:
            _log_runtime_event(
                log_context,
                "market_hours_bypassed",
                message="Market hours bypassed for strategy execution",
                execution_window=execution_window,
            )
        unsupported_reason = dca_execution_unsupported_reason(STRATEGY_PROFILE)
        if unsupported_reason is not None:
            _log_runtime_event(
                log_context,
                "strategy_execution_unsupported",
                message="Strategy requires fractional-share execution; skip",
                execution_window=execution_window,
                skip_reason=unsupported_reason,
                strategy_profile=STRATEGY_PROFILE,
            )
            finalize_runtime_report(
                report,
                status="skipped",
                diagnostics=rw._runtime_revision_diagnostics(skip_reason=unsupported_reason),
            )
            return "Unsupported Strategy", 200
        _log_runtime_event(
            log_context,
            "strategy_cycle_started",
            message="Starting strategy dry-run" if dry_run_only_override else "Starting strategy execution",
            execution_window=execution_window,
        )
        if dry_run_only_override is None:
            spr.publish_strategy_plugin_alerts(strategy_plugin_signals, report=report)
        from notifications.unified import notification_context

        with notification_context(report):
            execution_result = _run_strategy_core(
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
            _log_runtime_event(
                log_context,
                "strategy_signal_diagnostics",
                message="Strategy signal diagnostics",
                execution_window=execution_window,
                **signal_diagnostics,
            )
        has_signal_snapshot = _has_signal_snapshot_details(signal_snapshot)
        if has_signal_snapshot:
            _log_runtime_event(
                log_context,
                "strategy_signal_snapshot",
                message="Strategy signal snapshot",
                execution_window=execution_window,
                **signal_snapshot,
            )
        execution_summary = _summarize_cycle_result_for_report(
            execution_result,
            dry_run=bool(report.get("dry_run")),
            existing_summary=report.get("summary"),
        )
        account_observation = getattr(execution_result, "account_observation", None)
        if isinstance(account_observation, dict):
            declared_observation = declare_net_assets_currency(
                account_observation,
                net_assets_currency=os.getenv("SCHWAB_NET_ASSETS_CURRENCY"),
            )
            if declared_observation is not None:
                cash_declared_observation = declare_cash_balance_currency(
                    declared_observation,
                    cash_currency=os.getenv("SCHWAB_CASH_CURRENCY"),
                )
                if cash_declared_observation is not None:
                    execution_summary["account_observation"] = cash_declared_observation
        try:
            attach_cycle_execution_receipt(report, execution_result)
        except ValueError:
            # A legacy or test report may lack an attested release. Keep the
            # execution result intact and let the control plane show missing
            # evidence instead of changing a completed cycle into an error.
            pass
        from application.runtime_report_summary import execution_report_status
        finalize_runtime_report(
            report,
            status=execution_report_status(execution_summary),
            summary=execution_summary,
            diagnostics=rw._runtime_revision_diagnostics(
                signal=signal_diagnostics,
                **({"signal_snapshot": signal_snapshot} if has_signal_snapshot else {}),
            ),
        )
        _log_runtime_event(
            log_context,
            "strategy_cycle_completed",
            message="Strategy dry-run completed" if dry_run_only_override else "Strategy execution completed",
            execution_window=execution_window,
        )
        return response_body, 200
    except Exception as exc:
        append_runtime_report_error(
            report,
            stage="strategy_cycle",
            message="strategy_cycle_failed",
            error_type=type(exc).__name__,
        )
        finalize_runtime_report(
            report,
            status="error",
            diagnostics=rw._runtime_revision_diagnostics(),
        )
        _log_runtime_event(
            log_context,
            "strategy_cycle_failed",
            message=t("runtime_failure_log"),
            severity="ERROR",
            error_type=type(exc).__name__,
            error_message="strategy_cycle_failed",
        )
        error_message = nr._runtime_error_notification_message(exc, route_label="strategy_cycle")
        from notifications.unified import notification_context

        with notification_context(report):
            nr._publish_runtime_failure_notification(
                detailed_text=error_message,
                compact_text=error_message,
                exc=exc,
            )
        return "Error", 500
    finally:
        attach_signal_data_provenance(report)
        if "execution_receipt" not in report:
            try:
                attach_terminal_fallback_execution_receipt(report)
            except ValueError:
                # An unattested legacy target remains evidence-missing; the
                # report-only path must not alter execution behavior.
                pass
        try:
            if dry_run_only_override is None:
                report_path = _persist_execution_report(report)
            else:
                report_path = _persist_execution_report(report, dry_run_only_override=dry_run_only_override)
            print(f"execution_report {report_path}", flush=True)
            from notifications.unified import persist_completed_report
            coverage_confirmed = persist_completed_report(report)
            if nr._report_requires_digest_coverage(report) and not coverage_confirmed:
                nr._notify_digest_coverage_failure()
        except Exception as persist_exc:
            print(f"failed to persist execution report: {persist_exc}", flush=True)
            if nr._report_requires_digest_coverage(report):
                nr._notify_digest_coverage_failure()


def _paper_command_consumer_runtime_is_isolated() -> bool:
    runtime_target = getattr(RUNTIME_SETTINGS, "runtime_target", None)
    return bool(
        not getattr(RUNTIME_SETTINGS, "runtime_target_enabled", True)
        and getattr(RUNTIME_SETTINGS, "dry_run_only", False)
        and CASH_ONLY_EXECUTION
        and str(getattr(runtime_target, "execution_mode", "") or "").strip().lower() == "paper"
    )


def _paper_command_consumer_session_date() -> str:
    return datetime.now(ZoneInfo("America/New_York")).date().isoformat()


def _paper_command_consumer_binding() -> dict[str, str] | None:
    runtime_target = getattr(RUNTIME_SETTINGS, "runtime_target", None)
    account_scope = str(getattr(runtime_target, "account_scope", "") or "").strip()
    if not account_scope:
        return None
    return {
        "platform": "schwab",
        "account_scope": account_scope,
        "strategy_profile": STRATEGY_PROFILE,
    }


def _handle_paper_execution_command_consumer():
    """Run the isolated Schwab paper consumer without constructing an order port."""

    if not _paper_command_consumer_runtime_is_isolated():
        raise RuntimeError(
            "paper command consumer requires a disabled runtime target with PAPER dry-run settings"
        )
    if not getattr(RUNTIME_SETTINGS, "paper_execution_command_consumer_enabled", False):
        raise RuntimeError("paper command consumer is not enabled")

    composer = rw.build_composer()
    reporting_adapters = composer.build_reporting_adapters()
    log_context, report = reporting_adapters.start_run()
    expected_release = getattr(getattr(RUNTIME_SETTINGS, "runtime_target", None), "strategy_release", None)
    runtime_release_receipt = (
        build_runtime_loaded_receipt(strategy_release=expected_release)
        if expected_release is not None
        else None
    )
    client_box: dict[str, object] = {}
    market_data_box: dict[str, object] = {}

    def _client():
        if "client" not in client_box:
            client_box["client"] = composer.build_client()
        return client_box["client"]

    def _portfolio_loader():
        return composer.broker_adapters.build_portfolio_port(_client()).get_portfolio_snapshot()

    def _market_data_port_loader():
        if "market_data_port" not in market_data_box:
            market_data_box["market_data_port"] = composer.broker_adapters._build_market_data_port(_client())
        return market_data_box["market_data_port"]

    try:
        reporting_adapters.log_event(
            log_context,
            "paper_execution_command_consumer_started",
            message="Starting isolated Schwab paper execution command consumer",
        )
        result = consume_due_paper_execution_commands(
            store=build_execution_command_store_from_env(
                platform_env_prefix="SCHWAB",
                env_reader=os.getenv,
                project_id=PROJECT_ID,
            ),
            as_of_session=_paper_command_consumer_session_date(),
            claimant=str(os.getenv("K_SERVICE") or "schwab-paper-command-consumer"),
            portfolio_loader=_portfolio_loader,
            market_data_port_loader=_market_data_port_loader,
            managed_symbols=MANAGED_SYMBOLS,
            runtime_release_receipt=runtime_release_receipt,
            expected_strategy_release=expected_release,
            expected_command_binding=_paper_command_consumer_binding(),
        )
        finalize_runtime_report(
            report,
            status="ok" if result.get("status") == "ok" else "skipped",
            summary={"paper_execution_command_consumer": result},
        )
        reporting_adapters.log_event(
            log_context,
            "paper_execution_command_consumer_completed",
            message="Isolated Schwab paper execution command consumer completed",
            result_status=result.get("status"),
            commands_count=len(tuple(result.get("commands") or ())),
        )
        return "Paper Command Consumer OK", 200
    except Exception as exc:
        append_runtime_report_error(
            report,
            stage="paper_execution_command_consumer",
            message=str(exc),
            error_type=type(exc).__name__,
        )
        finalize_runtime_report(report, status="error")
        reporting_adapters.log_event(
            log_context,
            "paper_execution_command_consumer_failed",
            message="Isolated Schwab paper execution command consumer failed",
            severity="ERROR",
            error_type=type(exc).__name__,
        )
        return "Paper Command Consumer Error", 500
    finally:
        try:
            report_path = reporting_adapters._persist_execution_report(report)
            print(f"execution_report {report_path}", flush=True)
        except Exception as persist_exc:
            print(f"failed to persist execution report: {persist_exc}", flush=True)


def _handle_schwab_probe(*, response_body: str = "Probe OK"):
    composer = None
    log_context = None
    report = None
    try:
        composer = rw.build_composer()
        reporting_adapters = composer.build_reporting_adapters()
        log_context = reporting_adapters.build_log_context()
        report = _build_execution_report(log_context)
        _log_runtime_event(
            log_context,
            "health_probe_received",
            message="Received health probe request",
            execution_window="probe",
        )
        client = composer.build_client()
        # Route probes through the same guarded broker adapter as execution and
        # portfolio reads.  Calling the raw Schwab helper here bypassed the
        # bounded retry policy for transient 5xx responses (including the
        # provider's JS-SetBackendUrl fault), causing the runtime guard to
        # report a healthy service as failed on one transient response.
        snapshot = rw.build_broker_adapters()._fetch_managed_snapshot(client)
        summary = {
            "buying_power": float(snapshot.buying_power or 0.0),
            "total_equity": float(snapshot.total_equity or 0.0),
        }
        account_observation = build_account_observation(
            snapshot,
            net_assets_currency=os.getenv("SCHWAB_NET_ASSETS_CURRENCY"),
            cash_currency=os.getenv("SCHWAB_CASH_CURRENCY"),
        )
        if account_observation is not None:
            # A reporting-only read must not replace the strategy's filtered
            # snapshot or turn an unavailable inventory into a claimed empty set.
            try:
                inventory = collect_stock_inventory(
                    client,
                    account_hash=account_observation["account_hash"],
                    currency=account_observation.get("net_assets_currency"),
                )
                if inventory is not None:
                    account_observation.update(inventory)
                    inventory_diagnostics = inventory.get("broker_stock_inventory_diagnostics", {})
                    _log_runtime_event(
                        log_context,
                        "stock_inventory_observed",
                        message="Read-only stock inventory coverage",
                        inventory_status=inventory.get("broker_stock_inventory_status"),
                        inventory_reason=inventory_diagnostics.get("reason"),
                        native_positions_present=inventory_diagnostics.get("positions_field_present"),
                        native_positions_count=inventory_diagnostics.get("native_positions_count"),
                        projected_stock_count=inventory_diagnostics.get("projected_stock_count"),
                        inventory_fallback_attempted=inventory_diagnostics.get("fallback_attempted"),
                        inventory_fallback_reason=inventory_diagnostics.get("fallback_reason"),
                        linked_positions_present=inventory_diagnostics.get("linked_positions_field_present"),
                    )
            except Exception as exc:
                account_observation["broker_stock_inventory_status"] = "read_failed"
                _log_runtime_event(
                    log_context,
                    "stock_inventory_read_failed",
                    message="Optional read-only stock inventory unavailable",
                    error_type=type(exc).__name__,
                )
            summary["account_observation"] = account_observation
            if external_flow_enabled(os.environ) and account_observation.get("net_assets_currency") == "USD":
                try:
                    observed_at = datetime.fromisoformat(account_observation["observed_at"].replace("Z", "+00:00"))
                    report["account_external_flows"] = collect_external_flow_snapshot(
                        client,
                        account_hash=account_observation["account_hash"],
                        period_start=(observed_at - timedelta(days=1)).isoformat(),
                        period_end=account_observation["observed_at"],
                        currency=account_observation["net_assets_currency"],
                        enabled=True,
                    )
                except Exception as flow_exc:
                    _log_runtime_event(
                        log_context, "external_flow_capture_failed",
                        message="Optional external flow capture failed",
                        error_type=type(flow_exc).__name__,
                    )
        finalize_runtime_report(
            report,
            status="ok",
            summary=summary,
            diagnostics=rw._runtime_revision_diagnostics(),
        )
        _log_runtime_event(
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
                message="health_probe_failed",
                error_type=type(exc).__name__,
            )
            finalize_runtime_report(
                report,
                status="error",
                diagnostics=rw._runtime_revision_diagnostics(),
            )
        if log_context is not None:
            _log_runtime_event(
                log_context,
                "health_probe_failed",
                message=t("health_probe_title"),
                severity="ERROR",
                execution_window="probe",
                error_type=type(exc).__name__,
                error_message="health_probe_failed",
            )
        error_message = "\n".join((t("health_probe_title"), t("runtime_failure_result"), t("runtime_failure_action")))
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
                report_path = _persist_execution_report(report)
                print(f"execution_report {report_path}", flush=True)
        except Exception as persist_exc:
            print(f"failed to persist execution report: {persist_exc}", flush=True)


def _build_optional_c4_attachment(*, observations, candidate, runtime_target):
    """Keep the research-only C4 consumer out of production startup."""

    try:
        from application.c4_shadow_reconcile_runtime import (
            build_reconcile_c4_report_attachment,
        )
    except ModuleNotFoundError as exc:
        if exc.name != "us_equity_strategies.research.c4_shadow_zero_submit_cycle":
            raise
        return {
            "summary": {
                "c4_shadow_status": "PARKED",
                "c4_shadow_no_order": True,
                "c4_shadow_submission_attempted": False,
                "c4_shadow_execution_permitted": False,
            },
            "diagnostics": {
                "c4_shadow_zero_submit": {
                    "status": "PARKED",
                    "reason_codes": ["C4_CONSUMER_UNAVAILABLE"],
                    "no_order": True,
                    "proposed_orders_count": 0,
                    "submission_attempted": False,
                    "execution_permitted": False,
                    "execution_authorized": False,
                }
            },
        }
    return build_reconcile_c4_report_attachment(
        observations=observations,
        reconciliation_evidence=candidate.evidence,
        runtime_target=runtime_target,
        final_risk_assessment_provider=FINAL_RISK_ASSESSMENT_PROVIDER,
    )


def _handle_reconciliation():
    """Collect a no-order, fail-closed recovery candidate for Schwab.

    The response intentionally contains only contract digests and reason codes.
    It cannot change the runtime target, reset a breaker, or submit an order.
    """

    runtime_target = RUNTIME_SETTINGS.runtime_target
    try:
        validate_reconciliation_preconditions(
            runtime_target=runtime_target,
            collector=READ_ONLY_BROKER_RECONCILIATION_COLLECTOR,
        )
    except SchwabReconciliationReadError:
        return "Reconciliation Unavailable", 503

    composer = rw.build_composer(dry_run_only_override=True)
    reporting_adapters = composer.build_reporting_adapters()
    log_context, report = reporting_adapters.start_run()
    try:
        reporting_adapters.log_event(
            log_context,
            "broker_reconciliation_received",
            message="Received read-only broker reconciliation request",
            execution_window="reconciliation",
        )
        observations = READ_ONLY_BROKER_RECONCILIATION_COLLECTOR(
            composer.build_client(),
            fetch_account_snapshot=fetch_account_snapshot,
        )
        candidate = build_reconciliation_candidate(
            observations=observations,
            runtime_target=runtime_target,
            project_id=PROJECT_ID,
        )
        payload = validate_reconciliation_candidate(candidate)
        c4_attachment = _build_optional_c4_attachment(
            observations=observations,
            candidate=candidate,
            runtime_target=runtime_target,
        )
        finalize_runtime_report(
            report,
            status="ok",
            summary={
                "broker_reconciliation_permits_active_lkg": candidate.permits_active_lkg,
                "broker_reconciliation_blockers_count": len(candidate.recovery_blockers),
                "broker_reconciliation_ledger_records_count": candidate.execution_ledger_records_count,
                **c4_attachment["summary"],
            },
            diagnostics={
                "broker_reconciliation": payload,
                **c4_attachment["diagnostics"],
            },
        )
        reporting_adapters.log_event(
            log_context,
            "broker_reconciliation_completed",
            message="Read-only broker reconciliation completed",
            execution_window="reconciliation",
            permits_active_lkg=candidate.permits_active_lkg,
            blockers=[finding.value for finding in candidate.recovery_blockers],
            c4_shadow_status=c4_attachment["summary"].get("c4_shadow_status"),
            c4_shadow_no_order=c4_attachment["summary"].get("c4_shadow_no_order"),
        )
        # Public response contract unchanged: candidate receipt only.
        return json.dumps(payload, ensure_ascii=False), 200, {"Content-Type": "application/json"}
    except Exception as exc:
        append_runtime_report_error(
            report,
            stage="broker_reconciliation",
            message="Read-only broker reconciliation did not complete.",
            error_type=type(exc).__name__,
            failure_category="broker_reconciliation",
        )
        finalize_runtime_report(
            report,
            status="error",
            diagnostics={"broker_reconciliation_failure": type(exc).__name__},
        )
        reporting_adapters.log_event(
            log_context,
            "broker_reconciliation_failed",
            message="Read-only broker reconciliation failed",
            severity="ERROR",
            execution_window="reconciliation",
            error_type=type(exc).__name__,
        )
        return "Reconciliation Unavailable", 503
    finally:
        try:
            report_path = _persist_execution_report(report, dry_run_only_override=True)
            print(f"execution_report {report_path}", flush=True)
        except Exception as persist_exc:
            print(f"failed to persist reconciliation report: {type(persist_exc).__name__}", flush=True)


@app.route("/", methods=["POST"])
@app.route("/run", methods=["POST"])
def handle_schwab():
    return nr._route_with_runtime_error_fallback(
        _handle_schwab_cycle,
        route_label="POST /run",
    )


@app.route("/precheck", methods=["POST", "GET"])
@app.route("/dry-run", methods=["POST", "GET"])
def handle_schwab_dry_run():
    return nr._route_with_runtime_error_fallback(
        _handle_schwab_cycle,
        dry_run_only_override=True,
        response_body="Dry Run OK",
        route_label="POST /dry-run",
    )


@app.route("/paper-command-consumer", methods=["POST"])
def handle_paper_execution_command_consumer():
    return _handle_paper_execution_command_consumer()


@app.route("/probe", methods=["POST"])
def handle_schwab_probe():
    return nr._route_with_runtime_error_fallback(
        _handle_schwab_probe,
        route_label="POST /probe",
    )


@app.route("/reconcile", methods=["POST"])
def handle_reconciliation():
    return _handle_reconciliation()


@app.route("/monitor-dispatch", methods=["POST", "GET"])
def handle_monitor_dispatch():
    if request_method() == "GET":
        return "Monitor Dispatch OK - use POST to dispatch due monitor checks", 200
    try:
        result = dispatch_due_monitors(load_monitor_targets())
    except Exception as exc:
        return nr._handle_route_runtime_error(exc, route_label="POST /monitor-dispatch")
    return json.dumps(result, ensure_ascii=False), 200, {"Content-Type": "application/json"}


def request_method() -> str:
    try:
        from flask import request

        return request.method
    except Exception:
        return "GET"


@app.route("/healthz", methods=["GET"])
@app.route("/health", methods=["GET"])
def health():
    critical_errors: list[str] = []
    for module_path in (
        "application.runtime_composer",
        "application.runtime_reporting_adapters",
        "application.runtime_strategy_adapters",
        "runtime_execution_policy",
    ):
        try:
            importlib.import_module(module_path)
        except Exception as exc:
            critical_errors.append(f"{module_path}: {type(exc).__name__}: {exc}")
    if critical_errors:
        return json.dumps({"status": "unhealthy", "errors": critical_errors}, ensure_ascii=False), 500
    return "OK", 200

