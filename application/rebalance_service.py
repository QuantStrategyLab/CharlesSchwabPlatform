"""Application orchestration for CharlesSchwabPlatform."""

from __future__ import annotations

import re
import json
from datetime import datetime, timezone

from application.execution_service import execute_rebalance_cycle, ExecutionCycleResult
from application.runtime_dependencies import SchwabRebalanceConfig, SchwabRebalanceRuntime
from application.signal_snapshot import build_signal_snapshot
from notifications.events import NotificationPublisher, RenderedNotification
from notifications import renderers as notification_renderers
from quant_platform_kit.common.execution_state import build_execution_marker_key
from quant_platform_kit.common.models import QuoteSnapshot
from quant_platform_kit.common.notification_localization import (
    localize_notification_text as _base_localize_notification_text,
    translator_uses_zh as _base_translator_uses_zh,
)
from quant_platform_kit.common.port_adapters import (
    CallableExecutionPort,
    CallableMarketDataPort,
    CallableNotificationPort,
    CallablePortfolioPort,
)
from quant_platform_kit.common.strategy_plugins import attach_strategy_plugin_metadata
from quant_platform_kit.strategy_lifecycle.performance_monitor import try_record_platform_execution

_DETAIL_FIELD_SPLIT_RE = re.compile(r"\s+(?=[^\s=:：]+[=:：])")


def _record_platform_execution_telemetry(
    config: SchwabRebalanceConfig,
    execution_result: ExecutionCycleResult,
) -> None:
    profile = str(getattr(config, "strategy_profile", "") or "").strip()
    if not profile:
        return
    execution = dict(execution_result.execution or {})
    portfolio = dict(execution_result.portfolio or {})
    try_record_platform_execution(
        profile,
        {
            "platform": "schwab",
            "action_done": _has_submitted_orders(execution_result),
            "effective_date": execution.get("effective_date"),
            "signal_date": execution.get("signal_date"),
            "dry_run_only": bool(getattr(config, "dry_run_only", False)),
            "trade_logs_count": len(getattr(execution_result, "trade_logs", ()) or ()),
            "total_equity": portfolio.get("total_equity") or portfolio.get("equity"),
        },
    )


def _plan_portfolio(plan):
    return dict(plan.get("portfolio") or {})


def _plan_execution(plan):
    return dict(plan.get("execution") or {})


def _plan_allocation(plan):
    return dict(plan.get("allocation") or {})


def _noop_sleep(_seconds):
    return None


def _has_benchmark_context(execution):
    return any(
        float(execution.get(key) or 0.0) > 0.0
        for key in ("benchmark_price", "long_trend_value", "exit_line")
    )


def _translator_uses_zh(translator) -> bool:
    return _base_translator_uses_zh(translator)


def _localize_notification_text(text, *, translator):
    return _base_localize_notification_text(text, translator=translator)


def _split_detail_segment(text: str) -> list[str]:
    value = str(text or "").strip()
    if not value:
        return []
    if "=" not in value and ":" not in value and "：" not in value:
        return [value]
    return [part.strip() for part in _DETAIL_FIELD_SPLIT_RE.split(value) if part.strip()]


def _split_labeled_text(text: str) -> list[str]:
    segments = [segment.strip() for segment in str(text or "").split(" | ") if segment.strip()]
    if not segments:
        return []
    lines = [segments[0]]
    for segment in segments[1:]:
        lines.extend(_split_detail_segment(segment))
    return lines


def _format_label_value_lines(label: str, value: str) -> list[str]:
    parts = _split_labeled_text(value)
    if not parts:
        return []
    lines = [f"{label}: {parts[0]}"]
    lines.extend(f"  - {part}" for part in parts[1:])
    return lines


def _format_benchmark_lines(execution, *, translator) -> list[str]:
    if not _has_benchmark_context(execution):
        return []
    benchmark_symbol = str(execution["benchmark_symbol"])
    benchmark_price = float(execution["benchmark_price"])
    long_trend_value = float(execution["long_trend_value"])
    exit_line = float(execution["exit_line"])
    return [
        translator("benchmark_title", symbol=benchmark_symbol),
        f"  - {translator('benchmark_price', symbol=benchmark_symbol, value=f'{benchmark_price:.2f}')}",
        f"  - {translator('benchmark_ma200', value=f'{long_trend_value:.2f}')}",
        f"  - {translator('benchmark_exit', value=f'{exit_line:.2f}')}",
    ]


def _is_holding_segment(segment: str) -> bool:
    label, sep, value = str(segment or "").partition(":")
    symbol = label.strip().replace(".", "").replace("-", "")
    return bool(sep and symbol.isalnum() and "$" in value)


def _format_inline_segments(line: str, *, translator, holdings_title_emitted: bool) -> tuple[list[str], bool]:
    parts = [part.strip() for part in str(line or "").split(" | ") if part.strip()]
    if len(parts) <= 1:
        return [str(line or "").strip()], holdings_title_emitted

    if all(_is_holding_segment(part) for part in parts):
        lines = []
        if not holdings_title_emitted:
            lines.append(translator("holdings_title"))
            holdings_title_emitted = True
        lines.extend(f"  - {part}" for part in parts)
        return lines, holdings_title_emitted

    first, rest = parts[0], parts[1:]
    if first.startswith(("📊", "💰", "💵")):
        lines = [first]
        lines.extend(f"  - {part}" for part in rest)
        return lines, holdings_title_emitted
    return [f"  - {part}" for part in parts], holdings_title_emitted


def _format_dashboard_text(text: str, *, translator) -> str:
    raw_lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    formatted_lines: list[str] = []
    holdings_title_emitted = False
    for line in raw_lines:
        expanded, holdings_title_emitted = _format_inline_segments(
            line,
            translator=translator,
            holdings_title_emitted=holdings_title_emitted,
        )
        formatted_lines.extend(expanded)
    return "\n".join(formatted_lines)


def _format_holdings_lines(portfolio_rows, market_values, *, translator) -> list[str]:
    lines = [translator("holdings_title")]
    for row in portfolio_rows:
        for symbol in row:
            lines.append(f"  - {symbol}: ${market_values[symbol]:,.2f}")
    return lines


def _first_detail_line(text: str) -> str:
    parts = _split_labeled_text(text)
    return parts[0] if parts else ""


def _build_compact_trade_message(
    *,
    translator,
    strategy_display_name,
    dry_run_only,
    extra_notification_block,
    dashboard_text,
    separator,
    status_display,
    signal_display,
    trade_logs,
) -> str:
    lines = [
        translator("trade_header"),
        translator("strategy_label", name=strategy_display_name),
    ]
    if dry_run_only:
        lines.append(translator("dry_run_banner"))
    if extra_notification_block:
        lines.extend(line for line in extra_notification_block.splitlines() if line.strip())
    dashboard = str(dashboard_text or "").strip()
    if dashboard:
        lines.append(separator)
        lines.extend(line for line in dashboard.splitlines() if line.strip())
    status_summary = _first_detail_line(status_display)
    if status_summary:
        lines.append(f"📊 {status_summary}")
    signal_summary = _first_detail_line(signal_display)
    if signal_summary:
        lines.append(f"📊 {translator('signal_label')}: {signal_summary}")
    lines.extend(str(log).strip() for log in trade_logs if str(log).strip())
    return "\n".join(lines)


def _build_compact_heartbeat_message(
    *,
    translator,
    strategy_display_name,
    dry_run_only,
    extra_notification_block,
    total_equity,
    dashboard_text,
    separator,
    status_display,
    signal_display,
) -> str:
    lines = [
        translator("heartbeat_header"),
        translator("strategy_label", name=strategy_display_name),
        f"💰 {translator('equity')}: ${total_equity:,.2f}",
    ]
    if dry_run_only:
        lines.append(translator("dry_run_banner"))
    if extra_notification_block:
        lines.extend(line for line in extra_notification_block.splitlines() if line.strip())
    dashboard = str(dashboard_text or "").strip()
    if dashboard:
        lines.append(separator)
        lines.extend(line for line in dashboard.splitlines() if line.strip())
    status_summary = _first_detail_line(status_display)
    if status_summary:
        lines.append(f"📊 {status_summary}")
    signal_summary = _first_detail_line(signal_display)
    if signal_summary:
        lines.append(f"🎯 {translator('signal_label')}: {signal_summary}")
    lines.append(translator("no_trades"))
    return "\n".join(lines)


_localize_notification_text = notification_renderers._localize_notification_text
_format_dashboard_text = notification_renderers._format_dashboard_text


def _resolve_execution_account_scope(*, config: SchwabRebalanceConfig, plan: dict) -> str:
    account_hash = str(plan.get("account_hash") or "").strip()
    if account_hash:
        return account_hash
    configured_scope = str(getattr(config, "execution_state_account_scope", "") or "").strip()
    if configured_scope:
        return configured_scope
    return "PAPER" if bool(getattr(config, "dry_run_only", False)) else "LIVE"


def _build_execution_marker_key(*, config: SchwabRebalanceConfig, execution: dict, plan: dict) -> str:
    if not getattr(config, "execution_dedup_enabled", False):
        return ""
    execution_mode = "paper" if bool(getattr(config, "dry_run_only", False)) else "live"
    return build_execution_marker_key(
        platform="schwab",
        strategy_profile=getattr(config, "strategy_profile", "") or "unknown",
        account_scope=_resolve_execution_account_scope(config=config, plan=plan),
        execution_mode=execution_mode,
        signal_date=execution.get("signal_date"),
        effective_date=execution.get("effective_date"),
        execution_timing_contract=execution.get("execution_timing_contract"),
    )


def _execution_already_recorded_message(*, config: SchwabRebalanceConfig, execution: dict) -> str:
    message = config.translator(
        "execution_already_recorded",
        signal_date=str(execution.get("signal_date") or ""),
        effective_date=str(execution.get("effective_date") or ""),
    )
    if not message or message == "execution_already_recorded":
        message = (
            f"Execution already recorded for signal={execution.get('signal_date')} "
            f"effective={execution.get('effective_date')}"
        )
    return message


def _should_record_execution_marker(*, result: ExecutionCycleResult, config: SchwabRebalanceConfig) -> bool:
    if not getattr(config, "execution_dedup_enabled", False):
        return False
    return bool(tuple(getattr(result, "trade_logs", ()) or ()))


def _has_submitted_orders(result: ExecutionCycleResult) -> bool:
    return bool(tuple(getattr(result, "submitted_orders", ()) or ()))


def _record_execution_marker(
    *,
    config: SchwabRebalanceConfig,
    marker_key: str,
    result: ExecutionCycleResult,
    plan: dict,
    notify_issue,
) -> None:
    store = getattr(config, "execution_state_store", None)
    if not store or not marker_key:
        return
    try:
        store.record_marker(
            marker_key,
            metadata={
                "strategy_profile": getattr(config, "strategy_profile", ""),
                "account_scope": _resolve_execution_account_scope(config=config, plan=plan),
                "dry_run_only": bool(getattr(config, "dry_run_only", False)),
                "trade_logs_count": len(tuple(getattr(result, "trade_logs", ()) or ())),
                "signal_date": str(dict(getattr(result, "execution", {}) or {}).get("signal_date") or ""),
                "effective_date": str(dict(getattr(result, "execution", {}) or {}).get("effective_date") or ""),
            },
        )
    except Exception as exc:
        notify_issue(
            "Execution marker write failed",
            f"Marker: {marker_key}\n{type(exc).__name__}: {exc}",
        )


def _legacy_quote_snapshot(symbol, quote_snapshots) -> QuoteSnapshot:
    raw_snapshot = quote_snapshots[str(symbol).strip().upper()]
    return QuoteSnapshot(
        symbol=str(symbol).strip().upper(),
        as_of=datetime.now(timezone.utc),
        last_price=float(raw_snapshot.last_price),
        ask_price=(
            float(raw_snapshot.ask_price)
            if getattr(raw_snapshot, "ask_price", None) is not None
            else None
        ),
    )


def run_strategy_core(
    client=None,
    now_ny=None,
    *,
    runtime: SchwabRebalanceRuntime | None = None,
    config: SchwabRebalanceConfig | None = None,
    fetch_reference_history=None,
    fetch_managed_snapshot=None,
    fetch_managed_quotes=None,
    resolve_rebalance_plan=None,
    submit_equity_order=None,
    send_tg_message=None,
    translator=None,
    strategy_display_name=None,
    limit_buy_premium=None,
    sell_settle_delay_sec=None,
    dry_run_only=False,
    post_sell_refresh_attempts=1,
    post_sell_refresh_interval_sec=0.0,
    sleeper=_noop_sleep,
    extra_notification_lines=(),
    notify_no_trade_cycles=True,
):
    del now_ny
    if runtime is None:
        if not all(
            (
                client is not None,
                fetch_reference_history,
                fetch_managed_snapshot,
                fetch_managed_quotes,
                resolve_rebalance_plan,
                submit_equity_order,
                send_tg_message,
            )
        ):
            raise ValueError("Legacy Schwab rebalance call requires client plus fetch_reference_history/fetch_managed_snapshot/fetch_managed_quotes/resolve_rebalance_plan/submit_equity_order/send_tg_message")
        runtime = SchwabRebalanceRuntime(
            fetch_reference_history=lambda: fetch_reference_history(client),
            portfolio_port=CallablePortfolioPort(lambda: fetch_managed_snapshot(client)),
            market_data_port=CallableMarketDataPort(
                quote_loader=lambda symbol: _legacy_quote_snapshot(
                    symbol,
                    fetch_managed_quotes(client),
                )
            ),
            resolve_rebalance_plan=resolve_rebalance_plan,
            notifications=CallableNotificationPort(send_tg_message),
            execution_port_factory=lambda account_hash: CallableExecutionPort(
                lambda order_intent: submit_equity_order(client, account_hash, order_intent)
            ),
            submit_equity_order=lambda account_hash, order_intent: submit_equity_order(client, account_hash, order_intent),
        )
    if config is None:
        config = SchwabRebalanceConfig(
            translator=translator,
            strategy_display_name=strategy_display_name,
            limit_buy_premium=limit_buy_premium,
            sell_settle_delay_sec=sell_settle_delay_sec,
            dry_run_only=dry_run_only,
            post_sell_refresh_attempts=post_sell_refresh_attempts,
            post_sell_refresh_interval_sec=post_sell_refresh_interval_sec,
            sleeper=sleeper,
            extra_notification_lines=tuple(extra_notification_lines),
            notify_no_trade_cycles=bool(notify_no_trade_cycles),
        )
    sleeper_fn = config.sleeper or _noop_sleep
    notification_publisher = NotificationPublisher(
        log_message=lambda message: print(message, flush=True),
        send_message=runtime.notifications.send_text,
    )

    reference_history = runtime.fetch_reference_history()

    def load_plan(current_snapshot):
        current_snapshot = attach_strategy_plugin_metadata(
            current_snapshot,
            getattr(config, "strategy_plugin_signals", ()) or (),
        )
        current_plan = runtime.resolve_rebalance_plan(
            qqq_history=reference_history,
            snapshot=current_snapshot,
        )
        current_portfolio = _plan_portfolio(current_plan)
        current_execution = _plan_execution(current_plan)
        current_allocation = _plan_allocation(current_plan)
        if current_allocation.get("target_mode") != "value":
            raise ValueError("CharlesSchwabPlatform requires allocation.target_mode=value")
        return current_plan, current_portfolio, current_execution, current_allocation

    snapshot = runtime.portfolio_port.get_portfolio_snapshot()
    plan, portfolio, execution, allocation = load_plan(snapshot)
    execution_port = (
        runtime.execution_port_factory(plan["account_hash"])
        if runtime.execution_port_factory is not None
        else None
    )
    execution_marker_key = _build_execution_marker_key(config=config, execution=execution, plan=plan)
    execution_state_store = getattr(config, "execution_state_store", None)
    execution_already_recorded = False
    if execution_marker_key and execution_state_store:
        try:
            execution_already_recorded = bool(execution_state_store.has_marker(execution_marker_key))
        except Exception as exc:
            print(
                f"Execution marker read failed\nMarker: {execution_marker_key}\n{type(exc).__name__}: {exc}",
                flush=True,
            )
        if not execution_already_recorded and hasattr(execution_state_store, "has_prior_execution_report"):
            try:
                execution_already_recorded = bool(
                    execution_state_store.has_prior_execution_report(
                        platform="schwab",
                        strategy_profile=getattr(config, "strategy_profile", "") or "unknown",
                        account_scope=_resolve_execution_account_scope(config=config, plan=plan),
                        signal_date=execution.get("signal_date"),
                        effective_date=execution.get("effective_date"),
                        dry_run_only=bool(getattr(config, "dry_run_only", False)),
                    )
                )
            except Exception as exc:
                print(
                    f"Execution report dedup read failed\nMarker: {execution_marker_key}\n{type(exc).__name__}: {exc}",
                    flush=True,
                )

    if execution_already_recorded:
        message = _execution_already_recorded_message(config=config, execution=execution)
        print(message, flush=True)
        execution_result = ExecutionCycleResult(
            plan=plan,
            portfolio=portfolio,
            execution=execution,
            allocation=allocation,
            trade_logs=(),
        )
    else:
        execution_result = execute_rebalance_cycle(
            client=client,
            plan=plan,
            portfolio=portfolio,
            execution=execution,
            allocation=allocation,
            fetch_managed_snapshot=lambda _client: runtime.portfolio_port.get_portfolio_snapshot(),
            market_data_port=runtime.market_data_port,
            load_plan=load_plan,
            execution_port=execution_port,
            submit_equity_order=(
                (lambda _client, account_hash, order_intent: runtime.submit_equity_order(account_hash, order_intent))
                if runtime.submit_equity_order is not None
                else None
            ),
            translator=config.translator,
            limit_buy_premium=config.limit_buy_premium,
            limit_buy_premium_by_symbol=config.limit_buy_premium_by_symbol,
            sell_settle_delay_sec=config.sell_settle_delay_sec,
            dry_run_only=config.dry_run_only,
            post_sell_refresh_attempts=config.post_sell_refresh_attempts,
            post_sell_refresh_interval_sec=config.post_sell_refresh_interval_sec,
            sleeper=sleeper_fn,
            safe_haven_cash_substitute_threshold_usd=config.safe_haven_cash_substitute_threshold_usd,
            cash_only_execution=getattr(config, "cash_only_execution", True),
            notional_buy_execution=getattr(config, "notional_buy_execution", False),
            publish_order_issue=lambda message: notification_publisher.publish(
                RenderedNotification(
                    detailed_text=message,
                    compact_text=message,
                )
            ),
        )
        if _should_record_execution_marker(result=execution_result, config=config):
            _record_execution_marker(
                config=config,
                marker_key=execution_marker_key,
                result=execution_result,
                plan=plan,
                notify_issue=lambda title, detail: notification_publisher.publish(
                    RenderedNotification(
                        detailed_text=f"{title}\n{detail}",
                        compact_text=f"{title}\n{detail}",
                    )
                ),
            )
    portfolio = execution_result.portfolio
    execution = execution_result.execution
    signal_metadata = dict(execution.get("signal_metadata") or {})
    signal_metadata["cash_only_execution"] = bool(getattr(config, "cash_only_execution", True))
    execution["cash_only_execution"] = signal_metadata["cash_only_execution"]
    execution["signal_snapshot"] = build_signal_snapshot(
        platform="schwab",
        strategy_profile=config.strategy_profile,
        execution={
            **execution,
            "latest_price_source": "schwab_daily_history_with_live_quote_overlay",
        },
        metadata=signal_metadata,
        allocation=execution_result.allocation,
    )
    trade_logs = list(execution_result.trade_logs)

    if _has_submitted_orders(execution_result):
        notification_publisher.publish(
            notification_renderers.render_trade_notification(
                translator=config.translator,
                strategy_display_name=config.strategy_display_name,
                dry_run_only=config.dry_run_only,
                extra_notification_lines=config.extra_notification_lines,
                execution=execution,
                trade_logs=trade_logs,
                account_label=plan.get("account_hash", ""),
            )
        )
    elif getattr(config, "notify_no_trade_cycles", True):
        notification_publisher.publish(
            notification_renderers.render_heartbeat_notification(
                translator=config.translator,
                strategy_display_name=config.strategy_display_name,
                dry_run_only=config.dry_run_only,
                extra_notification_lines=config.extra_notification_lines,
                execution=execution,
                portfolio=portfolio,
                account_label=plan.get("account_hash", ""),
            )
        )
    else:
        print(
            "notification_suppressed "
            + json.dumps({"reason": "no_trade_or_error", "trade_logs_count": len(trade_logs)}, ensure_ascii=False),
            flush=True,
        )
    _record_platform_execution_telemetry(config, execution_result)
    return execution_result
