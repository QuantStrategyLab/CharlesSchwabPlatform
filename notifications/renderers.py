"""Notification rendering helpers for CharlesSchwabPlatform."""

from __future__ import annotations

from collections.abc import Mapping

from notifications.events import RenderedNotification
from quant_platform_kit.common.notification_localization import (
    localize_notification_text as _base_localize_notification_text,
)
from quant_platform_kit.notifications.renderer_base import (
    as_float_or_none as _as_float_or_none,
    build_timing_audit_lines as _build_timing_audit_lines_shared,
    build_tqqq_risk_control_lines as _build_tqqq_risk_control_lines_shared,
    effective_volatility_delever_threshold as _effective_volatility_delever_threshold,
    format_percent as _format_percent,
    format_percentile as _format_percentile,
    format_sample_count as _format_sample_count,
    format_signal_snapshot_line as _format_signal_snapshot_line_shared,
    format_tqqq_volatility_delever_allocation_detail as _format_tqqq_volatility_delever_allocation_detail,
    format_volatility_delever_threshold_detail as _format_volatility_delever_threshold_detail,
    compact_dashboard_lines as _compact_dashboard_lines,
    is_compact_dashboard_audit_line as _is_compact_dashboard_audit_line,
    is_truthy,
    localize_price_source_label as _localize_price_source_label,
    localize_timing_contract as _localize_timing_contract_shared,
    present as _present,
    relabel_dashboard_cash_labels as _relabel_dashboard_cash_labels_shared,
    split_detail_segment as _split_detail_segment,
    split_labeled_text as _split_labeled_text,
    translator_uses_zh as _translator_uses_zh,
)

def _has_benchmark_context(execution):
    return any(
        float(execution.get(key) or 0.0) > 0.0
        for key in ("benchmark_price", "long_trend_value", "exit_line")
    )


def _localize_notification_text(text, *, translator):
    return _base_localize_notification_text(text, translator=translator)


def _localize_timing_contract(contract: str, *, translator) -> str:
    """Thin wrapper — adds Schwab-specific notification localisation fallback."""
    result = _localize_timing_contract_shared(contract, translator=translator)
    if result and result not in ("当日执行", "same trading day", "次一交易日执行", "next trading day"):
        if "个交易日后执行" not in result and "next " not in result:
            return _localize_notification_text(result, translator=translator)
    return result


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


def _format_dashboard_text(text: str, *, translator, cash_only_execution: bool = True) -> str:
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
    result = "\n".join(formatted_lines)
    return _relabel_dashboard_cash_labels_shared(
        result,
        cash_only_execution=cash_only_execution,
        translator=translator,
    )


def _build_timing_audit_lines(execution, *, translator) -> list[str]:
    return _build_timing_audit_lines_shared(execution, translator=translator)


def _build_tqqq_risk_control_lines(execution, *, translator) -> list[str]:
    return _build_tqqq_risk_control_lines_shared(
        execution if isinstance(execution, Mapping) else {},
        translator=translator,
    )


def _format_signal_snapshot_line(snapshot, *, translator) -> str:
    return _format_signal_snapshot_line_shared(
        snapshot,
        translator=translator,
        localize_text=_localize_notification_text,
    )


def _format_holdings_lines(portfolio_rows, market_values, *, translator) -> list[str]:
    lines = [translator("holdings_title")]
    for row in portfolio_rows:
        for symbol in row:
            lines.append(f"  - {symbol}: ${market_values[symbol]:,.2f}")
    return lines


def _first_detail_line(text: str) -> str:
    parts = _split_labeled_text(text)
    return parts[0] if parts else ""


def _render_extra_notification_block(extra_notification_lines) -> str:
    block = "\n".join(
        str(line).strip() for line in extra_notification_lines if str(line).strip()
    )
    if not block:
        return ""
    return f"{block}\n"


def _format_account_line(account_label, *, translator) -> str:
    value = str(account_label or "").strip()
    if not value:
        return ""
    label = "账户" if _translator_uses_zh(translator) else "Account"
    return f"🆔 {label}: {value}"


def _format_market_status_line(status, *, translator) -> str:
    value = str(status or "").strip()
    if not value:
        return ""
    label = "市场状态" if _translator_uses_zh(translator) else "Market"
    return f"📊 {label}: {value}"


def _build_compact_trade_message(
    *,
    translator,
    strategy_display_name,
    account_label,
    dry_run_only,
    extra_notification_block,
    dashboard_text,
    separator,
    status_display,
    signal_display,
    timing_lines,
    signal_snapshot_line,
    risk_control_lines,
    trade_logs,
) -> str:
    lines = [
        translator("rebalance_title"),
        translator("strategy_label", name=strategy_display_name),
    ]
    account_line = _format_account_line(account_label, translator=translator)
    if account_line:
        lines.append(account_line)
    if dry_run_only:
        lines.append(translator("dry_run_banner"))
    if extra_notification_block:
        lines.extend(line for line in extra_notification_block.splitlines() if line.strip())
    dashboard = str(dashboard_text or "").strip()
    if dashboard:
        dashboard_lines = _compact_dashboard_lines(dashboard)
    else:
        dashboard_lines = []
    if dashboard_lines:
        lines.append(separator)
        lines.extend(dashboard_lines)
        lines.append(separator)
    if trade_logs:
        lines.extend(str(log).strip() for log in trade_logs if str(log).strip())
        lines.append(separator)
    return "\n".join(lines)


def _build_compact_heartbeat_message(
    *,
    translator,
    strategy_display_name,
    account_label,
    dry_run_only,
    extra_notification_block,
    total_equity,
    dashboard_text,
    separator,
    status_display,
    signal_display,
    timing_lines,
    signal_snapshot_line,
    risk_control_lines,
) -> str:
    lines = [
        translator("heartbeat_title"),
        translator("strategy_label", name=strategy_display_name),
        f"💰 {translator('equity')}: ${total_equity:,.2f}",
    ]
    account_line = _format_account_line(account_label, translator=translator)
    if account_line:
        lines.append(account_line)
    if dry_run_only:
        lines.append(translator("dry_run_banner"))
    if extra_notification_block:
        lines.extend(line for line in extra_notification_block.splitlines() if line.strip())
    dashboard = str(dashboard_text or "").strip()
    if dashboard:
        dashboard_lines = _compact_dashboard_lines(dashboard)
    else:
        dashboard_lines = []
    if dashboard_lines:
        lines.append(separator)
        lines.extend(dashboard_lines)
    lines.append(separator)
    lines.append(translator("no_trades"))
    return "\n".join(lines)


def render_trade_notification(
    *,
    translator,
    strategy_display_name,
    dry_run_only,
    extra_notification_lines,
    execution,
    trade_logs,
    account_label="",
) -> RenderedNotification:
    signal_display = _localize_notification_text(execution["signal_display"], translator=translator)
    status_display = _localize_notification_text(execution.get("status_display"), translator=translator)
    extra_notification_block = _render_extra_notification_block(extra_notification_lines)
    cash_only_execution = bool(execution.get("cash_only_execution", True))
    dashboard_text = _format_dashboard_text(
        str(execution["dashboard_text"]),
        translator=translator,
        cash_only_execution=cash_only_execution,
    )
    timing_lines = _build_timing_audit_lines(execution, translator=translator)
    signal_snapshot_line = _format_signal_snapshot_line(
        execution.get("signal_snapshot"),
        translator=translator,
    )
    risk_control_lines = _build_tqqq_risk_control_lines(execution, translator=translator)
    separator = str(execution["separator"])
    status_line = (
        "\n".join(_split_labeled_text(_format_market_status_line(status_display, translator=translator))) + "\n"
        if status_display
        else ""
    )
    risk_control_block = "\n".join(risk_control_lines)
    if risk_control_block:
        risk_control_block += "\n"
    dashboard_block = f"{dashboard_text}\n{separator}\n" if dashboard_text else ""
    trade_signal_lines = _format_label_value_lines(f"🎯 {translator('signal_label')}", signal_display)
    trade_signal_block = "\n".join(trade_signal_lines)
    dry_run_line = f"{translator('dry_run_banner')}\n" if dry_run_only else ""
    detailed_text = (
        f"{translator('trade_header')}\n"
        f"{translator('strategy_label', name=strategy_display_name)}\n"
        f"{_format_account_line(account_label, translator=translator) + chr(10) if account_label else ''}"
        f"{dry_run_line}"
        f"{extra_notification_block}"
        f"{chr(10).join(timing_lines) + chr(10) if timing_lines else ''}"
        f"{signal_snapshot_line + chr(10) if signal_snapshot_line else ''}"
        f"{status_line}"
        f"{risk_control_block}"
        f"{trade_signal_block}\n\n"
        f"{dashboard_block}"
        + "\n".join(trade_logs)
    )
    compact_text = _build_compact_trade_message(
        translator=translator,
        strategy_display_name=strategy_display_name,
        account_label=account_label,
        dry_run_only=dry_run_only,
        extra_notification_block=extra_notification_block,
        dashboard_text=dashboard_text,
        separator=separator,
        status_display=status_display,
        signal_display=signal_display,
        timing_lines=timing_lines,
        signal_snapshot_line=signal_snapshot_line,
        risk_control_lines=risk_control_lines,
        trade_logs=trade_logs,
    )
    return RenderedNotification(detailed_text=detailed_text, compact_text=compact_text)


def render_heartbeat_notification(
    *,
    translator,
    strategy_display_name,
    dry_run_only,
    extra_notification_lines,
    execution,
    portfolio,
    account_label="",
) -> RenderedNotification:
    signal_display = _localize_notification_text(execution["signal_display"], translator=translator)
    status_display = _localize_notification_text(execution.get("status_display"), translator=translator)
    extra_notification_block = _render_extra_notification_block(extra_notification_lines)
    cash_only_execution = bool(execution.get("cash_only_execution", True))
    dashboard_text = _format_dashboard_text(
        str(execution["dashboard_text"]),
        translator=translator,
        cash_only_execution=cash_only_execution,
    )
    timing_lines = _build_timing_audit_lines(execution, translator=translator)
    signal_snapshot_line = _format_signal_snapshot_line(
        execution.get("signal_snapshot"),
        translator=translator,
    )
    risk_control_lines = _build_tqqq_risk_control_lines(execution, translator=translator)
    separator = str(execution["separator"])
    total_equity = float(portfolio["total_equity"])
    portfolio_rows = tuple(portfolio["portfolio_rows"])
    market_values = dict(portfolio["market_values"])
    status_line = (
        "\n".join(_split_labeled_text(_format_market_status_line(status_display, translator=translator))) + "\n"
        if status_display
        else ""
    )
    risk_control_block = "\n".join(risk_control_lines)
    if risk_control_block:
        risk_control_block += "\n"
    dashboard_block = f"{dashboard_text}\n{separator}\n" if dashboard_text else ""
    benchmark_lines = _format_benchmark_lines(execution, translator=translator)
    benchmark_block = "\n".join(benchmark_lines) + "\n" if benchmark_lines else ""
    heartbeat_signal_lines = _format_label_value_lines(f"🎯 {translator('signal_label')}", signal_display)
    heartbeat_signal_block = "\n".join(heartbeat_signal_lines)
    if dashboard_block:
        portfolio_block = dashboard_block
    else:
        holdings_lines = _format_holdings_lines(portfolio_rows, market_values, translator=translator)
        portfolio_block = (
            f"💰 {translator('equity')}: ${total_equity:,.2f}\n"
            f"{separator}\n"
            + "\n".join(holdings_lines) + "\n"
            f"{separator}\n"
        )
    detailed_text = (
        f"{translator('heartbeat_header')}\n"
        f"{translator('strategy_label', name=strategy_display_name)}\n"
        f"{_format_account_line(account_label, translator=translator) + chr(10) if account_label else ''}"
        f"{extra_notification_block}"
        f"{portfolio_block}"
        f"{chr(10).join(timing_lines) + chr(10) if timing_lines else ''}"
        f"{signal_snapshot_line + chr(10) if signal_snapshot_line else ''}"
        f"{status_line}"
        f"{risk_control_block}"
        f"{heartbeat_signal_block}\n"
        f"{benchmark_block}"
        f"{separator}\n"
        f"{translator('no_trades')}"
    )
    compact_text = _build_compact_heartbeat_message(
        translator=translator,
        strategy_display_name=strategy_display_name,
        account_label=account_label,
        dry_run_only=dry_run_only,
        extra_notification_block=extra_notification_block,
        total_equity=total_equity,
        dashboard_text=dashboard_text,
        separator=separator,
        status_display=status_display,
        signal_display=signal_display,
        timing_lines=timing_lines,
        signal_snapshot_line=signal_snapshot_line,
        risk_control_lines=risk_control_lines,
    )
    return RenderedNotification(detailed_text=detailed_text, compact_text=compact_text)
