"""Notification rendering helpers for CharlesSchwabPlatform."""

from __future__ import annotations

from collections.abc import Mapping
import re

from notifications.events import RenderedNotification
from quant_platform_kit.common.notification_localization import (
    localize_notification_text as _base_localize_notification_text,
    translator_uses_zh as _base_translator_uses_zh,
)

_PRICE_SOURCE_LABELS = {
    "longbridge_candlesticks": ("LongBridge 日线K线", "LongBridge daily candlesticks"),
    "schwab_daily_history_with_live_quote_overlay": ("Schwab 日线历史", "Schwab daily history"),
    "firstrade_ohlc_with_live_quote_overlay": ("Firstrade OHLC", "Firstrade OHLC"),
    "market_quote": ("实时行情报价", "market quote"),
    "mixed_market_quote_snapshot_close": (
        "实时行情报价 + 快照收盘价回补",
        "market quote + snapshot close fallback",
    ),
    "mixed_market_quote_historical_close": (
        "实时行情报价 + 历史收盘价回补",
        "market quote + historical close fallback",
    ),
    "snapshot_close": ("快照收盘价", "snapshot close"),
    "historical_close": ("历史收盘价", "historical close"),
    "market_data": ("市场数据", "market data"),
}

try:
    from quant_platform_kit.common.notification_localization import (
        localize_price_source_label as _shared_localize_price_source_label,
    )
except ImportError:  # pragma: no cover - compatibility with older pinned shared wheels
    _shared_localize_price_source_label = None


def _localize_price_source_label(value, *, translator=None, locale=None):
    source = str(value or "").strip()
    use_zh = _base_translator_uses_zh(translator) if translator is not None else str(locale or "").startswith("zh")
    if not source:
        return "未知" if use_zh else "unknown"
    label = _PRICE_SOURCE_LABELS.get(source)
    if label is not None:
        return label[0] if use_zh else label[1]
    if _shared_localize_price_source_label is not None:
        return _shared_localize_price_source_label(source, translator=translator, locale=locale)
    return source.replace("_", " ")

_DETAIL_FIELD_SPLIT_RE = re.compile(r"\s+(?=[^\s=:：]+[=:：])")


def _has_benchmark_context(execution):
    return any(
        float(execution.get(key) or 0.0) > 0.0
        for key in ("benchmark_price", "long_trend_value", "exit_line")
    )


def _translator_uses_zh(translator) -> bool:
    return _base_translator_uses_zh(translator)


def _localize_notification_text(text, *, translator):
    return _base_localize_notification_text(text, translator=translator)


def _localize_timing_contract(contract: str, *, translator) -> str:
    value = str(contract or "").strip()
    if not value:
        return ""
    if value == "same_trading_day":
        return "当日执行" if _translator_uses_zh(translator) else "same trading day"
    if value == "next_trading_day":
        return "次一交易日执行" if _translator_uses_zh(translator) else "next trading day"
    match = re.fullmatch(r"next_(\d+)_trading_days", value)
    if match:
        count = int(match.group(1))
        if _translator_uses_zh(translator):
            return f"{count}个交易日后执行"
        return f"next {count} trading days"
    return _localize_notification_text(value, translator=translator)


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


def _build_timing_audit_lines(execution, *, translator) -> list[str]:
    signal_date = str(execution.get("signal_date") or "").strip()
    effective_date = str(execution.get("effective_date") or "").strip()
    contract = str(execution.get("execution_timing_contract") or "").strip()
    if not signal_date and not effective_date and not contract:
        return []
    label = "⏱ 执行时点" if _translator_uses_zh(translator) else "⏱ Timing"
    localized_contract = _localize_timing_contract(contract, translator=translator)
    if signal_date and effective_date:
        value = f"{signal_date} -> {effective_date}"
    else:
        value = signal_date or effective_date or localized_contract
    if localized_contract and localized_contract not in value:
        value = f"{value} ({localized_contract})" if value else localized_contract
    return [f"{label}: {value}"]


def _format_percent(value) -> str:
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "n/a"


def _format_percentile(value) -> str:
    try:
        percentile = float(value) * 100
    except (TypeError, ValueError):
        return "p?"
    if float(percentile).is_integer():
        return f"p{int(percentile)}"
    return f"p{percentile:.1f}"


def _format_sample_count(value) -> str:
    try:
        count = float(value)
    except (TypeError, ValueError):
        return "n/a"
    if float(count).is_integer():
        return str(int(count))
    return f"{count:.1f}"


def _present(value) -> bool:
    return value not in (None, "")


def _is_truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _effective_volatility_delever_threshold(execution, *, prefix: str):
    mode = str(execution.get(f"{prefix}_threshold_mode") or "").strip().lower()
    dynamic_threshold = execution.get(f"{prefix}_dynamic_threshold")
    if mode == "rolling_percentile" and _present(dynamic_threshold):
        return dynamic_threshold
    return execution.get(f"{prefix}_threshold")


def _format_volatility_delever_threshold_detail(execution, *, prefix: str, translator) -> str:
    mode = str(execution.get(f"{prefix}_threshold_mode") or "").strip().lower()
    fixed_threshold = execution.get(f"{prefix}_threshold")
    dynamic_threshold = execution.get(f"{prefix}_dynamic_threshold")
    if mode == "rolling_percentile":
        kwargs = {
            "percentile": _format_percentile(execution.get(f"{prefix}_dynamic_percentile")),
            "lookback": _format_sample_count(execution.get(f"{prefix}_dynamic_lookback")),
            "min_periods": _format_sample_count(execution.get(f"{prefix}_dynamic_min_periods")),
            "sample_count": _format_sample_count(execution.get(f"{prefix}_dynamic_sample_count")),
            "floor": _format_percent(execution.get(f"{prefix}_dynamic_floor")),
            "cap": _format_percent(execution.get(f"{prefix}_dynamic_cap")),
            "fixed_threshold": _format_percent(fixed_threshold),
        }
        if _present(dynamic_threshold):
            return translator("blend_gate_volatility_threshold_detail_dynamic", **kwargs)
        return translator("blend_gate_volatility_threshold_detail_dynamic_fallback", **kwargs)
    return translator(
        "blend_gate_volatility_threshold_detail_fixed",
        threshold=_format_percent(fixed_threshold),
    )


def _build_tqqq_risk_control_lines(execution, *, translator) -> list[str]:
    prefix = "dual_drive_volatility_delever"
    if not _is_truthy(execution.get(f"{prefix}_applied")):
        return []
    redirect_symbol = str(execution.get(f"{prefix}_redirect_symbol") or "QQQ").strip().upper()
    window = str(execution.get(f"{prefix}_window") or "5").strip()
    threshold = _effective_volatility_delever_threshold(execution, prefix=prefix)
    threshold_detail = _format_volatility_delever_threshold_detail(
        execution,
        prefix=prefix,
        translator=translator,
    )
    if str(execution.get(f"{prefix}_trigger_reason") or "").strip() == "hysteresis_hold":
        return [
            translator(
                "risk_control_tqqq_volatility_delever_hysteresis_dynamic",
                window=window,
                volatility=_format_percent(execution.get(f"{prefix}_metric")),
                exit_threshold=_format_percent(execution.get(f"{prefix}_exit_threshold")),
                threshold=_format_percent(threshold),
                threshold_detail=threshold_detail,
                source_symbol="TQQQ",
                redirect_symbol=redirect_symbol or "QQQ",
            )
        ]
    return [
        translator(
            "risk_control_tqqq_volatility_delever_applied_dynamic",
            window=window,
            volatility=_format_percent(execution.get(f"{prefix}_metric")),
            threshold=_format_percent(threshold),
            threshold_detail=threshold_detail,
            source_symbol="TQQQ",
            redirect_symbol=redirect_symbol or "QQQ",
        )
    ]


def _format_signal_snapshot_line(snapshot, *, translator) -> str:
    if not isinstance(snapshot, Mapping):
        return ""
    market_date = str(snapshot.get("market_date") or snapshot.get("signal_as_of") or "").strip()
    source = str(snapshot.get("latest_price_source") or "").strip()
    warning = snapshot.get("data_freshness_warning")
    if not market_date and not source and warning in (None, "", False):
        return ""
    if _translator_uses_zh(translator):
        parts = [
            f"日期 {market_date or '未知'}",
            f"数据源 {_localize_price_source_label(source, translator=translator)}",
        ]
        if warning not in (None, "", False):
            parts.append(f"提示 {_localize_notification_text(warning, translator=translator)}")
        return "🧾 信号快照: " + " | ".join(parts)
    parts = [
        f"date {market_date or 'unknown'}",
        f"source {_localize_price_source_label(source, translator=translator)}",
    ]
    if warning not in (None, "", False):
        parts.append(f"warning {warning}")
    return "🧾 Signal snapshot: " + " | ".join(parts)


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
        translator("trade_header"),
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
        lines.append(separator)
        lines.extend(line for line in dashboard.splitlines() if line.strip())
    lines.extend(timing_lines)
    if signal_snapshot_line:
        lines.append(signal_snapshot_line)
    status_summary = _first_detail_line(status_display)
    if status_summary:
        lines.append(_format_market_status_line(status_summary, translator=translator))
    lines.extend(risk_control_lines)
    signal_summary = _first_detail_line(signal_display)
    if signal_summary:
        lines.append(f"🎯 {translator('signal_label')}: {signal_summary}")
    if trade_logs:
        lines.append(separator)
    lines.extend(str(log).strip() for log in trade_logs if str(log).strip())
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
        translator("heartbeat_header"),
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
        lines.append(separator)
        lines.extend(line for line in dashboard.splitlines() if line.strip())
    lines.extend(timing_lines)
    if signal_snapshot_line:
        lines.append(signal_snapshot_line)
    status_summary = _first_detail_line(status_display)
    if status_summary:
        lines.append(_format_market_status_line(status_summary, translator=translator))
    lines.extend(risk_control_lines)
    signal_summary = _first_detail_line(signal_display)
    if signal_summary:
        lines.append(f"🎯 {translator('signal_label')}: {signal_summary}")
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
    dashboard_text = _format_dashboard_text(str(execution["dashboard_text"]), translator=translator)
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
    dashboard_text = _format_dashboard_text(str(execution["dashboard_text"]), translator=translator)
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
