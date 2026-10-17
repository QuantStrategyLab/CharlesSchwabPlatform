"""Compact bilingual digest. Pure rendering; never split one digest into sends."""
from __future__ import annotations

import os
from notification_mode import consolidation_enabled, routine_mode
from display_names import display_name
from daily_digest_notify import DailyDigestInput, _trade_counts, block_reason_text, counts_not_connected, is_blocked


def enabled() -> bool:
    return routine_mode() == "daily_digest"


def _units(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def render(payload: DailyDigestInput, *, max_units: int = 3900) -> str:
    if max_units < 256:
        raise ValueError("digest_budget_too_small")
    if consolidation_enabled():
        return _render_consolidated(payload, max_units=max_units)
    trades, problems = [], []
    for row in payload.runs:
        label = " / ".join(" ".join(str(v).split())[:70] for v in
                           (display_name("platforms", row.platform_id), display_name("accounts", row.account_hint or row.account_scope or row.execution_mode, platform=row.platform_id), display_name("strategies", row.strategy_profile, "zh", fallback=row.strategy_label or row.strategy_profile) + " / " + display_name("strategies", row.strategy_profile, "en", fallback=row.strategy_label or row.strategy_profile)))
        known_orders = row.order_count is not None and row.order_count_status == "known"
        known_fills = row.fill_count is not None and row.fill_count_status == "known"
        if (known_orders and row.order_count > 0) or (known_fills and row.fill_count > 0):
            orders = str(row.order_count) if known_orders else ("未接入统计 / not connected" if counts_not_connected(row) else "?")
            completion = row.execution_status in {"executed", "completed"}
            fill_text = f"成交 Fills {row.fill_count}" if known_fills else (
                "成交未接入统计 / Fill counting not connected" if counts_not_connected(row) else
                "已完成 / Completed" if completion else "成交数量未知 / Fill count unknown"
            )
            trades.append(f"• {label}：订单 Orders {orders} · {fill_text}")
        complete_orders = known_orders and row.order_count > 0 and row.execution_status in {"executed", "completed"}
        if row.status == "alert" or is_blocked(row) or not known_orders or (not known_fills and not complete_orders):
            # Do not expose producer prose, raw exceptions or private identities.
            reason = "执行异常 / Execution issue" if row.status == "alert" and known_orders and known_fills else "执行证据不完整 / Incomplete execution evidence"
            if is_blocked(row):
                reason = "已拦截 / Blocked"
                detail = block_reason_text(row)
                if detail:
                    reason += f" · {detail}"
            if counts_not_connected(row):
                counts = f"{_trade_counts(row, 'zh')} / {_trade_counts(row, 'en')}"
                reason = f"{reason} · {counts}" if is_blocked(row) else counts
            problems.append(f"• {label}：{reason}")
    coverage = payload.evidence_coverage
    if coverage:
        for count, text in (
            (len(coverage.github_failures) + len(coverage.candidate_failures), "来源读取失败 / Source read failure"),
            (len(coverage.stale_candidates), "候选过期 / Stale candidates"),
            (len(coverage.missing_targets), "缺少目标报告 / Target report missing"),
        ):
            if count:
                problems.append(f"• {text} ({count})")
        if not payload.runs and not coverage.is_complete_for_verified_idle and not problems:
            problems.append("• 缺少执行证据 / Execution evidence unavailable")
    elif not payload.runs:
        problems.append("• 缺少执行证据 / Execution evidence unavailable")
    if not trades and not problems:
        return ""
    header = f"每日交易摘要 / Daily trading digest · {payload.business_day}"
    rows = (["需关注 / Diagnostics"] + problems if problems else []) + (["交易 / Trading"] + trades if trades else [])
    lines = [header]
    for i, row in enumerate(rows):
        suffix = f"\n其余 {len(rows) - i} 项已省略 / {len(rows) - i} more items omitted"
        if _units("\n".join(lines + [row]) + suffix) > max_units:
            lines.append(suffix.strip())
            break
        lines.append(row)
    return "\n".join(lines)


def _render_consolidated(payload: DailyDigestInput, *, max_units: int) -> str:
    """One selected language and one message, including healthy zero-order runs."""
    if not payload.runs:
        return ""
    zh = not str(payload.locale).lower().startswith("en")
    summary = payload.notification_summary or {}
    coverage = payload.evidence_coverage
    complete = bool(coverage and coverage.is_complete_for_verified_idle and not summary.get("counts_scope_unknown", summary.get("counts_unknown")))
    lines = [f"每日交易摘要 · {payload.business_day}" if zh else f"Daily trading digest · {payload.business_day}"]
    lines.append(("窗口内运行次数未知" if zh else "Run count in window unknown") if summary.get("run_count_unknown") else
                 f"已收录运行 {len(payload.runs)} 次" if zh else f"Observed runs in window: {len(payload.runs)}")
    counts = []
    for name, label in (("order", "订单" if zh else "Orders"), ("fill", "成交" if zh else "Fills")):
        values = [getattr(row, name + "_count") for row in payload.runs]
        known = complete and all(value is not None and getattr(row, name + "_count_status") == "known"
                                 for row, value in zip(payload.runs, values))
        value = str(sum(values)) if known else ("全窗口统计未知" if zh else "whole-window count unknown")
        counts.append(f"{label}：{value}" if zh else f"{label}: {value}")
    lines.append(" · ".join(counts))
    if complete and all(row.order_count == 0 and row.order_count_status == "known" for row in payload.runs):
        lines.append(("本轮 0 单" if len(payload.runs) == 1 else "窗口内各轮均为 0 单") if zh else
                     ("0 orders this run" if len(payload.runs) == 1 else "0 orders in every observed run"))
    for platform, source in summary.get("source_inventory", {}).items():
        label = display_name("platforms", platform, "zh" if zh else "en")
        expected, covered, runs = source["expected_targets"], source["covered_targets"], source["runs"]
        runs = runs if runs is not None else ("未知" if zh else "unknown")
        state = f"{covered}/{expected}" if expected else ("未配置" if zh else "not configured")
        lines.append(f"{label}：运行 {runs} 次，覆盖 {state}" if zh else f"{label}: {runs} runs, coverage {state}")
    immediate = summary.get("immediate_events", 0)
    if immediate:
        lines.append(f"即时事件 {immediate} 项（仅记录）" if zh else f"Immediate events: {immediate} (record only)")
    for row in payload.runs:
        if not is_blocked(row):
            continue
        detail = block_reason_text(row, "zh" if zh else "en")
        detail = detail or ("已拦截，具体原因未知" if zh else "Blocked; specific reason unknown")
        label = display_name("platforms", row.platform_id, "zh" if zh else "en")
        lines.append(f"• {label}：{detail}" if zh else f"• {label}: {detail}")
    if not complete:
        lines.append("覆盖不完整，缺失统计保留未知" if zh else "Coverage incomplete; missing counts remain unknown")
    result = [lines[0]]
    for index, line in enumerate(lines[1:], 1):
        remaining = len(lines) - index
        suffix = f"其余 {remaining} 项已省略" if zh else f"{remaining} more items omitted"
        if _units("\n".join(result + [line, suffix])) > max_units:
            result.append(suffix)
            break
        result.append(line)
    return "\n".join(result)
