"""Presentation of receipt observations, separate from receipt acceptance."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
import os

from quant_platform_kit.notifications._redaction import redact_sensitive_text
from quant_platform_kit.common.operational_notification_localization import resolve_operational_notification_locale
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, urlunsplit


def render_receipt_alert(*, locale, observations=(), workflow_url="", scheduler_error=False,
                         list_failed=False, missing=False, due_at=None, unconfirmed=False,
                         attention_reason=""):
    zh = str(locale).startswith("zh")
    counts = Counter(item["category"] for item in observations)
    labels = {
        "blocked": ("被策略风控阻断", "blocked by strategy risk"),
        "time_skipped": ("因时间不符跳过", "skipped by schedule time"),
        "identity_skipped": ("因目标身份不符跳过", "skipped by target identity"),
        "unreadable": ("无法读取", "unreadable"),
        "rejected": ("未通过执行回执校验", "rejected by execution receipt checks"),
        "accepted": ("通过执行回执校验", "accepted by execution receipt checks"),
    }
    lines = ["⚠️ 执行回执检查" if zh else "⚠️ Execution receipt check"]
    if scheduler_error:
        lines.append("运行目标调度策略读取失败。" if zh else "Runtime schedule policy could not be read.")
    if list_failed:
        lines.append("执行回执列表读取失败。" if zh else "Execution receipt listing failed.")
    if missing:
        lines.append("缺少可接受的执行回执；保护保持生效。" if zh else
                     "An acceptable execution receipt is missing; protections remain active.")
    if unconfirmed:
        if attention_reason == "order_status_unknown":
            lines.append("订单状态尚待核对；请在券商核对订单及成交记录。" if zh else
                         "Order status needs review; check orders and fills with the broker.")
        else:
            lines.append("运行检查证据待核对；请查看该次运行记录。" if zh else
                         "Runtime check evidence is unconfirmed; inspect the workflow run.")
    for category, (cn, en) in labels.items():
        if counts[category]:
            lines.append(f"{counts[category]} 份{cn}" if zh else f"{counts[category]} report(s) {en}")
    if counts["blocked"]:
        lines.append("运行被阻断不视为成功；具体门禁原因以该次风控记录为准。" if zh else
                     "A blocked run is not accepted as success; inspect its guard record for the cause.")
    relevant = [item["updated"] for item in observations if item.get("updated") and
                item["category"] not in {"time_skipped", "identity_skipped"}]
    for label, stamp in (("latest", max(relevant) if relevant else None), ("due", due_at)):
        if not isinstance(stamp, datetime):
            continue
        value = stamp.astimezone(ZoneInfo("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S") + " SGT"
        title = ("最新相关回执时间" if label == "latest" else "最近应执行时间") if zh else (
            "Latest relevant receipt" if label == "latest" else "Latest scheduled execution")
        lines.append(f"{title}：{value}" if zh else f"{title}: {value}")
    return render_runtime_error(locale=locale, reason="real_error", workflow_url=workflow_url,
        problem="；".join(lines[1:]), occurred_at=None)


def render_runtime_error(*, reason="real_error", locale=None, profile=None,
                         occurred_at=None, workflow_url="", problem=None, action=None):
    """Plan P10 body until the coordinated shared formatter pin is published.

    Only caller-owned localized summaries may be supplied as problem/action;
    provider diagnostics and raw identities are deliberately never interpolated.
    """
    from .display_names import display_name
    zh = resolve_operational_notification_locale(locale or os.getenv("QSL_NOTIFY_LANG") or os.getenv("NOTIFY_LANG")) == "zh"
    lang = "zh" if zh else "en"
    stamp = occurred_at
    if isinstance(stamp, str):
        try:
            stamp = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError:
            stamp = None
    time = "未知" if zh else "unknown"
    if isinstance(stamp, datetime) and stamp.tzinfo is not None:
        time = stamp.astimezone(ZoneInfo("Asia/Singapore")).strftime("%Y-%m-%d %H:%M:%S SGT")
    link = "运行链接不可用" if zh else "Run link unavailable"
    try:
        parts = urlsplit(str(workflow_url or ""))
        # Do not turn credential-bearing URLs into apparently safe evidence.
        if parts.scheme == "https" and (parts.hostname in {"github.com", "example.invalid", "example.test"} or (parts.hostname or "").endswith(".example.invalid")) and not parts.username and not parts.password and not parts.query and not parts.fragment:
            link = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    except ValueError:
        pass
    problems = {
        "coverage": ("Schwab 日报候选记录未确认，日报可能缺少本次记录", "Schwab daily digest candidate record is unconfirmed; the digest may omit this record (coverage unknown)"),
        "runtime_guard": ("运行监控发现错误或证据缺失", "Runtime monitoring found an error or missing evidence"),
        "auth_failure": ("认证失败", "Authentication failed"),
        "blocked_run": ("本轮执行被阻断", "Execution blocked"),
        "order_status_unknown": ("订单状态尚待核对", "Order status needs review"),
        "real_error": ("运行发生错误，具体原因待核对", "Runtime error; the cause requires review"),
    }
    description = problem or problems.get(reason, problems["real_error"])[0 if zh else 1]
    strategy = display_name("strategies", profile, lang, fallback="策略未核实" if zh else "Strategy unverified")
    next_step = action or ("查看本次运行记录，核实输入和执行状态；未恢复前保持执行保护" if zh else
                           "Inspect this run and verify inputs and execution state; retain execution protections")
    lines = (["【资产管理｜运行错误】", f"时间：{time}", f"平台 / 策略：Schwab / {strategy}",
              f"问题：{description}", "影响：是否提交订单：未知；是否有待处理订单：未知",
              f"证据：Schwab 运行记录；{link}", f"处理：{next_step}", "事件：运行错误｜投递状态以回执为准"] if zh else
             ["[Asset management | Runtime error]", f"Time: {time}", f"Platform / strategy: Schwab / {strategy}",
              f"Problem: {description}", "Impact: Order submission: unknown; pending orders: unknown",
              f"Evidence: Schwab run record; {link}", f"Action: {next_step}", "Event: runtime error | Delivery status depends on receipts"])
    return redact_sensitive_text("\n".join(lines))
