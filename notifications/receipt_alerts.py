"""Presentation of receipt observations, separate from receipt acceptance."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, urlunsplit


def render_receipt_alert(*, locale, observations=(), workflow_url="", scheduler_error=False,
                         list_failed=False, missing=False, due_at=None, unconfirmed=False):
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
    if workflow_url:
        link = urlsplit(str(workflow_url))
        if link.scheme == "https" and link.hostname and not link.username and not link.password:
            lines.extend(["工作流运行记录：" if zh else "Workflow run:", urlunsplit((link.scheme, link.netloc, link.path, "", ""))])
    return "\n".join(lines)
