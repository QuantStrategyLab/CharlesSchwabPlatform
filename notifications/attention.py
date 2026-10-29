"""Local four-line rendering; shared attention decisions and identities stay intact."""

from __future__ import annotations

import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from quant_platform_kit.common.operational_notification_localization import (
    localize_attention_reason_code,
    operational_notification_text,
    resolve_operational_notification_locale,
)

from .display_names import display_name


def localize_guard_reason(lang, code):
    """Use Schwab's verified baseline wording; preserve unknown reason codes."""
    if code == "DAILY_LOSS_LIMIT_EXCEEDED":
        return ("相对基准净值的下降达到配置上限" if lang == "zh" else
                "The decline relative to the equity baseline reached the configured limit")
    return localize_attention_reason_code(lang, code)


def render_attention(*, decision, platform, account_alias, strategy_profile, guard_reason_codes=(), locale=None, daily_loss_context=None):
    lang = resolve_operational_notification_locale(locale or os.getenv("QSL_NOTIFY_LANG") or os.getenv("NOTIFY_LANG"))
    zh = lang == "zh"
    platform_name = display_name("platforms", platform, lang, fallback="交易平台" if zh else "Trading platform")
    strategy = display_name("strategies", strategy_profile, lang, fallback="策略" if zh else "Strategy")
    account = display_name("accounts", account_alias, lang, platform=platform, fallback="账户" if zh else "Account")
    # The general state belongs on the result line, never as a fabricated cause.
    reasons = tuple(dict.fromkeys(code for code in guard_reason_codes if code != "new_risk_prohibited"))
    if not reasons:
        reasons = tuple(code for code in decision.reason_codes if code != "new_risk_prohibited")
    descriptions = []
    for code in reasons:
        text = localize_guard_reason(lang, code)
        if text == code:
            continue
        # Existing helpers include English severity tokens in the Chinese text.
        if zh:
            text = re.sub(r"[（(](?:CRITICAL|REVIEW|WATCH)[）)]", "", text).strip()
        descriptions.append(text)
    reason = "；".join(descriptions) if zh else "; ".join(descriptions)
    if not reason:
        reason = ("原因尚未识别，请查看该次风控记录" if zh else "Reason unrecognized; inspect this run's guard record") if reasons else (
            "原因未提供" if zh else "Reason not provided")
    reason_line = (f"原因：{reason}" if zh else f"Reason: {reason}") if descriptions else reason
    level = decision.level.value
    key = "attention_result_new_risk_prohibited" if "new_risk_prohibited" in decision.reason_codes else f"attention_result_{level}"
    next_key = decision.next_step_key
    if level == "action" and "DAILY_LOSS_LIMIT_EXCEEDED" in reasons:
        next_key = "review_daily_loss"
    elif level == "action" and "new_risk_prohibited" in decision.reason_codes:
        next_key = "inspect_guard_reason"
    next_line = operational_notification_text(lang, f"attention_next_{next_key}")
    if "DAILY_LOSS_LIMIT_EXCEEDED" in reasons:
        context = daily_loss_context if isinstance(daily_loss_context, dict) else {}
        stamp = context.get("daily_loss_baseline_as_of")
        baseline = None
        if context.get("daily_loss_fact_status") == "verified" and isinstance(stamp, str):
            try:
                parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
                if parsed.tzinfo is not None:
                    baseline = parsed.astimezone(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M")
            except (ValueError, OverflowError):
                pass
        if zh:
            basis = f"基准为 {baseline}（纽约时间）的账户运行快照" if baseline else "基准时间和来源待核对"
            next_line = f"下一步：{basis}；核对资金进出及亏损上限，保持禁止新增风险"
        else:
            basis = f"Baseline: account runtime snapshot at {baseline} New York time" if baseline else "Baseline time and source require review"
            next_line = f"Next: {basis}; review cash flows and the loss limit, and keep new risk prohibited"
    emoji = {"ok": "✅", "watch": "👀", "action": "⚠️", "halt": "🚨"}[level]
    return "\n".join((
        f"{emoji} {platform_name} · {account} · {strategy}",
        operational_notification_text(lang, key),
        reason_line,
        next_line,
    ))


def publish_attention_telegram_transition(**kwargs):
    from quant_platform_kit.risk import attention_notify
    from .unified import enabled, event_for, route, observe_attention_recovery, stage_approved_gate_observation

    approved = kwargs.pop("approved_gate_reasons", [])
    daily_loss_context = kwargs.pop("daily_loss_context", None)
    sender = kwargs.get("telegram_sender") or attention_notify._default_telegram_sender
    text = render_attention(daily_loss_context=daily_loss_context, **{key: kwargs[key] for key in (
        "decision", "platform", "account_alias", "strategy_profile", "guard_reason_codes", "locale",
    ) if key in kwargs})

    def send_rendered(**delivery):
        return sender(**{**delivery, "text": text})

    if not enabled():
        return attention_notify.publish_attention_telegram_transition(**{**kwargs, "telegram_sender": send_rendered})
    decision = kwargs["decision"]
    from .approved_gates import clean_gate_reasons
    approved = clean_gate_reasons(approved)
    approved_only = (approved and set(kwargs.get("guard_reason_codes") or ()) == {"DAILY_LOSS_LIMIT_EXCEEDED"}
        and set(decision.reason_codes) <= {"new_risk_prohibited"} and decision.level.value == "action")
    urgent = decision.level.value in {"action", "halt"}
    reasons = tuple(sorted(set(kwargs.get("guard_reason_codes") or decision.reason_codes)))
    event = event_for("risk_escalation", profile=kwargs["strategy_profile"],
        reason="attention:" + ",".join(reasons))
    if approved_only and stage_approved_gate_observation(profile=kwargs["strategy_profile"], reasons=approved):
        observe_attention_recovery(event)
        return {"sent": 0, "skipped": 1, "failed": 0}
    if not urgent:
        # A non-notifying attention decision only reconciles its incident;
        # the real cycle owns the sole routine_cycle fact and its counts.
        observe_attention_recovery(event)
        return {"sent": 0, "skipped": 1, "failed": 0}
    observed = []

    def publish():
        counts = attention_notify.publish_attention_telegram_transition(**{
            **kwargs, "telegram_sender": send_rendered, "already_sent_keys": [], "record_sent_key": None,
        })
        observed.append(counts)
        return counts.get("sent", 0) > 0

    route(event, legacy=publish, stream="attention", state=decision.level.value,
        severity=decision.level.severity_order, recovered=not urgent)
    return observed[0] if observed else {"sent": 0, "skipped": 1, "failed": 0}
