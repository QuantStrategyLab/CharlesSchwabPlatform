#!/usr/bin/env python3
"""QuantSentinel daily digest message contract (zh/en).

Pure formatter: no Telegram I/O, no secret reads. Aggregators collect
which platforms/strategies actually ran (plus optional observation
fields when producers supply them), then call ``render_daily_digest``
and send via the unified bot secret ``quant-sentinel-telegram-bot-token``.

Observation fields (equity, holdings, signal, rebalance) are rendered
only when present on the evidence row — never invented.

Count semantics:
  - int 0 with known status / legacy producer row → verified zero（无成交）
  - None / field_status counts_unknown → unknown（未知）, never rendered as 0
"""

from __future__ import annotations

from display_names import display_name

import re

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Sequence

DigestLocale = Literal["zh", "en"]
DigestLayout = Literal["v1", "sections_v2"]
DIGEST_LAYOUTS: tuple[str, ...] = ("v1", "sections_v2")
DIGEST_SECTIONS: tuple[str, ...] = ("us_daily", "dca", "crypto_intraday")
RebalanceKind = Literal[
    "",
    "no_rebalance",
    "rebalance",
    "no_order",
    "pending",
]
EvidenceStatus = Literal[
    "verified_idle",
    "evidence_unknown",
    "has_runs",
]
CountFieldStatus = Literal["known", "unknown", ""]
MISSING_EVIDENCE_REASONS = frozenset({
    "EVIDENCE_NOT_FOUND", "EVIDENCE_UNREADABLE", "EVIDENCE_STALE_BUSINESS_DAY", "SOURCE_NOT_CONFIGURED",
})

_TEXTS: dict[str, dict[str, str]] = {
    "zh": {
        "daily_digest_title": "📡 {sender} · 收盘日报",
        "daily_digest_heartbeat_title": "📡 {sender} · 心跳",
        "date_label": "业务日",
        "window_label": "窗口",
        "ran_section": "【当日已运行】",
        "fills_label": "成交",
        "orders_label": "订单",
        "cycles_label": "周期",
        "no_fill": "无成交",
        "trade_count": "{label} {count} 笔",
        "no_orders": "无订单",
        "counts_unknown": "未知",
        "counts_not_connected": "未接入统计",
        "status_ok": "正常",
        "status_alert": "异常",
        "heartbeat_no_run_verified": "今日已验证无平台/策略实际运行。",
        "heartbeat_evidence_unknown": (
            "今日运行证据未知或读取失败，不能判定为链路正常。"
        ),
        "heartbeat_no_fill_verified": "今日有运行且已验证零成交。",
        "heartbeat_fills_unknown": "今日有运行，成交数未知（证据未覆盖）。",
        "heartbeat_orders_unknown": "今日有运行，订单证据缺失（证据未覆盖）。",
        # Legacy keys kept for callers/tests that still reference them.
        "heartbeat_no_run": "今日已验证无平台/策略实际运行。",
        "heartbeat_no_fill": "今日有运行且已验证零成交。",
        "footer": "通道：{sender}（统一 bot）· 仅含当日实际运行项",
        "platform_line": "· {platform} / {strategy} · {detail}",
        "block_heartbeat": "💓 【心跳检测】",
        "block_rebalance": "🔔 【调仓指令】",
        "block_no_order": "⚠️ 【未下单】",
        "block_pending": "⏳ 【待确认】",
        "block_blocked": "⚠️ 【已拦截】",
        "strategy_label": "🧭 策略",
        "equity_label": "💰 账户总权益",
        "holdings_label": "💼 持仓",
        "holdings_scope_strategy_symbols_only": "仅策略标的",
        "holdings_scope_stocks_only": "仅股票",
        "signal_prefix": "🎯 信号",
        "shares_unit": "股",
        "tip_prefix": "小账户提示",
        "conclusion_no_rebalance": "✅ 无需调仓",
        "conclusion_fills": "✅ 成交 {count}",
        "conclusion_no_order_prefix": "未下单",
        "platform_tag": "平台",
        "identity_tag": "身份",
        "stale_candidates_header": "⚠️ 以下账户今天的数据缺失或过期：",
        "no_trades": "无交易",
        "missing_evidence": "数据缺失：{code}（{explanation}）",
        "EVIDENCE_NOT_FOUND": "未找到当天的成交或订单证据",
        "EVIDENCE_UNREADABLE": "证据读取失败或格式无效",
        "EVIDENCE_STALE_BUSINESS_DAY": "证据业务日与日报不一致",
        "SOURCE_NOT_CONFIGURED": "未配置成交或订单证据来源",
        "test_label": "【测试】",
        "guard_recovery": "⚠️ 此前 {count} 次日报因无法确认发送记录而停止发送，请查看工作流回执。",
        "stale_candidate_line": "· {platform} / {strategy}（{detail}）",
        "stale_detail_day": "数据业务日 {day}",
        "stale_detail_undated": "未注明业务日",
        "heartbeat_all_candidates_stale": "今日候选数据均未更新，不能判定为链路正常。",
        "v2_title": "📡 {sender} · 日报 {date}",
        "v2_window": "窗口 {start} → {end}（上海时间）",
        "v2_section_attention": "⚠️ 待处理",
        "v2_stale_header": "以下账户今天的数据缺失或过期：",
        "v2_section_us_daily": "📈 美股日策略（纽约交易日 {day}）",
        "v2_section_dca": "💰 定投",
        "v2_section_crypto": "🪙 加密小时策略",
        "v2_section_coverage": "🧾 覆盖",
        "v2_us_market_closed": "休市",
        "v2_no_evidence": "本窗口无运行证据",
        "v2_attention_alert": "· {tag} {strategy}：状态异常{reason}",
        "v2_attention_status": "· {tag} {strategy}：{detail}",
        "v2_coverage_line": "已收录 {runs} 条运行；候选 {fresh} 条可用、{stale} 条过期已略去",
        "v2_coverage_github_failures": "GitHub 证据读取失败 {count} 处",
        "candidate_source_failures": "⚠️ 候选读取失败 {count} 处；本窗口覆盖不完整。",
        "v2_evidence_unknown": "运行证据未知或读取失败，不能判定为链路正常。",
        "v2_verified_idle": "已验证本窗口无平台/策略实际运行。",
    },
    "en": {
        "daily_digest_title": "📡 {sender} · Daily digest",
        "daily_digest_heartbeat_title": "📡 {sender} · Heartbeat",
        "date_label": "Business day",
        "window_label": "Window",
        "ran_section": "[Ran today]",
        "fills_label": "fills",
        "orders_label": "orders",
        "cycles_label": "cycles",
        "no_fill": "no fills",
        "counts_unknown": "unknown",
        "counts_not_connected": "not connected",
        "trade_count": "{label} {count}",
        "no_orders": "no orders",
        "status_ok": "ok",
        "status_alert": "alert",
        "heartbeat_no_run_verified": (
            "Verified: no platform/strategy actually ran today."
        ),
        "heartbeat_evidence_unknown": (
            "Run evidence is unknown or a source read failed; "
            "monitoring link health is not confirmed."
        ),
        "heartbeat_no_fill_verified": (
            "Runs completed with verified zero fills today."
        ),
        "heartbeat_fills_unknown": (
            "Runs completed but fill counts are unknown (evidence uncovered)."
        ),
        "heartbeat_orders_unknown": "Runs completed but order evidence is missing (evidence uncovered).",
        "heartbeat_no_run": (
            "Verified: no platform/strategy actually ran today."
        ),
        "heartbeat_no_fill": "Runs completed with verified zero fills today.",
        "footer": "Channel: {sender} (unified bot) · only entries that actually ran",
        "platform_line": "· {platform} / {strategy} · {detail}",
        "block_heartbeat": "💓 [Heartbeat]",
        "block_rebalance": "🔔 [Rebalance]",
        "block_no_order": "⚠️ [No order]",
        "block_pending": "⏳ [Pending]",
        "block_blocked": "⚠️ [Blocked]",
        "strategy_label": "🧭 Strategy",
        "equity_label": "💰 Account equity",
        "holdings_label": "💼 Holdings",
        "holdings_scope_strategy_symbols_only": "strategy symbols only",
        "holdings_scope_stocks_only": "stocks only",
        "signal_prefix": "🎯 Signal",
        "shares_unit": "sh",
        "tip_prefix": "Small-account note",
        "conclusion_no_rebalance": "✅ No rebalance needed",
        "conclusion_fills": "✅ fills {count}",
        "conclusion_no_order_prefix": "No order",
        "platform_tag": "Platform",
        "identity_tag": "Identity",
        "stale_candidates_header": "⚠️ These accounts have missing or stale data today:",
        "no_trades": "No trades",
        "missing_evidence": "Data missing: {code} ({explanation})",
        "EVIDENCE_NOT_FOUND": "No fill or order evidence for this business day",
        "EVIDENCE_UNREADABLE": "Evidence could not be read or has an invalid format",
        "EVIDENCE_STALE_BUSINESS_DAY": "Evidence belongs to a different business day",
        "SOURCE_NOT_CONFIGURED": "No fill or order evidence source is configured",
        "test_label": "[TEST]",
        "guard_recovery": "⚠️ {count} earlier digest attempts were blocked because send history could not be verified; check the workflow receipts.",
        "stale_candidate_line": "· {platform} / {strategy} ({detail})",
        "stale_detail_day": "data business day {day}",
        "stale_detail_undated": "no business day",
        "heartbeat_all_candidates_stale": (
            "All candidate data is stale today; monitoring link health is not confirmed."
        ),
        "v2_title": "📡 {sender} · Daily report {date}",
        "v2_window": "Window {start} → {end} (Asia/Shanghai)",
        "v2_section_attention": "⚠️ Needs attention",
        "v2_stale_header": "These accounts have missing or stale data today:",
        "v2_section_us_daily": "📈 US daily strategies (NY session {day})",
        "v2_section_dca": "💰 DCA",
        "v2_section_crypto": "🪙 Crypto intraday",
        "v2_section_coverage": "🧾 Coverage",
        "v2_us_market_closed": "Market closed",
        "v2_no_evidence": "No run evidence in this window",
        "v2_attention_alert": "· {tag} {strategy}: alert{reason}",
        "v2_attention_status": "· {tag} {strategy}: {detail}",
        "v2_coverage_line": "{runs} runs included; candidates {fresh} usable, {stale} stale omitted",
        "v2_coverage_github_failures": "GitHub evidence read failures: {count}",
        "candidate_source_failures": "⚠️ Candidate source read failures: {count}; window coverage is incomplete.",
        "v2_evidence_unknown": "Run evidence is unknown or a source read failed; link health is not confirmed.",
        "v2_verified_idle": "Verified: no platform/strategy actually ran in this window.",
    },
}

UNKNOWN_ACCOUNT_UID = "unknown"
UNKNOWN_TARGET_ID = "unknown"

# Producer machine tokens (snake_case) → locale UI labels.
# Free-form Chinese/English prose is never rewritten — only exact whole-string tokens.
_SIGNAL_TOKEN_DISPLAY: dict[str, dict[str, str]] = {
    "zh": {
        "no_action": "无需操作",
        "no_signal": "无信号",
        "no_rebalance": "无需调仓",
        "no_submission": "未提交",
        "not_due": "未到执行时点",
        "hold": "持有",
        "order_submitted": "订单已提交",
        "order_acknowledged": "券商已确认",
        "partially_filled": "部分成交",
        "filled": "已成交",
        "previewed": "仅预览",
        "blocked": "已拦截",
        "failed": "失败",
        "unknown": "未知",
        "reconciliation_required": "需对账",
        "risk_blocked": "风控拦截",
    },
    "en": {
        "no_action": "No action",
        "no_signal": "No signal",
        "no_rebalance": "No rebalance",
        "no_submission": "No submission",
        "not_due": "Not due",
        "hold": "Hold",
        "order_submitted": "Order submitted",
        "order_acknowledged": "Broker acknowledged",
        "partially_filled": "Partially filled",
        "filled": "Filled",
        "previewed": "Preview only",
        "blocked": "Blocked",
        "failed": "Failed",
        "unknown": "Unknown",
        "reconciliation_required": "Reconciliation required",
        "risk_blocked": "Risk blocked",
    },
}

# Bare conclusion tokens. Empty string → omit line (block title already covers it).
_CONCLUSION_TOKEN_DISPLAY: dict[str, dict[str, str]] = {
    "zh": {
        "no_order": "",
        "no_rebalance": "✅ 无需调仓",
        "pending": "⏳ 待确认",
        "rebalance": "🔔 调仓",
        "filled": "✅ 已成交",
        "partially_filled": "✅ 部分成交",
        "submitted": "已提交",
        "broker_acknowledged": "券商已确认",
        "previewed": "仅预览",
        "blocked": "已拦截",
        "risk_blocked": "风控拦截",
        "failed": "失败",
        "unknown": "未知",
        "reconciliation_required": "需对账",
        "not_due": "未到执行时点",
        "no_action": "无需操作",
        "no_signal": "无信号",
        "no_submission": "未提交",
    },
    "en": {
        "no_order": "",
        "no_rebalance": "✅ No rebalance needed",
        "pending": "⏳ Pending",
        "rebalance": "🔔 Rebalance",
        "filled": "✅ Filled",
        "partially_filled": "✅ Partially filled",
        "submitted": "Submitted",
        "broker_acknowledged": "Broker acknowledged",
        "previewed": "Preview only",
        "blocked": "Blocked",
        "risk_blocked": "Risk blocked",
        "failed": "Failed",
        "unknown": "Unknown",
        "reconciliation_required": "Reconciliation required",
        "not_due": "Not due",
        "no_action": "No action",
        "no_signal": "No signal",
        "no_submission": "No submission",
    },
}


def _localize_producer_token(
    locale: DigestLocale, raw: str, table: Mapping[str, Mapping[str, str]]
) -> str | None:
    """If ``raw`` is exactly a known machine token, return its locale label.

    Returns ``None`` when ``raw`` is free-form prose (caller keeps original).
    A mapped empty string means \"omit this line\".
    """

    text = str(raw or "").strip()
    if not text:
        return ""
    key = text.lower()
    locale_map = table.get(locale) or table.get("zh") or {}
    if key in locale_map:
        return locale_map[key]
    # Also accept when en table defines the token but zh missed it (defensive).
    if key in (table.get("en") or {}) or key in (table.get("zh") or {}):
        return (table.get(locale) or {}).get(key, text)
    return None


def normalize_locale(raw: str | None) -> DigestLocale:
    value = (raw or "zh").strip().lower()
    return "en" if value.startswith("en") else "zh"


def _t(locale: DigestLocale, key: str) -> str:
    return _TEXTS[locale][key].replace("{sender}", display_name("senders", "quant_sentinel", locale))


def _optional_float(raw: Any) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"expected numeric value, got {raw!r}") from exc


def _optional_nonneg_int(raw: Any) -> int | None:
    """Parse an optional non-negative int; None stays unknown."""

    if raw is None or raw == "":
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"expected integer count, got {raw!r}") from exc
    if value < 0:
        raise ValueError("counts must be non-negative")
    return value


def _format_qty(qty: float | None) -> str | None:
    if qty is None:
        return None
    if float(qty).is_integer():
        return str(int(qty))
    text = f"{qty:.6f}".rstrip("0").rstrip(".")
    return text or "0"


def _format_money(value: float, *, currency: str = "USD") -> str:
    code = (currency or "USD").strip().upper() or "USD"
    amount = f"{value:,.2f}"
    if code == "USD":
        return f"${amount}"
    return f"{code} {amount}"


def _format_equity(value: float, *, currency: str = "USD") -> str:
    code = (currency or "USD").strip().upper() or "USD"
    amount = f"{value:,.2f}"
    return f"{code} {amount}"


def _field_status_map(raw: Mapping[str, Any] | None) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, str] = {}
    for key, value in raw.items():
        status = str(value or "").strip().lower()
        if status:
            out[str(key)] = status
    return out


def count_is_unknown(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    parsed: int | None,
) -> bool:
    """True when producer/stub marks the count unknown (not verified zero)."""

    status_map = _field_status_map(
        raw.get("field_status") if isinstance(raw.get("field_status"), Mapping) else None
    )
    status = status_map.get(field_name, "")
    if status in {"unknown", "counts_unknown", "unverified"}:
        return True
    reason = str(raw.get("reason_code") or "").strip().lower()
    if reason in {"counts_unknown", "github_workflow_existence_only"} and field_name in {
        "fill_count",
        "order_count",
    }:
        # Existence-only stubs never assert verified zero fills/orders.
        if field_name not in raw or raw.get(field_name) is None:
            return True
        if status in {"unknown", "counts_unknown", "unverified"}:
            return True
    if field_name not in raw and parsed is None:
        # Absent key with no legacy int → unknown (new producers).
        # Legacy rows that omit the key but lack reason_code are treated as
        # explicit 0 only when filter supplies default via present int.
        return True
    return parsed is None


def resolve_count_field(
    raw: Mapping[str, Any],
    field_name: str,
    *,
    legacy_default_zero: bool = False,
) -> tuple[int | None, CountFieldStatus]:
    """Return (value, status). Legacy int 0 without field_status → known zero."""

    status_map = _field_status_map(
        raw.get("field_status") if isinstance(raw.get("field_status"), Mapping) else None
    )
    status_hint = status_map.get(field_name, "")
    if status_hint in {"unknown", "counts_unknown", "unverified"}:
        return None, "unknown"

    if field_name not in raw:
        reason = str(raw.get("reason_code") or "").strip().lower()
        if reason in {"counts_unknown", "github_workflow_existence_only"}:
            return None, "unknown"
        if legacy_default_zero:
            # Backward compat: old candidates omitting the key meant 0.
            return 0, "known"
        return None, "unknown"

    value = _optional_nonneg_int(raw.get(field_name))
    if value is None:
        return None, "unknown"
    return value, "known"


@dataclass(frozen=True)
class DigestHolding:
    """One holding line when a producer supplies position evidence."""

    symbol: str
    market_value: float | None = None
    quantity: float | None = None
    currency: str = "USD"

    def __post_init__(self) -> None:
        if not str(self.symbol).strip():
            raise ValueError("holding.symbol required")
        if self.market_value is not None and self.market_value < 0:
            raise ValueError("holding.market_value must be non-negative")
        if self.quantity is not None and self.quantity < 0:
            raise ValueError("holding.quantity must be non-negative")


@dataclass(frozen=True)
class DigestRunEntry:
    """One platform/strategy that actually ran in the digest window.

    Optional observation fields are schema-ready for producer evidence.
    Render omits any field that is empty/None — never invents numbers.

    Counts may be None (= unknown). Verified zero is int 0 with known status.
    """

    platform_id: str
    strategy_profile: str
    fill_count: int | None = 0
    order_count: int | None = 0
    cycle_count: int | None = 0
    status: Literal["ok", "alert"] = "ok"
    note: str = ""
    strategy_label: str = ""
    equity: float | None = None
    equity_currency: str = "USD"
    holdings: tuple[DigestHolding, ...] = ()
    # Optional coverage label for holdings (strategy_symbols_only | stocks_only).
    holdings_scope: str = ""
    signal_summary: str = ""
    rebalance_kind: RebalanceKind = ""
    rebalance_conclusion: str = ""
    tips: tuple[str, ...] = ()
    account_hint: str = ""
    opaque_account_uid: str = ""
    target_id: str = ""
    account_scope: str = ""
    execution_mode: str = ""
    fill_count_status: CountFieldStatus = "known"
    order_count_status: CountFieldStatus = "known"
    cycle_count_status: CountFieldStatus = "known"
    reason_code: str = ""
    missing_evidence_reason: str = ""
    evidence_provenance: str = ""
    # B11 optional producer axes (pass-through; central projects when empty).
    execution_status: str = ""
    evidence_persistence_status: str = ""
    evidence_delivery_status: str = ""
    # NOTIFY-04 B1: digest section (us_daily | dca | crypto_intraday); "" = infer.
    section: str = ""
    # One atomic notification observation; never used as count evidence.
    block_reason: Mapping[str, object] | None = None
    source_run_id: str = ""
    approved_gate_reasons: tuple[Mapping[str, object], ...] = ()

    def __post_init__(self) -> None:
        if not str(self.platform_id).strip():
            raise ValueError("platform_id required")
        if not str(self.strategy_profile).strip():
            raise ValueError("strategy_profile required")
        for name, value in (
            ("fill_count", self.fill_count),
            ("order_count", self.order_count),
            ("cycle_count", self.cycle_count),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.equity is not None and self.equity < 0:
            raise ValueError("equity must be non-negative")
        kind = str(self.rebalance_kind or "")
        allowed = {"", "no_rebalance", "rebalance", "no_order", "pending"}
        if kind not in allowed:
            raise ValueError(f"rebalance_kind must be one of {sorted(allowed)}")


@dataclass(frozen=True)
class EvidenceCoverage:
    """Source coverage for distinguishing verified idle vs read failure."""

    candidates_configured: bool = False
    candidates_loaded: bool = False
    candidates_count: int = 0
    github_enabled: bool = False
    github_sources_queried: int = 0
    github_sources_matched: int = 0
    github_failures: tuple[dict[str, Any], ...] = ()
    candidate_failures: tuple[dict[str, Any], ...] = ()
    # NOTIFY-04: candidate rows dropped because their business day did not match
    # the digest business day (or was missing). Each item is a redacted mapping
    # with platform/strategy/day/reason and optional account display hint.
    # Full details remain private; the public projection exports counts only.
    stale_candidates: tuple[dict[str, Any], ...] = ()
    missing_targets: tuple[dict[str, Any], ...] = ()
    suppressed_candidates_count: int = 0
    legacy_candidates_only: bool = False

    @property
    def stale_candidates_count(self) -> int:
        return len(self.stale_candidates)

    @property
    def has_source_failures(self) -> bool:
        return bool(self.github_failures or self.candidate_failures)

    @property
    def is_complete_for_verified_idle(self) -> bool:
        """True only when configured sources were consulted without failure."""

        if self.has_source_failures:
            return False
        # Stale producer data means the producer did not refresh: we cannot
        # claim a verified idle day.
        if self.stale_candidates or self.missing_targets or self.legacy_candidates_only:
            return False
        if self.github_enabled and self.github_sources_queried <= 0:
            return False
        if self.candidates_loaded or (
            self.github_enabled and self.github_sources_queried > 0
        ):
            return True
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidates_configured": self.candidates_configured,
            "candidates_loaded": self.candidates_loaded,
            "candidates_count": self.candidates_count,
            "github_enabled": self.github_enabled,
            "github_sources_queried": self.github_sources_queried,
            "github_sources_matched": self.github_sources_matched,
            "github_failures": list(self.github_failures),
            "candidate_failures": list(self.candidate_failures),
            "has_source_failures": self.has_source_failures,
            "is_complete_for_verified_idle": self.is_complete_for_verified_idle,
            "stale_candidates_count": self.stale_candidates_count,
            "stale_candidates": [dict(item) for item in self.stale_candidates],
            "missing_targets_count": len(self.missing_targets),
            "suppressed_candidates_count": self.suppressed_candidates_count,
        }


@dataclass(frozen=True)
class DailyDigestInput:
    business_day: str
    window_label: str = ""
    locale: DigestLocale | str = "zh"
    runs: Sequence[DigestRunEntry] = field(default_factory=tuple)
    evidence_coverage: EvidenceCoverage | None = None
    evidence_status: EvidenceStatus | str = ""
    # NOTIFY-04 B1: opt-in layout + Shanghai cut-off window (start/end local
    # "YYYY-MM-DD HH:MM" strings, see daily_digest_aggregator.digest_window_for).
    layout: DigestLayout | str = "v1"
    window: Mapping[str, Any] | None = None
    notification_summary: Mapping[str, Any] | None = None


def normalize_layout(raw: str | None) -> DigestLayout:
    value = str(raw or "").strip().lower()
    return "sections_v2" if value in {"sections_v2", "v2", "sections"} else "v1"


def total_fills(runs: Sequence[DigestRunEntry]) -> int | None:
    """Sum known fills. Returns None when any run has unknown fill_count."""

    total = 0
    saw_known = False
    for entry in runs:
        if entry.fill_count is None or entry.fill_count_status == "unknown":
            return None
        saw_known = True
        total += entry.fill_count
    return total if saw_known or not runs else 0


def fills_are_verified_zero(runs: Sequence[DigestRunEntry]) -> bool:
    if not runs:
        return False
    for entry in runs:
        if entry.fill_count is None or entry.fill_count_status == "unknown":
            return False
        if entry.fill_count != 0:
            return False
    return True


def has_known_positive_fills(runs: Sequence[DigestRunEntry]) -> bool:
    """True when any run reports a known fill_count > 0.

    Used for digest title selection so a successful-trade block is not
    hidden behind the heartbeat title when another run's fills are unknown.
    """

    for entry in runs:
        if (
            entry.fill_count is not None
            and entry.fill_count_status != "unknown"
            and entry.fill_count > 0
        ):
            return True
    return False


def has_observation(entry: DigestRunEntry) -> bool:
    """True when producer evidence includes displayable observation fields."""

    return bool(
        entry.equity is not None
        or entry.holdings
        or entry.signal_summary.strip()
        or entry.rebalance_conclusion.strip()
        or entry.rebalance_kind
        or entry.block_reason
        or entry.tips
    )


def observation_score(row: Mapping[str, Any]) -> int:
    """Rank evidence richness for merge (known counts + optional observation)."""

    score = 0
    for field_name in ("fill_count", "order_count", "cycle_count"):
        value, status = resolve_count_field(row, field_name, legacy_default_zero=False)
        if status == "known" and value is not None:
            score += value
    if row.get("equity") is not None and row.get("equity") != "":
        score += 10
    holdings = row.get("holdings") or row.get("positions") or ()
    if isinstance(holdings, (list, tuple)):
        score += min(len(holdings), 8)
    if str(row.get("signal_summary") or "").strip():
        score += 5
    if str(row.get("rebalance_conclusion") or "").strip() or str(
        row.get("rebalance_kind") or ""
    ).strip():
        score += 5
    if str(row.get("strategy_label") or "").strip():
        score += 1
    # Prefer producer rows over github existence stubs.
    provenance = str(row.get("evidence_provenance") or "").strip().lower()
    if provenance in {"candidates", "producer"}:
        score += 20
    if provenance in {"github_workflow_stub", "github_stub"}:
        score -= 5
    return score


def identity_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    """Aggregation identity: platform + strategy + account/target.

    Missing account/target use explicit sentinels. Callers must not freely
    cross-fill observation fields across unknown-identity rows.
    """

    platform_id = str(row.get("platform_id") or "").strip()
    strategy = str(row.get("strategy_profile") or "").strip()
    account_uid = str(row.get("opaque_account_uid") or "").strip()
    target = str(row.get("target_id") or "").strip()
    return (
        platform_id,
        strategy,
        account_uid or UNKNOWN_ACCOUNT_UID,
        target or UNKNOWN_TARGET_ID,
    )


def identity_is_unknown(row: Mapping[str, Any]) -> bool:
    account_uid = str(row.get("opaque_account_uid") or "").strip()
    target = str(row.get("target_id") or "").strip()
    return not account_uid and not target


def _format_count_part(
    locale: DigestLocale,
    *,
    label_key: str,
    value: int | None,
    status: CountFieldStatus,
) -> str | None:
    if status == "unknown" or value is None:
        if label_key == "fills_label":
            return f"{_t(locale, 'fills_label')} {_t(locale, 'counts_unknown')}"
        return f"{_t(locale, label_key)} {_t(locale, 'counts_unknown')}"
    if label_key == "fills_label" and value == 0:
        return _t(locale, "no_fill")
    if value == 0 and label_key != "fills_label":
        return None
    return f"{_t(locale, label_key)} {value}"


def _detail_for(entry: DigestRunEntry, locale: DigestLocale) -> str:
    parts: list[str] = []
    cycle_part = _format_count_part(
        locale,
        label_key="cycles_label",
        value=entry.cycle_count,
        status=entry.cycle_count_status,
    )
    if cycle_part and not (
        entry.cycle_count_status == "known"
        and entry.cycle_count == 0
    ):
        if entry.cycle_count_status == "unknown" or (
            entry.cycle_count is not None and entry.cycle_count > 0
        ):
            parts.append(cycle_part)

    parts.append(_trade_counts(entry, locale))

    parts.append(_t(locale, "status_ok" if entry.status == "ok" else "status_alert"))
    if entry.note.strip():
        parts.append(entry.note.strip())
    return " · ".join(parts)


def _trade_counts(entry: DigestRunEntry, locale: DigestLocale) -> str:
    if (entry.fill_count_status == "unknown" or entry.fill_count is None
            or entry.order_count_status == "unknown" or entry.order_count is None):
        # Keep a partially known count visible without claiming the other is zero.
        known = []
        for label, value, status in (
            ("fills_label", entry.fill_count, entry.fill_count_status),
            ("orders_label", entry.order_count, entry.order_count_status),
        ):
            if status != "unknown" and value is not None and value > 0:
                known.append(_t(locale, "trade_count").format(label=_t(locale, label), count=value))
        if counts_not_connected(entry):
            labels = [_t(locale, label) for label, value, status in (
                ("orders_label", entry.order_count, entry.order_count_status),
                ("fills_label", entry.fill_count, entry.fill_count_status),
            ) if status == "unknown" or value is None]
            separator = "、" if locale == "zh" else "/"
            unknown = f"{separator.join(labels)}：{_t(locale, 'counts_not_connected')}" if locale == "zh" else (
                f"{separator.join(labels)}: {_t(locale, 'counts_not_connected')}"
            )
        else:
            unknown = missing_evidence_text(locale, entry.missing_evidence_reason or entry.reason_code)
        return " / ".join(known + [unknown])
    if entry.fill_count == 0 and entry.order_count == 0:
        return _t(locale, "no_trades")
    parts = []
    for label, value, status in (
        ("fills_label", entry.fill_count, entry.fill_count_status),
        ("orders_label", entry.order_count, entry.order_count_status),
    ):
        if label == "orders_label" and value == 0:
            parts.append(_t(locale, "no_orders"))
        else:
            parts.append(_t(locale, "trade_count").format(label=_t(locale, label), count=value))
    return " / ".join(parts)


def counts_not_connected(entry: DigestRunEntry) -> bool:
    """A known unconnected counter is distinct from unreadable/absent evidence."""
    return (
        entry.platform_id == "schwab"
        and "schwab_fills_not_connected" in entry.reason_code.split("+")
        and entry.missing_evidence_reason in {"", "EVIDENCE_NOT_FOUND"}
        and (entry.fill_count is None or entry.fill_count_status == "unknown"
             or entry.order_count is None or entry.order_count_status == "unknown")
    )


def is_blocked(entry: DigestRunEntry) -> bool:
    return bool(entry.block_reason) or entry.rebalance_conclusion in {"blocked", "risk_blocked"} or entry.signal_summary in {"blocked", "risk_blocked"}


def _parse_block_reason(raw: object) -> dict[str, object] | None:
    """Ignore malformed/unknown producer facts without changing legacy copy."""
    if not isinstance(raw, Mapping):
        return None
    code = raw.get("reason_code")
    if not isinstance(code, str) or code not in {"consecutive_loss_limit_exceeded", "strategy_risk_rejected"}:
        return None
    observed, limit = raw.get("observed_value"), raw.get("limit")
    if code == "consecutive_loss_limit_exceeded":
        if (type(observed) is not int or type(limit) is not int
                or not 0 <= limit < observed <= 999_999_999):
            return None
    elif observed is not None or limit is not None:
        return None
    submitted = raw.get("order_submitted_this_run")
    if submitted is not None and type(submitted) is not bool:
        return None
    return {"reason_code": code, "observed_value": observed, "limit": limit,
            "order_submitted_this_run": submitted}


def block_reason_text(entry: DigestRunEntry, locale: DigestLocale | None = None) -> str:
    """Explicit locale selects one language; default preserves legacy bytes."""
    reason = _parse_block_reason(entry.block_reason) if entry.platform_id in {"schwab", "ibkr"} else None
    if not reason:
        return ""
    if locale is not None:
        parts = []
        if reason["reason_code"] == "consecutive_loss_limit_exceeded":
            observed, limit = reason["observed_value"], reason["limit"]
            parts.append(f"连续亏损 {observed} 次，超过上限 {limit} 次" if locale == "zh" else
                         f"Consecutive losses {observed} exceed limit {limit}")
        else:
            parts.append("策略风控拦截" if locale == "zh" else "Strategy risk rejected")
        if reason["order_submitted_this_run"] is False:
            parts.append("本轮 0 单" if locale == "zh" else "0 orders this run")
        elif reason["order_submitted_this_run"] is True:
            parts.append("本轮已提交订单" if locale == "zh" else "Orders submitted this run")
        return " · ".join(parts)
    parts = []
    if reason["reason_code"] == "consecutive_loss_limit_exceeded":
        observed, limit = reason["observed_value"], reason["limit"]
        parts.append(f"连续亏损 {observed} 次 > 上限 {limit} 次 / Consecutive losses {observed} > limit {limit}")
    elif entry.platform_id == "ibkr":
        parts.append("策略风控拦截 / Strategy risk rejected")
    if reason["order_submitted_this_run"] is False:
        parts.append("本轮未下单 / No orders submitted this run")
    elif reason["order_submitted_this_run"] is True:
        parts.append("本轮已提交订单 / Orders submitted this run")
    return " · ".join(parts)


def _counts_notice(payload: DailyDigestInput, locale: DigestLocale) -> str:
    unknown = [entry for entry in payload.runs if entry.fill_count is None or entry.fill_count_status == "unknown"
               or entry.order_count is None or entry.order_count_status == "unknown"]
    coverage = payload.evidence_coverage
    if unknown and all(counts_not_connected(entry) for entry in unknown) and not (
        coverage and (coverage.has_source_failures or coverage.stale_candidates or coverage.missing_targets)
    ):
        # Each account already names its unconnected counters.
        return ""
    if global_total_fills(payload) is None:
        return _t(locale, "heartbeat_fills_unknown")
    return _t(locale, "heartbeat_orders_unknown") if unknown else ""


def missing_evidence_text(locale: DigestLocale, reason: str = "") -> str:
    code = reason if reason in MISSING_EVIDENCE_REASONS else "EVIDENCE_NOT_FOUND"
    return _t(locale, "missing_evidence").format(code=code, explanation=_t(locale, code))


def _source_missing_reason(coverage: EvidenceCoverage | None) -> str:
    if coverage is not None and coverage.has_source_failures:
        return "EVIDENCE_UNREADABLE"
    return "EVIDENCE_NOT_FOUND" if coverage is not None and coverage.candidates_configured else "SOURCE_NOT_CONFIGURED"


def label_delivery_text(text: str, locale: str, *, test: bool = False, previous_failures: int = 0) -> str:
    loc = normalize_locale(locale)
    prefix = [_t(loc, "test_label")] if test else []
    if previous_failures:
        prefix.append(_t(loc, "guard_recovery").format(count=previous_failures))
    return "\n".join(prefix + [text])


def _block_title(entry: DigestRunEntry, locale: DigestLocale) -> str:
    if is_blocked(entry):
        return _t(locale, "block_blocked")
    kind = entry.rebalance_kind or ""
    if kind == "rebalance":
        return _t(locale, "block_rebalance")
    if kind == "no_order":
        return _t(locale, "block_no_order")
    if kind == "pending":
        return _t(locale, "block_pending")
    if kind == "no_rebalance":
        return _t(locale, "block_heartbeat")
    fill = entry.fill_count if entry.fill_count_status != "unknown" else None
    order = entry.order_count if entry.order_count_status != "unknown" else None
    if (fill is not None and fill > 0) or (order is not None and order > 0):
        return _t(locale, "block_rebalance")
    return _t(locale, "block_heartbeat")


_SCOPE_TAG_ALLOWED = frozenset({"live", "paper", "shadow", "research"})
_HEX_HINT_RE = re.compile(r"^[0-9a-fA-F]{12,}$")
# Broker account ids like IBKR ``U00000008`` / ``u00000008``.
_BROKER_ACCOUNT_ID_RE = re.compile(r"^[Uu]\d{5,}$")
# Scope forms that embed the broker id: ``live-u00000008``, ``paper_U00000009``.
_SCOPE_EMBEDDED_ACCOUNT_RE = re.compile(
    r"^(?:live|paper|shadow|research)[_-]([Uu]\d{5,})$",
    re.IGNORECASE,
)


def _normalize_scope_tag(raw: str) -> str:
    """Return lowercase live/paper/shadow/research when value looks like a mode."""

    value = str(raw or "").strip().lower()
    if not value:
        return ""
    # Accept plain mode or prefixed forms like live_us / paper-us.
    if value in _SCOPE_TAG_ALLOWED:
        return value
    for mode in _SCOPE_TAG_ALLOWED:
        if value == mode or value.startswith(f"{mode}_") or value.startswith(f"{mode}-"):
            return mode
    return ""


def _normalize_broker_account_id(raw: str) -> str:
    """Return ``U######`` when ``raw`` is a broker account id (IBKR-style).

    Existing ops config uses uppercase ``U``; lowercase input is normalized.
    """

    value = str(raw or "").strip()
    if not value:
        return ""
    if _BROKER_ACCOUNT_ID_RE.fullmatch(value):
        return "U" + value[1:]
    match = _SCOPE_EMBEDDED_ACCOUNT_RE.fullmatch(value)
    if match:
        acc = match.group(1)
        return "U" + acc[1:]
    return ""


def _human_account_hint(raw: str) -> str:
    """Return account_hint when it is already a good human label.

    Reject empty values and opaque hex hashes (those are not display tags).
    IBKR-style ids such as ``U00000009`` are kept (normalized to ``U######``).
    """

    hint = str(raw or "").strip()
    if not hint:
        return ""
    if _HEX_HINT_RE.fullmatch(hint):
        return ""
    broker = _normalize_broker_account_id(hint)
    if broker:
        return broker
    return hint


def _account_label(entry: DigestRunEntry) -> str:
    """Resolve the account half of ``[{platform_id} {account_label}]``.

    Preference (universal):
      1. human ``account_hint`` / broker id (e.g. IBKR ``U00000008``)
      2. broker id embedded in ``account_scope`` (``live-u00000008``)
      3. broker id on ``opaque_account_uid`` when it is already ``U######``
      4. normalized scope ``live|paper|shadow|research``
      5. short ``opaque_account_uid`` / ``target_id`` last resorts
    """

    hint = _human_account_hint(entry.account_hint)
    if hint:
        return hint
    for raw in (entry.account_scope, entry.execution_mode, entry.opaque_account_uid):
        broker = _normalize_broker_account_id(raw)
        if broker:
            return broker
    for raw in (entry.account_scope, entry.execution_mode):
        scope = _normalize_scope_tag(raw)
        if scope:
            return scope
    uid = str(entry.opaque_account_uid or "").strip()
    if uid and uid.lower() != UNKNOWN_ACCOUNT_UID:
        if len(uid) > 12:
            return uid[:8]
        return uid
    target = str(entry.target_id or "").strip()
    if target and target.lower() != UNKNOWN_TARGET_ID:
        return target
    return ""


def account_block_tag(entry: DigestRunEntry) -> str:
    """Account tag body for ``[tag]`` block titles.

    Universal form: ``{platform_id} {account_label}`` — e.g. ``ibkr U00000008``,
    ``schwab live``, ``firstrade paper``. Never hard-code a single venue.
    Returns empty string when nothing usable is present.
    """

    platform = str(entry.platform_id or "").strip()
    label = display_name("accounts", _account_label(entry), platform=entry.platform_id)
    if not label:
        return ""
    if not platform:
        return label
    # Avoid ``ibkr ibkr U…`` if label already carries the platform prefix.
    prefix = platform.lower() + " "
    if label.lower().startswith(prefix):
        return label
    return f"{platform} {label}"


def _display_strategy(entry: DigestRunEntry, locale="zh") -> str:
    return display_name("strategies", entry.strategy_profile, locale, fallback=entry.strategy_label.strip() or entry.strategy_profile)


_HOLDINGS_SCOPES = frozenset({"strategy_symbols_only", "stocks_only"})


def _parse_holdings_scope(raw: Any) -> str:
    """Accept only known coverage tokens; anything else renders without a label."""

    text = str(raw or "").strip()
    return text if text in _HOLDINGS_SCOPES else ""


def _holdings_header(scope: str, locale: DigestLocale) -> str:
    label = _t(locale, "holdings_label")
    if scope in _HOLDINGS_SCOPES:
        scope_text = _t(locale, f"holdings_scope_{scope}")
        if locale == "en":
            return f"{label} ({scope_text})"
        return f"{label}（{scope_text}）"
    return label


def _holding_line(holding: DigestHolding, locale: DigestLocale) -> str:
    symbol = holding.symbol.strip()
    pieces: list[str] = []
    if holding.market_value is not None:
        pieces.append(
            _format_money(holding.market_value, currency=holding.currency)
        )
    qty = _format_qty(holding.quantity)
    if qty is not None:
        pieces.append(f"{qty}{_t(locale, 'shares_unit')}")
    if pieces:
        return f"- {symbol}: {' / '.join(pieces)}"
    return f"- {symbol}"


def _default_conclusion(entry: DigestRunEntry, locale: DigestLocale) -> str:
    raw = entry.rebalance_conclusion.strip()
    if raw:
        localized = _localize_producer_token(locale, raw, _CONCLUSION_TOKEN_DISPLAY)
        if localized is not None:
            # Bare machine token: use mapped label (may be "" to omit).
            if localized:
                return localized
            # Empty mapping → fall through to kind-based defaults.
        else:
            # Free-form producer prose (often already zh) — keep as-is.
            return raw
    kind = entry.rebalance_kind or ""
    if kind == "no_rebalance":
        return _t(locale, "conclusion_no_rebalance")
    # Trade-success fallback when producer left conclusion empty but counts known.
    fill = entry.fill_count if entry.fill_count_status != "unknown" else None
    if fill is not None and fill > 0:
        return _t(locale, "conclusion_fills").format(count=fill)
    return ""


def _render_observation_block(entry: DigestRunEntry, locale: DigestLocale) -> list[str]:
    lines: list[str] = []
    title = _block_title(entry, locale)
    tag = account_block_tag(entry)
    if tag:
        lines.append(f"[{tag}] {title}")
    else:
        lines.append(title)
    lines.append(f"{_t(locale, 'strategy_label')}: {_display_strategy(entry, locale)}")
    if entry.equity is not None:
        lines.append(
            f"{_t(locale, 'equity_label')}: "
            f"{_format_equity(entry.equity, currency=entry.equity_currency)}"
        )
    if entry.holdings:
        lines.append(_holdings_header(entry.holdings_scope, locale))
        for holding in entry.holdings:
            lines.append(_holding_line(holding, locale))
    if entry.signal_summary.strip() and not (is_blocked(entry) and entry.signal_summary in {"blocked", "risk_blocked"}):
        signal_raw = entry.signal_summary.strip()
        signal_display = _localize_producer_token(
            locale, signal_raw, _SIGNAL_TOKEN_DISPLAY
        )
        if signal_display is None:
            signal_display = signal_raw
        if signal_display:
            lines.append(
                f"- {_t(locale, 'signal_prefix')}: {signal_display}"
            )
    for tip in entry.tips:
        tip_text = str(tip).strip()
        if tip_text:
            lines.append(f"- {_t(locale, 'tip_prefix')}: {tip_text}")
    lines.append(_trade_counts(entry, locale))
    from notification_mode import consolidation_enabled
    reason = block_reason_text(entry, locale if consolidation_enabled() else None)
    if reason:
        lines.append(reason)
    if entry.cycle_count_status == "unknown" or entry.cycle_count is None:
        lines.append(_format_count_part(locale, label_key="cycles_label", value=entry.cycle_count,
                                       status=entry.cycle_count_status))
    conclusion = _default_conclusion(entry, locale)
    if conclusion and entry.rebalance_conclusion != "blocked":
        lines.append(conclusion)
    lines.append(f"{_t(locale, 'platform_tag')}: {entry.platform_id}")
    return lines


def _render_run_line(entry: DigestRunEntry, locale: DigestLocale) -> list[str]:
    if has_observation(entry):
        return _render_observation_block(entry, locale)
    return [
        _t(locale, "platform_line").format(
            platform=entry.platform_id,
            strategy=_display_strategy(entry, locale),
            detail=_detail_for(entry, locale),
        )
    ]


def resolve_evidence_status(
    payload: DailyDigestInput,
) -> EvidenceStatus:
    if payload.evidence_status in {
        "verified_idle",
        "evidence_unknown",
        "has_runs",
    }:
        return payload.evidence_status  # type: ignore[return-value]
    runs = tuple(payload.runs)
    if runs:
        return "has_runs"
    coverage = payload.evidence_coverage
    if coverage is not None and coverage.is_complete_for_verified_idle:
        return "verified_idle"
    # Safer default: empty without proven coverage ≠ verified idle.
    return "evidence_unknown"


def global_total_fills(payload: DailyDigestInput) -> int | None:
    """An all-source total requires complete, current evidence; keep run facts."""
    coverage = payload.evidence_coverage
    if coverage is not None and (
        coverage.has_source_failures or coverage.stale_candidates or coverage.missing_targets or coverage.legacy_candidates_only
    ):
        return None
    if not payload.runs and resolve_evidence_status(payload) != "verified_idle":
        return None
    return total_fills(payload.runs)


def _render_stale_candidates(
    coverage: EvidenceCoverage | None, locale: DigestLocale
) -> list[str]:
    """Localized note listing candidate rows dropped as stale (NOTIFY-04)."""

    items = tuple(coverage.missing_targets or coverage.stale_candidates) if coverage is not None else ()
    if not items:
        return []
    lines = [_t(locale, "stale_candidates_header")]
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        platform = str(item.get("platform_id") or "").strip() or "unknown"
        strategy = (
            str(item.get("strategy_label") or "").strip()
            or str(item.get("strategy_profile") or "").strip()
            or "unknown"
        )
        strategy = display_name("strategies", item.get("strategy_profile"), locale, fallback=strategy)
        day = str(item.get("candidate_business_day") or "").strip()
        detail = (
            _t(locale, "stale_detail_day").format(day=day)
            if day
            else _t(locale, "stale_detail_undated")
        )
        reason = str(item.get("reason_code") or "")
        code = reason if reason in MISSING_EVIDENCE_REASONS else "EVIDENCE_STALE_BUSINESS_DAY"
        detail = missing_evidence_text(locale, code) + (f"; {detail}" if "candidate_business_day" in item else "")
        hint = str(item.get("account_hint") or "").strip()
        if hint:
            strategy = f"{strategy} [{hint}]"
        key = (platform, strategy, detail)
        if key in seen:
            continue
        seen.add(key)
        lines.append(
            _t(locale, "stale_candidate_line").format(
                platform=platform, strategy=strategy, detail=detail
            )
        )
    return lines


def render_daily_digest(payload: DailyDigestInput) -> str:
    """Render one Telegram message body (never includes tokens or chat ids).

    Omits schedule window and channel footer lines (product preference).
    Multiple observation blocks are separated by a blank line.
    """

    if normalize_layout(str(payload.layout)) == "sections_v2":
        return render_daily_digest_sections(payload)
    locale = normalize_locale(str(payload.locale))
    runs = tuple(payload.runs)
    lines: list[str] = []
    rich = any(has_observation(entry) for entry in runs)
    evidence_status = resolve_evidence_status(payload)
    known_fills = global_total_fills(payload)
    coverage = payload.evidence_coverage
    candidate_failure_line = (
        _t(locale, "candidate_source_failures").format(count=len(coverage.candidate_failures))
        if coverage is not None and coverage.candidate_failures else ""
    )

    if not runs:
        lines.append(_t(locale, "daily_digest_heartbeat_title"))
        lines.append(f"{_t(locale, 'date_label')}: {payload.business_day}")
        stale_lines = _render_stale_candidates(payload.evidence_coverage, locale)
        if evidence_status == "verified_idle":
            lines.append(_t(locale, "heartbeat_no_run_verified"))
        elif stale_lines:
            lines.append(_t(locale, "heartbeat_all_candidates_stale"))
        else:
            lines.append(_t(locale, "heartbeat_evidence_unknown"))
        if evidence_status != "verified_idle" and not stale_lines:
            lines.append(missing_evidence_text(locale, _source_missing_reason(coverage)))
        if stale_lines:
            lines.append("")
            lines.extend(stale_lines)
        if candidate_failure_line:
            lines.append(candidate_failure_line)
        return "\n".join(lines)

    # Prefer 收盘日报 when any run has known fills > 0, even if other runs'
    # fill counts are unknown (mixed multi-platform digest).
    if has_known_positive_fills(runs) or (known_fills is not None and known_fills > 0):
        lines.append(_t(locale, "daily_digest_title"))
    else:
        lines.append(_t(locale, "daily_digest_heartbeat_title"))
    lines.append(f"{_t(locale, 'date_label')}: {payload.business_day}")
    counts_missing = any(entry.order_count_status == "unknown" or entry.order_count is None for entry in runs)
    notice = _counts_notice(payload, locale)
    if notice:
        lines.append(notice)
    if not rich:
        if known_fills is None or counts_missing:
            lines.append(_t(locale, "ran_section"))
        elif known_fills == 0:
            lines.append(_t(locale, "heartbeat_no_fill_verified"))
            lines.append(_t(locale, "ran_section"))
        else:
            lines.append(_t(locale, "ran_section"))

    for index, entry in enumerate(runs):
        block = _render_run_line(entry, locale)
        # Separate multi-platform / multi-run blocks for readability.
        # Observation blocks always get a blank line; thin list rows stay compact.
        if index > 0 and (
            has_observation(entry)
            or has_observation(runs[index - 1])
            or rich
        ):
            lines.append("")
        lines.extend(block)
    stale_lines = _render_stale_candidates(payload.evidence_coverage, locale)
    if stale_lines:
        lines.append("")
        lines.extend(stale_lines)
    if candidate_failure_line:
        lines.append("")
        lines.append(candidate_failure_line)
    return "\n".join(lines)


def _entry_section(entry: DigestRunEntry) -> str:
    return entry.section if entry.section in DIGEST_SECTIONS else "us_daily"


def _render_section_runs(entries: Sequence[DigestRunEntry], locale: DigestLocale) -> list[str]:
    lines: list[str] = []
    for index, entry in enumerate(entries):
        if index > 0:
            lines.append("")
        lines.extend(_render_run_line(entry, locale))
    return lines


def _is_weekend(day: str) -> bool:
    from datetime import date as _date

    try:
        return _date.fromisoformat(day).weekday() >= 5
    except ValueError:
        return False


def render_daily_digest_sections(payload: DailyDigestInput) -> str:
    """NOTIFY-04 B1 layout: one report, four sections, Shanghai cut-off window.

    Sections: ⚠️ attention → 📈 US daily → 💰 DCA → 🪙 crypto intraday → 🧾
    coverage. Empty DCA / crypto sections are omitted (they only appear on days
    they ran); the US section says market-closed on NY weekends and "no run
    evidence" otherwise. Numbers only come from evidence (same rules as v1).
    """

    locale = normalize_locale(str(payload.locale))
    runs = tuple(payload.runs)
    window = dict(payload.window or {})
    coverage = payload.evidence_coverage
    evidence_status = resolve_evidence_status(payload)
    report_date = str(window.get("report_date") or payload.business_day)
    lines = [_t(locale, "v2_title").format(date=report_date)]
    if window.get("start_local") and window.get("end_local"):
        lines.append(
            _t(locale, "v2_window").format(
                start=window["start_local"], end=window["end_local"]
            )
        )

    if runs:
        notice = _counts_notice(payload, locale)
        if notice:
            lines.append(notice)

    attention: list[str] = []
    if coverage is not None and coverage.candidate_failures:
        attention.append(_t(locale, "candidate_source_failures").format(count=len(coverage.candidate_failures)))
    for entry in runs:
        if entry.status == "alert":
            if is_blocked(entry) or counts_not_connected(entry):
                detail = _default_conclusion(entry, locale) if is_blocked(entry) else ""
                from notification_mode import consolidation_enabled
                reason = block_reason_text(entry, locale if consolidation_enabled() else None)
                if reason:
                    detail = f"{detail} · {reason}" if detail else reason
                if counts_not_connected(entry):
                    detail = " · ".join(filter(None, (detail, _trade_counts(entry, locale))))
                attention.append(_t(locale, "v2_attention_status").format(
                    tag=account_block_tag(entry), strategy=_display_strategy(entry, locale), detail=detail,
                ))
                continue
            reason = f"（{entry.reason_code}）" if locale == "zh" and entry.reason_code else (
                f" ({entry.reason_code})" if entry.reason_code else ""
            )
            attention.append(
                _t(locale, "v2_attention_alert").format(
                    tag=account_block_tag(entry),
                    strategy=_display_strategy(entry, locale),
                    reason=reason,
                )
            )
    stale_lines = _render_stale_candidates(coverage, locale)
    if not runs and evidence_status != "verified_idle":
        attention.append(
            _t(locale, "heartbeat_all_candidates_stale")
            if stale_lines
            else _t(locale, "v2_evidence_unknown")
        )
        if not stale_lines:
            attention.append(missing_evidence_text(locale, _source_missing_reason(coverage)))
    if stale_lines:
        attention.append(_t(locale, "v2_stale_header"))
        attention.extend(stale_lines[1:])
    if attention:
        lines.append("")
        lines.append(_t(locale, "v2_section_attention"))
        lines.extend(attention)

    grouped: dict[str, list[DigestRunEntry]] = {key: [] for key in DIGEST_SECTIONS}
    for entry in runs:
        grouped[_entry_section(entry)].append(entry)

    lines.append("")
    lines.append(_t(locale, "v2_section_us_daily").format(day=payload.business_day))
    if grouped["us_daily"]:
        lines.extend(_render_section_runs(grouped["us_daily"], locale))
    elif _is_weekend(payload.business_day):
        lines.append(_t(locale, "v2_us_market_closed"))
    elif evidence_status == "verified_idle":
        lines.append(_t(locale, "v2_verified_idle"))
    else:
        lines.append(_t(locale, "v2_no_evidence"))

    for key, title_key in (("dca", "v2_section_dca"), ("crypto_intraday", "v2_section_crypto")):
        if grouped[key]:
            lines.append("")
            lines.append(_t(locale, title_key))
            lines.extend(_render_section_runs(grouped[key], locale))

    lines.append("")
    lines.append(_t(locale, "v2_section_coverage"))
    lines.append(
        _t(locale, "v2_coverage_line").format(
            runs=len(runs),
            fresh=coverage.candidates_count if coverage is not None else 0,
            stale=coverage.stale_candidates_count if coverage is not None else 0,
        )
    )
    if coverage is not None and coverage.github_failures:
        lines.append(
            _t(locale, "v2_coverage_github_failures").format(
                count=len(coverage.github_failures)
            )
        )
    if coverage is not None and coverage.candidate_failures:
        lines.append(_t(locale, "candidate_source_failures").format(count=len(coverage.candidate_failures)))
    return "\n".join(lines)


def _parse_holdings(raw: Any) -> tuple[DigestHolding, ...]:
    if raw is None or raw == "":
        return ()
    if not isinstance(raw, (list, tuple)):
        raise ValueError("holdings must be a list")
    out: list[DigestHolding] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ValueError("each holding must be an object")
        symbol = str(item.get("symbol") or "").strip()
        if not symbol:
            continue
        currency = str(item.get("currency") or item.get("ccy") or "USD").strip() or "USD"
        qty_raw = item.get("quantity", item.get("qty", item.get("shares")))
        value_raw = item.get(
            "market_value", item.get("value", item.get("market_value_usd"))
        )
        out.append(
            DigestHolding(
                symbol=symbol,
                market_value=_optional_float(value_raw),
                quantity=_optional_float(qty_raw),
                currency=currency,
            )
        )
    return tuple(out)


def _parse_tips(raw: Any) -> tuple[str, ...]:
    if raw is None or raw == "":
        return ()
    if isinstance(raw, str):
        text = raw.strip()
        return (text,) if text else ()
    if isinstance(raw, (list, tuple)):
        tips = [str(item).strip() for item in raw if str(item).strip()]
        return tuple(tips)
    raise ValueError("tips must be a string or list")


def _parse_rebalance_kind(raw: Any) -> RebalanceKind:
    value = str(raw or "").strip().lower()
    if not value:
        return ""
    aliases = {
        "no_rebalance": "no_rebalance",
        "none": "no_rebalance",
        "heartbeat": "no_rebalance",
        "rebalance": "rebalance",
        "order": "rebalance",
        "no_order": "no_order",
        "skipped": "no_order",
        "pending": "pending",
        "pending_submit": "pending",
    }
    kind = aliases.get(value, value)
    allowed: set[str] = {"", "no_rebalance", "rebalance", "no_order", "pending"}
    if kind not in allowed:
        raise ValueError(f"unsupported rebalance_kind: {raw!r}")
    return kind  # type: ignore[return-value]


def _parse_actually_ran(raw: Mapping[str, Any]) -> bool | None:
    """Return True/False when key present; None when absent (do not default True)."""

    if "actually_ran" not in raw:
        return None
    value = raw.get("actually_ran")
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"unsupported actually_ran value: {value!r}")


def _parse_section(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    return value if value in DIGEST_SECTIONS else ""


def filter_runs_for_digest(
    candidates: Sequence[Mapping[str, object]],
) -> list[DigestRunEntry]:
    """Keep only rows with explicit ``actually_ran=True``.

    Absent ``actually_ran`` is treated as incomplete evidence and excluded
    (no longer defaults to True). Optional observation keys are copied when
    present and never fabricated here.

    Count compat: an explicit int 0 without field_status remains verified zero.
    New stubs must omit counts or set field_status/reason_code for unknown.
    """

    from notification_mode import consolidation_enabled
    out: list[DigestRunEntry] = []
    for raw in candidates:
        ran = _parse_actually_ran(raw)
        if ran is not True:
            continue
        holdings_raw = raw.get("holdings", raw.get("positions"))
        # Old explicit integer counts remain compatible. Missing fields and
        # signal labels carry no count evidence, including producer rows.
        fill_count, fill_status = resolve_count_field(raw, "fill_count")
        order_count, order_status = resolve_count_field(raw, "order_count")
        cycle_count, cycle_status = resolve_count_field(raw, "cycle_count")

        out.append(
            DigestRunEntry(
                platform_id=str(raw["platform_id"]),
                strategy_profile=str(raw["strategy_profile"]),
                fill_count=fill_count,
                order_count=order_count,
                cycle_count=cycle_count,
                status="alert" if str(raw.get("status", "ok")) == "alert" else "ok",
                note=str(raw.get("note", "") or ""),
                strategy_label=str(raw.get("strategy_label") or "").strip(),
                equity=_optional_float(raw.get("equity", raw.get("equity_usd"))),
                equity_currency=str(
                    raw.get("equity_currency") or raw.get("currency") or "USD"
                ).strip()
                or "USD",
                holdings=_parse_holdings(holdings_raw),
                holdings_scope=_parse_holdings_scope(raw.get("holdings_scope")),
                signal_summary=str(raw.get("signal_summary") or "").strip(),
                rebalance_kind=_parse_rebalance_kind(raw.get("rebalance_kind")),
                rebalance_conclusion=str(raw.get("rebalance_conclusion") or "").strip(),
                block_reason=_parse_block_reason(raw.get("block_reason")) if raw.get("platform_id") in {"schwab", "ibkr"} else None,
                source_run_id=str(raw.get("source_run_id") or "") if consolidation_enabled() else "",
                approved_gate_reasons=tuple(raw.get("_validated_approved_gate_reasons", ())) if consolidation_enabled() else (),
                tips=_parse_tips(raw.get("tips") or raw.get("tip")),
                account_hint=str(raw.get("account_hint") or "").strip(),
                opaque_account_uid=str(raw.get("opaque_account_uid") or "").strip(),
                target_id=str(raw.get("target_id") or "").strip(),
                account_scope=str(
                    raw.get("account_scope") or raw.get("accountScope") or ""
                ).strip(),
                execution_mode=str(
                    raw.get("execution_mode") or raw.get("executionMode") or ""
                ).strip(),
                fill_count_status=fill_status,
                order_count_status=order_status,
                cycle_count_status=cycle_status,
                reason_code=str(raw.get("reason_code") or "").strip(),
                missing_evidence_reason=str(raw.get("missing_evidence_reason") or "").strip(),
                section=_parse_section(raw.get("section")),
                evidence_provenance=str(raw.get("evidence_provenance") or "").strip(),
                execution_status=str(raw.get("execution_status") or "").strip(),
                evidence_persistence_status=str(
                    raw.get("evidence_persistence_status") or ""
                ).strip(),
                evidence_delivery_status=str(
                    raw.get("evidence_delivery_status") or ""
                ).strip(),
            )
        )
    return out


__all__ = [
    "DIGEST_LAYOUTS",
    "DIGEST_SECTIONS",
    "DailyDigestInput",
    "DigestHolding",
    "DigestRunEntry",
    "EvidenceCoverage",
    "UNKNOWN_ACCOUNT_UID",
    "UNKNOWN_TARGET_ID",
    "account_block_tag",
    "filter_runs_for_digest",
    "normalize_layout",
    "render_daily_digest_sections",
    "fills_are_verified_zero",
    "has_known_positive_fills",
    "has_observation",
    "identity_is_unknown",
    "identity_key",
    "normalize_locale",
    "observation_score",
    "render_daily_digest",
    "resolve_count_field",
    "resolve_evidence_status",
    "total_fills",
    "global_total_fills",
]
