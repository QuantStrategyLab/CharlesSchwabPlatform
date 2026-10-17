"""Notification vocabulary from QPK 07e24c0c; trading dependency stays frozen."""
from quant_platform_kit.common.operational_notification_localization import (
    operational_notification_text as _text,
    localize_attention_reason_code as _reason,
    resolve_operational_notification_locale,
)

_TEXTS = {'zh': {'attention_reason_unknown': '无法解释此原因码，请查看该次运行的风控记录', 'attention_reason_new_risk_prohibited': '风控已拦截；调用方未提供具体原因码', 'attention_reason_DAILY_LOSS_LIMIT_EXCEEDED': '当日亏损达到配置上限，已拦截新增风险', 'attention_reason_DAILY_LOSS_UNKNOWN_FAIL_CLOSED': '当日亏损或上限无法核实，暂不允许新增风险', 'attention_reason_OBSERVATION_NOT_COMPLETE': '账户快照不完整', 'attention_reason_RECONCILIATION_NOT_VERIFIED': '账户对账尚未核实', 'attention_reason_CIRCUIT_BREAKER_OPEN': '熔断保护已开启', 'attention_reason_EQUITY_UNKNOWN_FAIL_CLOSED': '账户净值无法核实', 'attention_reason_SNAPSHOT_VALIDATION_FAIL_CLOSED': '账户快照未通过校验', 'attention_reason_CAPITAL_ENVELOPE_NEW_RISK_PROHIBITED': '资金风险约束不允许新增风险', 'attention_reason_broker_outcome_unknown': '券商订单结果未知，禁止重复提交', 'attention_reason_RECONCILE_ONLY': '账户处于仅对账状态', 'attention_next_review_daily_loss': '下一步：在管理站核对当日净值、资金进出和亏损上限；保持禁止新增风险，确认意图不会解除保护', 'attention_next_inspect_guard_reason': '下一步：在管理站查看该次运行的风控原因码和账户状态；保持保护，确认意图不会解除保护'}, 'en': {'attention_reason_unknown': "unrecognized reason code; inspect this run's guard record", 'attention_reason_new_risk_prohibited': 'a risk guard blocked additions; the caller did not provide the specific reason code', 'attention_reason_DAILY_LOSS_LIMIT_EXCEEDED': 'daily loss reached the configured limit; new risk additions were blocked', 'attention_reason_DAILY_LOSS_UNKNOWN_FAIL_CLOSED': 'daily loss or its limit could not be verified; new risk remains blocked', 'attention_reason_OBSERVATION_NOT_COMPLETE': 'account snapshot is incomplete', 'attention_reason_RECONCILIATION_NOT_VERIFIED': 'account reconciliation is not verified', 'attention_reason_CIRCUIT_BREAKER_OPEN': 'the circuit breaker is open', 'attention_reason_EQUITY_UNKNOWN_FAIL_CLOSED': 'account equity could not be verified', 'attention_reason_SNAPSHOT_VALIDATION_FAIL_CLOSED': 'account snapshot validation failed', 'attention_reason_CAPITAL_ENVELOPE_NEW_RISK_PROHIBITED': 'capital risk limits prohibit new risk additions', 'attention_reason_broker_outcome_unknown': 'broker order outcome is unknown; do not resubmit', 'attention_reason_RECONCILE_ONLY': 'the account is in reconcile-only mode', 'attention_next_review_daily_loss': "Next: review today's equity, cash flows and daily-loss limit in the console; keep new risk blocked. Confirming intent does not release the guard", 'attention_next_inspect_guard_reason': "Next: inspect this run's guard reason codes and account state; keep protections active. Confirming intent does not release the guard"}}


def operational_notification_text(locale, key, **values):
    lang = resolve_operational_notification_locale(locale)
    return _TEXTS[lang][key].format(**values) if key in _TEXTS[lang] else _text(locale, key, **values)


def localize_attention_reason_code(locale, code):
    lang = resolve_operational_notification_locale(locale)
    return _TEXTS[lang].get("attention_reason_" + str(code), _reason(locale, code))
