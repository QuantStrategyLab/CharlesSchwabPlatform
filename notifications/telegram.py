"""Telegram notification and i18n helpers for CharlesSchwabPlatform."""

from __future__ import annotations

import re

try:
    from quant_platform_kit.common.notification_localization import (
        merge_strategy_plugin_i18n as _merge_strategy_plugin_i18n,
    )
except ImportError:  # pragma: no cover - compatibility with older pinned shared wheels
    _merge_strategy_plugin_i18n = None

_TELEGRAM_MARKET_SYMBOL_LINK_RE = re.compile(r"(?<![A-Za-z0-9_])([A-Z0-9]{1,12})\.([A-Z]{2,4})(?![A-Za-z0-9_])")
_TELEGRAM_MARKET_SYMBOL_LINK_JOINER = "\u2060"


def _break_telegram_market_symbol_auto_links(value) -> str:
    text = str(value or "")
    return _TELEGRAM_MARKET_SYMBOL_LINK_RE.sub(
        lambda match: f"{match.group(1)}.{_TELEGRAM_MARKET_SYMBOL_LINK_JOINER}{match.group(2)}",
        text,
    )


SIGNAL_ICONS = {
    "hold": "💎",
    "entry": "🚀",
    "reduce": "⚠️",
    "exit": "🔴",
    "idle": "💤",
}


I18N = {
    "zh": {
        "trade_header": "🔔 【调仓指令】",
        "heartbeat_header": "💓 【心跳检测】",
        "error_header": "🚨 【策略异常】",
        "strategy_label": "🧭 策略: {name}",
        "signal_label": "信号",
        "signal_monthly_snapshot_waiting": "月度快照节奏 | 等待进入执行窗口",
        "status_monthly_snapshot_waiting_window": "不执行 | 原因=当前不在月度执行窗口 | 快照日期={snapshot_as_of} | 允许日期={allowed_dates}",
        "status_no_execution_window_after_snapshot": "不执行 | 原因=快照后没有可用执行窗口 | 快照日期={snapshot_as_of}",
        "snapshot_guard_decision_proceed": "继续",
        "snapshot_guard_decision_no_op": "不执行",
        "snapshot_guard_decision_fail_closed": "关闭执行",
        "dry_run_banner": "🧪 模拟运行，本轮不提交真实订单",
        "health_probe_title": "🔎 【连接探针】",
        "health_probe_error_prefix": "健康探针异常:\n",
        "dashboard_label": "📊 资产看板",
        "holdings_title": "💼 持仓",
        "benchmark_title": "📈 {symbol} 基准",
        "benchmark_price": "{symbol}: {value}",
        "benchmark_ma200": "MA200: {value}",
        "benchmark_exit": "退出线: {value}",
        "equity": "净值",
        "buying_power": "购买力",
        "reserved_cash": "预留现金",
        "investable_cash": "可投资现金",
        "cash_label": "现金",
        "no_trades": "✅ 无需调仓",
        "separator": "━━━━━━━━━━━━━━━━━━",
        "signal_hold": "趋势持有",
        "signal_entry": "入场信号",
        "signal_reduce": "减仓信号",
        "signal_exit": "离场信号",
        "signal_idle": "等待信号",
        "market_status_risk_on": "🚀 风险开启（{asset}）",
        "market_status_delever": "🛡️ 降杠杆（{asset}）",
        "signal_risk_on": "SOXL 站上 {window} 日均线，持有 SOXL，交易层风险仓位 {ratio}",
        "signal_delever": "SOXL 跌破 {window} 日均线，切换至 SOXX，交易层风险仓位 {ratio}",
        "market_status_blend_gate_risk_on": "🚀 风险开启（{asset}）",
        "market_status_blend_gate_defensive": "🛡️ 降杠杆（{asset}）",
        "signal_blend_gate_risk_on": "{trend_symbol} 站上 {window} 日门槛线，持有 SOXL {soxl_ratio} + SOXX {soxx_ratio}",
        "signal_blend_gate_defensive": "{trend_symbol} 跌破门槛线，防守持有 SOXX {soxx_ratio}",
        "market_status_blend_gate_overlay_capped": "🧯 风控降档（{asset}）",
        "signal_blend_gate_overlay_capped": "{trend_symbol} 仍在 {window} 日门槛线上方，但触发风控降档（{reasons}），目标仓位 {allocation_text}",
        "risk_control_tqqq_volatility_delever_applied": "🛡️ 风控: QQQ {window} 日年化波动率 {volatility} 高于 {threshold}，{source_symbol} 转向 {redirect_symbol}（{allocation_detail}）",
        "risk_control_tqqq_volatility_delever_applied_dynamic": "🛡️ 风控: QQQ {window} 日年化波动率 {volatility} 高于实际阈值 {threshold}（{threshold_detail}），{source_symbol} 转向 {redirect_symbol}（{allocation_detail}）",
        "risk_control_tqqq_volatility_delever_hysteresis": "🛡️ 风控: QQQ {window} 日年化波动率 {volatility} 仍高于退出阈值 {exit_threshold}，维持 {source_symbol} 转向 {redirect_symbol}（{allocation_detail}）",
        "risk_control_tqqq_volatility_delever_hysteresis_dynamic": "🛡️ 风控: QQQ {window} 日年化波动率 {volatility} 仍高于退出阈值 {exit_threshold}；入场实际阈值 {threshold}（{threshold_detail}），维持 {source_symbol} 转向 {redirect_symbol}（{allocation_detail}）",
        "tqqq_volatility_delever_allocation_detail": "杠杆仓位：TQQQ 保留 {retained_ratio}，{redirect_symbol} {redirected_ratio}",
        "tqqq_signal_reason_entry_trend": "原因：QQQ 高于 MA200，MA20 斜率为正",
        "tqqq_signal_reason_entry_pullback": "原因：QQQ 低于 MA200，但站上 MA20 且回撤反弹确认",
        "tqqq_signal_reason_hold_trend": "原因：已持有风险仓位，QQQ 仍高于 MA200",
        "tqqq_signal_reason_exit_ma200": "原因：QQQ 跌破 MA200 退出线",
        "tqqq_signal_reason_idle_waiting": "原因：等待 QQQ 站上 MA200 且 MA20 斜率转正",
        "tqqq_signal_reason_macro_delever": "原因：宏观风控降低杠杆",
        "tqqq_signal_reason_macro_defense": "原因：宏观风控转入防守",
        "tqqq_signal_reason_crisis_defense": "原因：危机防御转入避险仓位",
        "blend_gate_reason_rsi_cap": "RSI 超阈值",
        "blend_gate_reason_bollinger_cap": "突破布林上轨",
        "blend_gate_reason_volatility_delever": "{symbol} {window} 日年化波动率 {volatility} 高于 {threshold}，SOXL 转向 {redirect_symbol}",
        "blend_gate_reason_volatility_delever_dynamic": "{symbol} {window} 日年化波动率 {volatility} 高于实际阈值 {threshold}（{threshold_detail}），SOXL 转向 {redirect_symbol}",
        "blend_gate_volatility_threshold_detail_dynamic": "动态 {percentile}，{lookback}日窗口，范围 {floor}-{cap}，样本 {sample_count}",
        "blend_gate_volatility_threshold_detail_dynamic_fallback": "动态样本不足，回退固定 {fixed_threshold}（样本 {sample_count}/{min_periods}，{percentile}）",
        "blend_gate_volatility_threshold_detail_fixed": "固定阈值 {threshold}",
        "limit_buy": "限价买入",
        "market_buy": "市价买入",
        "market_sell": "市价卖出",
        "shares": "股",
        "submitted": "已下发，尚未确认成交；限价单可能未成交、取消或继续挂单",
        "failed": "失败",
        "exception": "异常",
        "buy_label": "买入",
        "limit_buy_cmd": "限价买入指令",
        "market_buy_cmd": "市价买入指令",
        "market_sell_cmd": "市价卖出指令",
        "dry_run_trade_log": "🧪 模拟下单：{command} {symbol}: {quantity}{shares}",
        "dry_run_trade_log_with_price": "🧪 模拟下单：{command} {symbol} (${price}): {quantity}{shares}",
        "buy_deferred": "ℹ️ [买入说明] {detail}",
        "buy_deferred_small_account_cash_substitution": "{symbol} 目标金额 ${diff} 低于 1 股价格 ${price}；为避免超过目标仓位，小账户本轮保留现金，不回补 {cash_symbols}",
        "small_account_allocation_drift": "📏 整数股偏离：若本轮订单全部成交，{details}",
        "small_account_allocation_drift_detail": "{symbol} 预计 {projected_weight} vs 目标 {target_weight}（{drift_weight}）",
        "buy_lifted_small_account_whole_share": "ℹ️ [买入说明] {symbols} 目标金额接近 1 股；小账户整数股兼容，本轮允许按 1 股下单",
        "post_sell_buying_power_unreleased": "ℹ️ 卖出后购买力未释放，本轮跳过买入，等待下次执行",
        "cash_sweep_rebuy": "🏦 [尾部回补] 剩余购买力回补 {symbol}: {quantity}{shares} @ ${price}",
        "order_id_suffix": "（订单号: {order_id}）",
        "small_account_warning_note": "小账户提示：净值 {portfolio_equity} 低于建议 {min_recommended_equity}；{reason}",
        "small_account_warning_reason_integer_shares_min_position_value_may_prevent_backtest_replication": "整数股和最小仓位限制可能导致实盘无法完全复现回测",
        "strategy_plugin_line": "🧩 插件：{plugin} | 启用：{enabled} | 状态：{route} | 提醒：{action}",
        "strategy_plugin_enabled_true": "是",
        "strategy_plugin_enabled_false": "否",
        "strategy_plugin_consumption_auto": "🧩 插件本次影响：已按策略规则参与本轮仓位计算",
        "strategy_plugin_consumption_auto_defend": "🧩 插件本次影响：已触发防守规则并参与本轮仓位计算",
        "strategy_plugin_consumption_auto_delever": "🧩 插件本次影响：已触发降杠杆规则并参与本轮仓位计算",
        "strategy_plugin_consumption_loaded_not_applied": "🧩 插件本次影响：已加载；该状态未启用自动仓位改写",
        "strategy_plugin_consumption_review_only": "🧩 插件本次影响：仅通知复核；当前状态未触发自动仓位改写",
        "strategy_plugin_consumption_unavailable": "🧩 插件本次影响：未加载可用插件信号",

        "strategy_plugin_alert_subject": "🚨 策略插件告警：{plugin} | {route}",
        "strategy_plugin_alert_title": "🚨 【策略插件告警】",
        "strategy_plugin_alert_context": "运行环境：{context}",
        "strategy_plugin_alert_strategy": "策略：{strategy}",
        "strategy_plugin_alert_plugin": "插件：{plugin}",
        "strategy_plugin_alert_status": "状态：{route}",
        "strategy_plugin_alert_action": "人工处理建议：{action}",
        "strategy_plugin_alert_mode": "模式：{mode}",
        "strategy_plugin_alert_as_of": "信号时间：{as_of}",
        "strategy_plugin_alert_guidance": "处置建议：{guidance}",
        "strategy_plugin_alert_scope_note": "执行范围：{scope_note}",
        "strategy_plugin_alert_scope": "仅作人工复核提醒；插件不会自动下单或改仓位",
        "strategy_plugin_name_crisis_response_shadow": "危机观察通知",
        "strategy_plugin_name_macro_risk_governor": "宏观风险控制通知",
        "strategy_plugin_name_market_regime_control": "市场状态控制",
        "strategy_plugin_name_panic_reversal_shadow": "恐慌反转观察通知",
        "strategy_plugin_name_taco_rebound_shadow": "TACO 反弹观察通知",
        "strategy_plugin_mode_shadow": "影子观察",
        "strategy_plugin_route_blocked": "已阻断",
        "strategy_plugin_route_crisis": "危机",
        "strategy_plugin_route_delever": "降杠杆",
        "strategy_plugin_route_no_action": "未触发",
        "strategy_plugin_route_opportunity_watch": "机会观察",
        "strategy_plugin_route_panic_reversal": "恐慌反转",
        "strategy_plugin_route_risk_off": "风险关闭",
        "strategy_plugin_route_risk_reduced": "风险降低",
        "strategy_plugin_route_true_crisis": "真危机",
        "strategy_plugin_route_taco_rebound": "TACO 反弹确认",
        "strategy_plugin_route_unknown_route": "未知状态",
        "strategy_plugin_route_watch": "观察",
        "strategy_plugin_action_no_action": "不操作",
        "strategy_plugin_action_watch_only": "仅通知",
        "strategy_plugin_action_notify_manual_review": "通知人工复核",
        "strategy_plugin_action_defend": "防守",
        "strategy_plugin_action_delever": "降杠杆",
        "strategy_plugin_action_blocked": "已阻断",
        "strategy_plugin_action_monitor": "持续观察",
        "strategy_plugin_action_unknown_action": "未知提醒",
        "strategy_plugin_guidance_crisis_response_shadow_true_crisis_defend": "优先考虑降低杠杆或清理杠杆仓位，暂停加仓；如需保留风险敞口，先降到可承受的小仓位。",
        "strategy_plugin_guidance_crisis_response_shadow_no_action_blocked": "危机路线被风控阻断；先核对数据新鲜度和外部情境，不建议仅凭此条加仓。",
        "strategy_plugin_guidance_macro_risk_governor_delever_delever": "宏观风险控制建议降低杠杆敞口；是否执行由策略侧可回测规则和仓位适配器决定。",
        "strategy_plugin_guidance_macro_risk_governor_crisis_defend": "宏观危机信号建议风险仓位转向防守或现金类资产，直到压力缓和。",
        "strategy_plugin_guidance_market_regime_control_risk_off_defend": "市场状态控制进入风险关闭；机会类信号先不执行，风险仓位应保持防守。",
        "strategy_plugin_guidance_market_regime_control_risk_reduced_delever": "市场状态控制建议降杠杆；自动仓位调整只按策略侧已批准的可回测规则执行。",
        "strategy_plugin_guidance_market_regime_control_opportunity_watch_notify_manual_review": "仅作人工复核：市场状态允许有限机会观察，但插件本身不会下单或直接改仓位。",
        "strategy_plugin_guidance_market_regime_control_blocked_blocked": "市场状态控制被数据质量或新鲜度保护阻断；先核对数据源和产物，再决定是否人工处理。",
        "strategy_plugin_guidance_taco_rebound_shadow_taco_rebound_notify_manual_review": "TACO 仅提示可能的反弹窗口；可考虑小仓位、分批、预设止损/失效条件的人工博弈，不建议一次性满仓。",
        "strategy_name_tqqq_growth_income": "TQQQ 增长收益",
        "strategy_name_soxl_soxx_trend_income": "SOXL/SOXX 半导体趋势收益",
        "strategy_name_global_etf_rotation": "全球 ETF 轮动",
        "strategy_name_global_etf_confidence_vol_gate": "全球 ETF 置信波动门控",
        "strategy_name_russell_1000_multi_factor_defensive": "罗素1000多因子",
        "strategy_name_tech_communication_pullback_enhancement": "科技通信回调增强",
        "strategy_name_qqq_tech_enhancement": "科技通信回调增强",
        "strategy_name_russell_top50_leader_rotation": "罗素 Top50 领涨轮动",
        "strategy_name_mega_cap_leader_rotation_top50_balanced": "美股超大盘50强平衡龙头轮动",
        "strategy_name_ibit_smart_dca": "IBIT 比特币 ETF 智能定投",
    },
    "en": {
        "trade_header": "🔔 【Trade Execution Report】",
        "heartbeat_header": "💓 【Heartbeat】",
        "error_header": "🚨 【Strategy Error】",
        "strategy_label": "🧭 Strategy: {name}",
        "signal_label": "Signal",
        "signal_monthly_snapshot_waiting": "monthly snapshot cadence | waiting inside execution window",
        "status_monthly_snapshot_waiting_window": "no-op | reason=outside monthly execution window | snapshot_as_of={snapshot_as_of} | allowed={allowed_dates}",
        "status_no_execution_window_after_snapshot": "no-op | reason=no execution window after snapshot | snapshot_as_of={snapshot_as_of}",
        "snapshot_guard_decision_proceed": "proceed",
        "snapshot_guard_decision_no_op": "no_op",
        "snapshot_guard_decision_fail_closed": "fail_closed",
        "dry_run_banner": "🧪 Dry run only; no live orders submitted",
        "health_probe_title": "🔎 【Health Probe】",
        "health_probe_error_prefix": "Health probe error:\n",
        "dashboard_label": "📊 Dashboard",
        "holdings_title": "💼 Holdings",
        "benchmark_title": "📈 {symbol} Benchmark",
        "benchmark_price": "{symbol}: {value}",
        "benchmark_ma200": "MA200: {value}",
        "benchmark_exit": "Exit: {value}",
        "equity": "Equity",
        "buying_power": "Buying Power",
        "reserved_cash": "Reserved Cash",
        "investable_cash": "Investable Cash",
        "cash_label": "Cash",
        "no_trades": "✅ No rebalance needed",
        "separator": "━━━━━━━━━━━━━━━━━━",
        "signal_hold": "Trend Hold",
        "signal_entry": "Entry Signal",
        "signal_reduce": "Reduce Signal",
        "signal_exit": "Exit Signal",
        "signal_idle": "Idle",
        "market_status_risk_on": "🚀 RISK-ON ({asset})",
        "market_status_delever": "🛡️ DE-LEVER ({asset})",
        "signal_risk_on": "SOXL above {window}d MA, hold SOXL, risk {ratio}",
        "signal_delever": "SOXL below {window}d MA, switch to SOXX, risk {ratio}",
        "market_status_blend_gate_risk_on": "🚀 RISK-ON ({asset})",
        "market_status_blend_gate_defensive": "🛡️ DE-LEVER ({asset})",
        "signal_blend_gate_risk_on": "{trend_symbol} above {window}d gated entry, hold SOXL {soxl_ratio} + SOXX {soxx_ratio}",
        "signal_blend_gate_defensive": "{trend_symbol} below gated entry, hold defensive SOXX {soxx_ratio}",
        "market_status_blend_gate_overlay_capped": "🧯 RISK-CAP ({asset})",
        "signal_blend_gate_overlay_capped": "{trend_symbol} stays above the {window}d gate, but risk cap ({reasons}) cuts exposure to {allocation_text}",
        "risk_control_tqqq_volatility_delever_applied": "🛡️ Risk control: QQQ {window}d annualized volatility {volatility} is above {threshold}; {source_symbol} redirects to {redirect_symbol} ({allocation_detail})",
        "risk_control_tqqq_volatility_delever_applied_dynamic": "🛡️ Risk control: QQQ {window}d annualized volatility {volatility} is above effective threshold {threshold} ({threshold_detail}); {source_symbol} redirects to {redirect_symbol} ({allocation_detail})",
        "risk_control_tqqq_volatility_delever_hysteresis": "🛡️ Risk control: QQQ {window}d annualized volatility {volatility} remains above the exit threshold {exit_threshold}; keep {source_symbol} redirected to {redirect_symbol} ({allocation_detail})",
        "risk_control_tqqq_volatility_delever_hysteresis_dynamic": "🛡️ Risk control: QQQ {window}d annualized volatility {volatility} remains above exit threshold {exit_threshold}; entry effective threshold {threshold} ({threshold_detail}); keep {source_symbol} redirected to {redirect_symbol} ({allocation_detail})",
        "tqqq_volatility_delever_allocation_detail": "leveraged sleeve: TQQQ retained {retained_ratio}, {redirect_symbol} {redirected_ratio}",
        "tqqq_signal_reason_entry_trend": "reason: QQQ is above MA200 and MA20 slope is positive",
        "tqqq_signal_reason_entry_pullback": "reason: QQQ is below MA200 but above MA20 with a confirmed pullback rebound",
        "tqqq_signal_reason_hold_trend": "reason: existing risk sleeve remains active while QQQ stays above MA200",
        "tqqq_signal_reason_exit_ma200": "reason: QQQ fell below the MA200 exit line",
        "tqqq_signal_reason_idle_waiting": "reason: waiting for QQQ to reclaim MA200 with positive MA20 slope",
        "tqqq_signal_reason_macro_delever": "reason: macro risk governor reduced leverage",
        "tqqq_signal_reason_macro_defense": "reason: macro risk governor moved the strategy defensive",
        "tqqq_signal_reason_crisis_defense": "reason: crisis defense moved the strategy to the safe sleeve",
        "blend_gate_reason_rsi_cap": "RSI over threshold",
        "blend_gate_reason_bollinger_cap": "price above upper band",
        "blend_gate_reason_volatility_delever": "{symbol} {window}d annualized volatility {volatility} is above {threshold}; redirect SOXL to {redirect_symbol}",
        "blend_gate_reason_volatility_delever_dynamic": "{symbol} {window}d annualized volatility {volatility} is above effective threshold {threshold} ({threshold_detail}); redirect SOXL to {redirect_symbol}",
        "blend_gate_volatility_threshold_detail_dynamic": "dynamic {percentile}, {lookback}d lookback, bounded {floor}-{cap}, samples {sample_count}",
        "blend_gate_volatility_threshold_detail_dynamic_fallback": "dynamic warm-up, fallback fixed {fixed_threshold} (samples {sample_count}/{min_periods}, {percentile})",
        "blend_gate_volatility_threshold_detail_fixed": "fixed threshold {threshold}",
        "limit_buy": "Limit Buy",
        "market_buy": "Market Buy",
        "market_sell": "Market Sell",
        "shares": " shares",
        "submitted": "submitted; fill not confirmed, a limit order may remain open or be canceled unfilled",
        "failed": "failed",
        "exception": "error",
        "buy_label": "Buy",
        "limit_buy_cmd": "Limit Buy",
        "market_buy_cmd": "Market Buy",
        "market_sell_cmd": "Market Sell",
        "dry_run_trade_log": "🧪 DRY_RUN {command} {symbol}: {quantity}{shares}",
        "dry_run_trade_log_with_price": "🧪 DRY_RUN {command} {symbol} (${price}): {quantity}{shares}",
        "buy_deferred": "ℹ️ [Buy note] {detail}",
        "buy_deferred_small_account_cash_substitution": "{symbol} target ${diff} is below the 1-share price ${price}; to avoid exceeding the target allocation, this small account keeps cash this cycle and does not rebuy {cash_symbols}",
        "small_account_allocation_drift": "📏 Integer-share drift: if this cycle's orders fully fill, {details}",
        "small_account_allocation_drift_detail": "{symbol} projected {projected_weight} vs target {target_weight} ({drift_weight})",
        "buy_lifted_small_account_whole_share": "ℹ️ [Buy note] {symbols} target is close to one share; small-account whole-share compatibility allows a 1-share order this cycle",
        "post_sell_buying_power_unreleased": "ℹ️ Buying power did not update after the sell; skipped buys until the next run",
        "cash_sweep_rebuy": "🏦 [tail rebuy] residual buying power rebought {symbol}: {quantity}{shares} @ ${price}",
        "order_id_suffix": "(ID: {order_id})",
        "small_account_warning_note": "small account warning: portfolio equity {portfolio_equity} is below recommended {min_recommended_equity}; {reason}",
        "small_account_warning_reason_integer_shares_min_position_value_may_prevent_backtest_replication": "integer-share minimum position sizing may prevent backtest replication",
        "strategy_plugin_line": "🧩 Plugin: {plugin} | enabled: {enabled} | status: {route} | notice: {action}",
        "strategy_plugin_enabled_true": "yes",
        "strategy_plugin_enabled_false": "no",
        "strategy_plugin_consumption_auto": "🧩 Plugin impact this run: included in this cycle's position calculation under strategy rules",
        "strategy_plugin_consumption_auto_defend": "🧩 Plugin impact this run: triggered defensive rules and joined this cycle's position calculation",
        "strategy_plugin_consumption_auto_delever": "🧩 Plugin impact this run: triggered de-levering rules and joined this cycle's position calculation",
        "strategy_plugin_consumption_loaded_not_applied": "🧩 Plugin impact this run: loaded; this state did not enable automatic position rewrites",
        "strategy_plugin_consumption_review_only": "🧩 Plugin impact this run: review-only notice; current state did not trigger automatic position rewrites",
        "strategy_plugin_consumption_unavailable": "🧩 Plugin impact this run: no usable plugin signal loaded",

        "strategy_plugin_alert_subject": "🚨 Strategy plugin alert: {plugin} | {route}",
        "strategy_plugin_alert_title": "🚨 【Strategy Plugin Alert】",
        "strategy_plugin_alert_context": "Context: {context}",
        "strategy_plugin_alert_strategy": "Strategy: {strategy}",
        "strategy_plugin_alert_plugin": "Plugin: {plugin}",
        "strategy_plugin_alert_status": "Status: {route}",
        "strategy_plugin_alert_action": "Notice: {action}",
        "strategy_plugin_alert_mode": "Mode: {mode}",
        "strategy_plugin_alert_as_of": "Signal as-of: {as_of}",
        "strategy_plugin_alert_guidance": "Manual guidance: {guidance}",
        "strategy_plugin_alert_scope_note": "Execution scope: {scope_note}",
        "strategy_plugin_alert_scope": "Manual review notice only; the plugin does not place orders or change allocations",
        "strategy_plugin_name_crisis_response_shadow": "Crisis Watch Notice",
        "strategy_plugin_name_macro_risk_governor": "Macro Risk Governor Notice",
        "strategy_plugin_name_market_regime_control": "Market Regime Control",
        "strategy_plugin_name_panic_reversal_shadow": "Panic Reversal Watch Notice",
        "strategy_plugin_name_taco_rebound_shadow": "TACO Rebound Watch Notice",
        "strategy_plugin_mode_shadow": "shadow",
        "strategy_plugin_route_blocked": "blocked",
        "strategy_plugin_route_crisis": "crisis",
        "strategy_plugin_route_delever": "de-lever",
        "strategy_plugin_route_no_action": "no alert",
        "strategy_plugin_route_opportunity_watch": "opportunity watch",
        "strategy_plugin_route_panic_reversal": "panic reversal",
        "strategy_plugin_route_risk_off": "risk off",
        "strategy_plugin_route_risk_reduced": "risk reduced",
        "strategy_plugin_route_true_crisis": "true crisis",
        "strategy_plugin_route_taco_rebound": "TACO rebound confirmed",
        "strategy_plugin_route_unknown_route": "unknown status",
        "strategy_plugin_route_watch": "watch",
        "strategy_plugin_action_no_action": "no action",
        "strategy_plugin_action_watch_only": "notify only",
        "strategy_plugin_action_notify_manual_review": "notify manual review",
        "strategy_plugin_action_defend": "defend",
        "strategy_plugin_action_delever": "de-lever",
        "strategy_plugin_action_blocked": "blocked",
        "strategy_plugin_action_monitor": "watch",
        "strategy_plugin_action_unknown_action": "unknown notice",
        "strategy_plugin_guidance_crisis_response_shadow_true_crisis_defend": "Consider reducing or clearing leveraged exposure, then pause new risk additions; if keeping exposure, resize it to a small amount you can tolerate.",
        "strategy_plugin_guidance_crisis_response_shadow_no_action_blocked": "A guard blocked the crisis route; verify data freshness and external context before acting on this alert.",
        "strategy_plugin_guidance_macro_risk_governor_delever_delever": "The macro risk governor suggests reducing leveraged exposure; execution is controlled by strategy-side backtestable rules and position adapters.",
        "strategy_plugin_guidance_macro_risk_governor_crisis_defend": "The macro crisis signal suggests moving the risk sleeve toward defensive or cash-like exposure until stress de-escalates.",
        "strategy_plugin_guidance_market_regime_control_risk_off_defend": "Market regime control is risk-off; opportunity signals should stay blocked and risk exposure should remain defensive.",
        "strategy_plugin_guidance_market_regime_control_risk_reduced_delever": "Market regime control suggests de-levering; automatic position changes only follow strategy-side approved, backtestable rules.",
        "strategy_plugin_guidance_market_regime_control_opportunity_watch_notify_manual_review": "Manual review only: the market regime allows bounded opportunity watch, but the plugin does not place orders or directly change allocations.",
        "strategy_plugin_guidance_market_regime_control_blocked_blocked": "Market regime control was blocked by data-quality or freshness guards; verify source data and artifacts before manual action.",
        "strategy_plugin_guidance_taco_rebound_shadow_taco_rebound_notify_manual_review": "TACO only flags a possible rebound window; consider a small staged manual probe with a predefined invalidation level instead of full-size exposure.",
        "strategy_name_tqqq_growth_income": "TQQQ Growth Income",
        "strategy_name_soxl_soxx_trend_income": "SOXL/SOXX Semiconductor Trend Income",
        "strategy_name_global_etf_rotation": "Global ETF Rotation",
        "strategy_name_global_etf_confidence_vol_gate": "Global ETF Confidence Vol Gate",
        "strategy_name_russell_1000_multi_factor_defensive": "Russell 1000 Multi-Factor",
        "strategy_name_tech_communication_pullback_enhancement": "Tech/Communication Pullback Enhancement",
        "strategy_name_qqq_tech_enhancement": "Tech/Communication Pullback Enhancement",
        "strategy_name_russell_top50_leader_rotation": "Russell Top50 Leader Rotation",
        "strategy_name_mega_cap_leader_rotation_top50_balanced": "Mega Cap Leader Rotation Top50 Balanced",
        "strategy_name_ibit_smart_dca": "IBIT Smart DCA",
    },
}

if _merge_strategy_plugin_i18n is not None:
    _PLATFORM_I18N = {locale: dict(values) for locale, values in I18N.items()}
    try:
        I18N = _merge_strategy_plugin_i18n(I18N, shared_wins=False)
    except TypeError:
        I18N = _merge_strategy_plugin_i18n(I18N)
        for locale, values in _PLATFORM_I18N.items():
            I18N.setdefault(locale, {}).update(values)


def build_translator(lang):
    def translate(key, **kwargs):
        active_lang = lang if lang in I18N else "en"
        template = I18N[active_lang].get(key, key)
        return template.format(**kwargs) if kwargs else template

    return translate


def build_signal_text(translate_fn):
    def signal_text(icon_key):
        emoji = SIGNAL_ICONS.get(icon_key, "❓")
        name = translate_fn(f"signal_{icon_key}")
        return f"{emoji} {name}"

    return signal_text


def build_strategy_display_name(translate_fn):
    def strategy_display_name(profile: str, *, fallback_name: str | None = None) -> str:
        key = f"strategy_name_{str(profile or '').strip()}"
        translated = translate_fn(key)
        if translated != key:
            return translated
        fallback = str(fallback_name or "").strip()
        if fallback:
            return fallback
        return str(profile or "").strip()

    return strategy_display_name


def build_sender(token, chat_id, *, requests_module=None):
    if requests_module is None:
        import requests as requests_module

    def send_tg_message(message):
        if not token or not chat_id:
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        try:
            requests_module.post(
                url,
                json={"chat_id": chat_id, "text": _break_telegram_market_symbol_auto_links(message)},
                timeout=15,
            )
        except Exception as exc:
            print(f"Telegram send failed: {type(exc).__name__}", flush=True)

    return send_tg_message
