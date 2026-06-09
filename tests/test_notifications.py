import unittest
import sys
import types
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


requests_stub = types.ModuleType("requests")
requests_stub.post = lambda *args, **kwargs: None

with patch.dict(sys.modules, {"requests": requests_stub}):
    from notifications.renderers import render_heartbeat_notification
    from notifications.telegram import build_sender, build_signal_text, build_strategy_display_name, build_translator

from strategy_registry import SUPPORTED_STRATEGY_PROFILES


class FakeRequests:
    def __init__(self):
        self.calls = []

    def post(self, url, json, timeout):
        self.calls.append((url, json, timeout))
        return object()


class NotificationTests(unittest.TestCase):
    def test_build_translator_supports_chinese(self):
        translate = build_translator("zh")
        self.assertEqual(translate("equity"), "净值")
        self.assertEqual(translate("holdings_title"), "💼 持仓")
        self.assertEqual(translate("benchmark_title", symbol="QQQ"), "📈 QQQ 基准")
        self.assertEqual(translate("benchmark_exit", value="598.38"), "退出线: 598.38")
        self.assertEqual(translate("market_status_blend_gate_risk_on", asset="SOXX+SOXL"), "🚀 风险开启（SOXX+SOXL）")
        self.assertEqual(
            translate(
                "signal_blend_gate_risk_on",
                trend_symbol="SOXX",
                window=140,
                soxl_ratio="70.0%",
                soxx_ratio="20.0%",
            ),
            "SOXX 站上 140 日门槛线，持有 SOXL 70.0% + SOXX 20.0%",
        )
        self.assertEqual(
            translate("market_status_blend_gate_overlay_capped", asset="SOXX"),
            "🧯 风控降档（SOXX）",
        )
        self.assertEqual(
            translate(
                "signal_blend_gate_overlay_capped",
                trend_symbol="SOXX",
                window=140,
                reasons="SOXX 10 日年化波动率 68.3% 高于 55.0%，SOXL 转向 SOXX",
                allocation_text="SOXX 90.0%",
            ),
            "SOXX 仍在 140 日门槛线上方，但触发风控降档（SOXX 10 日年化波动率 68.3% 高于 55.0%，SOXL 转向 SOXX），目标仓位 SOXX 90.0%",
        )
        self.assertEqual(
            translate(
                "blend_gate_reason_volatility_delever",
                symbol="SOXX",
                window=10,
                volatility="55.0%",
                threshold="50.0%",
                redirect_symbol="SOXX",
            ),
            "SOXX 10 日年化波动率 55.0% 高于 50.0%，SOXL 转向 SOXX",
        )
        self.assertEqual(
            translate(
                "blend_gate_reason_volatility_delever_dynamic",
                symbol="SOXX",
                window=10,
                volatility="61.0%",
                threshold="60.0%",
                threshold_detail=translate(
                    "blend_gate_volatility_threshold_detail_dynamic",
                    percentile="p95",
                    lookback="252",
                    floor="50.0%",
                    cap="75.0%",
                    sample_count="252",
                ),
                redirect_symbol="SOXX",
            ),
            "SOXX 10 日年化波动率 61.0% 高于实际阈值 60.0%（动态 p95，252日窗口，范围 50.0%-75.0%，样本 252），SOXL 转向 SOXX",
        )
        en_translate = build_translator("en")
        self.assertEqual(
            en_translate(
                "blend_gate_reason_volatility_delever_dynamic",
                symbol="SOXX",
                window=10,
                volatility="61.0%",
                threshold="60.0%",
                threshold_detail=en_translate(
                    "blend_gate_volatility_threshold_detail_dynamic",
                    percentile="p95",
                    lookback="252",
                    floor="50.0%",
                    cap="75.0%",
                    sample_count="252",
                ),
                redirect_symbol="SOXX",
            ),
            "SOXX 10d annualized volatility 61.0% is above effective threshold 60.0% (dynamic p95, 252d lookback, bounded 50.0%-75.0%, samples 252); redirect SOXL to SOXX",
        )
        self.assertEqual(
            translate(
                "small_account_warning_note",
                portfolio_equity="$0",
                min_recommended_equity="$1,000",
                reason=translate(
                    "small_account_warning_reason_integer_shares_min_position_value_may_prevent_backtest_replication"
                ),
            ),
            "小账户提示：净值 $0 低于建议 $1,000；整数股和最小仓位限制可能导致实盘无法完全复现回测",
        )

    def test_supported_strategy_profiles_have_translated_names(self):
        zh_name = build_strategy_display_name(build_translator("zh"))
        en_name = build_strategy_display_name(build_translator("en"))

        self.assertEqual(zh_name("global_etf_confidence_vol_gate"), "全球 ETF 置信波动门控")
        self.assertEqual(en_name("global_etf_confidence_vol_gate"), "Global ETF Confidence Vol Gate")
        for profile in SUPPORTED_STRATEGY_PROFILES:
            self.assertNotEqual(zh_name(profile), profile)
            self.assertNotEqual(en_name(profile), profile)

    def test_heartbeat_signal_snapshot_localizes_price_source_and_status_label(self):
        rendered = render_heartbeat_notification(
            translator=build_translator("zh"),
            strategy_display_name="SOXL/SOXX 半导体趋势收益",
            dry_run_only=False,
            extra_notification_lines=(),
            execution={
                "dashboard_text": "",
                "separator": "━━━━━━━━━━━━━━━━━━",
                "signal_snapshot": {
                    "market_date": "2026-05-28",
                    "latest_price_source": "schwab_daily_history_with_live_quote_overlay",
                    "quote_overlay_used": None,
                },
                "status_display": "🚀 风险开启（SOXX+SOXL）",
                "signal_display": "SOXX 站上 140 日门槛线，持有 SOXL 70.0% + SOXX 20.0%",
            },
            portfolio={
                "total_equity": 970.25,
                "portfolio_rows": (("SOXL",),),
                "market_values": {"SOXL": 677.28},
            },
            account_label="demo",
        )

        self.assertIn("数据源 Schwab 日线历史", rendered.compact_text)
        self.assertNotIn("报价覆盖", rendered.compact_text)
        self.assertIn("📊 市场状态: 🚀 风险开启（SOXX+SOXL）", rendered.compact_text)
        self.assertNotIn("schwab_daily_history_with_live_quote_overlay", rendered.compact_text)

    def test_build_signal_text_formats_icon_and_label(self):
        signal_text = build_signal_text(build_translator("en"))
        self.assertEqual(signal_text("hold"), "💎 Trend Hold")

    def test_build_sender_posts_to_telegram(self):
        fake_requests = FakeRequests()
        sender = build_sender("token-1", "chat-1", requests_module=fake_requests)
        sender("hello")
        self.assertEqual(len(fake_requests.calls), 1)
        url, payload, timeout = fake_requests.calls[0]
        self.assertIn("token-1", url)
        self.assertEqual(payload["chat_id"], "chat-1")
        self.assertEqual(payload["text"], "hello")
        self.assertEqual(timeout, 15)


if __name__ == "__main__":
    unittest.main()
