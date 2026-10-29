import unittest

from notifications.renderers import render_trade_notification
from notifications.telegram import build_translator


class InlineRiskNotificationTests(unittest.TestCase):
    def _render(self, locale, logs):
        return render_trade_notification(
            translator=build_translator(locale),
            strategy_display_name="Synthetic strategy",
            dry_run_only=False,
            extra_notification_lines=(),
            execution={"dashboard_text": "", "separator": "---", "signal_display": "hold"},
            trade_logs=logs,
        )

    def test_known_and_unknown_reasons_in_real_trade_rendering(self):
        reasons = "DAILY_LOSS_LIMIT_EXCEEDED, CIRCUIT_BREAKER_OPEN, FUTURE_GUARD_REASON"
        raw_gate = ("[Account new-risk gate] disposition=BLOCK_NEW_RISK observation=COMPLETE "
                    "reconciliation=VERIFIED breaker=OPEN reasons=" + reasons)
        for lang, note, loss, breaker, guard in (
            ("zh", "ℹ️ [买入说明]", "相对基准净值的下降达到配置上限", "熔断保护已开启", "账户风险检查"),
            ("en", "ℹ️ [Buy note]", "The decline relative to the equity baseline reached the configured limit",
             "the circuit breaker is open", "Account risk check"),
        ):
            with self.subTest(locale=lang):
                raw_note = f"{note} [Account new-risk gate] {reasons}"
                logs = [raw_gate, raw_note]
                rendered = self._render(lang, logs)
                for code in reasons.split(", "):
                    self.assertEqual(rendered.detailed_text.count(code), 2)
                    self.assertIn(code, rendered.compact_text)
                for text in (loss, breaker, guard):
                    self.assertEqual(rendered.detailed_text.count(text), 2)
                self.assertNotIn("[Account new-risk gate]", rendered.detailed_text)
                self.assertEqual(logs, [raw_gate, raw_note])

    def test_unknown_and_unmatched_lines_are_preserved(self):
        for lang, note in (("zh", "ℹ️ [买入说明]"), ("en", "ℹ️ [Buy note]")):
            with self.subTest(locale=lang):
                unknown = f"{note} [Account new-risk gate] FUTURE_GUARD_REASON"
                unmatched = f"{note} [Account new-risk gate] FUTURE-GUARD: original detail"
                normal = f"{note} ordinary detail"
                rendered = self._render(lang, [unknown, unmatched, normal])
                self.assertIn("FUTURE_GUARD_REASON", rendered.detailed_text)
                self.assertIn(unmatched, rendered.detailed_text)
                self.assertIn(normal, rendered.detailed_text)


if __name__ == "__main__":
    unittest.main()
