"""B07 Phase B: Schwab input builder prefetches nasdaq smart DCA signal history."""

from __future__ import annotations

import unittest
from types import SimpleNamespace

import pandas as pd

from application.runtime_strategy_adapters import SchwabRuntimeStrategyAdapters


def _adapters(*, profile: str = "nasdaq_sp500_smart_dca", signal_symbols=("QQQ", "SPY"), history_by_symbol=None):
    history_by_symbol = history_by_symbol or {
        "QQQ": pd.Series([100.0, 101.0], index=pd.to_datetime(["2026-01-01", "2026-01-02"])),
        "SPY": pd.Series([200.0, 201.0], index=pd.to_datetime(["2026-01-01", "2026-01-02"])),
    }

    class BrokerAdapters:
        def build_market_history_loader(self, _market_data_port):
            def load(_client, symbol, *_args, **_kwargs):
                key = str(symbol).strip().upper()
                if key not in history_by_symbol:
                    return pd.Series(dtype=float)
                return history_by_symbol[key]

            return load

        def build_price_history(self, _market_data_port, symbol: str):
            return [{"close": 1.0, "high": 1.0, "low": 1.0}]

    return SchwabRuntimeStrategyAdapters(
        strategy_runtime=SimpleNamespace(),
        strategy_profile=profile,
        strategy_runtime_config={"signal_symbols": signal_symbols},
        available_inputs=frozenset({"market_history", "portfolio_snapshot"}),
        benchmark_symbol="QQQ",
        managed_symbols=("QQQM", "SPLG"),
        signal_text_fn=str,
        translator=lambda key, **_kwargs: key,
        broker_adapters=BrokerAdapters(),
        build_strategy_evaluation_inputs_fn=lambda **_kwargs: {},
        map_strategy_decision_to_plan_fn=lambda *_args, **_kwargs: {},
        build_strategy_plugin_report_payload_fn=lambda *_args, **_kwargs: {},
        load_configured_strategy_plugin_signals_fn=lambda *_args, **_kwargs: (),
        parse_strategy_plugin_mounts_fn=lambda raw: raw,
    )


class NasdaqDcaPrefetchTests(unittest.TestCase):
    def test_fetch_reference_history_includes_prefetched_mapping(self):
        adapters = _adapters()
        market_inputs = adapters.fetch_reference_history(market_data_port=object())
        self.assertIn("market_history", market_inputs)
        self.assertIn("prefetched_market_history", market_inputs)
        prefetched = market_inputs["prefetched_market_history"]
        self.assertEqual(set(prefetched), {"QQQ", "SPY"})
        self.assertEqual(len(prefetched["QQQ"]), 2)
        self.assertTrue(callable(market_inputs["market_history"]))

    def test_prefetch_fail_closed_on_empty_history(self):
        adapters = _adapters(history_by_symbol={"QQQ": pd.Series(dtype=float), "SPY": pd.Series([1.0])})
        with self.assertRaisesRegex(RuntimeError, "empty for signal symbol 'QQQ'"):
            adapters.fetch_reference_history(market_data_port=object())

    def test_non_nasdaq_dca_profile_skips_prefetch(self):
        adapters = _adapters(profile="ibit_smart_dca")
        market_inputs = adapters.fetch_reference_history(market_data_port=object())
        self.assertIn("market_history", market_inputs)
        self.assertNotIn("prefetched_market_history", market_inputs)


if __name__ == "__main__":
    unittest.main()
