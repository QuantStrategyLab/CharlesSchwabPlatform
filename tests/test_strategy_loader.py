import unittest


class StrategyLoaderTests(unittest.TestCase):
    def test_load_strategy_entrypoint_resolves_hybrid_growth_income(self):
        try:
            from strategy_loader import load_strategy_entrypoint_for_profile

            entrypoint = load_strategy_entrypoint_for_profile("hybrid_growth_income")
        except ModuleNotFoundError as exc:
            if exc.name in {"numpy", "pandas"}:
                self.skipTest(f"{exc.name} is not installed")
            raise

        self.assertEqual(entrypoint.manifest.profile, "hybrid_growth_income")
        self.assertEqual(
            entrypoint.manifest.default_config["managed_symbols"],
            ("TQQQ", "BOXX", "SPYI", "QQQI"),
        )

    def test_load_strategy_entrypoint_resolves_hybrid_growth_income_alias(self):
        try:
            from strategy_loader import load_strategy_entrypoint_for_profile

            entrypoint = load_strategy_entrypoint_for_profile("qqq_tqqq_growth_income")
        except ModuleNotFoundError as exc:
            if exc.name in {"numpy", "pandas"}:
                self.skipTest(f"{exc.name} is not installed")
            raise

        self.assertEqual(
            entrypoint.manifest.profile,
            "hybrid_growth_income",
        )

    def test_load_strategy_runtime_adapter_declares_available_inputs(self):
        from strategy_loader import load_strategy_runtime_adapter_for_profile

        adapter = load_strategy_runtime_adapter_for_profile("hybrid_growth_income")

        self.assertEqual(
            adapter.available_inputs,
            frozenset({"benchmark_history", "portfolio_snapshot"}),
        )
        self.assertEqual(adapter.portfolio_input_name, "portfolio_snapshot")

    def test_load_strategy_runtime_adapter_supports_semiconductor_on_schwab(self):
        from strategy_loader import load_strategy_runtime_adapter_for_profile

        adapter = load_strategy_runtime_adapter_for_profile("semiconductor_rotation_income")

        self.assertEqual(
            adapter.available_inputs,
            frozenset({"derived_indicators", "portfolio_snapshot"}),
        )
        self.assertEqual(adapter.portfolio_input_name, "portfolio_snapshot")


if __name__ == "__main__":
    unittest.main()
