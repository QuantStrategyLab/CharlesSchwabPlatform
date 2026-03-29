import unittest


class StrategyLoaderTests(unittest.TestCase):
    def test_load_allocation_module_resolves_hybrid_growth_income(self):
        try:
            from strategy_loader import load_allocation_module

            module = load_allocation_module("hybrid_growth_income")
        except ModuleNotFoundError as exc:
            if exc.name in {"numpy", "pandas"}:
                self.skipTest(f"{exc.name} is not installed")
            raise

        self.assertEqual(
            module.__name__,
            "us_equity_strategies.strategies.hybrid_growth_income",
        )


if __name__ == "__main__":
    unittest.main()
