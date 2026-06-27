from __future__ import annotations

import importlib
import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
QPK_SRC = ROOT.parent / "QuantPlatformKit" / "src"
if str(QPK_SRC) not in sys.path:
    sys.path.insert(0, str(QPK_SRC))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant_platform_kit.common.execution_capabilities import (
    FRACTIONAL_SHARE_EXECUTION_CAPABILITY,
    FRACTIONAL_SHARE_EXECUTION_SKIP_REASON,
)
from quant_platform_kit.common.strategies import PlatformCapabilityMatrix, StrategyCatalog, StrategyDefinition

_DCA_DEFINITION = StrategyDefinition(
    profile="nasdaq_sp500_smart_dca",
    domain="us_equity",
    supported_platforms=frozenset({"schwab", "firstrade"}),
    compatible_capabilities=frozenset({FRACTIONAL_SHARE_EXECUTION_CAPABILITY}),
)
_FAKE_CATALOG = StrategyCatalog(
    definitions={
        "nasdaq_sp500_smart_dca": _DCA_DEFINITION,
        "ibit_smart_dca": StrategyDefinition(
            profile="ibit_smart_dca",
            domain="us_equity",
            supported_platforms=frozenset({"schwab", "firstrade"}),
            compatible_capabilities=frozenset({FRACTIONAL_SHARE_EXECUTION_CAPABILITY}),
        ),
        "global_etf_rotation": StrategyDefinition(
            profile="global_etf_rotation",
            domain="us_equity",
            supported_platforms=frozenset({"schwab"}),
            compatible_capabilities=frozenset(),
        ),
    }
)
_FAKE_CAPABILITY_MATRIX = PlatformCapabilityMatrix(
    platform_id="schwab",
    supported_domains=frozenset({"us_equity"}),
    supported_target_modes=frozenset({"weight", "value"}),
    supported_inputs=frozenset(),
    supported_capabilities=frozenset(),
)

_fake_registry = types.ModuleType("strategy_registry")
_fake_registry.PLATFORM_CAPABILITY_MATRIX = _FAKE_CAPABILITY_MATRIX
_fake_registry.STRATEGY_CATALOG = _FAKE_CATALOG


class RuntimeExecutionPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._original_registry = sys.modules.get("strategy_registry")
        sys.modules["strategy_registry"] = _fake_registry
        policy = importlib.import_module("runtime_execution_policy")
        cls._policy = importlib.reload(policy)

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._original_registry is None:
            sys.modules.pop("strategy_registry", None)
        else:
            sys.modules["strategy_registry"] = cls._original_registry

    def test_dca_profiles_are_deferred_on_schwab(self) -> None:
        for profile in ("nasdaq_sp500_smart_dca", "ibit_smart_dca"):
            self.assertEqual(
                self._policy.dca_execution_unsupported_reason(profile),
                FRACTIONAL_SHARE_EXECUTION_SKIP_REASON,
            )
            self.assertFalse(self._policy.notional_buy_execution_enabled(profile))

    def test_rotation_profile_uses_whole_share_mode(self) -> None:
        self.assertIsNone(self._policy.dca_execution_unsupported_reason("global_etf_rotation"))
        self.assertFalse(self._policy.notional_buy_execution_enabled("global_etf_rotation"))


if __name__ == "__main__":
    unittest.main()
