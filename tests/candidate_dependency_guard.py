"""Gate candidate-only integration tests when published pins predate the APIs."""
import importlib
import os

import pytest


def require_candidate_dependencies():
    requirements = {
        "quant_platform_kit.common.strategy_risk_state": (
            "DailyRiskStateBootstrapAuthorization", "coordinate_daily_strategy_risk_state"),
        "quant_platform_kit.common.decision_consumption_receipts": (
            "attach_decision_consumption_receipt", "attach_decision_execution_link"),
        "us_equity_strategies.tqqq_daily_state": (
            "prepare_tqqq_daily_state", "finish_tqqq_daily_state"),
        "us_equity_strategies.decision_observation": ("attach_optional_decision_receipt",),
    }
    try:
        for module_name, names in requirements.items():
            module = importlib.import_module(module_name)
            for name in names:
                if not hasattr(module, name):
                    raise ImportError(f"{module_name} lacks {name}")
    except ImportError as exc:
        if os.environ.get("REQUIRE_QSL_STRATEGY_INTEGRATION") == "1":
            raise RuntimeError("required candidate integration dependencies are unavailable") from exc
        pytest.skip("candidate integration APIs are unavailable with published pins", allow_module_level=True)
