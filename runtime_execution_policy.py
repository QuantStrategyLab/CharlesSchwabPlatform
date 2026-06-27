from __future__ import annotations

from quant_platform_kit.common.execution_capabilities import (
    FRACTIONAL_SHARE_EXECUTION_SKIP_REASON,
    definition_requires_fractional_share_execution,
    fractional_share_execution_unsupported_reason,
)
from quant_platform_kit.common.strategies import normalize_profile_name
from strategy_registry import PLATFORM_CAPABILITY_MATRIX, STRATEGY_CATALOG


def dca_execution_unsupported_reason(strategy_profile: str) -> str | None:
    return fractional_share_execution_unsupported_reason(
        strategy_profile,
        strategy_catalog=STRATEGY_CATALOG,
        capability_matrix=PLATFORM_CAPABILITY_MATRIX,
    )


def notional_buy_execution_enabled(strategy_profile: str) -> bool:
    if dca_execution_unsupported_reason(strategy_profile) is not None:
        return False
    normalized_profile = normalize_profile_name(strategy_profile)
    definition = STRATEGY_CATALOG.definitions.get(normalized_profile)
    if definition is None:
        return False
    return definition_requires_fractional_share_execution(definition)


__all__ = (
    "FRACTIONAL_SHARE_EXECUTION_SKIP_REASON",
    "dca_execution_unsupported_reason",
    "notional_buy_execution_enabled",
)
