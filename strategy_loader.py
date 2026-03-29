from __future__ import annotations

from types import ModuleType

from quant_platform_kit.common.strategies import load_strategy_component_module

from strategy_registry import SCHWAB_PLATFORM, resolve_strategy_definition


def load_allocation_module(raw_profile: str | None) -> ModuleType:
    definition = resolve_strategy_definition(
        raw_profile,
        platform_id=SCHWAB_PLATFORM,
    )
    return load_strategy_component_module(
        definition,
        component_name="allocation",
    )
