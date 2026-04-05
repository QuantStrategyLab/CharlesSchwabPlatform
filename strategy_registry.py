from __future__ import annotations

from us_equity_strategies import get_strategy_catalog

from quant_platform_kit.common.strategies import (
    PlatformStrategyPolicy,
    StrategyDefinition,
    US_EQUITY_DOMAIN,
    build_platform_profile_matrix,
    get_enabled_profiles_for_platform,
    resolve_platform_strategy_definition,
)

SCHWAB_PLATFORM = "schwab"

DEFAULT_STRATEGY_PROFILE = "hybrid_growth_income"
ROLLBACK_STRATEGY_PROFILE = DEFAULT_STRATEGY_PROFILE

SCHWAB_ENABLED_PROFILES = frozenset({"hybrid_growth_income"})

PLATFORM_SUPPORTED_DOMAINS: dict[str, frozenset[str]] = {
    SCHWAB_PLATFORM: frozenset({US_EQUITY_DOMAIN}),
}
STRATEGY_CATALOG = get_strategy_catalog()
PLATFORM_POLICY = PlatformStrategyPolicy(
    platform_id=SCHWAB_PLATFORM,
    supported_domains=PLATFORM_SUPPORTED_DOMAINS[SCHWAB_PLATFORM],
    enabled_profiles=SCHWAB_ENABLED_PROFILES,
    default_profile=DEFAULT_STRATEGY_PROFILE,
    rollback_profile=ROLLBACK_STRATEGY_PROFILE,
)

SUPPORTED_STRATEGY_PROFILES = SCHWAB_ENABLED_PROFILES


def get_supported_profiles_for_platform(platform_id: str) -> frozenset[str]:
    return get_enabled_profiles_for_platform(platform_id, policy=PLATFORM_POLICY)


def get_platform_profile_matrix() -> list[dict[str, object]]:
    return build_platform_profile_matrix(STRATEGY_CATALOG, policy=PLATFORM_POLICY)


def resolve_strategy_definition(
    raw_value: str | None,
    *,
    platform_id: str,
) -> StrategyDefinition:
    return resolve_platform_strategy_definition(
        raw_value,
        platform_id=platform_id,
        strategy_catalog=STRATEGY_CATALOG,
        policy=PLATFORM_POLICY,
    )
