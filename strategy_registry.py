from __future__ import annotations

from dataclasses import dataclass

US_EQUITY_DOMAIN = "us_equity"
CRYPTO_DOMAIN = "crypto"

SCHWAB_PLATFORM = "schwab"


@dataclass(frozen=True)
class StrategyDefinition:
    profile: str
    domain: str
    supported_platforms: frozenset[str]


DEFAULT_STRATEGY_PROFILE = "hybrid_growth_income"

STRATEGY_DEFINITIONS: dict[str, StrategyDefinition] = {
    DEFAULT_STRATEGY_PROFILE: StrategyDefinition(
        profile=DEFAULT_STRATEGY_PROFILE,
        domain=US_EQUITY_DOMAIN,
        supported_platforms=frozenset({SCHWAB_PLATFORM}),
    ),
}

PLATFORM_SUPPORTED_DOMAINS: dict[str, frozenset[str]] = {
    SCHWAB_PLATFORM: frozenset({US_EQUITY_DOMAIN}),
}

SUPPORTED_STRATEGY_PROFILES = frozenset(STRATEGY_DEFINITIONS)


def get_supported_profiles_for_platform(platform_id: str) -> frozenset[str]:
    return frozenset(
        profile
        for profile, definition in STRATEGY_DEFINITIONS.items()
        if platform_id in definition.supported_platforms
        and definition.domain in PLATFORM_SUPPORTED_DOMAINS.get(platform_id, frozenset())
    )


def resolve_strategy_definition(
    raw_value: str | None,
    *,
    platform_id: str,
) -> StrategyDefinition:
    profile = (raw_value or DEFAULT_STRATEGY_PROFILE).strip().lower()
    supported = ", ".join(sorted(get_supported_profiles_for_platform(platform_id)))
    definition = STRATEGY_DEFINITIONS.get(profile)

    if definition is None or platform_id not in definition.supported_platforms:
        raise ValueError(
            f"Unsupported STRATEGY_PROFILE: {profile}. Supported values: {sorted(get_supported_profiles_for_platform(platform_id))}"
        )

    if definition.domain not in PLATFORM_SUPPORTED_DOMAINS.get(platform_id, frozenset()):
        raise ValueError(
            f"Unsupported strategy domain {definition.domain!r} for platform {platform_id!r}"
        )

    return definition
