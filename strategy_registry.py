from __future__ import annotations

from us_equity_strategies import (
    get_platform_runtime_adapter,
    get_runtime_enabled_profiles,
    get_strategy_catalog,
)
from quant_us_combo_strategies import (
    get_platform_runtime_adapter as get_combo_runtime_adapter,
    get_runtime_enabled_profiles as get_combo_runtime_enabled_profiles,
    get_strategy_catalog as get_combo_strategy_catalog,
)

from quant_platform_kit.common.execution_capabilities import (
    FRACTIONAL_SHARE_EXECUTION_CAPABILITY,
)
from quant_platform_kit.common.strategies import (
    PlatformCapabilityMatrix,
    PlatformStrategyPolicy,
    StrategyCatalog,
    StrategyDefinition,
    StrategyMetadata,
    US_EQUITY_DOMAIN,
    build_platform_profile_matrix,
    build_platform_profile_status_matrix,
    derive_enabled_profiles_for_platform,
    derive_eligible_profiles_for_platform,
    get_enabled_profiles_for_platform,
    get_catalog_strategy_metadata,
    resolve_platform_strategy_definition,
)

SCHWAB_PLATFORM = "schwab"
COMBOS_DOMAIN = "quant_combo"
TECH_COMMUNICATION_PULLBACK_PROFILE = "tech_communication_pullback_enhancement"

SCHWAB_EXCLUDED_LIVE_PROFILES = frozenset(
    {
        TECH_COMMUNICATION_PULLBACK_PROFILE,
    }
)
SCHWAB_ROLLOUT_ALLOWLIST = (
    get_runtime_enabled_profiles()
    | get_combo_runtime_enabled_profiles()
) - SCHWAB_EXCLUDED_LIVE_PROFILES

PLATFORM_SUPPORTED_DOMAINS: dict[str, frozenset[str]] = {
    SCHWAB_PLATFORM: frozenset({US_EQUITY_DOMAIN, COMBOS_DOMAIN}),
}


def _merge_strategy_catalogs(*catalogs: StrategyCatalog) -> StrategyCatalog:
    definitions: dict[str, StrategyDefinition] = {}
    metadata: dict[str, StrategyMetadata] = {}
    compatible_platforms: dict[str, frozenset[str]] = {}
    profile_aliases: dict[str, str] = {}
    for catalog in catalogs:
        for profile, definition in catalog.definitions.items():
            if profile in definitions and definitions[profile] != definition:
                raise ValueError(f"Duplicate strategy definition for profile {profile!r}")
            definitions[profile] = definition
        for profile, value in catalog.metadata.items():
            if profile in metadata and metadata[profile] != value:
                raise ValueError(f"Duplicate strategy metadata for profile {profile!r}")
            metadata[profile] = value
        for profile, platforms in catalog.compatible_platforms.items():
            if profile in compatible_platforms and compatible_platforms[profile] != platforms:
                raise ValueError(f"Duplicate strategy platform compatibility for profile {profile!r}")
            compatible_platforms[profile] = platforms
        for alias, profile_name in catalog.profile_aliases.items():
            if alias in profile_aliases and profile_aliases[alias] != profile_name:
                raise ValueError(f"Duplicate strategy alias {alias!r}")
            profile_aliases[alias] = profile_name
    return StrategyCatalog(
        definitions=definitions,
        metadata=metadata,
        compatible_platforms=compatible_platforms,
        profile_aliases=profile_aliases,
    )


STRATEGY_CATALOG = _merge_strategy_catalogs(
    get_strategy_catalog(),
    get_combo_strategy_catalog(),
)
PLATFORM_CAPABILITY_MATRIX = PlatformCapabilityMatrix(
    platform_id=SCHWAB_PLATFORM,
    supported_domains=PLATFORM_SUPPORTED_DOMAINS[SCHWAB_PLATFORM],
    supported_target_modes=frozenset({"weight", "value"}),
    supported_inputs=frozenset(
        {
            "benchmark_history",
            "market_history",
            "portfolio_snapshot",
            "feature_snapshot",
            "derived_indicators",
            "indicators",
            "account_state",
            "snapshot",
            "russell_snapshot",
            "current_holdings",
            "market_data",
        }
    ),
    # Schwab supports fractional / notional equity orders via the native
    # ``quantityType=DOLLARS`` API.  Paper-tested 2026-06-29.
    # Non-DCA profiles continue whole-share execution.
    supported_capabilities=frozenset({FRACTIONAL_SHARE_EXECUTION_CAPABILITY}),
)
COMBO_STRATEGY_PROFILES = frozenset(get_combo_strategy_catalog().definitions)


def _get_platform_runtime_adapter_router(profile: str, *, platform_id: str):
    """Route to the correct adapter based on the profile's domain."""
    if profile in COMBO_STRATEGY_PROFILES:
        return get_combo_runtime_adapter(profile, platform_id=platform_id)
    return get_platform_runtime_adapter(profile, platform_id=platform_id)


ELIGIBLE_STRATEGY_PROFILES = derive_eligible_profiles_for_platform(
    STRATEGY_CATALOG,
    capability_matrix=PLATFORM_CAPABILITY_MATRIX,
    runtime_adapter_loader=lambda profile: _get_platform_runtime_adapter_router(
        profile,
        platform_id=SCHWAB_PLATFORM,
    ),
) - SCHWAB_EXCLUDED_LIVE_PROFILES
SCHWAB_ENABLED_PROFILES = derive_enabled_profiles_for_platform(
    STRATEGY_CATALOG,
    capability_matrix=PLATFORM_CAPABILITY_MATRIX,
    runtime_adapter_loader=lambda profile: _get_platform_runtime_adapter_router(
        profile,
        platform_id=SCHWAB_PLATFORM,
    ),
    rollout_allowlist=SCHWAB_ROLLOUT_ALLOWLIST,
)
PLATFORM_POLICY = PlatformStrategyPolicy(
    platform_id=SCHWAB_PLATFORM,
    supported_domains=PLATFORM_SUPPORTED_DOMAINS[SCHWAB_PLATFORM],
    enabled_profiles=SCHWAB_ENABLED_PROFILES,
    default_profile="",
    rollback_profile="",
    require_explicit_profile=True,
)

SUPPORTED_STRATEGY_PROFILES = SCHWAB_ENABLED_PROFILES
_SELECTION_ROLE_FIELDS = frozenset({"is_default", "is_rollback"})


def _without_selection_role_fields(row: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in row.items() if key not in _SELECTION_ROLE_FIELDS}


def get_eligible_profiles_for_platform(platform_id: str) -> frozenset[str]:
    if platform_id != SCHWAB_PLATFORM:
        return frozenset()
    return ELIGIBLE_STRATEGY_PROFILES


def get_supported_profiles_for_platform(platform_id: str) -> frozenset[str]:
    return get_enabled_profiles_for_platform(platform_id, policy=PLATFORM_POLICY)


def get_platform_profile_matrix() -> list[dict[str, object]]:
    return [
        _without_selection_role_fields(row)
        for row in build_platform_profile_matrix(STRATEGY_CATALOG, policy=PLATFORM_POLICY)
    ]


def get_platform_profile_status_matrix() -> list[dict[str, object]]:
    return [
        _without_selection_role_fields(row)
        for row in build_platform_profile_status_matrix(
            STRATEGY_CATALOG,
            policy=PLATFORM_POLICY,
            eligible_profiles=ELIGIBLE_STRATEGY_PROFILES,
        )
    ]


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


def resolve_strategy_metadata(
    raw_value: str | None,
    *,
    platform_id: str,
):
    definition = resolve_strategy_definition(raw_value, platform_id=platform_id)
    return get_catalog_strategy_metadata(STRATEGY_CATALOG, definition.profile)
