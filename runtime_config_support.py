from __future__ import annotations

import os
from dataclasses import dataclass


DEFAULT_STRATEGY_PROFILE = "hybrid_growth_income"
SUPPORTED_STRATEGY_PROFILES = {DEFAULT_STRATEGY_PROFILE}
DEFAULT_NOTIFY_LANG = "en"


@dataclass(frozen=True)
class PlatformRuntimeSettings:
    strategy_profile: str
    notify_lang: str


def resolve_strategy_profile() -> str:
    strategy_profile = os.getenv("STRATEGY_PROFILE", DEFAULT_STRATEGY_PROFILE).strip()
    if strategy_profile not in SUPPORTED_STRATEGY_PROFILES:
        raise ValueError(
            "Unsupported STRATEGY_PROFILE: "
            f"{strategy_profile}. Supported values: {sorted(SUPPORTED_STRATEGY_PROFILES)}"
        )
    return strategy_profile


def load_platform_runtime_settings() -> PlatformRuntimeSettings:
    return PlatformRuntimeSettings(
        strategy_profile=resolve_strategy_profile(),
        notify_lang=os.getenv("NOTIFY_LANG", DEFAULT_NOTIFY_LANG),
    )
