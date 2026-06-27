"""Compatibility shim; implementation lives in us_equity_strategies.signals."""

from us_equity_strategies.signals import (
    DEFAULT_MARKET_SIGNAL_CACHE_DIR,
    MARKET_SIGNAL_REFERENCE_CONSUMPTION_AUDIT,
    MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF,
    MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF_INDEX,
    default_market_signal_inputs_when_unconfigured,
    extract_consumer_market_signal_inputs_from_reference,
    market_signal_consumer_for_strategy_profile,
    resolve_external_market_signal_inputs,
)

__all__ = [
    "DEFAULT_MARKET_SIGNAL_CACHE_DIR",
    "MARKET_SIGNAL_REFERENCE_CONSUMPTION_AUDIT",
    "MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF",
    "MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF_INDEX",
    "default_market_signal_inputs_when_unconfigured",
    "extract_consumer_market_signal_inputs_from_reference",
    "market_signal_consumer_for_strategy_profile",
    "resolve_external_market_signal_inputs",
]
