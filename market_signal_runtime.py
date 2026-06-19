from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from us_equity_strategies.signals import (
    IBIT_SMART_DCA_MARKET_SIGNAL_CONSUMER,
    MARKET_SIGNAL_REFERENCE_CONSUMPTION_AUDIT,
    MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF,
    MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF_INDEX,
    SOXL_SOXX_TREND_INCOME_MARKET_SIGNAL_CONSUMER,
    extract_consumer_market_signal_inputs_from_reference,
)


IBIT_SMART_DCA_PROFILE = "ibit_smart_dca"
SOXL_SOXX_TREND_INCOME_PROFILE = "soxl_soxx_trend_income"
MARKET_SIGNAL_CONSUMER_BY_PROFILE = {
    IBIT_SMART_DCA_PROFILE: IBIT_SMART_DCA_MARKET_SIGNAL_CONSUMER,
    SOXL_SOXX_TREND_INCOME_PROFILE: SOXL_SOXX_TREND_INCOME_MARKET_SIGNAL_CONSUMER,
}
DEFAULT_MARKET_SIGNAL_CACHE_DIR = "/tmp/quant-platform-market-signals"


def resolve_external_market_signal_inputs(
    *,
    strategy_profile: str,
    available_inputs: Iterable[str],
    runtime_settings: Any,
    as_of: Any = None,
    logger: Callable[[str], None] = print,
    client_factory: Any = None,
) -> dict[str, Any]:
    normalized_profile = str(strategy_profile or "").strip().lower()
    consumer = MARKET_SIGNAL_CONSUMER_BY_PROFILE.get(normalized_profile)
    if consumer is None:
        return {}
    if "derived_indicators" not in {str(item) for item in available_inputs or ()}:
        return {}

    reference_type, reference = _market_signal_reference(runtime_settings)
    if reference is None:
        if bool(getattr(runtime_settings, "market_signal_required", False)):
            raise RuntimeError(
                f"{normalized_profile} external market signal is required "
                "but no signal reference is configured"
            )
        if normalized_profile == IBIT_SMART_DCA_PROFILE:
            return {"derived_indicators": {}}
        return {}

    market_inputs, metadata = extract_consumer_market_signal_inputs_from_reference(
        reference,
        reference_type=reference_type,
        consumer=consumer,
        cache_dir=_market_signal_cache_dir(runtime_settings),
        as_of=_market_signal_as_of(as_of),
        client_factory=client_factory,
        fallback_mode=_market_signal_fallback_mode(runtime_settings),
        fallback_max_stale_days=_market_signal_max_stale_days(runtime_settings),
    )
    logger(
        "market_signal_inputs_loaded | "
        f"profile={strategy_profile} reference_type={metadata.get('reference_type')} "
        f"source_uri={metadata.get('source_uri') or reference} "
        f"materialized_count={metadata.get('materialized_count')} "
        f"fallback_used={bool(metadata.get('artifact_fallback_used'))}"
    )
    return dict(market_inputs)


def _market_signal_reference(runtime_settings: Any) -> tuple[str, str | None]:
    consumption_audit_uri = _optional_string(
        getattr(runtime_settings, "market_signal_consumption_audit_uri", None)
    )
    if consumption_audit_uri:
        return MARKET_SIGNAL_REFERENCE_CONSUMPTION_AUDIT, consumption_audit_uri

    handoff_manifest_uri = _optional_string(
        getattr(runtime_settings, "market_signal_handoff_manifest_uri", None)
    )
    if handoff_manifest_uri:
        return MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF, handoff_manifest_uri

    handoff_index_uri = _optional_string(
        getattr(runtime_settings, "market_signal_handoff_index_uri", None)
    )
    if handoff_index_uri:
        return MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF_INDEX, handoff_index_uri

    return MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF_INDEX, None


def _market_signal_cache_dir(runtime_settings: Any) -> Path:
    configured = _optional_string(getattr(runtime_settings, "market_signal_cache_dir", None))
    return Path(configured or DEFAULT_MARKET_SIGNAL_CACHE_DIR)


def _market_signal_fallback_mode(runtime_settings: Any) -> str:
    return _optional_string(getattr(runtime_settings, "market_signal_fallback_mode", None)) or "none"


def _market_signal_max_stale_days(runtime_settings: Any) -> int:
    value = getattr(runtime_settings, "market_signal_max_stale_days", None)
    if value is None or str(value).strip() == "":
        return 3
    return max(0, int(value))


def _market_signal_as_of(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text[:10] if text else None


def _optional_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None
