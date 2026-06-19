from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from us_equity_strategies.signals import (
    IBIT_SMART_DCA_MARKET_SIGNAL_CONSUMER,
    MARKET_SIGNAL_REFERENCE_CONSUMPTION_AUDIT,
    MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF,
    MARKET_SIGNAL_REFERENCE_PLATFORM_HANDOFF_INDEX,
    extract_consumer_market_signal_inputs_from_reference,
)


IBIT_SMART_DCA_PROFILE = "ibit_smart_dca"
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
    if str(strategy_profile or "").strip().lower() != IBIT_SMART_DCA_PROFILE:
        return {}
    if "derived_indicators" not in {str(item) for item in available_inputs or ()}:
        return {}

    reference_type, reference = _market_signal_reference(runtime_settings)
    if reference is None:
        if bool(getattr(runtime_settings, "market_signal_required", False)):
            raise RuntimeError("IBIT external market signal is required but no signal reference is configured")
        return {"derived_indicators": {}}

    market_inputs, metadata = extract_consumer_market_signal_inputs_from_reference(
        reference,
        reference_type=reference_type,
        consumer=IBIT_SMART_DCA_MARKET_SIGNAL_CONSUMER,
        cache_dir=_market_signal_cache_dir(runtime_settings),
        as_of=_market_signal_as_of(as_of),
        client_factory=client_factory,
    )
    logger(
        "market_signal_inputs_loaded | "
        f"profile={strategy_profile} reference_type={metadata.get('reference_type')} "
        f"source_uri={metadata.get('source_uri') or reference} "
        f"materialized_count={metadata.get('materialized_count')}"
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
