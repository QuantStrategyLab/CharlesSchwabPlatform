from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import market_signal_runtime


def test_non_ibit_profile_does_not_load_market_signal():
    settings = SimpleNamespace(market_signal_required=True)

    assert (
        market_signal_runtime.resolve_external_market_signal_inputs(
            strategy_profile="soxl_soxx_trend_income",
            available_inputs={"derived_indicators"},
            runtime_settings=settings,
        )
        == {}
    )


def test_ibit_without_reference_provides_empty_indicator_input():
    settings = SimpleNamespace(market_signal_required=False)

    assert market_signal_runtime.resolve_external_market_signal_inputs(
        strategy_profile="ibit_smart_dca",
        available_inputs={"derived_indicators"},
        runtime_settings=settings,
    ) == {"derived_indicators": {}}


def test_ibit_required_reference_missing_raises():
    settings = SimpleNamespace(market_signal_required=True)

    with pytest.raises(RuntimeError, match="external market signal is required"):
        market_signal_runtime.resolve_external_market_signal_inputs(
            strategy_profile="ibit_smart_dca",
            available_inputs={"derived_indicators"},
            runtime_settings=settings,
        )


def test_ibit_handoff_index_reference_is_extracted(monkeypatch, tmp_path):
    calls: dict[str, object] = {}

    def fake_extract(
        reference,
        *,
        reference_type,
        consumer,
        cache_dir,
        as_of,
        client_factory=None,
        fallback_mode=None,
        fallback_max_stale_days=None,
    ):
        calls["extract"] = (
            reference,
            reference_type,
            consumer,
            cache_dir,
            as_of,
            client_factory,
            fallback_mode,
            fallback_max_stale_days,
        )
        return {"derived_indicators": {"BTC": {"mvrv_z_score": 1.0}}}, {
            "reference_type": reference_type,
            "source_uri": reference,
            "materialized_count": 2,
        }

    monkeypatch.setattr(
        market_signal_runtime,
        "extract_consumer_market_signal_inputs_from_reference",
        fake_extract,
    )
    settings = SimpleNamespace(
        market_signal_handoff_index_uri="gs://signals/platform_handoffs/index.json",
        market_signal_cache_dir=str(tmp_path),
        market_signal_required=False,
        market_signal_fallback_mode="last_valid",
        market_signal_max_stale_days=5,
    )

    assert market_signal_runtime.resolve_external_market_signal_inputs(
        strategy_profile="ibit_smart_dca",
        available_inputs={"derived_indicators"},
        runtime_settings=settings,
        as_of=datetime(2026, 6, 19, tzinfo=timezone.utc),
        logger=lambda _message: None,
        client_factory=object,
    ) == {"derived_indicators": {"BTC": {"mvrv_z_score": 1.0}}}
    assert calls["extract"] == (
        "gs://signals/platform_handoffs/index.json",
        "platform_handoff_index",
        "us_equity:ibit_smart_dca",
        tmp_path,
        "2026-06-19",
        object,
        "last_valid",
        5,
    )
