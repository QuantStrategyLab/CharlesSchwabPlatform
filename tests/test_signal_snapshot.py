from application.signal_snapshot import build_signal_snapshot


def test_includes_soxl_dynamic_volatility_fields():
    snapshot = build_signal_snapshot(
        platform="schwab",
        strategy_profile="soxl_soxx_trend_income",
        execution={
            "blend_gate_volatility_delever_threshold_mode": "rolling_percentile",
            "blend_gate_volatility_delever_threshold": 0.60,
            "blend_gate_volatility_delever_dynamic_threshold": 0.60,
            "blend_gate_volatility_delever_dynamic_sample_count": 252,
            "blend_gate_volatility_delever_dynamic_percentile": 0.95,
            "blend_gate_volatility_delever_metric": 0.61,
            "blend_gate_volatility_delever_triggered": True,
        },
    )

    indicators = snapshot["indicators"]
    assert indicators["blend_gate_volatility_delever_threshold_mode"] == "rolling_percentile"
    assert indicators["blend_gate_volatility_delever_dynamic_threshold"] == 0.60
    assert indicators["blend_gate_volatility_delever_dynamic_sample_count"] == 252
    assert indicators["blend_gate_volatility_delever_triggered"] is True
