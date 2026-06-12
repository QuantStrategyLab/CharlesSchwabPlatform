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


def test_includes_tqqq_volatility_delever_fields():
    snapshot = build_signal_snapshot(
        platform="schwab",
        strategy_profile="tqqq_growth_income",
        execution={
            "dual_drive_volatility_delever_threshold_mode": "rolling_percentile",
            "dual_drive_volatility_delever_threshold": 0.28,
            "dual_drive_volatility_delever_exit_threshold": 0.24,
            "dual_drive_volatility_delever_dynamic_threshold": 0.30,
            "dual_drive_volatility_delever_dynamic_sample_count": 252,
            "dual_drive_volatility_delever_dynamic_percentile": 0.90,
            "dual_drive_volatility_delever_metric": 0.312,
            "dual_drive_volatility_delever_applied": True,
            "dual_drive_volatility_delever_veto_reason": "taco_rebound_context",
            "dual_drive_volatility_delever_taco_veto_enabled": True,
            "dual_drive_volatility_delever_removed_value": 4500.0,
            "dual_drive_macro_risk_governor_applied": True,
            "dual_drive_macro_risk_governor_route": "risk_reduced",
            "dual_drive_crisis_defense_destination": "BOXX",
            "market_regime_control_route": "risk_reduced",
            "market_regime_control_reason_codes": ("macro:vix_crisis_level",),
            "dual_drive_volatility_delever_redirect_symbol": "QQQM",
        },
    )

    indicators = snapshot["indicators"]
    assert indicators["dual_drive_volatility_delever_threshold_mode"] == "rolling_percentile"
    assert indicators["dual_drive_volatility_delever_dynamic_threshold"] == 0.30
    assert indicators["dual_drive_volatility_delever_dynamic_sample_count"] == 252
    assert indicators["dual_drive_volatility_delever_applied"] is True
    assert indicators["dual_drive_volatility_delever_veto_reason"] == "taco_rebound_context"
    assert indicators["dual_drive_volatility_delever_taco_veto_enabled"] is True
    assert indicators["dual_drive_volatility_delever_removed_value"] == 4500.0
    assert indicators["dual_drive_macro_risk_governor_applied"] is True
    assert indicators["dual_drive_macro_risk_governor_route"] == "risk_reduced"
    assert indicators["dual_drive_crisis_defense_destination"] == "BOXX"
    assert indicators["market_regime_control_route"] == "risk_reduced"
    assert indicators["market_regime_control_reason_codes"] == ["macro:vix_crisis_level"]
    assert indicators["dual_drive_volatility_delever_redirect_symbol"] == "QQQM"
