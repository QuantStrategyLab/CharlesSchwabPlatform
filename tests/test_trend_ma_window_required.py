import pytest

from application.runtime_strategy_adapters import required_trend_ma_window


def test_reads_manifest_value():
    assert required_trend_ma_window({"trend_ma_window": 140}) == 140
    assert required_trend_ma_window({"trend_ma_window": "140"}) == 140


@pytest.mark.parametrize("config", [{}, {"trend_ma_window": None}, {"trend_ma_window": ""}])
def test_missing_fails_closed_instead_of_150(config):
    with pytest.raises(ValueError, match="trend_ma_window missing"):
        required_trend_ma_window(config)


@pytest.mark.parametrize("raw", [0, -5, "abc", 140.5, True])
def test_invalid_rejected(raw):
    with pytest.raises(ValueError):
        required_trend_ma_window({"trend_ma_window": raw})


def test_live_soxl_manifest_has_window():
    from us_equity_strategies.manifests import soxl_soxx_trend_income_manifest

    assert required_trend_ma_window(dict(soxl_soxx_trend_income_manifest.default_config)) == 140
