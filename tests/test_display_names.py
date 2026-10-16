import importlib.util
import json
from pathlib import Path
from unittest.mock import patch


def load_resolver():
    root = Path(__file__).resolve().parents[1]
    path = root / "notifications/display_names.py"
    spec = importlib.util.spec_from_file_location("display_names_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.display_name


def test_display_mapping_and_unknown_names_do_not_rewrite_inputs():
    resolve = load_resolver()
    identity = {"platform": "binance", "account_key": "crypto_combo", "strategy_profile": "crypto_live_pool_rotation"}
    original = dict(identity)
    assert resolve("accounts", identity["account_key"], platform=identity["platform"]) == "live"
    assert resolve("accounts", "paper", platform="binance") == "paper"
    assert resolve("accounts", "crypto_combo", "zh", platform="binance") == "实盘"
    assert resolve("lanes", "paper", "zh") == "模拟"
    assert resolve("accounts", "crypto_combo", platform="other") == "crypto_combo"
    assert resolve("strategies", identity["strategy_profile"], "en") == "Crypto Pool Rotation"
    assert resolve("strategies", identity["strategy_profile"], "zh-CN") == "加密池轮动"
    assert resolve("strategies", "custom", fallback="Operator label") == "Operator label"
    assert resolve("strategies", "global_macro_etf_rotation", "en") == "Global ETF Rotation"
    assert identity == original


def test_display_overrides_and_bad_configuration_keep_alerts_renderable():
    resolve = load_resolver()
    override = {"strategies": {"crypto_live_pool_rotation": {"zh": "加密-轮动-202609", "en": "Crypto-Rotation-202609"}}, "accounts": {"binance": {"crypto_combo": "custom live"}}}
    with patch.dict("os.environ", {"QSL_DISPLAY_NAMES_JSON": json.dumps(override)}):
        assert resolve("strategies", "crypto_live_pool_rotation", "en") == "Crypto-Rotation-202609"
        assert resolve("accounts", "crypto_combo", platform="binance") == "custom live"
    for raw in ("{", "[]", "null", '{"strategy_aliases":{"custom":[]},"strategies":[]}'):
        with patch.dict("os.environ", {"QSL_DISPLAY_NAMES_JSON": raw}):
            assert resolve("strategies", "custom", fallback="Urgent") == "Urgent"
