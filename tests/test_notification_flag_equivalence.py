"""Goldens captured at bb4cafb7c9b252ada9730ed607e377d1639ff9d5 with pinned dependencies."""

import json
from pathlib import Path

import pytest

from notif_equivalence_harness import record_cycle


@pytest.mark.parametrize("flag", [None, "", "false", "invalid"])
@pytest.mark.parametrize("scenario", ["healthy", "blocked", "order", "dry_run"])
@pytest.mark.parametrize("locale", ["zh", "en"])
def test_flags_off_real_rebalance_entrypoint_matches_main_calls_and_output(monkeypatch, flag, scenario, locale):
    monkeypatch.setenv("ACCOUNT_NEW_RISK_GATE", "0")
    monkeypatch.delenv("QSL_ROUTINE_NOTIFICATION_MODE", raising=False)
    monkeypatch.delenv("QSL_DIGEST_ROUTINE_ORDERS", raising=False)
    if flag is None:
        monkeypatch.delenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", flag)
    expected = json.loads((Path(__file__).parent / "fixtures" / "notif_main_calls.json").read_text())
    # JSON normalization preserves sequence order, broker arguments, execution
    # persistence position, every send byte, and all result fields.
    actual = json.loads(json.dumps(record_cycle(scenario, locale), default=str))
    assert actual == expected[f"{scenario}:{locale}"]


@pytest.mark.parametrize("cost", [None, "80.00"])
def test_optional_main_cost_fact_normalizes_only_unknown_and_preserves_known_values(monkeypatch, cost):
    from application import rebalance_service

    monkeypatch.setenv("ACCOUNT_NEW_RISK_GATE", "0")
    monkeypatch.delenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", raising=False)
    original = rebalance_service.build_account_observation

    def observation(snapshot, **kwargs):
        result = original(snapshot, **kwargs)
        result["broker_reported_positions"][0]["avg_cost"] = cost
        return result

    monkeypatch.setattr(rebalance_service, "build_account_observation", observation)
    result = record_cycle("healthy")
    position = result["result"]["account_observation"]["broker_reported_positions"][0]
    if cost is None:
        assert "avg_cost" not in position
    else:
        assert position["avg_cost"] == cost
