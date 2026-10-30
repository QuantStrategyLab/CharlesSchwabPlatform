"""Goldens captured at bb4cafb7c9b252ada9730ed607e377d1639ff9d5 with pinned dependencies."""

import json
from pathlib import Path

import pytest

from notif_equivalence_harness import record_cycle


@pytest.mark.parametrize("flag", [None, "", "false", "invalid"])
@pytest.mark.parametrize("scenario", ["healthy", "blocked", "order", "dry_run"])
@pytest.mark.parametrize("locale", ["zh", "en"])
def test_flags_off_preserves_business_calls_and_urgent_output(monkeypatch, flag, scenario, locale):
    monkeypatch.setenv("ACCOUNT_NEW_RISK_GATE", "0")
    monkeypatch.delenv("QSL_ROUTINE_NOTIFICATION_MODE", raising=False)
    monkeypatch.delenv("QSL_DIGEST_ROUTINE_ORDERS", raising=False)
    monkeypatch.delenv("QSL_NOTIFY_LANG", raising=False)
    monkeypatch.setenv("NOTIFY_LANG", locale)
    if flag is None:
        monkeypatch.delenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", raising=False)
    else:
        monkeypatch.setenv("SCHWAB_UNIFIED_NOTIFICATION_ENABLED", flag)
    expected = json.loads((Path(__file__).parent / "fixtures" / "notif_main_calls.json").read_text())
    # Preserve all business calls and results; notification-only evidence is new.
    actual = json.loads(json.dumps(record_cycle(scenario, locale), default=str))
    previous = expected[f"{scenario}:{locale}"]
    assert [call for call in actual["calls"] if call[0] != "send"] == [
        call for call in previous["calls"] if call[0] != "send"]
    actual_sends = [call for call in actual["calls"] if call[0] == "send"]
    previous_sends = [call for call in previous["calls"] if call[0] == "send"]
    if scenario == "healthy":
        assert len(actual_sends) == 1
        assert ("摘要" if locale == "zh" else "coverage") in actual_sends[0][1].lower()
    else:
        assert actual_sends == previous_sends
    execution = actual["result"]["execution"]
    for field in ("notification_cycle_completed", "notification_order_issue", "notification_events"):
        execution.pop(field, None)
    assert actual["result"] == previous["result"]


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
