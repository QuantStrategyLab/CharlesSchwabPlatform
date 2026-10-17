"""Goldens captured at 6c890e3fdfbc0f8dcd7f96b5a634cc482ba3606c with pinned dependencies."""

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
