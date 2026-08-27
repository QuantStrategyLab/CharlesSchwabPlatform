from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from application.paper_execution_command_consumer import (  # noqa: E402
    SCHWAB_PAPER_EXECUTION_INTENT_SCHEMA_VERSION,
    consume_due_paper_execution_commands,
)
from quant_platform_kit.common.execution_commands import (  # noqa: E402
    ExecutionCommand,
    ExecutionCommandState,
    ExecutionCommandStore,
)
from quant_platform_kit.common.models import PortfolioSnapshot, Position, QuoteSnapshot  # noqa: E402
from quant_platform_kit.common.paper_execution_admission import (  # noqa: E402
    build_paper_risk_admission_receipt,
)
from quant_platform_kit.common.strategy_release import build_runtime_loaded_receipt  # noqa: E402


def _release_identity() -> dict[str, str]:
    return {
        "release_id": "soxl-p2-v3.20260824",
        "manifest_sha256": "a" * 64,
        "strategy_revision": "soxl-p2-v3",
        "config_sha256": "b" * 64,
        "risk_policy_sha256": "c" * 64,
        "evidence_sha256": "d" * 64,
        "plugin_bundle_sha256": "e" * 64,
        "effective_session": "2026-08-25",
    }


def _binding() -> dict[str, str]:
    return {
        "platform": "schwab",
        "account_scope": "paper",
        "strategy_profile": "soxl_soxx_trend_income",
    }


def _command(*, platform: str = "schwab") -> ExecutionCommand:
    release = _release_identity()
    receipt = build_paper_risk_admission_receipt(
        strategy_profile="soxl_soxx_trend_income",
        release_id=release["release_id"],
        risk_policy_sha256=release["risk_policy_sha256"],
        decision_digest="d" * 64,
        effective_session="2026-08-25",
        disposition="allow_new_risk",
        reason_codes=(),
    )
    return ExecutionCommand.from_decision(
        platform=platform,
        account_scope="paper",
        strategy_profile="soxl_soxx_trend_income",
        execution_mode="paper",
        signal_date="2026-08-24",
        effective_date="2026-08-25",
        execution_timing_contract="next_trading_day",
        decision_digest="d" * 64,
        intent={
            "schema_version": SCHWAB_PAPER_EXECUTION_INTENT_SCHEMA_VERSION,
            "target_mode": "value",
            "targets": {"SOXL": 100.0, "BOXX": 100.0},
            "strategy_symbols": ["SOXL", "BOXX"],
            "strategy_release": release,
            "paper_risk_admission_receipt": receipt.to_dict(),
        },
        created_at="2026-08-24T20:00:00+00:00",
    )


def _portfolio(*, include_unmanaged: bool = False) -> PortfolioSnapshot:
    positions = [Position(symbol="SOXL", quantity=20.0, market_value=200.0)]
    if include_unmanaged:
        positions.append(Position(symbol="AAPL", quantity=1.0, market_value=100.0))
    return PortfolioSnapshot(
        as_of=datetime(2026, 8, 25, tzinfo=timezone.utc),
        total_equity=1000.0 + (100.0 if include_unmanaged else 0.0),
        cash_balance=800.0,
        positions=tuple(positions),
    )


class _MarketDataPort:
    def get_quote(self, symbol: str) -> QuoteSnapshot:
        return QuoteSnapshot(
            symbol=symbol,
            as_of=datetime(2026, 8, 25, tzinfo=timezone.utc),
            last_price=10.0,
        )


def _consume(store: ExecutionCommandStore, **overrides) -> dict[str, object]:
    release = _release_identity()
    values: dict[str, object] = {
        "store": store,
        "as_of_session": "2026-08-25",
        "claimant": "schwab-paper-command-verify",
        "portfolio_loader": _portfolio,
        "market_data_port_loader": _MarketDataPort,
        "managed_symbols": ("SOXL", "BOXX"),
        "runtime_release_receipt": build_runtime_loaded_receipt(strategy_release=release),
        "expected_strategy_release": release,
        "expected_command_binding": _binding(),
    }
    values.update(overrides)
    return consume_due_paper_execution_commands(**values)


def test_schwab_consumer_records_reconciled_paper_proposals(tmp_path: Path) -> None:
    store = ExecutionCommandStore(local_dir=tmp_path)
    command = _command()
    assert store.enqueue(command)

    result = _consume(store)

    assert result["commands"] == [
        {
            "command_id": command.command_id,
            "status": "filled",
            "proposals_count": 2,
            "would_block": False,
        }
    ]
    assert store.current_state(command) is ExecutionCommandState.FILLED
    proposals = store.events(command)[1].details["proposals"]
    assert [proposal["symbol"] for proposal in proposals] == ["BOXX", "SOXL"]
    assert [proposal["exposure_effect"] for proposal in proposals] == ["increases", "reduces"]
    assert proposals[0]["details"]["side"] == "buy"


def test_schwab_consumer_does_not_read_broker_facts_for_another_platform(tmp_path: Path) -> None:
    store = ExecutionCommandStore(local_dir=tmp_path)
    command = _command(platform="longbridge")
    assert store.enqueue(command)

    def _unexpected_read():
        raise AssertionError("command binding must block before a broker read")

    result = _consume(
        store,
        portfolio_loader=_unexpected_read,
        market_data_port_loader=_unexpected_read,
    )

    assert result["commands"][0]["status"] == "rejected"
    receipt = store.events(command)[-1].details["runtime_command_gate_receipts"][0]
    assert receipt["reasons"] == ["command_platform_mismatch"]


def test_schwab_consumer_rejects_unreconciled_account_evidence(tmp_path: Path) -> None:
    store = ExecutionCommandStore(local_dir=tmp_path)
    command = _command()
    assert store.enqueue(command)

    result = _consume(store, portfolio_loader=lambda: _portfolio(include_unmanaged=True))

    assert result["commands"][0]["status"] == "rejected"
    receipt = store.events(command)[-1].details["runtime_command_gate_receipts"][0]
    assert receipt["mode"] == "halted"
    assert "position_reconciliation_mismatch" in receipt["reasons"]
