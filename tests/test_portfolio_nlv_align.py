from __future__ import annotations

from datetime import datetime, timezone

from application.portfolio_nlv_align import align_cash_only_sleeve_to_broker_liquidation
from quant_platform_kit.common.models import PortfolioSnapshot, Position


def test_align_cash_only_sleeve_to_broker_liquidation_shrinks_cash() -> None:
    snapshot = PortfolioSnapshot(
        as_of=datetime.now(timezone.utc),
        total_equity=472.0,
        buying_power=40.0,
        cash_balance=40.0,
        positions=(Position(symbol="SOXL", quantity=3.0, market_value=450.0),),
        metadata={
            "total_equity_source": "broker_liquidation_value",
            "cash_available_for_trading": 40.0,
            "account_hash": "acct",
        },
    )

    aligned = align_cash_only_sleeve_to_broker_liquidation(
        snapshot,
        cash_only_execution=True,
    )

    assert aligned.total_equity == 472.0
    assert aligned.metadata["strategy_equity_before_nlv_align"] == 490.0
    assert aligned.metadata["broker_liquidation_value"] == 472.0
    assert aligned.cash_balance == 22.0
    assert aligned.buying_power == 22.0
    assert aligned.metadata["cash_available_for_trading"] == 22.0
    assert aligned.metadata["broker_cash_available_for_trading"] == 40.0


def test_align_cash_only_sleeve_skipped_when_not_cash_only() -> None:
    snapshot = PortfolioSnapshot(
        as_of=datetime.now(timezone.utc),
        total_equity=472.0,
        buying_power=40.0,
        cash_balance=40.0,
        positions=(Position(symbol="SOXL", quantity=3.0, market_value=450.0),),
        metadata={
            "total_equity_source": "broker_liquidation_value",
            "cash_available_for_trading": 40.0,
        },
    )

    aligned = align_cash_only_sleeve_to_broker_liquidation(
        snapshot,
        cash_only_execution=False,
    )

    assert aligned is snapshot


def test_align_cash_only_sleeve_noop_when_already_within_nlv() -> None:
    snapshot = PortfolioSnapshot(
        as_of=datetime.now(timezone.utc),
        total_equity=500.0,
        buying_power=50.0,
        cash_balance=50.0,
        positions=(Position(symbol="SOXL", quantity=1.0, market_value=400.0),),
        metadata={
            "total_equity_source": "broker_liquidation_value",
            "cash_available_for_trading": 50.0,
        },
    )

    aligned = align_cash_only_sleeve_to_broker_liquidation(
        snapshot,
        cash_only_execution=True,
    )

    assert aligned is snapshot
