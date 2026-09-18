"""Tests for Schwab daily-loss fact producer (cashflow-adjusted, fail-soft)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from application.daily_loss_fact_producer import (
    EXTERNAL_CASH_FLOW_TYPES,
    derive_daily_loss_usd,
    produce_daily_loss_fact,
    select_session_baseline,
    summarize_verified_external_cash_flow,
)

NY = ZoneInfo("America/New_York")


def _tx(*, type: str, status: str, time: str, net_amount: float, asset_types: list[str] | None = None):
    items = []
    for asset in asset_types or ["CURRENCY"]:
        items.append({"instrument": {"assetType": asset}})
    return {
        "type": type,
        "status": status,
        "time": time,
        "netAmount": net_amount,
        "transferItems": items,
    }


def _report(*, finished_at: str, equity: float, status: str = "ok"):
    return {
        "status": status,
        "finished_at": finished_at,
        "summary": {"total_equity": equity, "buying_power": 1.0},
    }


def test_whitelist_constant_matches_frozen_v1():
    assert EXTERNAL_CASH_FLOW_TYPES == frozenset(
        {
            "CASH_RECEIPT",
            "CASH_DISBURSEMENT",
            "WIRE_IN",
            "WIRE_OUT",
            "ACH_RECEIPT",
            "ACH_DISBURSEMENT",
            "ELECTRONIC_FUND",
        }
    )


def test_flow_counts_valid_pure_currency_cash_and_ignores_trade_and_journal():
    transactions = [
        _tx(type="CASH_RECEIPT", status="VALID", time="2026-09-18T10:00:00+0000", net_amount=100.0),
        _tx(type="CASH_DISBURSEMENT", status="VALID", time="2026-09-18T11:00:00+0000", net_amount=-40.0),
        _tx(
            type="TRADE",
            status="VALID",
            time="2026-09-18T12:00:00+0000",
            net_amount=-50.0,
            asset_types=["COLLECTIVE_INVESTMENT", "CURRENCY"],
        ),
        _tx(type="JOURNAL", status="VALID", time="2026-09-18T12:30:00+0000", net_amount=10.0),
    ]
    result = summarize_verified_external_cash_flow(
        transactions,
        window_start=datetime(2026, 9, 18, 9, 0, tzinfo=ZoneInfo("UTC")),
        window_end=datetime(2026, 9, 18, 13, 0, tzinfo=ZoneInfo("UTC")),
    )
    assert result.status == "verified"
    assert result.net_flow == 60.0
    assert result.event_count == 2


def test_non_valid_whitelisted_event_marks_window_unverified():
    transactions = [
        _tx(type="CASH_RECEIPT", status="PENDING", time="2026-09-18T10:00:00+0000", net_amount=100.0),
    ]
    result = summarize_verified_external_cash_flow(
        transactions,
        window_start=datetime(2026, 9, 18, 9, 0, tzinfo=ZoneInfo("UTC")),
        window_end=datetime(2026, 9, 18, 13, 0, tzinfo=ZoneInfo("UTC")),
    )
    assert result.status == "unverified"
    assert result.net_flow is None


def test_derive_daily_loss_ignores_pure_deposit_and_withdrawal():
    # baseline 500, deposit +100 → equity 600, loss 0
    assert derive_daily_loss_usd(500.0, 100.0, 600.0) == 0.0
    # baseline 500, withdraw -100 → equity 400, loss 0
    assert derive_daily_loss_usd(500.0, -100.0, 400.0) == 0.0
    # baseline 500, no flow, equity 450 → loss 50
    assert derive_daily_loss_usd(500.0, 0.0, 450.0) == 50.0
    assert derive_daily_loss_usd(None, 0.0, 450.0) is None
    assert derive_daily_loss_usd(500.0, None, 450.0) is None


def test_select_session_baseline_prefers_latest_in_prior_close_to_open_window():
    # 2026-09-17 was Wednesday; use fixed open/close to avoid calendar flakiness in unit
    session_open = datetime(2026, 9, 18, 9, 30, tzinfo=NY)
    prior_close = datetime(2026, 9, 17, 16, 0, tzinfo=NY)
    reports = [
        _report(finished_at="2026-09-17T19:35:15+00:00", equity=590.34),  # 15:35 ET
        _report(finished_at="2026-09-17T20:00:00+00:00", equity=591.0),  # later same window
        _report(finished_at="2026-09-18T13:35:12+00:00", equity=245.37),  # after open — exclude
        _report(finished_at="2026-09-16T20:00:00+00:00", equity=580.0),  # before prior close
    ]
    baseline = select_session_baseline(
        reports,
        session_open=session_open,
        prior_session_close=prior_close,
    )
    assert baseline is not None
    assert baseline.equity_usd == 591.0


def test_produce_fact_end_to_end_with_injected_loaders():
    session_open = datetime(2026, 9, 18, 9, 30, tzinfo=NY)
    prior_close = datetime(2026, 9, 17, 16, 0, tzinfo=NY)
    baseline_as_of = datetime(2026, 9, 17, 15, 35, 15, tzinfo=NY)

    def reports_loader():
        return [
            {
                "status": "ok",
                "finished_at": baseline_as_of.astimezone(ZoneInfo("UTC")).isoformat(),
                "summary": {"total_equity": 590.34},
            }
        ]

    def transactions_loader(*, start, end):
        assert start <= end
        return [
            _tx(
                type="CASH_DISBURSEMENT",
                status="VALID",
                time="2026-09-18T08:30:34+0000",
                net_amount=-350.0,
            ),
            _tx(
                type="TRADE",
                status="VALID",
                time="2026-09-18T14:00:00+0000",
                net_amount=-10.0,
                asset_types=["EQUITY", "CURRENCY"],
            ),
        ]

    fact = produce_daily_loss_fact(
        current_equity_usd=245.37,
        reference_now=datetime(2026, 9, 18, 13, 45, tzinfo=NY),
        session_open=session_open,
        prior_session_close=prior_close,
        reports_loader=reports_loader,
        transactions_loader=transactions_loader,
    )
    assert fact is not None
    assert fact.status == "verified"
    # 590.34 + (-350) - 245.37 = ~-5.03 → loss 0 (withdrawal-adjusted)
    assert fact.daily_loss_usd == 0.0
    assert fact.verified_net_external_flow == -350.0


def test_produce_fact_returns_none_when_transaction_loader_fails():
    session_open = datetime(2026, 9, 18, 9, 30, tzinfo=NY)
    prior_close = datetime(2026, 9, 17, 16, 0, tzinfo=NY)

    def reports_loader():
        return [
            _report(finished_at="2026-09-17T19:35:15+00:00", equity=590.34),
        ]

    def transactions_loader(*, start, end):
        raise RuntimeError("broker unavailable")

    fact = produce_daily_loss_fact(
        current_equity_usd=500.0,
        reference_now=datetime(2026, 9, 18, 13, 45, tzinfo=NY),
        session_open=session_open,
        prior_session_close=prior_close,
        reports_loader=reports_loader,
        transactions_loader=transactions_loader,
    )
    assert fact is None
