"""Tests for Schwab daily-loss fact producer (cashflow-adjusted, fail-soft)."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from application.daily_loss_fact_producer import (
    EXTERNAL_CASH_FLOW_TYPES,
    derive_daily_loss_usd,
    fetch_schwab_transactions,
    produce_daily_loss_fact,
    produce_daily_loss_fact_attempt,
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


def test_identified_external_flow_with_bad_or_missing_time_is_unverified_not_zero():
    """F2: baseline 500, deposit +100, equity 550 → loss 50 when time valid; never verified/0."""
    window_start = datetime(2026, 9, 18, 9, 0, tzinfo=ZoneInfo("UTC"))
    window_end = datetime(2026, 9, 18, 13, 0, tzinfo=ZoneInfo("UTC"))
    valid = [
        _tx(type="CASH_RECEIPT", status="VALID", time="2026-09-18T10:00:00+0000", net_amount=100.0),
    ]
    ok = summarize_verified_external_cash_flow(
        valid, window_start=window_start, window_end=window_end
    )
    assert ok.status == "verified"
    assert ok.net_flow == 100.0
    assert derive_daily_loss_usd(500.0, ok.net_flow, 550.0) == 50.0

    for bad in (
        _tx(type="CASH_RECEIPT", status="VALID", time="invalid", net_amount=100.0),
        {
            "type": "CASH_RECEIPT",
            "status": "VALID",
            "netAmount": 100.0,
            "transferItems": [{"instrument": {"assetType": "CURRENCY"}}],
        },
        _tx(type="CASH_RECEIPT", status="VALID", time="", net_amount=100.0),
    ):
        result = summarize_verified_external_cash_flow(
            [bad], window_start=window_start, window_end=window_end
        )
        assert result.status == "unverified"
        assert result.net_flow is None
        assert derive_daily_loss_usd(500.0, result.net_flow, 550.0) is None


def test_cash_flow_window_timezone_endpoints_withdrawal_and_internal_trade():
    window_start = datetime(2026, 9, 18, 9, 0, tzinfo=NY)
    window_end = datetime(2026, 9, 18, 16, 0, tzinfo=NY)
    transactions = [
        # Same instant as start (exclusive) → safely outside.
        _tx(type="WIRE_IN", status="VALID", time="2026-09-18T09:00:00-0400", net_amount=25.0),
        # Inside via offset without colon.
        _tx(type="ACH_DISBURSEMENT", status="VALID", time="2026-09-18T12:00:00-0400", net_amount=-40.0),
        # Explicitly after end → outside.
        _tx(type="WIRE_OUT", status="VALID", time="2026-09-18T16:00:01-0400", net_amount=-10.0),
        # Internal trade is not external capital.
        _tx(
            type="TRADE",
            status="VALID",
            time="2026-09-18T11:00:00-0400",
            net_amount=-200.0,
            asset_types=["EQUITY", "CURRENCY"],
        ),
        # End boundary inclusive.
        _tx(type="CASH_RECEIPT", status="VALID", time="2026-09-18T16:00:00-0400", net_amount=15.0),
    ]
    result = summarize_verified_external_cash_flow(
        transactions, window_start=window_start, window_end=window_end
    )
    assert result.status == "verified"
    assert result.net_flow == -25.0
    assert result.event_count == 2


def test_produce_omits_when_external_flow_time_unverified():
    session_open = datetime(2026, 9, 18, 9, 30, tzinfo=NY)
    prior_close = datetime(2026, 9, 17, 16, 0, tzinfo=NY)
    attempt = produce_daily_loss_fact_attempt(
        current_equity_usd=550.0,
        reference_now=datetime(2026, 9, 18, 13, 45, tzinfo=NY),
        session_open=session_open,
        prior_session_close=prior_close,
        reports_loader=lambda: [_report(finished_at="2026-09-17T19:35:15+00:00", equity=500.0)],
        transactions_loader=lambda **_: [
            _tx(type="CASH_RECEIPT", status="VALID", time="invalid", net_amount=100.0),
        ],
    )
    assert attempt.status == "omitted"
    assert attempt.fact is None
    assert "unverified" in attempt.reason


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


def test_select_session_baseline_uses_previous_session_day_after_weekend():
    session_open = datetime(2026, 9, 21, 9, 30, tzinfo=NY)
    prior_close = datetime(2026, 9, 18, 16, 0, tzinfo=NY)
    reports = [
        _report(finished_at="2026-09-18T19:35:15+00:00", equity=260.94),
        _report(finished_at="2026-09-17T19:35:15+00:00", equity=250.00),
    ]

    baseline = select_session_baseline(
        reports,
        session_open=session_open,
        prior_session_close=prior_close,
    )

    assert baseline is not None
    assert baseline.equity_usd == 260.94
    assert baseline.source == "runtime_report_prior_session"


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


def test_produce_attempt_exposes_transactions_load_failed_reason():
    session_open = datetime(2026, 9, 18, 9, 30, tzinfo=NY)
    prior_close = datetime(2026, 9, 17, 16, 0, tzinfo=NY)
    attempt = produce_daily_loss_fact_attempt(
        current_equity_usd=500.0,
        reference_now=datetime(2026, 9, 18, 13, 45, tzinfo=NY),
        session_open=session_open,
        prior_session_close=prior_close,
        reports_loader=lambda: [_report(finished_at="2026-09-17T19:35:15+00:00", equity=590.34)],
        transactions_loader=lambda **_: (_ for _ in ()).throw(RuntimeError("broker unavailable")),
    )
    assert attempt.status == "omitted"
    assert attempt.reason == "transactions_load_failed"
    assert attempt.fact is None


def test_c1_dual_account_first_hash_differs_from_expected_is_reachable():
    """C1: synthetic dual-account chain — [0] must not silently replace expected_account_hash."""
    fetched: list[str] = []

    class _Resp:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def json(self):
            return self._payload

    class _Client:
        def get_account_numbers(self):
            return _Resp(
                [
                    {"hashValue": "acct-first"},
                    {"hashValue": "acct-expected"},
                ]
            )

        def get_transactions(self, account_hash, **_kwargs):
            fetched.append(account_hash)
            return _Resp([])

    start = datetime(2026, 9, 17, 16, 0, tzinfo=NY)
    end = datetime(2026, 9, 18, 14, 0, tzinfo=NY)
    # Reachability: without binding, loader historically preferred numbers[0].
    fetch_schwab_transactions(_Client(), start=start, end=end, expected_account_hash="acct-expected")
    assert fetched == ["acct-expected"]
    fetched.clear()
    # Multi-account with no expected identity must fail closed (reuse portfolio identity rules).
    try:
        fetch_schwab_transactions(_Client(), start=start, end=end)
        raised = False
    except RuntimeError:
        raised = True
    assert raised is True
