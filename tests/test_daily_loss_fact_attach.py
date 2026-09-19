"""Attach path for daily-loss fact into portfolio snapshot."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from application.daily_loss_fact_producer import attach_daily_loss_fact_to_portfolio

NY = ZoneInfo("America/New_York")


def test_attach_preserves_explicit_daily_loss():
    portfolio = {
        "total_equity": 500.0,
        "account_new_risk_snapshot": {"daily_loss_usd": 12.5},
    }
    out = attach_daily_loss_fact_to_portfolio(
        portfolio,
        client=object(),
        reports_loader=lambda: (_ for _ in ()).throw(RuntimeError("should not load")),
        transactions_loader=lambda **_: (_ for _ in ()).throw(RuntimeError("should not load")),
    )
    assert out["account_new_risk_snapshot"]["daily_loss_usd"] == 12.5


def test_attach_injects_verified_fact():
    session_open = datetime(2026, 9, 18, 9, 30, tzinfo=NY)
    # Force bounds via produce path: use loaders only; attach resolves calendar.
    # Provide reports that fall in fallback window relative to a real calendar day.
    reports = [
        {
            "status": "ok",
            "finished_at": "2026-09-17T20:05:00+00:00",
            "summary": {"total_equity": 500.0},
        }
    ]

    def transactions_loader(*, start, end):
        return [
            {
                "type": "CASH_RECEIPT",
                "status": "VALID",
                "time": "2026-09-18T12:00:00+0000",
                "netAmount": 50.0,
                "transferItems": [{"instrument": {"assetType": "CURRENCY"}}],
            }
        ]

    # Monkeypatch session bounds by calling produce through attach with custom loaders;
    # attach uses resolve_nasdaq_session_bounds(now). Use a weekday afternoon NY time.
    now = datetime(2026, 9, 18, 14, 0, tzinfo=NY)
    out = attach_daily_loss_fact_to_portfolio(
        {"total_equity": 480.0, "strategy_profile": "soxl_soxx_trend_income"},
        client=object(),
        reference_now=now,
        reports_loader=lambda: reports,
        transactions_loader=transactions_loader,
    )
    # 500 + 50 - 480 = 70 loss
    assert out.get("daily_loss_usd") == 70.0
    assert out["account_new_risk_snapshot"]["daily_loss_fact_status"] == "verified"


def test_attach_default_loader_binds_expected_account_from_portfolio_metadata():
    """C1 consumer seam: portfolio metadata identity is passed into fetch (not numbers[0])."""
    from unittest.mock import patch

    prior = datetime(2026, 9, 17, 16, 0, tzinfo=NY)
    session_open = datetime(2026, 9, 18, 9, 30, tzinfo=NY)
    reports = [
        {
            "status": "ok",
            "finished_at": "2026-09-17T20:05:00+00:00",
            "summary": {"total_equity": 500.0},
        }
    ]
    now = datetime(2026, 9, 18, 14, 0, tzinfo=NY)
    with patch(
        "application.daily_loss_fact_producer.resolve_nasdaq_session_bounds",
        return_value=(prior, session_open),
    ):
        with patch(
            "application.daily_loss_fact_producer.fetch_schwab_transactions",
            return_value=[],
        ) as fetch:
            attach_daily_loss_fact_to_portfolio(
                {
                    "total_equity": 480.0,
                    "metadata": {"account_hash": "acct-expected"},
                    "strategy_profile": "soxl_soxx_trend_income",
                },
                client=object(),
                reference_now=now,
                reports_loader=lambda: reports,
                transactions_loader=None,
            )
    assert fetch.call_count == 1
    assert fetch.call_args.kwargs["expected_account_hash"] == "acct-expected"
