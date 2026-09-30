from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from application.account_observation import build_account_observation


def test_owner_declared_usd_applies_only_to_verified_nlv_and_preserves_observation() -> None:
    as_of = datetime(2026, 9, 30, 18, 45, tzinfo=timezone(timedelta(hours=8)))
    positions = (SimpleNamespace(symbol="TEST", quantity=1, market_value=12345.67),)
    snapshot = SimpleNamespace(
        as_of=as_of,
        total_equity=12345.67,
        buying_power=1200.0,
        cash_balance=0.0,
        positions=positions,
        metadata={
            "account_hash": "synthetic-account-id",
            "total_equity_source": "broker_liquidation_value",
            "broker_cash_available_for_trading": 900.0,
            "cash_available_for_withdrawal": 750.0,
        },
    )

    observation = build_account_observation(snapshot, net_assets_currency="USD")

    assert observation is not None
    assert observation["account_hash"] == "synthetic-account-id"
    assert observation["observed_at"] == "2026-09-30T10:45:00Z"
    assert observation["net_assets"] == "12345.67"
    assert observation["net_assets_source"] == "liquidationValue"
    assert observation["net_assets_currency"] == "USD"
    assert observation["net_assets_currency_source"] == "owner_confirmed"
    assert observation["currency"] is None
    assert observation["available_for_trading"] == "900.0"
    assert observation["available_for_withdrawal"] == "750.0"
    assert "cash_balance" not in observation
    assert "cash" not in observation
    assert snapshot.as_of is as_of
    assert snapshot.total_equity == 12345.67
    assert snapshot.positions is positions


def test_cash_availability_does_not_create_cash_balance_or_zero_net_assets() -> None:
    snapshot = SimpleNamespace(
        as_of=datetime(2026, 9, 30, 10, tzinfo=timezone.utc),
        total_equity=0.0,
        buying_power=500.0,
        cash_balance=None,
        positions=(),
        metadata={
            "account_hash": "synthetic-account-id",
            "broker_cash_available_for_trading": 500.0,
        },
    )

    observation = build_account_observation(snapshot)
    assert build_account_observation(snapshot, net_assets_currency="USD") is None

    assert observation is not None
    assert observation["net_assets"] is None
    assert observation["net_assets_source"] is None
    assert observation["net_assets_currency"] is None
    assert observation["net_assets_currency_source"] is None
    assert observation["currency"] is None
    assert observation["available_for_trading"] == "500.0"
    assert "cash_balance" not in observation
    assert "cash" not in observation
