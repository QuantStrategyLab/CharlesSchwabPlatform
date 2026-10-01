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
            "broker_cash_balance": "123.4500",
            "broker_cash_balance_source": "cashBalance",
            "broker_account_type": "PROVIDER_UNKNOWN",
            "broker_account_type_source": "securitiesAccount.type",
        },
    )

    observation = build_account_observation(
        snapshot, net_assets_currency="USD", cash_currency="USD"
    )

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
    assert observation["cash_balance"] == "123.4500"
    assert observation["cash_balance_source"] == "cashBalance"
    assert observation["cash_currency"] == "USD"
    assert observation["cash_currency_source"] == "owner_confirmed"
    assert observation["broker_account_type"] == "PROVIDER_UNKNOWN"
    assert observation["broker_account_type_source"] == "securitiesAccount.type"
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


def test_nlv_currency_does_not_confirm_cash_and_invalid_optional_facts_are_omitted() -> None:
    snapshot = SimpleNamespace(
        as_of=datetime(2026, 9, 30, 10, tzinfo=timezone.utc),
        total_equity=123.45,
        buying_power=500.0,
        cash_balance=0.0,
        positions=(),
        metadata={
            "account_hash": "synthetic-account-id",
            "total_equity_source": "broker_liquidation_value",
            "broker_cash_balance": "1234567890123456.123456789",
            "broker_cash_balance_source": "cashBalance",
            "broker_account_type": "not a token",
            "broker_account_type_source": "securitiesAccount.type",
        },
    )

    observation = build_account_observation(snapshot, net_assets_currency="USD")

    assert observation is not None
    assert observation["net_assets_currency"] == "USD"
    assert observation["net_assets_currency_source"] == "owner_confirmed"
    assert "cash_balance" not in observation
    assert observation["cash_currency"] is None
    assert observation["cash_currency_source"] is None
    assert "broker_account_type" not in observation
    assert "broker_account_type_source" not in observation


def test_cash_currency_confirmation_requires_exact_usd_and_native_cash_fact() -> None:
    snapshot = SimpleNamespace(
        as_of=datetime(2026, 9, 30, 10, tzinfo=timezone.utc),
        total_equity=123.45,
        buying_power=500.0,
        cash_balance=0.0,
        positions=(),
        metadata={
            "account_hash": "synthetic-account-id",
            "broker_cash_balance": "0",
            "broker_cash_balance_source": "cashBalance",
        },
    )

    for declared_currency, expected in ((None, None), ("USD ", None), ("EUR", None), ("USD", "USD")):
        observation = build_account_observation(snapshot, cash_currency=declared_currency)
        assert observation is not None
        assert observation["cash_currency"] == expected
        assert observation["cash_currency_source"] == ("owner_confirmed" if expected else None)
