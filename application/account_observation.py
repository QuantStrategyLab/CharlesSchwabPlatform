"""Safe projection of an already-read Schwab account snapshot for reports."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


def expected_account_hash_from_selector(account_selector: Any) -> str | None:
    """Resolve only an explicit single-account selector; preserve legacy live lookup."""

    if account_selector is None:
        return None
    if isinstance(account_selector, str):
        selectors = (account_selector,)
    else:
        try:
            selectors = tuple(account_selector)
        except TypeError:
            raise ValueError("Schwab account selector is invalid") from None
    if not selectors:
        return None
    if len(selectors) != 1:
        raise ValueError("Schwab account selector must identify at most one account")
    selector = selectors[0]
    if not isinstance(selector, str) or not selector.strip():
        raise ValueError("Schwab account selector is invalid")
    if selector == "live":
        return None
    return selector


def _money_text(value: Any) -> str | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not amount.is_finite():
        return None
    return format(amount, "f")


def build_account_observation(
    snapshot: Any,
    *,
    net_assets_currency: str | None = None,
) -> dict[str, object] | None:
    """Project verified values without changing the snapshot or raising into execution."""

    try:
        metadata = getattr(snapshot, "metadata", None)
        if not isinstance(metadata, Mapping):
            return None
        account_hash = metadata.get("account_hash")
        observed_at = getattr(snapshot, "as_of", None)
        if not isinstance(account_hash, str) or not account_hash.strip():
            return None
        if not isinstance(observed_at, datetime) or observed_at.tzinfo is None:
            return None
        if observed_at.utcoffset() is None:
            return None

        net_assets = None
        net_assets_source = None
        if metadata.get("total_equity_source") == "broker_liquidation_value":
            net_assets = _money_text(getattr(snapshot, "total_equity", None))
            if net_assets is not None:
                net_assets_source = "liquidationValue"

        raw_available_for_trading = metadata.get(
            "broker_cash_available_for_trading",
            metadata.get("cash_available_for_trading"),
        )
        available_for_trading = _money_text(raw_available_for_trading)
        available_for_withdrawal = _money_text(
            metadata.get("cash_available_for_withdrawal")
        )

        observation: dict[str, object] = {
            "account_hash": account_hash,
            "currency": None,
            "observed_at": observed_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "net_assets": net_assets,
            "net_assets_source": net_assets_source,
            "available_for_trading": available_for_trading,
            "available_for_trading_source": (
                "cashAvailableForTrading" if available_for_trading is not None else None
            ),
            "available_for_withdrawal": available_for_withdrawal,
            "available_for_withdrawal_source": (
                "cashAvailableForWithdrawal" if available_for_withdrawal is not None else None
            ),
        }
        return declare_net_assets_currency(
            observation,
            net_assets_currency=net_assets_currency,
        )
    except Exception:
        # Reporting must never change the outcome of an already-run strategy cycle.
        return None


def declare_net_assets_currency(
    observation: Mapping[str, object] | None,
    *,
    net_assets_currency: str | None = None,
) -> dict[str, object] | None:
    """Add only the user's explicit currency declaration for verified NLV."""

    if not isinstance(observation, Mapping):
        return None
    projected = dict(observation)
    projected["net_assets_currency"] = None
    projected["net_assets_currency_source"] = None
    if net_assets_currency in (None, ""):
        return projected
    if net_assets_currency != "USD":
        return None
    if projected.get("net_assets") is None or projected.get("net_assets_source") != "liquidationValue":
        return None
    projected["net_assets_currency"] = "USD"
    projected["net_assets_currency_source"] = "owner_confirmed"
    return projected


__all__ = [
    "build_account_observation",
    "declare_net_assets_currency",
    "expected_account_hash_from_selector",
]
