"""Safe projection of an already-read Schwab account snapshot for reports."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any


_ACCOUNT_TYPE_TOKEN = re.compile(r"[A-Za-z_]{1,32}\Z", re.ASCII)
_CASH_MONEY_TEXT = re.compile(r"^-?(?:0|[1-9]\d*)(?:\.\d+)?$")


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


def _cash_money_text(value: Any) -> str | None:
    """Accept the bounded decimal text contract used by account-facts cash rows."""

    if not isinstance(value, str) or _CASH_MONEY_TEXT.fullmatch(value) is None:
        return None
    whole, _, fraction = value.lstrip("-").partition(".")
    if len(whole) > 15 or len(fraction) > 8:
        return None
    try:
        amount = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not amount.is_finite():
        return None
    return value


def _position_decimal_text(value: Any) -> str | None:
    """Finite decimal text bounded to 8 fractional / 15 total digits, else None."""
    if isinstance(value, bool):
        return None
    text = _money_text(value)
    if text is None:
        return None
    amount = Decimal(text).quantize(Decimal("0.00000001")).normalize()
    if amount == 0:
        amount = Decimal(0)
    text = format(amount, "f")
    whole, _, fraction = text.lstrip("-").partition(".")
    if len(whole) + len(fraction) > 15:
        return None
    return text


POSITIONS_SCOPE = "strategy_symbols_only"
_POSITION_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9./ -]{0,31}\Z", re.ASCII)
_MAX_POSITIONS = 64


def build_broker_reported_positions(snapshot: Any) -> list[dict[str, object]] | None:
    """Project positions the broker already returned; None means unknown/omit.

    Schwab's snapshot only lists strategy symbols, so the scope label is
    ``strategy_symbols_only``. An empty or malformed position set is treated as
    unknown (``None``) rather than written as an empty list.
    """

    try:
        positions = getattr(snapshot, "positions", None)
        if not isinstance(positions, (tuple, list)) or not positions:
            return None
        if len(positions) > _MAX_POSITIONS:
            return None
        rows: list[dict[str, object]] = []
        seen: set[str] = set()
        for position in positions:
            symbol = getattr(position, "symbol", None)
            if not isinstance(symbol, str):
                return None
            symbol = symbol.strip().upper()
            if _POSITION_SYMBOL.fullmatch(symbol) is None or symbol in seen:
                return None
            seen.add(symbol)
            quantity = _position_decimal_text(getattr(position, "quantity", None))
            market_value = _position_decimal_text(getattr(position, "market_value", None))
            if quantity is None or market_value is None:
                return None
            rows.append(
                {
                    "symbol": symbol,
                    "quantity": quantity,
                    "market_value": market_value,
                    # Schwab position rows carry no currency; currency is only
                    # declared downstream from the owner-confirmed account currency.
                    "currency": None,
                }
            )
        rows.sort(key=lambda row: str(row["symbol"]))
        return rows
    except Exception:
        return None


def _attach_positions(observation: dict[str, object], snapshot: Any) -> None:
    positions = build_broker_reported_positions(snapshot)
    if positions:
        observation["broker_reported_positions"] = positions
        observation["broker_reported_positions_scope"] = POSITIONS_SCOPE


def build_account_observation(
    snapshot: Any,
    *,
    net_assets_currency: str | None = None,
    cash_currency: str | None = None,
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

        raw_cash_balance = metadata.get("broker_cash_balance")
        raw_cash_balance_source = metadata.get("broker_cash_balance_source")
        cash_balance = _cash_money_text(raw_cash_balance)
        raw_account_type = metadata.get("broker_account_type")
        raw_account_type_source = metadata.get("broker_account_type_source")

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
        if cash_balance is not None and raw_cash_balance_source == "cashBalance":
            observation["cash_balance"] = cash_balance
            observation["cash_balance_source"] = "cashBalance"
        if (
            isinstance(raw_account_type, str)
            and _ACCOUNT_TYPE_TOKEN.fullmatch(raw_account_type) is not None
            and raw_account_type_source == "securitiesAccount.type"
        ):
            observation["broker_account_type"] = raw_account_type
            observation["broker_account_type_source"] = "securitiesAccount.type"
        # Optional, fail-soft: any problem omits the positions keys entirely.
        _attach_positions(observation, snapshot)
        declared_observation = declare_net_assets_currency(
            observation,
            net_assets_currency=net_assets_currency,
        )
        return declare_cash_balance_currency(
            declared_observation,
            cash_currency=cash_currency,
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


def declare_cash_balance_currency(
    observation: Mapping[str, object] | None,
    *,
    cash_currency: str | None = None,
) -> dict[str, object] | None:
    """Apply an independent owner-confirmed currency only to native cashBalance."""

    if not isinstance(observation, Mapping):
        return None
    projected = dict(observation)
    projected["cash_currency"] = None
    projected["cash_currency_source"] = None
    if (
        cash_currency != "USD"
        or not isinstance(projected.get("cash_balance"), str)
        or projected.get("cash_balance_source") != "cashBalance"
    ):
        return projected
    projected["cash_currency"] = "USD"
    projected["cash_currency_source"] = "owner_confirmed"
    return projected


__all__ = [
    "POSITIONS_SCOPE",
    "build_account_observation",
    "build_broker_reported_positions",
    "declare_cash_balance_currency",
    "declare_net_assets_currency",
    "expected_account_hash_from_selector",
]
