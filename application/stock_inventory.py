"""Native stock inventory for read-only observations, independent of strategy sizing."""

from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any


_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9./ -]{0,31}\Z", re.ASCII)
_NON_STOCK_TYPES = frozenset({"OPTION", "MUTUAL_FUND", "FIXED_INCOME", "CASH_EQUIVALENT", "CURRENCY"})


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        return None
    text = str(value)
    if len(text) > 64:
        return None
    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None
    if not amount.is_finite():
        return None
    _, digits, exponent = amount.as_tuple()
    if max(1, len(digits) + exponent) > 15 or max(0, -exponent) > 8:
        return None
    return amount


def project_stock_inventory(payload: Any, *, currency: str | None) -> list[dict[str, Any]] | None:
    """None means unknown; an explicit valid list can prove an empty stock scope."""
    if currency != "USD" or not isinstance(payload, Mapping):
        return None
    account = payload.get("securitiesAccount")
    if not isinstance(account, Mapping):
        return None
    native = account.get("positions")
    if not isinstance(native, list) or len(native) > 512:
        return None
    rows = []
    seen: set[str] = set()
    for position in native:
        if not isinstance(position, Mapping) or not isinstance(position.get("instrument"), Mapping):
            return None
        instrument = position["instrument"]
        asset_type = instrument.get("assetType")
        if not isinstance(asset_type, str):
            return None
        if asset_type in _NON_STOCK_TYPES:
            continue
        if asset_type != "EQUITY":
            return None
        symbol = instrument.get("symbol")
        if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None or symbol in seen:
            return None
        long_quantity = _decimal(position.get("longQuantity"))
        short_quantity = _decimal(position.get("shortQuantity"))
        if long_quantity is None or short_quantity is None or min(long_quantity, short_quantity) < 0:
            return None
        seen.add(symbol)
        market_value = _decimal(position.get("marketValue"))
        rows.append({
            "symbol": symbol,
            "quantity": format(long_quantity - short_quantity, "f"),
            "market_value": format(market_value, "f") if market_value is not None else None,
            "currency": "USD",
            "currency_source": "owner_confirmed",
        })
        if len(rows) > 64:
            return None
    return sorted(rows, key=lambda row: row["symbol"])


def collect_stock_inventory(client: Any, *, account_hash: str, currency: str | None) -> dict[str, Any] | None:
    """One account GET, using the account already verified by the guarded snapshot."""
    if not isinstance(account_hash, str) or not account_hash.strip() or currency != "USD":
        return None
    from schwab.client import Client

    response = client.get_account(account_hash, fields=Client.Account.Fields.POSITIONS)
    response.raise_for_status()
    rows = project_stock_inventory(response.json(), currency=currency)
    if rows is None:
        return None
    return {
        "broker_reported_positions": rows,
        "broker_reported_positions_scope": "stocks_only",
        "broker_reported_positions_complete": True,
        "broker_reported_positions_observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
