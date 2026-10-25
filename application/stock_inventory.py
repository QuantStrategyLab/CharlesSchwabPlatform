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


def project_stock_inventory(
    payload: Any, *, currency: str | None, diagnostics: dict[str, Any] | None = None,
) -> list[dict[str, Any]] | None:
    """None means unknown; an explicit valid list can prove an empty stock scope."""
    def unknown(reason: str) -> None:
        if diagnostics is not None:
            diagnostics["reason"] = reason
        return None

    if currency != "USD" or not isinstance(payload, Mapping):
        return unknown("currency_or_payload_unusable")
    account = payload.get("securitiesAccount")
    if not isinstance(account, Mapping):
        return unknown("account_object_missing")
    native = account.get("positions")
    if diagnostics is not None:
        diagnostics["positions_field_present"] = "positions" in account
        diagnostics["native_positions_count"] = len(native) if isinstance(native, list) else None
    if "positions" not in account:
        return unknown("positions_field_missing")
    if not isinstance(native, list) or len(native) > 512:
        return unknown("positions_list_unusable")
    rows = []
    seen: set[str] = set()
    for position in native:
        if not isinstance(position, Mapping) or not isinstance(position.get("instrument"), Mapping):
            return unknown("position_instrument_unusable")
        instrument = position["instrument"]
        asset_type = instrument.get("assetType")
        if not isinstance(asset_type, str):
            return unknown("instrument_type_missing")
        if asset_type in _NON_STOCK_TYPES:
            continue
        if asset_type != "EQUITY":
            return unknown("instrument_type_unsupported")
        symbol = instrument.get("symbol")
        if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None or symbol in seen:
            return unknown("stock_symbol_unusable_or_duplicate")
        long_quantity = _decimal(position.get("longQuantity"))
        short_quantity = _decimal(position.get("shortQuantity"))
        if long_quantity is None or short_quantity is None or min(long_quantity, short_quantity) < 0:
            return unknown("stock_quantity_unusable")
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
            return unknown("stock_inventory_too_large")
    if diagnostics is not None:
        diagnostics["projected_stock_count"] = len(rows)
        diagnostics["reason"] = "available"
    return sorted(rows, key=lambda row: row["symbol"])


def _single_linked_account_payload(
    client: Any, account_hash: str, *, diagnostics: dict[str, Any],
) -> Mapping[str, Any] | None:
    """Bounded alternative read only when the credential links exactly this account."""
    numbers = getattr(client, "get_account_numbers", None)
    accounts = getattr(client, "get_accounts", None)
    if not callable(numbers) or not callable(accounts):
        diagnostics["fallback_reason"] = "reader_unavailable"
        return None
    response = numbers()
    response.raise_for_status()
    identities = response.json()
    if not isinstance(identities, list):
        diagnostics["fallback_reason"] = "mapping_unusable"
        return None
    if len(identities) != 1:
        diagnostics["fallback_reason"] = "mapping_not_single"
        return None
    identity = identities[0]
    if (not isinstance(identity, Mapping) or identity.get("hashValue") != account_hash
            or not isinstance(identity.get("accountNumber"), str) or not identity["accountNumber"]):
        diagnostics["fallback_reason"] = "mapping_unverified"
        return None
    from schwab.client import Client

    diagnostics["fallback_attempted"] = True
    response = accounts(fields=Client.Account.Fields.POSITIONS)
    response.raise_for_status()
    linked = response.json()
    if not isinstance(linked, list) or len(linked) != 1 or not isinstance(linked[0], Mapping):
        diagnostics["fallback_reason"] = "linked_payload_unusable"
        return None
    account = linked[0].get("securitiesAccount")
    if not isinstance(account, Mapping) or account.get("accountNumber") != identity["accountNumber"]:
        diagnostics["fallback_reason"] = "linked_identity_unverified"
        return None
    diagnostics["fallback_reason"] = "linked_read_completed"
    diagnostics["linked_positions_field_present"] = "positions" in account
    return linked[0]


def collect_stock_inventory(client: Any, *, account_hash: str, currency: str | None) -> dict[str, Any] | None:
    """Read the verified account; try the linked view only for a single-account credential."""
    if not isinstance(account_hash, str) or not account_hash.strip() or currency != "USD":
        return None
    from schwab.client import Client

    response = client.get_account(account_hash, fields=Client.Account.Fields.POSITIONS)
    response.raise_for_status()
    diagnostics: dict[str, Any] = {
        "fallback_attempted": False,
        "fallback_reason": "not_needed",
        "linked_positions_field_present": None,
    }
    rows = project_stock_inventory(response.json(), currency=currency, diagnostics=diagnostics)
    if diagnostics.get("reason") == "positions_field_missing":
        linked = _single_linked_account_payload(client, account_hash, diagnostics=diagnostics)
        if linked is not None:
            rows = project_stock_inventory(linked, currency=currency, diagnostics=diagnostics)
    state = {
        "broker_stock_inventory_status": "available" if rows is not None else "unknown",
        "broker_stock_inventory_diagnostics": diagnostics,
    }
    if rows is None:
        return state
    return {
        **state,
        "broker_reported_positions": rows,
        "broker_reported_positions_scope": "stocks_only",
        "broker_reported_positions_complete": True,
        "broker_reported_positions_observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
