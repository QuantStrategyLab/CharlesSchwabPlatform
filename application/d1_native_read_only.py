"""Read-only parse of recorded Schwab order payloads.

The parser copies only fields the installed ``schwab-py`` 1.5.1 code actually
reads or enumerates.  It does not call the provider.  Cumulative filled
quantity stays cumulative.  Missing per-fill identity or fee stays unknown.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

import schwab
from schwab.client import Client
from schwab.contrib.orders import _FIELDS_AND_SETTERS
from schwab.orders.common import EquityInstruction, OptionInstruction


class D1NativeInputError(ValueError):
    """The recorded payload is not an object this parser can inspect."""


_SDK_ORDER_FIELDS = frozenset(name for name, _setter, _enum in _FIELDS_AND_SETTERS)
_ORDER_STATUSES = frozenset(item.value for item in Client.Order.Status)
_EQUITY_INSTRUCTIONS = frozenset(item.value for item in EquityInstruction)
_OPTION_INSTRUCTIONS = frozenset(item.value for item in OptionInstruction)
_LEG_INSTRUCTIONS = _EQUITY_INSTRUCTIONS | _OPTION_INSTRUCTIONS
_UNSUPPORTED_ACTIVITY_KEYS = frozenset(
    {
        "orderActivityCollection",
        "executionLegs",
        "activityId",
        "commission",
        "fees",
        "transferItems",
    }
)


def parse_recorded_native(payload: Any, *, input_label: str) -> dict[str, Any]:
    """Parse one recorded order document.  ``input_label`` must be ``synthetic`` here."""

    if input_label != "synthetic":
        raise D1NativeInputError("recorded native parse in D1-A accepts only synthetic inputs")
    if not isinstance(payload, Mapping):
        raise D1NativeInputError("recorded native payload must be an object")
    unsupported: set[str] = {
        "per_fill_event_id",
        "per_fill_fee",
        "correction_or_reversal",
        "cumulative_filled_quantity_to_execution_increment",
        "transactions_response_body",
        "net_position_owner_assignment",
    }
    orders = _orders_from_payload(payload, unsupported)
    transactions = payload.get("transactions")
    transaction_count = len(transactions) if isinstance(transactions, list) else None
    positions = payload.get("positions")
    symbols: list[str] = []
    if isinstance(positions, list):
        for item in positions:
            if isinstance(item, Mapping):
                symbol = item.get("symbol") or item.get("instrument")
                if isinstance(symbol, Mapping):
                    symbol = symbol.get("symbol")
                if isinstance(symbol, str) and symbol.strip():
                    symbols.append(symbol.strip().upper())
    return {
        "schema_version": "d1_native_read_only.v1",
        "sdk_version": schwab.__version__,
        "input_label": "synthetic",
        "native_observed": False,
        "orders": orders,
        "execution_events": [],
        "unsupported": sorted(unsupported),
        "external_positions": {
            "symbols": symbols,
            "owner_assigned": False,
            "owner_id": None,
        },
        "transactions": {
            "parsed": False,
            "count": transaction_count,
            "fee": None,
        },
    }


def _orders_from_payload(payload: Mapping[str, Any], unsupported: set[str]) -> list[dict[str, Any]]:
    raw_orders = payload.get("orders", payload.get("order"))
    if raw_orders is None and _looks_like_order(payload):
        raw_orders = [payload]
    if isinstance(raw_orders, Mapping):
        raw_orders = [raw_orders]
    if not isinstance(raw_orders, list):
        return []
    parsed: list[dict[str, Any]] = []
    for item in raw_orders:
        if isinstance(item, Mapping):
            parsed.extend(_parse_order(item, unsupported))
    return parsed


def _looks_like_order(payload: Mapping[str, Any]) -> bool:
    return "orderId" in payload or "orderLegCollection" in payload or "status" in payload


def _parse_order(order: Mapping[str, Any], unsupported: set[str]) -> list[dict[str, Any]]:
    _note_unsupported_keys(order, unsupported)
    current = {
        "order_id": _order_id(order.get("orderId")),
        "status": _status(order.get("status"), unsupported),
        "sdk_fields": _sdk_fields(order),
        "legs": _legs(order.get("orderLegCollection"), unsupported),
        "cumulative_filled_quantity": _cumulative(order.get("filledQuantity"), unsupported),
        "cumulative_semantics": None,
        "fee": None,
        "per_fill_event_id": None,
        "owner_id": None,
    }
    if current["cumulative_filled_quantity"] is not None:
        current["cumulative_semantics"] = "not_an_increment"
    if current["order_id"] is None:
        unsupported.add("missing_order_id")
    children: list[dict[str, Any]] = []
    raw_children = order.get("childOrderStrategies")
    if isinstance(raw_children, list):
        for child in raw_children:
            if isinstance(child, Mapping):
                children.extend(_parse_order(child, unsupported))
    return [current, *children]


def _note_unsupported_keys(value: Any, unsupported: set[str], *, depth: int = 0) -> None:
    if depth > 6:
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key in _UNSUPPORTED_ACTIVITY_KEYS:
                unsupported.add(str(key))
            _note_unsupported_keys(item, unsupported, depth=depth + 1)
    elif isinstance(value, list):
        for item in value:
            _note_unsupported_keys(item, unsupported, depth=depth + 1)


def _order_id(value: Any) -> str | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _status(value: Any, unsupported: set[str]) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    status = value.strip().upper()
    if status not in _ORDER_STATUSES:
        unsupported.add("unsupported_order_status")
        return None
    return status


def _sdk_fields(order: Mapping[str, Any]) -> dict[str, Any]:
    copied: dict[str, Any] = {}
    for key in sorted(_SDK_ORDER_FIELDS):
        if key not in order:
            continue
        value = order[key]
        if isinstance(value, (str, int, float)) and not isinstance(value, bool):
            copied[key] = value
    return copied


def _legs(value: Any, unsupported: set[str]) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    legs: list[dict[str, Any]] = []
    for leg in value:
        if not isinstance(leg, Mapping):
            continue
        instruction = leg.get("instruction")
        if not isinstance(instruction, str) or instruction.strip().upper() not in _LEG_INSTRUCTIONS:
            unsupported.add("unsupported_leg_instruction")
            continue
        instrument = leg.get("instrument")
        symbol = instrument.get("symbol") if isinstance(instrument, Mapping) else None
        if not isinstance(symbol, str) or not symbol.strip():
            unsupported.add("missing_leg_symbol")
            continue
        asset_type = instrument.get("assetType") if isinstance(instrument, Mapping) else None
        leg_type = leg.get("orderLegType")
        legs.append(
            {
                "instruction": instruction.strip().upper(),
                "symbol": symbol.strip().upper(),
                "quantity": _quantity_text(leg.get("quantity")),
                "order_leg_type": leg_type.strip().upper() if isinstance(leg_type, str) and leg_type.strip() else None,
                "asset_type": asset_type.strip().upper() if isinstance(asset_type, str) and asset_type.strip() else None,
            }
        )
    return legs


def _cumulative(value: Any, unsupported: set[str]) -> str | None:
    if value is None:
        return None
    text = _quantity_text(value)
    if text is None:
        unsupported.add("unreadable_filled_quantity")
        return None
    return text


def _quantity_text(value: Any) -> str | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not number.is_finite() or number < 0:
        return None
    if number == 0:
        return "0"
    return format(number.normalize(), "f")
