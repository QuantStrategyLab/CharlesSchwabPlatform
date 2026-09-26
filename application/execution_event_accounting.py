"""Local, deterministic accounting for explicitly normalized execution events.

This module is an offline accounting seam.  It does not parse Schwab order
reports and does not turn cumulative order quantities into execution events.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Mapping


_CENT = Decimal("0.01")
_SUPPORTED_EVENT_TYPE = "FILL"
_TERMINAL_STATUSES = {"CANCELED", "FILLED", "REJECTED", "EXPIRED"}
_ORDER_STATUSES = _TERMINAL_STATUSES | {"NEW", "WORKING", "PARTIALLY_FILLED", "PENDING_CANCEL", "UNKNOWN"}


class ExecutionEventError(ValueError):
    """An event or order fact cannot be safely accepted."""


@dataclass(frozen=True)
class EventReceipt:
    event_id: str
    event_digest: str
    owner_id: str
    duplicate: bool = False


def _decimal(value: Any, label: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ExecutionEventError(f"{label} must be an explicit decimal value")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ExecutionEventError(f"{label} must be a finite decimal value") from None
    if not result.is_finite() or (positive and result <= 0) or (nonnegative and result < 0):
        raise ExecutionEventError(f"{label} is outside the accepted range")
    return result


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _money(value: Decimal) -> str:
    return format(value.quantize(_CENT, rounding=ROUND_HALF_UP), ".2f")


def _json_digest(value: Mapping[str, Any]) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ExecutionEventError(f"{label} is required")
    return value.strip()


class ExecutionEventLedger:
    """A small append-and-replay ledger backed by atomic local JSON replace."""

    schema_version = "schwab_execution_event_ledger.v1"

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._intents: dict[str, dict[str, str]] = {}
        self._orders: dict[str, dict[str, Any]] = {}
        self._events: dict[str, dict[str, Any]] = {}
        self._order_updates: dict[str, dict[str, Any]] = {}
        self._quarantined: set[str] = set()
        self._unsupported: set[str] = set()
        if self.path.exists():
            self._load()
        self._rebuild()

    def record_order_intent(
        self,
        *,
        intent_id: str,
        order_id: str | None,
        owner_id: str,
        symbol: str,
        side: str,
        quantity: Any,
        reserved_amount: Any = "0",
    ) -> None:
        intent_id = _required_text(intent_id, "intent_id")
        owner_id = _required_text(owner_id, "owner_id")
        symbol = _required_text(symbol, "symbol").upper()
        side = _required_text(side, "side").upper()
        if side not in {"BUY", "SELL"}:
            raise ExecutionEventError("side must be BUY or SELL")
        qty = _decimal(quantity, "quantity", positive=True)
        reserve = _decimal(reserved_amount, "reserved_amount", nonnegative=True)
        normalized = {
            "intent_id": intent_id,
            "order_id": _required_text(order_id, "order_id") if order_id is not None else "",
            "owner_id": owner_id,
            "symbol": symbol,
            "side": side,
            "quantity": _decimal_text(qty),
            "reserved_amount": _money(reserve),
        }
        existing = self._intents.get(intent_id)
        if existing is not None and existing != normalized:
            raise ExecutionEventError("intent binding conflict")
        bound = normalized["order_id"]
        if bound:
            prior = self._orders.get(bound)
            if prior and prior["intent_id"] != intent_id:
                raise ExecutionEventError("order binding conflict")
        self._intents[intent_id] = normalized
        if bound:
            self._orders.setdefault(bound, self._new_order_state(normalized))
        self._rebuild()
        self._save()

    def bind_order_identity(self, *, intent_id: str, order_id: str) -> None:
        intent_id = _required_text(intent_id, "intent_id")
        order_id = _required_text(order_id, "order_id")
        intent = self._intents.get(intent_id)
        if intent is None:
            raise ExecutionEventError("unknown internal intent")
        existing = intent["order_id"]
        if existing and existing != order_id:
            raise ExecutionEventError("intent already has a different order binding")
        other = self._orders.get(order_id)
        if other is not None and other["intent_id"] != intent_id:
            raise ExecutionEventError("order binding conflict")
        intent["order_id"] = order_id
        self._orders.setdefault(order_id, self._new_order_state(intent))
        self._rebuild()
        self._save()

    def reconcile_unknown_order_identity(self, *, intent_id: str) -> str:
        """Refuse to infer a broker identity from approximate order attributes."""
        if intent_id not in self._intents or not self._intents[intent_id]["order_id"]:
            return "verified-safe-reject"
        return "identity_known_requires_explicit_order_query"

    def record_execution_event(self, event: Mapping[str, Any]) -> EventReceipt:
        if not isinstance(event, Mapping):
            raise ExecutionEventError("execution event must be an object")
        event_id = _required_text(event.get("event_id"), "event_id")
        event_type = _required_text(event.get("event_type"), "event_type").upper()
        if event_type != _SUPPORTED_EVENT_TYPE:
            self._unsupported.add(event_id)
            self._rebuild()
            self._save()
            raise ExecutionEventError(f"unsupported execution event type: {event_type}")
        fee_input = event.get("fee")
        if fee_input is None:
            raise ExecutionEventError("explicit fee is required; missing fee is unknown")
        fee_source = _required_text(event.get("fee_source"), "fee_source")
        fee_source_scope = fee_source.lower().replace("-", "_").replace(" ", "_")
        if any(token in fee_source_scope for token in ("cumulative", "aggregate", "order_commission", "order_level")):
            raise ExecutionEventError("cumulative or order-level commission cannot be used as a per-fill fee source")
        if not any(token in fee_source_scope for token in ("per_fill", "per_execution", "fill_level", "execution_level")):
            raise ExecutionEventError("fee_source must explicitly identify a per-fill fee source")
        order_id = _required_text(event.get("order_id"), "order_id")
        intent = self._orders.get(order_id)
        if intent is None:
            raise ExecutionEventError("execution event has no persisted intent-to-order owner binding")
        for field, expected in (("owner_id", intent["owner_id"]), ("symbol", intent["symbol"]), ("side", intent["side"])):
            supplied = event.get(field)
            if supplied is not None and str(supplied).strip().upper() != expected.upper():
                raise ExecutionEventError(f"execution event {field} conflicts with the persisted order binding")
        quantity = _decimal(event.get("quantity"), "quantity", positive=True)
        price = _decimal(event.get("price"), "price", positive=True)
        fee = _decimal(fee_input, "fee", nonnegative=True).quantize(_CENT, rounding=ROUND_HALF_UP)
        event_time = _required_text(event.get("event_time"), "event_time")
        try:
            parsed_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        except ValueError:
            raise ExecutionEventError("event_time must be an ISO-8601 timestamp") from None
        if parsed_time.tzinfo is None:
            raise ExecutionEventError("event_time must include a timezone")
        event_time = parsed_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        normalized = {
            "event_id": event_id,
            "order_id": order_id,
            "owner_id": intent["owner_id"],
            "symbol": intent["symbol"],
            "side": intent["side"],
            "quantity": _decimal_text(quantity),
            "price": _decimal_text(price),
            "fee": _money(fee),
            "fee_source": fee_source,
            "event_time": event_time,
            "event_type": event_type,
        }
        digest = _json_digest(normalized)
        normalized["event_digest"] = digest
        existing = self._events.get(event_id)
        if existing is not None:
            if existing["event_digest"] == digest:
                return EventReceipt(event_id, digest, intent["owner_id"], duplicate=True)
            self._quarantined.add(event_id)
            self._rebuild()
            self._save()
            raise ExecutionEventError("event id conflict: conflicting payload quarantined")
        self._events[event_id] = normalized
        self._rebuild()
        self._save()
        return EventReceipt(event_id, digest, intent["owner_id"])

    def record_order_update(
        self,
        *,
        order_id: str,
        status: str,
        cumulative_filled_quantity: Any,
    ) -> None:
        order_id = _required_text(order_id, "order_id")
        if order_id not in self._orders:
            raise ExecutionEventError("order update has no persisted intent binding")
        status = _required_text(status, "status").upper()
        if status not in _ORDER_STATUSES:
            raise ExecutionEventError("unsupported order status")
        cumulative = _decimal(cumulative_filled_quantity, "cumulative_filled_quantity", nonnegative=True)
        prior_update = self._order_updates.get(order_id)
        if prior_update is not None and prior_update["status"] in _TERMINAL_STATUSES:
            if status in _TERMINAL_STATUSES and status != prior_update["status"]:
                raise ExecutionEventError("conflicting terminal order status")
            if status not in _TERMINAL_STATUSES:
                status = prior_update["status"]
            cumulative = max(cumulative, Decimal(prior_update["cumulative_filled_quantity"]))
        self._order_updates[order_id] = {
            "status": status,
            "cumulative_filled_quantity": _decimal_text(cumulative),
        }
        self._rebuild()
        self._save()

    def snapshot(self) -> dict[str, Any]:
        return json.loads(json.dumps(self._snapshot))

    @staticmethod
    def _new_order_state(intent: Mapping[str, str]) -> dict[str, Any]:
        return {
            "intent_id": intent["intent_id"],
            "owner_id": intent["owner_id"],
            "symbol": intent["symbol"],
            "side": intent["side"],
            "requested_quantity": intent["quantity"],
            "reserved_amount": intent["reserved_amount"],
            "status": "NEW",
            "terminal": False,
            "cumulative_filled_quantity": None,
            "recorded_filled_quantity": "0",
            "reconciliation_status": "incomplete",
            "reservation_status": "pending",
        }

    def _rebuild(self) -> None:
        orders = {
            order_id: self._new_order_state(self._intents[order["intent_id"]])
            for order_id, order in self._orders.items()
        }
        owners: dict[str, dict[str, Any]] = {}
        account: dict[str, Any] = {
            "positions": {},
            "principal": Decimal("0"),
            "fees": Decimal("0"),
            "cash_economic_delta": Decimal("0"),
            "reserved_amount": Decimal("0"),
            "pending_settlement_items": [],
        }
        for intent in self._intents.values():
            owners.setdefault(
                intent["owner_id"],
                {"positions": {}, "principal": Decimal("0"), "fees": Decimal("0"),
                 "cash_economic_delta": Decimal("0"), "reserved_amount": Decimal("0"),
                 "pending_settlement_items": []},
            )
        event_order = sorted(
            self._events.values(),
            key=lambda item: (item["event_time"], item["event_id"]),
        )
        for event in event_order:
            owner = owners.setdefault(
                event["owner_id"],
                {"positions": {}, "principal": Decimal("0"), "fees": Decimal("0"),
                 "cash_economic_delta": Decimal("0"), "reserved_amount": Decimal("0"),
                 "pending_settlement_items": []},
            )
            quantity = Decimal(event["quantity"])
            price = Decimal(event["price"])
            fee = Decimal(event["fee"])
            principal = (quantity * price).quantize(_CENT, rounding=ROUND_HALF_UP)
            signed_quantity = quantity if event["side"] == "BUY" else -quantity
            cash_delta = -principal - fee if event["side"] == "BUY" else principal - fee
            owner["positions"][event["symbol"]] = Decimal(owner["positions"].get(event["symbol"], "0")) + signed_quantity
            owner["principal"] += principal if event["side"] == "BUY" else -principal
            owner["fees"] += fee
            owner["cash_economic_delta"] += cash_delta
            item = {
                "event_id": event["event_id"], "owner_id": event["owner_id"],
                "symbol": event["symbol"], "side": event["side"], "principal": _money(principal),
                "fee": _money(fee), "cash_economic_delta": _money(cash_delta),
                "settlement_status": "pending",
            }
            owner["pending_settlement_items"].append(item)
            account["positions"][event["symbol"]] = Decimal(account["positions"].get(event["symbol"], "0")) + signed_quantity
            account["principal"] += principal if event["side"] == "BUY" else -principal
            account["fees"] += fee
            account["cash_economic_delta"] += cash_delta
            account["pending_settlement_items"].append(item)
            orders[event["order_id"]]["recorded_filled_quantity"] = _decimal_text(
                Decimal(orders[event["order_id"]]["recorded_filled_quantity"]) + quantity
            )
        for order_id, update in self._order_updates.items():
            order = orders[order_id]
            order["status"] = update["status"]
            order["terminal"] = update["status"] in _TERMINAL_STATUSES
            order["cumulative_filled_quantity"] = update["cumulative_filled_quantity"]
        for order in orders.values():
            reported = order["cumulative_filled_quantity"]
            recorded = Decimal(order["recorded_filled_quantity"])
            filled_status_mismatch = (
                order["status"] == "FILLED"
                and Decimal(order["requested_quantity"]) != Decimal(reported or "0")
            )
            if reported is None or Decimal(reported) != recorded or filled_status_mismatch:
                order["reconciliation_status"] = "incomplete"
                order["reservation_status"] = "pending"
            else:
                order["reconciliation_status"] = "complete"
                order["reservation_status"] = "released" if order["terminal"] else "pending"
            if order["reservation_status"] == "pending":
                reserved = Decimal(order["reserved_amount"])
                owners[order["owner_id"]]["reserved_amount"] += reserved
                account["reserved_amount"] += reserved
        owner_json = {}
        for owner_id, data in owners.items():
            owner_json[owner_id] = {
                "positions": {key: _decimal_text(value) for key, value in sorted(data["positions"].items()) if value},
                "principal": _money(data["principal"]),
                "fees": _money(data["fees"]),
                "cash_economic_delta": _money(data["cash_economic_delta"]),
                "reserved_amount": _money(data["reserved_amount"]),
                "pending_settlement_items": data["pending_settlement_items"],
            }
        self._orders = orders
        self._snapshot = {
            "schema_version": self.schema_version,
            "intents": {key: dict(value) for key, value in sorted(self._intents.items())},
            "orders": {key: dict(value) for key, value in sorted(orders.items())},
            "events": [dict(item) for item in event_order],
            "quarantined_event_ids": sorted(self._quarantined),
            "unsupported_event_ids": sorted(self._unsupported),
            "owners": owner_json,
            "account": {
                "positions": {key: _decimal_text(value) for key, value in sorted(account["positions"].items()) if value},
                "principal": _money(account["principal"]),
                "fees": _money(account["fees"]),
                "cash_economic_delta": _money(account["cash_economic_delta"]),
                "reserved_amount": _money(account["reserved_amount"]),
                "pending_settlement_items": account["pending_settlement_items"],
            },
        }

    def _load(self) -> None:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ExecutionEventError(f"ledger cannot be loaded: {type(exc).__name__}") from None
        if raw.get("schema_version") != self.schema_version:
            raise ExecutionEventError("unsupported execution-event ledger schema")
        self._intents = {key: dict(value) for key, value in raw["intents"].items()}
        self._events = {event["event_id"]: dict(event) for event in raw["events"]}
        self._order_updates = {
            key: {
                "status": order["status"],
                "cumulative_filled_quantity": order["cumulative_filled_quantity"],
            }
            for key, order in raw["orders"].items()
            if order["cumulative_filled_quantity"] is not None
        }
        self._orders = {}
        for intent in self._intents.values():
            order_id = intent["order_id"]
            if order_id:
                self._orders[order_id] = self._new_order_state(intent)
        self._quarantined = set(raw.get("quarantined_event_ids", []))
        self._unsupported = set(raw.get("unsupported_event_ids", []))

    def _save(self) -> None:
        payload = json.dumps(self._snapshot, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        fd, temp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_name, self.path)
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
