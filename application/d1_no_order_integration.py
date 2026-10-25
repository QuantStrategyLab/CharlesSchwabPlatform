"""Local D1 no-order entry.

Calls the installed strategy entrypoint, plan mapper, execution claim, and
execution-event ledger.  The final transport refuses trading methods before
any client call.  Native records stay partial when per-fill evidence is absent.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from decision_mapper import map_strategy_decision_to_plan
from quant_platform_kit.common.execution_state import ExecutionMarkerStore
from quant_platform_kit.common.execution_receipts import resolve_execution_receipt_fact
from quant_platform_kit.common.strategy_contracts import StrategyContext
from strategy_loader import load_strategy_entrypoint_for_profile

from application.d1_native_read_only import parse_recorded_native
from application.execution_claim import claim_execution_marker
from application.execution_event_accounting import ExecutionEventLedger


_PROFILE = "soxl_soxx_trend_income"
_MARKER_KEY = "d1-no-order/synthetic/soxl_soxx_trend_income/2026-09-26"
_LEDGER_STATUSES = frozenset(
    {"NEW", "WORKING", "PARTIALLY_FILLED", "PENDING_CANCEL", "CANCELED", "FILLED", "REJECTED", "EXPIRED", "UNKNOWN"}
)
_TRADING_METHODS = ("place_order", "cancel_order", "replace_order", "submit_order")


class NoOrderTransportError(RuntimeError):
    """A trading method was refused before the broker client was called."""


class D1IntegrationError(ValueError):
    """The local no-order entry cannot accept this input."""


class NoOrderBrokerTransport:
    """Final broker boundary.  Trading methods never reach ``client``."""

    def __init__(self, client: Any):
        # Retain only an audit identity; this transport cannot expose a broker client.
        self._bound_client_id = id(client)
        self.refused: list[str] = []

    @property
    def bound_client_id(self) -> int:
        return self._bound_client_id

    def place_order(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse("place_order")

    def cancel_order(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse("cancel_order")

    def replace_order(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse("replace_order")

    def submit_order(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse("submit_order")

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        def _refused(*_args: Any, **_kwargs: Any) -> None:
            self._refuse(name)

        return _refused

    def _refuse(self, name: str) -> None:
        self.refused.append(name)
        raise NoOrderTransportError(name)


def run_no_order_cycle(
    state_dir: str | Path,
    *,
    transport: NoOrderBrokerTransport,
    snapshot: Any,
    derived_indicators: Mapping[str, Any],
    input_label: str,
) -> dict[str, Any]:
    """Run one synthetic local cycle and persist claim plus cycle facts."""

    _require_synthetic(input_label)
    root = Path(state_dir)
    root.mkdir(parents=True, exist_ok=True)
    calls: list[str] = []
    entrypoint = load_strategy_entrypoint_for_profile(_PROFILE)
    context = StrategyContext(
        as_of=datetime(2026, 9, 26, tzinfo=timezone.utc),
        market_data={"derived_indicators": dict(derived_indicators), "input_label": "synthetic"},
        portfolio=snapshot,
        runtime_config={"option_overlay_enabled": False, "income_layer_enabled": False},
    )
    decision = entrypoint.evaluate(context)
    calls.append(f"{entrypoint.evaluate.__module__}.{entrypoint.evaluate.__qualname__}")
    plan = map_strategy_decision_to_plan(
        decision,
        snapshot=snapshot,
        strategy_profile=_PROFILE,
    )
    calls.append(f"{map_strategy_decision_to_plan.__module__}.{map_strategy_decision_to_plan.__qualname__}")
    store = _claim_store(root)
    claimed = claim_execution_marker(
        store,
        _MARKER_KEY,
        metadata={
            "input_label": "synthetic",
            "no_order": True,
            "strategy_profile": _PROFILE,
            "dry_run_only": False,
        },
    )
    calls.append(f"{claim_execution_marker.__module__}.{claim_execution_marker.__qualname__}")
    targets = dict((plan.get("allocation") or {}).get("targets") or {})
    positive_targets = {symbol: value for symbol, value in targets.items() if float(value or 0.0) > 0.0}
    trading_attempts = 0
    for _symbol in positive_targets:
        trading_attempts += 1
        _probe(transport, "place_order")
    for method in _TRADING_METHODS:
        _probe(transport, method)
    diagnostics = dict(decision.diagnostics)
    risk_blocked = diagnostics.get("risk_gate") == "REJECT"
    protection_outcome, protection_confirmation = resolve_execution_receipt_fact(
        dry_run=False,
        submission_attempted=False,
        reconciliation_required=False,
        risk_blocked=risk_blocked,
    )
    result = {
        "schema_version": "d1_no_order_cycle.v1",
        "cycle_date": context.as_of.date().isoformat(),
        "input_label": "synthetic",
        "native_observed": False,
        "strategy_profile": _PROFILE,
        "call_path": calls,
        "risk_gate": diagnostics.get("risk_gate"),
        "protective_action": {
            "request": "new_risk_admission",
            "decision": diagnostics.get("risk_gate"),
            "outcome": protection_outcome,
            "confirmation": protection_confirmation,
            "effect": "zero_positive_targets" if not positive_targets else "targets_pending_no_order_guard",
        },
        "risk_flags": list(decision.risk_flags),
        "execution_status": (plan.get("execution") or {}).get("execution_status"),
        "no_op_reason": (plan.get("execution") or {}).get("no_op_reason"),
        "positive_target_count": len(positive_targets),
        "trading_attempts_before_guard": trading_attempts,
        "transport_refused": list(transport.refused),
        "bound_client_id": transport.bound_client_id,
        "claim_acquired": bool(claimed),
        "marker_key": _MARKER_KEY,
    }
    _write_json(root / "cycle.json", result)
    return result


def observe_expected_cycle(state_dir: str | Path, *, expected_date: str) -> dict[str, Any]:
    """Detect a missing local cycle receipt without sending a notification.

    The date inside the receipt is authoritative; a recent file timestamp is not.
    The existing marker store deduplicates one local alert event across processes.
    """

    root = Path(state_dir)
    expected = datetime.fromisoformat(expected_date).date().isoformat()
    receipt_path = root / "cycle.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8")) if receipt_path.exists() else None
    present = isinstance(receipt, Mapping) and receipt.get("cycle_date") == expected
    alert_created = False
    if not present:
        alert_created = bool(claim_execution_marker(
            _claim_store(root / "observation"),
            f"d1-missing-cycle/{expected}",
            metadata={"expected_date": expected, "alert_kind": "missing_cycle_receipt", "notification_sent": False},
        ))
    return {
        "expected_date": expected,
        "receipt_present": present,
        "alert_event_created": alert_created,
        "notification_sent": False,
    }


def record_synthetic_explicit_case(state_dir: str | Path) -> dict[str, Any]:
    """Record the existing 3-share synthetic case through the real ledger."""

    ledger = ExecutionEventLedger(Path(state_dir) / "ledger.json")
    ledger.record_order_intent(
        intent_id="owner-a-intent",
        order_id="synthetic-order-1",
        owner_id="owner-a",
        symbol="BOXX",
        side="BUY",
        quantity="6",
        reserved_amount="600",
    )
    first = _explicit_fill("exec-1", "synthetic-order-1", "2", "100", "0.40", "2026-09-27T10:00:00Z")
    second = _explicit_fill("exec-2", "synthetic-order-1", "1", "99", "0.20", "2026-09-27T10:01:00Z")
    ledger.record_execution_event(first)
    ledger.record_order_update(
        order_id="synthetic-order-1",
        status="PENDING_CANCEL",
        cumulative_filled_quantity="2",
    )
    ledger.record_execution_event(second)
    ledger.record_order_update(
        order_id="synthetic-order-1",
        status="CANCELED",
        cumulative_filled_quantity="3",
    )
    return ledger.snapshot()


def import_recorded_native_observations(
    state_dir: str | Path,
    payload: Mapping[str, Any],
    *,
    input_label: str,
) -> dict[str, Any]:
    """Keep native facts partial.  Do not create fills or assign owners."""

    _require_synthetic(input_label)
    parsed = parse_recorded_native(payload, input_label=input_label)
    root = Path(state_dir)
    ledger_path = root / "ledger.json"
    order_updates: list[dict[str, Any]] = []
    if ledger_path.exists():
        ledger = ExecutionEventLedger(ledger_path)
        known = set(ledger.snapshot()["orders"])
        for order in parsed["orders"]:
            order_id = order["order_id"]
            status = order["status"]
            cumulative = order["cumulative_filled_quantity"]
            if order_id in known and status in _LEDGER_STATUSES and cumulative is not None:
                ledger.record_order_update(
                    order_id=order_id,
                    status=status,
                    cumulative_filled_quantity=cumulative,
                )
                order_updates.append({"order_id": order_id, "applied": "cumulative_check_only"})
            elif order_id:
                order_updates.append({"order_id": order_id, "applied": "unallocated_no_owner"})
    partial = {
        "schema_version": "d1_native_partial.v1",
        "input_label": "synthetic",
        "native_observed": False,
        "parsed": parsed,
        "order_updates": order_updates,
        "fill_events_created": 0,
    }
    _write_json(root / "native_partial.json", partial)
    return partial


def recover_no_order_state(state_dir: str | Path) -> dict[str, Any]:
    """Reload persisted claim, ledger, and native partial facts."""

    root = Path(state_dir)
    ledger = ExecutionEventLedger(root / "ledger.json")
    before = ledger.snapshot()
    events = before["events"]
    replay = ledger.record_execution_event(events[0]) if events else None
    after = ledger.snapshot()
    claim_again = claim_execution_marker(
        _claim_store(root),
        _MARKER_KEY,
        metadata={"input_label": "synthetic", "no_order": True, "replay": True},
    )
    partial_path = root / "native_partial.json"
    partial = json.loads(partial_path.read_text(encoding="utf-8")) if partial_path.exists() else None
    return {
        "schema_version": "d1_no_order_recovery.v1",
        "input_label": "synthetic",
        "native_observed": False,
        "replay_duplicate": bool(replay.duplicate) if replay is not None else None,
        "snapshot_unchanged": before == after,
        "account": after["account"],
        "event_count": len(after["events"]),
        "claim_acquired_on_replay": bool(claim_again),
        "native_partial": partial,
    }


def _probe(transport: NoOrderBrokerTransport, name: str) -> None:
    try:
        getattr(transport, name)()
    except NoOrderTransportError:
        return


def _claim_store(root: Path) -> ExecutionMarkerStore:
    return ExecutionMarkerStore(local_dir=root / "claims", cloud_prefix_uri=None)


def _explicit_fill(event_id: str, order_id: str, quantity: str, price: str, fee: str, event_time: str) -> dict[str, str]:
    return {
        "event_id": event_id,
        "order_id": order_id,
        "quantity": quantity,
        "price": price,
        "fee": fee,
        "fee_source": "synthetic_declared_per_fill_fee",
        "event_time": event_time,
        "event_type": "FILL",
    }


def _require_synthetic(input_label: str) -> None:
    if input_label != "synthetic":
        raise D1IntegrationError("D1 local entry accepts only synthetic inputs")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
