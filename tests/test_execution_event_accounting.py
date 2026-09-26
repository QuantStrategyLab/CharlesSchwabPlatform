from __future__ import annotations

from decimal import Decimal

import pytest

from application.execution_event_accounting import (
    ExecutionEventError,
    ExecutionEventLedger,
)


def _ledger(tmp_path):
    return ExecutionEventLedger(tmp_path / "ledger.json")


def _event(event_id, *, order_id="order-a", quantity="2", price="100", fee="0.40",
           event_type="FILL", event_time="2026-09-27T10:00:00Z"):
    return {
        "event_id": event_id,
        "order_id": order_id,
        "quantity": quantity,
        "price": price,
        "fee": fee,
        "fee_source": "synthetic_declared_per_fill_fee",
        "event_time": event_time,
        "event_type": event_type,
    }


def test_intent_binds_owner_before_fill_and_multiple_owners_share_symbol(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_order_intent(
        intent_id="owner-a-intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="4",
    )
    ledger.record_order_intent(
        intent_id="owner-b-intent", order_id="order-b", owner_id="owner-b",
        symbol="BOXX", side="BUY", quantity="3",
    )

    first = ledger.record_execution_event(_event("fill-a", order_id="order-a", quantity="2"))
    second = ledger.record_execution_event(_event("fill-b", order_id="order-b", quantity="1"))

    assert first.owner_id == "owner-a"
    assert second.owner_id == "owner-b"
    state = ledger.snapshot()
    assert state["account"]["positions"]["BOXX"] == "3"
    assert sum(Decimal(owner["positions"].get("BOXX", "0")) for owner in state["owners"].values()) == Decimal("3")
    assert state["account"]["fees"] == "0.80"


def test_incremental_fills_pending_cancel_late_fill_and_terminal_first_converge(tmp_path):
    left = _ledger(tmp_path / "left")
    right = _ledger(tmp_path / "right")
    for ledger in (left, right):
        ledger.record_order_intent(
            intent_id="intent", order_id="order-a", owner_id="owner-a",
            symbol="BOXX", side="BUY", quantity="6", reserved_amount="600",
        )
    left.record_execution_event(_event("fill-1", quantity="2", price="100", fee="0.40"))
    left.record_order_update(order_id="order-a", status="PENDING_CANCEL", cumulative_filled_quantity="2")
    left.record_execution_event(_event("fill-2", quantity="1", price="99", fee="0.20", event_time="2026-09-27T10:01:00Z"))
    left.record_order_update(order_id="order-a", status="CANCELED", cumulative_filled_quantity="3")

    right.record_order_update(order_id="order-a", status="CANCELED", cumulative_filled_quantity="3")
    before_detail = right.snapshot()["orders"]["order-a"]
    assert before_detail["reconciliation_status"] == "incomplete"
    assert before_detail["reservation_status"] == "pending"
    right.record_execution_event(_event("fill-1", quantity="2", price="100", fee="0.40"))
    right.record_execution_event(_event("fill-2", quantity="1", price="99", fee="0.20", event_time="2026-09-27T10:01:00Z"))

    assert left.snapshot() == right.snapshot()
    account = left.snapshot()["account"]
    assert account["positions"]["BOXX"] == "3"
    assert account["principal"] == "299.00"
    assert account["fees"] == "0.60"
    assert account["cash_economic_delta"] == "-299.60"
    assert left.snapshot()["orders"]["order-a"]["status"] == "CANCELED"
    assert left.snapshot()["orders"]["order-a"]["reservation_status"] == "released"


def test_exact_duplicate_is_idempotent_but_conflict_is_quarantined(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_order_intent(
        intent_id="intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="4",
    )
    ledger.record_order_intent(
        intent_id="lost-intent", order_id=None, owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="1",
    )
    event = _event("stable-id")
    first = ledger.record_execution_event(event)
    duplicate = ledger.record_execution_event(dict(event))
    assert duplicate.duplicate is True
    assert duplicate.event_digest == first.event_digest
    assert ledger.snapshot()["account"]["positions"]["BOXX"] == "2"

    with pytest.raises(ExecutionEventError, match="conflict"):
        ledger.record_execution_event({**event, "price": "101"})
    assert "stable-id" in ledger.snapshot()["quarantined_event_ids"]
    assert ledger.snapshot()["account"]["positions"]["BOXX"] == "2"


def test_missing_fee_and_unsupported_correction_do_not_book(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_order_intent(
        intent_id="intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="4",
    )
    with pytest.raises(ExecutionEventError, match="fee"):
        ledger.record_execution_event({key: value for key, value in _event("missing-fee").items() if key not in {"fee", "fee_source"}})
    with pytest.raises(ExecutionEventError, match="unsupported"):
        ledger.record_execution_event(_event("correction", event_type="CORRECTION"))
    assert ledger.snapshot()["account"]["positions"] == {}
    assert ledger.snapshot()["account"]["fees"] == "0.00"


def test_order_cumulative_commission_is_not_accepted_as_a_per_fill_fee(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_order_intent(
        intent_id="intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="4",
    )
    with pytest.raises(ExecutionEventError, match="per-fill fee source"):
        ledger.record_execution_event({
            **_event("cumulative-fee"),
            "fee_source": "order_cumulative_commission",
        })
    assert ledger.snapshot()["account"]["positions"] == {}
    assert ledger.snapshot()["account"]["fees"] == "0.00"


def test_restart_replays_persisted_event_once_and_unknown_identity_never_resubmits(tmp_path):
    path = tmp_path / "ledger.json"
    ledger = ExecutionEventLedger(path)
    ledger.record_order_intent(
        intent_id="intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="4",
    )
    ledger.record_execution_event(_event("stable-id"))
    before = ledger.snapshot()

    restarted = ExecutionEventLedger(path)
    assert restarted.snapshot() == before
    assert restarted.record_execution_event(_event("stable-id")).duplicate is True
    assert restarted.snapshot() == before
    assert restarted.reconcile_unknown_order_identity(intent_id="lost-intent") == "verified-safe-reject"
    assert restarted.snapshot() == before


def test_order_intent_owner_binding_is_immutable_and_cancel_pending_is_nonterminal(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_order_intent(
        intent_id="intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="SELL", quantity="2",
    )
    with pytest.raises(ExecutionEventError, match="binding"):
        ledger.record_order_intent(
            intent_id="other-intent", order_id="order-a", owner_id="owner-b",
            symbol="BOXX", side="SELL", quantity="2",
        )
    ledger.record_order_update(order_id="order-a", status="PENDING_CANCEL", cumulative_filled_quantity="0")
    assert ledger.snapshot()["orders"]["order-a"]["terminal"] is False


def test_event_cannot_override_intent_bound_owner_symbol_or_side(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_order_intent(
        intent_id="intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="4",
    )
    with pytest.raises(ExecutionEventError, match="symbol conflicts"):
        ledger.record_execution_event({**_event("bad-symbol"), "symbol": "SOXL"})
    with pytest.raises(ExecutionEventError, match="side conflicts"):
        ledger.record_execution_event({**_event("bad-side"), "side": "SELL"})
    assert ledger.snapshot()["account"]["positions"] == {}


def test_terminal_order_state_does_not_regress_on_late_pending_cancel_report(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_order_intent(
        intent_id="intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="2", reserved_amount="200",
    )
    ledger.record_order_update(order_id="order-a", status="CANCELED", cumulative_filled_quantity="1")
    ledger.record_order_update(order_id="order-a", status="PENDING_CANCEL", cumulative_filled_quantity="0")
    order = ledger.snapshot()["orders"]["order-a"]
    assert order["status"] == "CANCELED"
    assert order["cumulative_filled_quantity"] == "1"
    assert order["reconciliation_status"] == "incomplete"
    assert order["reservation_status"] == "pending"


def test_cumulative_fill_evidence_is_monotonic_across_nonterminal_and_terminal_updates(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_order_intent(
        intent_id="intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="6", reserved_amount="600",
    )
    ledger.record_execution_event(_event("fill-1", quantity="1", fee="0.10"))
    ledger.record_order_update(order_id="order-a", status="PENDING_CANCEL", cumulative_filled_quantity="3")
    ledger.record_order_update(order_id="order-a", status="CANCELED", cumulative_filled_quantity="1")
    order = ledger.snapshot()["orders"]["order-a"]
    assert order["cumulative_filled_quantity"] == "3"
    assert order["recorded_filled_quantity"] == "1"
    assert order["reconciliation_status"] == "incomplete"
    assert order["reservation_status"] == "pending"


def test_conflicting_quarantined_event_invalidates_reconciled_order_without_erasing_fill(tmp_path):
    ledger = _ledger(tmp_path)
    ledger.record_order_intent(
        intent_id="intent", order_id="order-a", owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="2", reserved_amount="200",
    )
    original = _event("stable-id", quantity="1", fee="0.10")
    ledger.record_execution_event(original)
    ledger.record_order_update(order_id="order-a", status="CANCELED", cumulative_filled_quantity="1")
    complete = ledger.snapshot()["orders"]["order-a"]
    assert complete["reconciliation_status"] == "complete"
    assert complete["reservation_status"] == "released"

    with pytest.raises(ExecutionEventError, match="conflict"):
        ledger.record_execution_event({**original, "price": "101"})
    state = ledger.snapshot()
    order = state["orders"]["order-a"]
    assert order["reconciliation_status"] == "incomplete"
    assert order["reservation_status"] == "pending"
    assert state["account"]["positions"]["BOXX"] == "1"
    assert state["account"]["fees"] == "0.10"


def test_conflicting_event_id_on_another_order_quarantines_original_order_too(tmp_path):
    ledger = _ledger(tmp_path)
    for owner, order in (("owner-a", "order-a"), ("owner-b", "order-b")):
        ledger.record_order_intent(
            intent_id=f"intent-{owner}", order_id=order, owner_id=owner,
            symbol="BOXX", side="BUY", quantity="1", reserved_amount="100",
        )
    original = _event("shared-id", order_id="order-a", quantity="1", fee="0.10")
    ledger.record_execution_event(original)
    ledger.record_order_update(order_id="order-a", status="CANCELED", cumulative_filled_quantity="1")
    before = ledger.snapshot()
    assert before["orders"]["order-a"]["reconciliation_status"] == "complete"
    assert before["orders"]["order-a"]["reservation_status"] == "released"

    with pytest.raises(ExecutionEventError, match="conflict"):
        ledger.record_execution_event({**original, "order_id": "order-b"})
    state = ledger.snapshot()
    original_order = state["orders"]["order-a"]
    assert original_order["reconciliation_status"] == "incomplete"
    assert original_order["reservation_status"] == "pending"
    assert state["account"]["positions"] == {"BOXX": "1"}
    assert state["account"]["fees"] == "0.10"
    assert state["owners"]["owner-a"]["positions"] == {"BOXX": "1"}
    assert state["owners"]["owner-b"]["positions"] == {}


def test_unbound_intent_reservation_is_persisted_once_until_terminal_release(tmp_path):
    path = tmp_path / "ledger.json"
    ledger = ExecutionEventLedger(path)
    ledger.record_order_intent(
        intent_id="intent", order_id=None, owner_id="owner-a",
        symbol="BOXX", side="BUY", quantity="2", reserved_amount="200",
    )
    unbound = ledger.snapshot()
    assert unbound["owners"]["owner-a"]["reserved_amount"] == "200.00"
    assert unbound["account"]["reserved_amount"] == "200.00"

    ledger.bind_order_identity(intent_id="intent", order_id="order-a")
    bound = ledger.snapshot()
    assert bound["owners"]["owner-a"]["reserved_amount"] == "200.00"
    assert bound["account"]["reserved_amount"] == "200.00"
    restarted = ExecutionEventLedger(path)
    assert restarted.snapshot() == bound
    restarted.record_order_update(order_id="order-a", status="CANCELED", cumulative_filled_quantity="0")
    released = restarted.snapshot()
    assert released["owners"]["owner-a"]["reserved_amount"] == "0.00"
    assert released["account"]["reserved_amount"] == "0.00"
