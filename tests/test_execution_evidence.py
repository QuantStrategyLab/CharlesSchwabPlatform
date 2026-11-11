from types import SimpleNamespace

import pytest

from application.execution_evidence import extract_execution_evidence


def test_accepted_order_is_observed_without_claiming_fills():
    order = {"symbol": "TQQQ", "side": "buy", "status": "accepted", "broker_order_id": "123456789"}
    result = SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=(order,))
    assert extract_execution_evidence(result, dry_run=False) == ([order], None)


def test_unknown_unidentified_duplicate_or_dry_run_order_not_claimed_complete():
    known = {"status": "accepted", "broker_order_id": "123456789"}
    for result, dry in ((SimpleNamespace(execution={"broker_submission_done": False}, submitted_orders=(known,)), False),
                        (SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=({"status": "accepted"},)), False),
                        (SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=(known, known)), False),
                        (SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=(known,)), True),
                        (SimpleNamespace(execution={}, submitted_orders=()), False)):
        assert extract_execution_evidence(result, dry_run=dry) == (None, None)


def test_order_issue_leaves_cycle_order_coverage_unknown():
    order = {"status": "accepted", "broker_order_id": "123456789"}
    result = SimpleNamespace(execution={"broker_submission_done": True, "notification_order_issue": True}, submitted_orders=(order,))
    assert extract_execution_evidence(result, dry_run=False) == (None, None)


@pytest.mark.parametrize("order_id", [
    "orders", "/orders", "", "0", 0, -1, True, 1.0, "0123", " 123 ", "１２３",
    {"id": "123"}, ["123"],
])
def test_invalid_order_identity_cannot_become_accepted_receipt_evidence(order_id):
    order = {"symbol": "TQQQ", "side": "buy", "status": "accepted", "broker_order_id": order_id}
    result = SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=(order,))
    assert extract_execution_evidence(result, dry_run=False) == (None, None)
    assert result.submitted_orders[0]["broker_order_id"] == order_id


def test_same_canonical_order_identity_cannot_be_counted_twice_across_types():
    orders = ({"status": "accepted", "broker_order_id": 123456789},
              {"status": "accepted", "broker_order_id": "123456789"})
    result = SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=orders)
    assert extract_execution_evidence(result, dry_run=False) == (None, None)


@pytest.mark.parametrize("order_id", [123456789, "123456789"])
def test_canonical_order_identity_is_observed_without_claiming_a_fill(order_id):
    order = {"status": "accepted", "broker_order_id": order_id}
    result = SimpleNamespace(execution={"broker_submission_done": True}, submitted_orders=(order,))
    assert extract_execution_evidence(result, dry_run=False) == ([order], None)
