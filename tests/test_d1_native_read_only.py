from __future__ import annotations

import pytest

from application.d1_native_read_only import D1NativeInputError, parse_recorded_native


def _synthetic_order() -> dict:
    return {
        "input_label": "synthetic",
        "orders": [
            {
                "orderId": 9001,
                "status": "FILLED",
                "orderType": "MARKET",
                "orderStrategyType": "SINGLE",
                "filledQuantity": 3,
                "commission": 0.65,
                "orderActivityCollection": [
                    {
                        "activityId": "should-not-become-event",
                        "executionLegs": [{"price": 99, "quantity": 3, "legId": 7}],
                    }
                ],
                "orderLegCollection": [
                    {
                        "orderLegType": "EQUITY",
                        "instruction": "BUY",
                        "quantity": 3,
                        "instrument": {"symbol": "BOXX", "assetType": "EQUITY"},
                    }
                ],
            }
        ],
        "positions": [{"symbol": "SOXL", "longQuantity": 10}],
        "transactions": [{"transactionId": "t-1", "fees": {"commission": 1}}],
    }


def test_synthetic_order_keeps_partial_facts_without_fill_identity_or_fee():
    parsed = parse_recorded_native(_synthetic_order(), input_label="synthetic")
    order = parsed["orders"][0]

    assert parsed["native_observed"] is False
    assert parsed["input_label"] == "synthetic"
    assert parsed["execution_events"] == []
    assert order["order_id"] == "9001"
    assert order["status"] == "FILLED"
    assert order["cumulative_filled_quantity"] == "3"
    assert order["cumulative_semantics"] == "not_an_increment"
    assert order["fee"] is None
    assert order["per_fill_event_id"] is None
    assert order["owner_id"] is None
    assert order["legs"][0]["symbol"] == "BOXX"
    assert order["sdk_fields"]["orderType"] == "MARKET"
    assert parsed["external_positions"]["owner_assigned"] is False
    assert parsed["external_positions"]["owner_id"] is None
    assert parsed["transactions"]["parsed"] is False
    assert parsed["transactions"]["fee"] is None
    assert "should-not-become-event" not in str(parsed["execution_events"])
    assert "per_fill_fee" in parsed["unsupported"]
    assert "per_fill_event_id" in parsed["unsupported"]
    assert "net_position_owner_assignment" in parsed["unsupported"]


def test_missing_order_id_is_not_replaced():
    payload = {
        "orders": [
            {
                "status": "WORKING",
                "filledQuantity": 1,
                "orderLegCollection": [
                    {
                        "instruction": "SELL",
                        "quantity": 1,
                        "instrument": {"symbol": "SOXL", "assetType": "EQUITY"},
                    }
                ],
            }
        ]
    }
    parsed = parse_recorded_native(payload, input_label="synthetic")

    assert parsed["orders"][0]["order_id"] is None
    assert parsed["execution_events"] == []
    assert "missing_order_id" in parsed["unsupported"]


def test_same_price_and_quantity_do_not_share_an_invented_id():
    leg = {
        "instruction": "BUY",
        "quantity": 1,
        "instrument": {"symbol": "BOXX", "assetType": "EQUITY"},
    }
    payload = {
        "orders": [
            {"status": "WORKING", "price": "10", "orderLegCollection": [leg]},
            {"status": "WORKING", "price": "10", "orderLegCollection": [dict(leg)]},
        ]
    }
    parsed = parse_recorded_native(payload, input_label="synthetic")

    assert [order["order_id"] for order in parsed["orders"]] == [None, None]
    assert parsed["execution_events"] == []


def test_non_synthetic_input_is_rejected():
    with pytest.raises(D1NativeInputError):
        parse_recorded_native({"orders": []}, input_label="account")
