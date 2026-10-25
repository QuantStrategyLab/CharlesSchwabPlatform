from __future__ import annotations

import json
import io
import os
import signal
import subprocess
import sys
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from quant_platform_kit.common.models import OrderIntent, PortfolioSnapshot

from application.d1_no_order_integration import (
    D1IntegrationError,
    NoOrderBrokerTransport,
    NoOrderTransportError,
    import_recorded_native_observations,
    observe_expected_cycle,
    record_synthetic_explicit_case,
    recover_no_order_state,
    run_no_order_cycle,
)
from application.execution_event_accounting import ExecutionEventLedger
from application.offline_fault_acceptance import _buy_book, _run_cycle


class _CapturingClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def place_order(self, *_args, **_kwargs):
        self.calls.append("place_order")

    def cancel_order(self, *_args, **_kwargs):
        self.calls.append("cancel_order")

    def replace_order(self, *_args, **_kwargs):
        self.calls.append("replace_order")

    def submit_order(self, *_args, **_kwargs):
        self.calls.append("submit_order")

    def get_orders_for_account(self, *_args, **_kwargs):
        self.calls.append("get_orders_for_account")


def _snapshot() -> PortfolioSnapshot:
    return PortfolioSnapshot(
        as_of=datetime(2026, 9, 26, tzinfo=timezone.utc),
        total_equity=10000.0,
        buying_power=10000.0,
        cash_balance=10000.0,
        positions=(),
        metadata={"account_hash": "synthetic-d1", "input_label": "synthetic"},
    )


def _indicators() -> dict:
    return {
        "SOXL": {"price": 40.0, "ma_trend": 30.0},
        "SOXX": {"price": 200.0, "ma_trend": 180.0},
    }


def _native_payload() -> dict:
    return {
        "orders": [
            {
                "orderId": 9001,
                "status": "FILLED",
                "orderType": "MARKET",
                "filledQuantity": 3,
                "commission": 0.65,
                "orderActivityCollection": [{"activityId": "should-not-become-event"}],
                "orderLegCollection": [
                    {
                        "instruction": "BUY",
                        "quantity": 3,
                        "instrument": {"symbol": "BOXX", "assetType": "EQUITY"},
                    }
                ],
            }
        ],
        "positions": [{"symbol": "SOXL", "longQuantity": 10}],
    }


def test_transport_refuses_trading_methods_without_calling_the_client():
    client = _CapturingClient()
    transport = NoOrderBrokerTransport(client)
    assert not hasattr(transport, "_client")

    for name in ("place_order", "cancel_order", "replace_order", "submit_order", "get_orders_for_account"):
        with pytest.raises(NoOrderTransportError):
            getattr(transport, name)(OrderIntent(symbol="BOXX", side="buy", quantity=1))

    assert client.calls == []
    assert transport.refused == [
        "place_order",
        "cancel_order",
        "replace_order",
        "submit_order",
        "get_orders_for_account",
    ]


def test_positive_synthetic_plan_reaches_real_execution_port_but_not_client():
    client = _CapturingClient()
    transport = NoOrderBrokerTransport(client)
    portfolio, allocation = _buy_book()
    with patch("application.execution_service.maybe_publish_attention_for_admission", return_value={
        "sent": 0, "skipped": 0, "failed": 0,
    }), redirect_stdout(io.StringIO()):
        result, port_calls = _run_cycle(portfolio, allocation, transport.submit_order)
    assert len(port_calls) == 1
    assert transport.refused == ["submit_order"]
    assert client.calls == []
    assert [order["status"] for order in result.submitted_orders] == ["unknown"]
    assert result.execution["execution_status"] == "pending_reconciliation"


def test_cycle_calls_real_components_and_does_not_reach_the_client(tmp_path: Path):
    client = _CapturingClient()
    transport = NoOrderBrokerTransport(client)
    result = run_no_order_cycle(
        tmp_path,
        transport=transport,
        snapshot=_snapshot(),
        derived_indicators=_indicators(),
        input_label="synthetic",
    )

    assert result["risk_gate"] == "REJECT"
    assert result["protective_action"] == {
        "request": "new_risk_admission",
        "decision": "REJECT",
        "outcome": "risk_blocked",
        "confirmation": "not_applicable",
        "effect": "zero_positive_targets",
    }
    assert "rejected:capital_base" in result["risk_flags"]
    assert result["positive_target_count"] == 0
    assert result["claim_acquired"] is True
    assert result["input_label"] == "synthetic"
    assert "decision_mapper.map_strategy_decision_to_plan" in result["call_path"]
    assert "application.execution_claim.claim_execution_marker" in result["call_path"]
    assert any(item.endswith("CallableStrategyEntrypoint.evaluate") for item in result["call_path"])
    assert client.calls == []
    assert transport.refused == ["place_order", "cancel_order", "replace_order", "submit_order"]
    again = run_no_order_cycle(
        tmp_path,
        transport=NoOrderBrokerTransport(client),
        snapshot=_snapshot(),
        derived_indicators=_indicators(),
        input_label="synthetic",
    )
    assert again["claim_acquired"] is False
    assert client.calls == []


def test_native_cumulative_quantity_does_not_become_a_fill(tmp_path: Path):
    ledger = ExecutionEventLedger(tmp_path / "ledger.json")
    ledger.record_order_intent(
        intent_id="intent-42",
        order_id="42",
        owner_id="owner-a",
        symbol="BOXX",
        side="BUY",
        quantity="2",
        reserved_amount="20",
    )
    partial = import_recorded_native_observations(
        tmp_path,
        {
            "orders": [
                {
                    "orderId": 42,
                    "status": "FILLED",
                    "filledQuantity": 2,
                    "orderLegCollection": [
                        {
                            "instruction": "BUY",
                            "quantity": 2,
                            "instrument": {"symbol": "BOXX", "assetType": "EQUITY"},
                        }
                    ],
                }
            ]
        },
        input_label="synthetic",
    )
    snapshot = ExecutionEventLedger(tmp_path / "ledger.json").snapshot()

    assert partial["fill_events_created"] == 0
    assert snapshot["events"] == []
    assert snapshot["orders"]["42"]["cumulative_filled_quantity"] == "2"
    assert snapshot["orders"]["42"]["recorded_filled_quantity"] == "0"
    assert snapshot["orders"]["42"]["reconciliation_status"] == "incomplete"
    assert snapshot["account"]["positions"] == {}
    assert partial["parsed"]["orders"][0]["fee"] is None
    assert partial["parsed"]["orders"][0]["owner_id"] is None


def test_new_process_recovery_replays_synthetic_case_once(tmp_path: Path):
    client = _CapturingClient()
    run_no_order_cycle(
        tmp_path,
        transport=NoOrderBrokerTransport(client),
        snapshot=_snapshot(),
        derived_indicators=_indicators(),
        input_label="synthetic",
    )
    record_synthetic_explicit_case(tmp_path)
    partial = import_recorded_native_observations(tmp_path, _native_payload(), input_label="synthetic")
    before = ExecutionEventLedger(tmp_path / "ledger.json").snapshot()
    env = {key: os.environ[key] for key in ("PATH", "HOME", "LANG", "VIRTUAL_ENV") if key in os.environ}
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json; from application.d1_no_order_integration import recover_no_order_state; "
                "print(json.dumps(recover_no_order_state(%r)))" % str(tmp_path)
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert completed.returncode == 0, completed.stderr
    recovered = json.loads(completed.stdout)
    assert recovered["replay_duplicate"] is True
    assert recovered["snapshot_unchanged"] is True
    assert recovered["claim_acquired_on_replay"] is False
    assert recovered["event_count"] == 2
    assert recovered["account"]["positions"]["BOXX"] == "3"
    assert recovered["account"]["principal"] == "299.00"
    assert recovered["account"]["fees"] == "0.60"
    assert recovered["account"]["cash_economic_delta"] == "-299.60"
    assert recovered["native_partial"]["fill_events_created"] == 0
    assert recovered["native_partial"]["parsed"]["execution_events"] == []
    assert recovered["native_partial"]["parsed"]["orders"][0]["order_id"] == "9001"
    assert recovered["native_partial"]["parsed"]["external_positions"]["owner_assigned"] is False
    assert before["account"] == recovered["account"]
    assert partial["parsed"]["orders"][0]["fee"] is None
    assert client.calls == []


def test_non_synthetic_cycle_is_rejected(tmp_path: Path):
    with pytest.raises(D1IntegrationError):
        run_no_order_cycle(
            tmp_path,
            transport=NoOrderBrokerTransport(_CapturingClient()),
            snapshot=_snapshot(),
            derived_indicators=_indicators(),
            input_label="live",
        )


def test_missing_cycle_alert_deduplicates_across_processes(tmp_path: Path):
    run_no_order_cycle(
        tmp_path,
        transport=NoOrderBrokerTransport(_CapturingClient()),
        snapshot=_snapshot(),
        derived_indicators=_indicators(),
        input_label="synthetic",
    )
    assert observe_expected_cycle(tmp_path, expected_date="2026-09-26") == {
        "expected_date": "2026-09-26",
        "receipt_present": True,
        "alert_event_created": False,
        "notification_sent": False,
    }
    first = observe_expected_cycle(tmp_path, expected_date="2026-09-27")
    assert first["receipt_present"] is False
    assert first["alert_event_created"] is True
    completed = subprocess.run(
        [sys.executable, "-c", (
            "import json; from application.d1_no_order_integration import observe_expected_cycle; "
            "print(json.dumps(observe_expected_cycle(%r, expected_date='2026-09-27')))" % str(tmp_path)
        )],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
    )
    assert completed.returncode == 0, completed.stderr
    second = json.loads(completed.stdout)
    assert second["receipt_present"] is False
    assert second["alert_event_created"] is False
    assert second["notification_sent"] is False


def test_forced_process_stop_preserves_claim_and_known_economics(tmp_path: Path):
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])}
    child = subprocess.run(
        [sys.executable, "-c", (
            "import os,signal; from application.d1_no_order_integration import "
            "_MARKER_KEY,_claim_store,record_synthetic_explicit_case; "
            "from application.execution_claim import claim_execution_marker; "
            "from pathlib import Path; root=Path(%r); "
            "claim_execution_marker(_claim_store(root),_MARKER_KEY,metadata={'phase':'before-event'}); "
            "record_synthetic_explicit_case(root); os.kill(os.getpid(),signal.SIGKILL)" % str(tmp_path)
        )],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert child.returncode == -signal.SIGKILL
    recovered = subprocess.run(
        [sys.executable, "-c", (
            "import json; from application.d1_no_order_integration import recover_no_order_state; "
            "print(json.dumps(recover_no_order_state(%r)))" % str(tmp_path)
        )],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert recovered.returncode == 0, recovered.stderr
    state = json.loads(recovered.stdout)
    assert state["claim_acquired_on_replay"] is False
    assert state["replay_duplicate"] is True
    assert state["snapshot_unchanged"] is True
    assert state["event_count"] == 2
    assert state["account"]["positions"]["BOXX"] == "3"
    assert state["account"]["principal"] == "299.00"
    assert state["account"]["fees"] == "0.60"
