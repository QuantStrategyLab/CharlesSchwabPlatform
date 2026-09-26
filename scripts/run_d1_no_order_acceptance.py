#!/usr/bin/env python3
"""Run the local synthetic D1 no-order acceptance, including a new process."""

from __future__ import annotations

import json
import io
import os
import subprocess
import sys
import tempfile
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from quant_platform_kit.common.models import PortfolioSnapshot

from application.d1_no_order_integration import (
    NoOrderBrokerTransport,
    import_recorded_native_observations,
    observe_expected_cycle,
    record_synthetic_explicit_case,
    run_no_order_cycle,
)
from application.offline_fault_acceptance import _buy_book, _run_cycle, _scenario_risk_pause


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


def _child_env() -> dict[str, str]:
    env = {key: os.environ[key] for key in ("PATH", "HOME", "LANG", "VIRTUAL_ENV") if key in os.environ}
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    return env


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["--recover"]:
        from application.d1_no_order_integration import recover_no_order_state

        recovery = recover_no_order_state(argv[1])
        recovery["missing_cycle_recheck"] = observe_expected_cycle(argv[1], expected_date="2026-09-27")
        print(json.dumps(recovery, sort_keys=True, separators=(",", ":")))
        return 0
    client = _CapturingClient()
    transport = NoOrderBrokerTransport(client)
    positive_transport = NoOrderBrokerTransport(client)
    with patch("application.execution_service.maybe_publish_attention_for_admission", return_value={
        "sent": 0, "skipped": 0, "failed": 0,
    }), redirect_stdout(io.StringIO()):
        positive_portfolio, positive_allocation = _buy_book()
        positive_result, positive_port_calls = _run_cycle(
            positive_portfolio, positive_allocation, positive_transport.submit_order,
        )
        risk_pause = _scenario_risk_pause()
    with tempfile.TemporaryDirectory(prefix="schwab-d1-no-order-") as state_dir:
        cycle = run_no_order_cycle(
            state_dir,
            transport=transport,
            snapshot=PortfolioSnapshot(
                as_of=datetime(2026, 9, 26, tzinfo=timezone.utc),
                total_equity=10000.0,
                buying_power=10000.0,
                cash_balance=10000.0,
                positions=(),
                metadata={"account_hash": "synthetic-d1", "input_label": "synthetic"},
            ),
            derived_indicators={
                "SOXL": {"price": 40.0, "ma_trend": 30.0},
                "SOXX": {"price": 200.0, "ma_trend": 180.0},
            },
            input_label="synthetic",
        )
        record_synthetic_explicit_case(state_dir)
        present_receipt = observe_expected_cycle(state_dir, expected_date="2026-09-26")
        missing_first = observe_expected_cycle(state_dir, expected_date="2026-09-27")
        native = import_recorded_native_observations(
            state_dir,
            {
                "orders": [
                    {
                        "orderId": 9001,
                        "status": "FILLED",
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
                ]
            },
            input_label="synthetic",
        )
        completed = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--recover", state_dir],
            check=False,
            capture_output=True,
            text=True,
            env=_child_env(),
        )
    recovery = json.loads(completed.stdout) if completed.returncode == 0 else {}
    report = {
        "schema_version": "d1_no_order_acceptance.v1",
        "input_label": "synthetic",
        "native_observed": False,
        "research_only": True,
        "offline": True,
        "no_account_connection": True,
        "client_trading_calls": client.calls,
        "transport_refused": cycle["transport_refused"],
        "positive_execution_probe": {
            "real_execution_entry": "application.execution_service.execute_rebalance_cycle",
            "port_attempt_count": len(positive_port_calls),
            "transport_refused": positive_transport.refused,
            "execution_status": positive_result.execution.get("execution_status"),
            "submitted_order_statuses": [item.get("status") for item in positive_result.submitted_orders],
            "broker_client_calls": list(client.calls),
        },
        "risk_pause": risk_pause,
        "risk_gate": cycle["risk_gate"],
        "protective_action": cycle["protective_action"],
        "claim_acquired": cycle["claim_acquired"],
        "recovery_returncode": completed.returncode,
        "recovery": recovery,
        "present_receipt": present_receipt,
        "missing_cycle_first": missing_first,
        "native_fee": native["parsed"]["orders"][0]["fee"],
        "native_event_id": native["parsed"]["orders"][0]["per_fill_event_id"],
        "native_fills_created": native["fill_events_created"],
    }
    ok = (
        completed.returncode == 0
        and client.calls == []
        and report["transport_refused"] == ["place_order", "cancel_order", "replace_order", "submit_order"]
        and len(positive_port_calls) == 1
        and positive_transport.refused == ["submit_order"]
        and report["positive_execution_probe"]["submitted_order_statuses"] == ["unknown"]
        and risk_pause["classification"] == "supported"
        and risk_pause["assertions"]["paused_buy_submit_count"] == 0
        and risk_pause["assertions"]["paused_sell_submit_count"] == 0
        and cycle["risk_gate"] == "REJECT"
        and cycle["protective_action"]["outcome"] == "risk_blocked"
        and cycle["protective_action"]["effect"] == "zero_positive_targets"
        and recovery.get("replay_duplicate") is True
        and recovery.get("snapshot_unchanged") is True
        and recovery.get("claim_acquired_on_replay") is False
        and present_receipt["receipt_present"] is True
        and missing_first["alert_event_created"] is True
        and recovery.get("missing_cycle_recheck", {}).get("alert_event_created") is False
        and recovery.get("account", {}).get("positions", {}).get("BOXX") == "3"
        and recovery.get("account", {}).get("principal") == "299.00"
        and recovery.get("account", {}).get("fees") == "0.60"
        and recovery.get("account", {}).get("cash_economic_delta") == "-299.60"
        and native["fill_events_created"] == 0
        and native["parsed"]["orders"][0]["fee"] is None
        and native["parsed"]["execution_events"] == []
    )
    print(json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
