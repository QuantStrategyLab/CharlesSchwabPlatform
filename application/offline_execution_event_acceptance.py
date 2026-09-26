"""Synthetic E01-E06 acceptance for local execution-event accounting."""

from __future__ import annotations

import json
import io
from collections.abc import Mapping
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import patch

from application.execution_event_accounting import ExecutionEventError, ExecutionEventLedger
from application.offline_fault_acceptance import run_offline_fault_acceptance


def _json_ready(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(f"acceptance result is not JSON-safe: {type(value).__name__}")


def _ledger(root: Path, name: str) -> ExecutionEventLedger:
    return ExecutionEventLedger(root / name / "ledger.json")


def _intent(ledger: ExecutionEventLedger, *, intent_id: str, order_id: str | None, owner_id: str,
            symbol: str = "BOXX", side: str = "BUY", quantity: str = "6",
            reserved_amount: str = "600") -> None:
    ledger.record_order_intent(
        intent_id=intent_id, order_id=order_id, owner_id=owner_id, symbol=symbol,
        side=side, quantity=quantity, reserved_amount=reserved_amount,
    )


def _fill(event_id: str, order_id: str, quantity: str, price: str, fee: str,
          event_time: str = "2026-09-27T10:00:00Z", event_type: str = "FILL") -> dict[str, str]:
    return {
        "event_id": event_id, "order_id": order_id, "quantity": quantity, "price": price,
        "fee": fee, "fee_source": "synthetic_declared_per_fill_fee",
        "event_time": event_time, "event_type": event_type,
    }


def _find(acceptance: Mapping[str, Any], scenario_id: str) -> Mapping[str, Any]:
    return next(item for item in acceptance["scenarios"] if item["scenario_id"] == scenario_id)


def _case_e01(root: Path) -> dict[str, Any]:
    ledger = _ledger(root, "E01")
    _intent(ledger, intent_id="owner-a-intent", order_id="synthetic-order-1", owner_id="owner-a")
    first = _fill("exec-1", "synthetic-order-1", "2", "100", "0.40")
    receipt = ledger.record_execution_event(first)
    duplicate = ledger.record_execution_event(first)
    ledger.record_order_update(
        order_id="synthetic-order-1", status="PENDING_CANCEL", cumulative_filled_quantity="2",
    )
    pending = ledger.snapshot()["orders"]["synthetic-order-1"]
    ledger.record_execution_event(_fill(
        "exec-2", "synthetic-order-1", "1", "99", "0.20", "2026-09-27T10:01:00Z",
    ))
    ledger.record_order_update(
        order_id="synthetic-order-1", status="CANCELED", cumulative_filled_quantity="3",
    )
    snapshot = ledger.snapshot()
    account = snapshot["account"]
    return {
        "case_id": "E01", "classification": "implemented_offline",
        "call_path": [
            "application.execution_event_accounting.ExecutionEventLedger.record_order_intent",
            "application.execution_event_accounting.ExecutionEventLedger.record_execution_event",
            "application.execution_event_accounting.ExecutionEventLedger.record_order_update",
        ],
        "input_evidence": "synthetic_normalized_incremental_fill_events",
        "assertions": {
            "event_digest_present": bool(receipt.event_digest), "duplicate_is_idempotent": duplicate.duplicate,
            "pending_cancel_is_nonterminal": pending["status"] == "PENDING_CANCEL" and not pending["terminal"],
            "shares": account["positions"]["BOXX"], "principal": account["principal"],
            "fees": account["fees"], "cash_economic_delta": account["cash_economic_delta"],
            "cancelled_order_preserves_recorded_fills": snapshot["orders"]["synthetic-order-1"]["recorded_filled_quantity"] == "3",
        },
        "limitations": ["不把券商累计 filledQuantity 转换为成交事件。"],
    }


def _case_e02(root: Path) -> dict[str, Any]:
    path = root / "E02" / "ledger.json"
    ledger = ExecutionEventLedger(path)
    _intent(ledger, intent_id="intent", order_id="order", owner_id="owner")
    event = _fill("fill", "order", "2", "100", "0.40")
    ledger.record_execution_event(event)
    checkpoint = ledger.snapshot()
    restarted = ExecutionEventLedger(path)
    replay = restarted.record_execution_event(event)
    after = restarted.snapshot()
    return {
        "case_id": "E02", "classification": "implemented_offline",
        "call_path": [
            "application.execution_event_accounting.ExecutionEventLedger._save",
            "application.execution_event_accounting.ExecutionEventLedger._load",
            "application.execution_event_accounting.ExecutionEventLedger.record_execution_event",
        ],
        "input_evidence": "synthetic_local_atomic_snapshot_and_replay",
        "assertions": {
            "restart_snapshot_equal": checkpoint == after,
            "replayed_event_duplicate": replay.duplicate,
            "shares_after_restart": after["account"]["positions"]["BOXX"],
            "cash_after_restart": after["account"]["cash_economic_delta"],
        },
        "limitations": ["本地原子文件替换不构成跨主机或网络 exactly-once。"],
    }


def _case_e03(root: Path) -> dict[str, Any]:
    ledger = _ledger(root, "E03")
    for owner, buy_order, sell_order in (("owner-a", "a-buy", "a-sell"), ("owner-b", "b-buy", "b-sell")):
        _intent(ledger, intent_id=f"{owner}-buy", order_id=buy_order, owner_id=owner,
                side="BUY", quantity="4", reserved_amount="500")
        _intent(ledger, intent_id=f"{owner}-sell", order_id=sell_order, owner_id=owner,
                side="SELL", quantity="2", reserved_amount="0")
    ledger.record_execution_event(_fill("a-buy-fill", "a-buy", "4", "50", "0.40"))
    ledger.record_execution_event(_fill("a-sell-fill", "a-sell", "1", "52", "0.10", "2026-09-27T10:01:00Z"))
    ledger.record_execution_event(_fill("b-buy-fill", "b-buy", "2", "51", "0.20", "2026-09-27T10:02:00Z"))
    ledger.record_execution_event(_fill("b-sell-fill", "b-sell", "1", "53", "0.10", "2026-09-27T10:03:00Z"))
    state = ledger.snapshot()
    owners = state["owners"]
    account = state["account"]
    summed_shares = sum((int(owner["positions"].get("BOXX", "0")) for owner in owners.values()), 0)
    summed_cash = sum((Decimal(owner["cash_economic_delta"]) for owner in owners.values()), Decimal("0"))
    summed_principal = sum((Decimal(owner["principal"]) for owner in owners.values()), Decimal("0"))
    summed_fees = sum((Decimal(owner["fees"]) for owner in owners.values()), Decimal("0"))
    summed_reserved = sum((Decimal(owner["reserved_amount"]) for owner in owners.values()), Decimal("0"))
    return {
        "case_id": "E03", "classification": "implemented_offline",
        "call_path": [
            "application.execution_event_accounting.ExecutionEventLedger.record_order_intent",
            "application.execution_event_accounting.ExecutionEventLedger.record_execution_event",
            "application.execution_event_accounting.ExecutionEventLedger.snapshot",
        ],
        "input_evidence": "synthetic_owner_bound_boxx_buy_sell_events",
        "assertions": {
            "owner_count": len(owners), "account_shares": account["positions"]["BOXX"],
            "owner_share_sum": str(summed_shares), "owner_totals_equal_account": str(summed_shares) == account["positions"]["BOXX"]
                and summed_cash == Decimal(account["cash_economic_delta"])
                and summed_principal == Decimal(account["principal"])
                and summed_fees == Decimal(account["fees"])
                and summed_reserved == Decimal(account["reserved_amount"]),
            "owner_reservations": {key: value["reserved_amount"] for key, value in owners.items()},
            "account_reserved_amount": account["reserved_amount"],
            "account_cash_economic_delta": account["cash_economic_delta"],
            "account_fees": account["fees"],
            "account_pending_settlement_item_count": len(account["pending_settlement_items"]),
            "owner_pending_settlement_item_count": sum(len(owner["pending_settlement_items"]) for owner in owners.values()),
        },
        "limitations": ["合成归属不证明真实账户历史仓位的 owner 拆分。"],
    }


def _case_e04(root: Path) -> dict[str, Any]:
    ledger = _ledger(root, "E04")
    _intent(ledger, intent_id="intent", order_id=None, owner_id="owner", quantity="1", reserved_amount="100")
    unknown_result = ledger.reconcile_unknown_order_identity(intent_id="intent")
    unknown_state = ledger.snapshot()
    ledger.bind_order_identity(intent_id="intent", order_id="query-confirmed-order")
    bound_state = ledger.snapshot()
    ledger.record_order_update(
        order_id="query-confirmed-order", status="FILLED", cumulative_filled_quantity="1",
    )
    ledger.record_execution_event(_fill("confirmed-fill", "query-confirmed-order", "1", "100", "0.25"))
    state = ledger.snapshot()
    return {
        "case_id": "E04", "classification": "verified-safe-reject",
        "call_path": [
            "application.execution_event_accounting.ExecutionEventLedger.reconcile_unknown_order_identity",
            "application.execution_event_accounting.ExecutionEventLedger.bind_order_identity",
            "application.execution_event_accounting.ExecutionEventLedger.record_execution_event",
        ],
        "input_evidence": "synthetic_unknown_response_then_exact_order_identity_query",
        "assertions": {
            "unknown_identity_result": unknown_result,
            "unknown_identity_stays_unbound": unknown_state["intents"]["intent"]["order_id"] == "",
            "unknown_identity_books_no_fill": unknown_state["events"] == [],
            "unbound_owner_reservation": unknown_state["owners"]["owner"]["reserved_amount"],
            "unbound_account_reservation": unknown_state["account"]["reserved_amount"],
            "binding_reservation_not_double_counted": bound_state["owners"]["owner"]["reserved_amount"] == "100.00"
                and bound_state["account"]["reserved_amount"] == "100.00",
            "exact_identity_reconciled": state["orders"]["query-confirmed-order"]["reconciliation_status"] == "complete",
            "exact_identity_shares": state["account"]["positions"]["BOXX"],
        },
        "limitations": ["精确订单身份的分支为合成查询证据；本实现不调用券商账户或网络。"],
    }


def _case_e05(root: Path) -> dict[str, Any]:
    ledger = _ledger(root, "E05")
    _intent(ledger, intent_id="intent", order_id="order", owner_id="owner", quantity="5", reserved_amount="500")
    ledger.record_order_update(order_id="order", status="CANCELED", cumulative_filled_quantity="3")
    incomplete = ledger.snapshot()["orders"]["order"]
    event = _fill("event-id", "order", "1", "100", "0.20")
    ledger.record_execution_event(event)
    conflict_rejected = False
    try:
        ledger.record_execution_event({**event, "price": "101"})
    except ExecutionEventError:
        conflict_rejected = True
    unsupported_rejected = False
    try:
        ledger.record_execution_event(_fill("reversal-id", "order", "1", "100", "0.20", event_type="REVERSAL"))
    except ExecutionEventError:
        unsupported_rejected = True
    state = ledger.snapshot()
    order = state["orders"]["order"]
    return {
        "case_id": "E05", "classification": "verified-safe-reject",
        "call_path": [
            "application.execution_event_accounting.ExecutionEventLedger.record_order_update",
            "application.execution_event_accounting.ExecutionEventLedger.record_execution_event",
        ],
        "input_evidence": "synthetic_terminal_gap_unknown_detail_and_conflicting_event_id",
        "assertions": {
            "terminal_gap_initially_incomplete": incomplete["reconciliation_status"] == "incomplete",
            "reservation_stays_pending": incomplete["reservation_status"] == "pending",
            "conflict_rejected": conflict_rejected,
            "conflict_quarantined": "event-id" in state["quarantined_event_ids"],
            "unsupported_reversal_rejected": unsupported_rejected,
            "shares_only_from_incremental_detail": state["account"]["positions"]["BOXX"] == "1",
            "still_incomplete_after_one_of_three_reported_shares": order["reconciliation_status"] == "incomplete",
        },
        "limitations": ["缺页或未知查询不会补造未观测的成交详情。"],
    }


def _case_e06(root: Path) -> dict[str, Any]:
    with patch("application.runtime_broker_adapters.time.sleep", lambda _seconds: None):
        with redirect_stdout(io.StringIO()):
            previous = run_offline_fault_acceptance(root / "existing-seams")
    response_loss = _find(previous, "accepted_submit_response_loss")
    restart = _find(previous, "restart_around_claim")
    query = _find(previous, "query_429_disconnect_unknown")
    pause = _find(previous, "risk_pause")
    assertions = {
        "response_loss_no_second_submit": response_loss["assertions"]["second_submit_count"] == 0,
        "claim_restart_safe_reject": restart["assertions"]["restarted_claim_acquired"] is False,
        "safe_query_unknown_no_new_risk": query["assertions"]["new_risk_submit_count"] == 0,
        "risk_pause_blocks_new_risk": pause["assertions"]["paused_buy_submit_count"] == 0,
        "risk_pause_allows_safe_query": pause["assertions"]["safe_query_after_pause_count"] == 1,
        "funding_control_regression_command_required": True,
    }
    return {
        "case_id": "E06", "classification": "supported" if all(
            value for key, value in assertions.items() if key != "funding_control_regression_command_required"
        ) else "unsupported",
        "call_path": sorted({path for item in (response_loss, restart, query, pause) for path in item["call_path"]}),
        "input_evidence": "existing_local_offline_fault_acceptance_synthetic_boundary",
        "assertions": assertions,
        "regression_tests": [
            "tests/test_offline_fault_acceptance.py",
            "tests/test_execution_claim.py",
            "tests/test_rebalance_service.py cash-sweep funding cases",
        ],
        "limitations": ["风险暂停来自本地执行路径的已有测试，不证明生产部署开关状态。"],
    }


def run_offline_execution_event_acceptance(state_dir: str | Path) -> dict[str, Any]:
    """Run the frozen six-case suite using only synthetic events and local files."""
    root = Path(state_dir)
    root.mkdir(parents=True, exist_ok=True)
    cases = [
        _case_e01(root), _case_e02(root), _case_e03(root),
        _case_e04(root), _case_e05(root), _case_e06(root),
    ]
    return _json_ready({
        "schema_version": "schwab_offline_execution_event_acceptance.v1",
        "research_only": True,
        "offline": True,
        "no_account_connection": True,
        "cases": cases,
        "native_adapter": {
            "order_lookup": "documented_surface",
            "exact_order_id_lookup": "documented_surface",
            "order_status_enum": "documented_surface",
            "stable_per_fill_id": "unsupported_unverified",
            "per_fill_fee": "unsupported_unverified",
            "correction_reversal_fields": "unsupported_unverified",
        },
    })
