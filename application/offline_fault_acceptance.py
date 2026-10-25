"""Deterministic offline fault acceptance for the fixed Schwab base.

External broker HTTP, cloud report URIs, and clocks stay outside the judged
seams. Dedup, reconciliation, query retry, and new-risk prevention are whatever
the existing functions do.
"""

from __future__ import annotations

import inspect
import json
import os
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from application.broker_reconciliation import (
    _cash_accounting_facts,
    _position_accounting_facts,
    collect_read_only_reconciliation_observations,
)
from application.execution_claim import claim_execution_marker
from application.execution_service import execute_rebalance_cycle
from application.runtime_broker_adapters import (
    SchwabRuntimeBrokerAdapters,
    build_runtime_broker_adapters,
)
from notifications.telegram import build_translator
from quant_platform_kit.common.execution_state import ExecutionMarkerStore
from quant_platform_kit.common.models import ExecutionReport, QuoteSnapshot
from quant_platform_kit.common.port_adapters import CallableExecutionPort, CallableMarketDataPort

BASE_COMMIT = "fba4896d21efbf5b9c2bdf0db75823150e1427af"
RESPONSE_LOSS_MARKER_KEY = "offline-fault/accepted-submit-response-loss"
RESTART_MARKER_KEY = "offline-fault/restart-around-claim"
_FIXED_NOW = datetime(2026, 9, 27, tzinfo=timezone.utc)
_ACCOUNT_HASH = "offline-acct"
_FILL_STREAM_REASON = "order_fill_quantity_is_not_a_timestamped_execution_stream"


def _qual(fn: Any) -> str:
    return f"{fn.__module__}.{fn.__qualname__}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(f"offline fault result is not serializable: {type(value).__name__}")


@contextmanager
def _without_cloud_report_uri():
    key = "EXECUTION_REPORT_GCS_URI"
    saved = os.environ.get(key)
    os.environ.pop(key, None)
    try:
        yield
    finally:
        if saved is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = saved


def _quotes() -> CallableMarketDataPort:
    return CallableMarketDataPort(
        quote_loader=lambda symbol: QuoteSnapshot(
            symbol=symbol,
            as_of="2026-09-27T00:00:00+00:00",
            last_price=100.0,
            ask_price=100.0,
        )
    )


def _run_cycle(portfolio: dict[str, Any], allocation: dict[str, Any], submit) -> tuple[Any, list[Any]]:
    execution = {"trade_threshold_value": 10.0, "reserved_cash": 0.0}
    plan = {
        "account_hash": _ACCOUNT_HASH,
        "portfolio": portfolio,
        "allocation": allocation,
        "execution": execution,
    }
    calls: list[Any] = []

    def port_submit(intent):
        calls.append(intent)
        return submit(intent)

    result = execute_rebalance_cycle(
        client=object(),
        plan=plan,
        portfolio=portfolio,
        execution=execution,
        allocation=allocation,
        fetch_managed_snapshot=lambda _client: None,
        market_data_port=_quotes(),
        load_plan=lambda _snapshot: (plan, portfolio, execution, allocation),
        execution_port=CallableExecutionPort(port_submit),
        translator=build_translator("en"),
        limit_buy_premium=1.0,
        sell_settle_delay_sec=0,
        publish_order_issue=lambda _message: None,
    )
    return result, calls


def _buy_book() -> tuple[dict[str, Any], dict[str, Any]]:
    portfolio = {
        "market_values": {"SOXL": 0.0},
        "quantities": {"SOXL": 0},
        "total_equity": 50000.0,
        "liquid_cash": 1500.0,
        "cash_sweep_symbol": None,
        "account_new_risk_snapshot": {
            "observation_status": "COMPLETE",
            "reconciliation_status": "VERIFIED",
            "circuit_breaker_state": "CLOSED",
            "equity_usd": 50000.0,
        },
    }
    allocation = {
        "target_mode": "value",
        "strategy_symbols": ("SOXL",),
        "risk_symbols": ("SOXL",),
        "income_symbols": (),
        "safe_haven_symbols": (),
        "targets": {"SOXL": 400.0},
    }
    return portfolio, allocation


def _held_book(*, paused: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    snapshot = {
        "observation_status": "COMPLETE",
        "reconciliation_status": "VERIFIED",
        "circuit_breaker_state": "OPEN" if paused else "CLOSED",
        "equity_usd": 50000.0,
    }
    portfolio = {
        "market_values": {"SOXL": 0.0, "BOXX": 850.0},
        "quantities": {"SOXL": 0, "BOXX": 8},
        "total_equity": 50000.0,
        "liquid_cash": 5000.0,
        "cash_sweep_symbol": None,
        "account_new_risk_snapshot": snapshot,
    }
    if paused:
        portfolio["durable_circuit_breaker_state"] = "OPEN"
    allocation = {
        "target_mode": "value",
        "strategy_symbols": ("SOXL", "BOXX"),
        "risk_symbols": ("SOXL", "BOXX"),
        "income_symbols": (),
        "safe_haven_symbols": (),
        "targets": {"SOXL": 400.0, "BOXX": 1000.0},
    }
    return portfolio, allocation


class _BrokerResponse:
    def __init__(self, payload: object, status_code: int = 200):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def _collect_observations(
    orders: list[dict[str, Any]],
    positions: tuple[Any, ...],
    cash_balance: float,
):
    class _Client:
        def get_account_numbers(self):
            return _BrokerResponse([{"hashValue": _ACCOUNT_HASH}])

        def get_orders_for_account(self, account_hash, **kwargs):
            del account_hash, kwargs
            return _BrokerResponse(list(orders))

    def fetch_account_snapshot(_client, *, strategy_symbols=()):
        del strategy_symbols
        return SimpleNamespace(
            cash_balance=cash_balance,
            buying_power=cash_balance,
            total_equity=cash_balance,
            positions=positions,
            metadata={"account_hash": _ACCOUNT_HASH},
        )

    return collect_read_only_reconciliation_observations(
        _Client(),
        fetch_account_snapshot=fetch_account_snapshot,
        now=_FIXED_NOW,
    )


def _order(order_id: int, status: str, filled: float) -> dict[str, Any]:
    return {
        "orderId": order_id,
        "status": status,
        "orderType": "LIMIT",
        "orderStrategyType": "SINGLE",
        "enteredTime": "2026-09-26T14:00:00Z",
        "filledQuantity": filled,
        "remainingQuantity": 3,
        "commission": 1.25,
        "orderLegCollection": [
            {
                "instruction": "BUY",
                "quantity": 5,
                "instrument": {"symbol": "SOXL", "assetType": "EQUITY"},
            }
        ],
    }


def _lost_response(_intent):
    raise TimeoutError("synthetic response lost")


def _accepted_report(intent):
    return ExecutionReport(
        symbol=intent.symbol,
        side=intent.side,
        quantity=intent.quantity,
        status="accepted",
        broker_order_id="synthetic-order",
    )


def _side_count(calls: list[Any], side: str) -> int:
    return sum(1 for intent in calls if str(getattr(intent, "side", "")).lower() == side)


def _scenario_response_loss(state_dir: Path) -> dict[str, Any]:
    store = ExecutionMarkerStore(local_dir=str(state_dir), cloud_prefix_uri=None)
    portfolio, allocation = _buy_book()
    first_claim = claim_execution_marker(
        store,
        RESPONSE_LOSS_MARKER_KEY,
        metadata={"phase": "before-submit"},
    )
    first_calls: list[Any] = []
    result = None
    if first_claim:
        result, first_calls = _run_cycle(portfolio, allocation, _lost_response)
        store.record_outcome(
            RESPONSE_LOSS_MARKER_KEY,
            metadata={
                "execution_status": str(dict(result.execution).get("execution_status") or ""),
                "broker_submission_done": bool(dict(result.execution).get("broker_submission_done")),
                "submitted_orders": list(result.submitted_orders),
            },
        )
    submitted = list(getattr(result, "submitted_orders", ()) or ())
    broker_order_id = next((order.get("broker_order_id") for order in submitted if order.get("broker_order_id")), None)
    second_claim = claim_execution_marker(
        store,
        RESPONSE_LOSS_MARKER_KEY,
        metadata={"phase": "after-response-loss"},
    )
    second_calls: list[Any] = []
    if second_claim:
        _again, second_calls = _run_cycle(portfolio, allocation, _lost_response)
    execution = dict(getattr(result, "execution", {}) or {})
    blocked = second_claim is False and len(second_calls) == 0 and broker_order_id is None
    pending = execution.get("execution_status") == "pending_reconciliation"
    classification = "verified-safe-reject" if blocked and pending and len(first_calls) == 1 else "unsupported"
    return {
        "scenario_id": "accepted_submit_response_loss",
        "classification": classification,
        "call_path": [
            _qual(claim_execution_marker),
            _qual(execute_rebalance_cycle),
            _qual(ExecutionMarkerStore.record_outcome),
        ],
        "events": ["claim", "accepted_submit_response_lost", "outcome_recorded", "second_claim"],
        "assertions": {
            "first_claim_acquired": first_claim is True,
            "first_submit_count": len(first_calls),
            "second_claim_acquired": second_claim is True,
            "second_submit_count": len(second_calls),
            "broker_order_id": broker_order_id,
            "order_status_query_count": 0,
            "execution_status": execution.get("execution_status"),
            "broker_submission_done": execution.get("broker_submission_done"),
        },
        "limitations": [
            "没有 broker order id 时不能编造同一订单身份再查询。",
            "claim 拒绝第二次提交只说明安全拒绝，不证明网络恰好一次送达。",
        ],
    }


def _scenario_partial_fill() -> dict[str, Any]:
    orders = [
        _order(42, "WORKING", 2),
        _order(42, "WORKING", 2),
        _order(43, "PENDING_CANCEL", 2),
    ]
    observations = _collect_observations(
        orders,
        (SimpleNamespace(symbol="SOXL", quantity=2.0, market_value=200.0),),
        100.0,
    )
    open_orders = [dict(order) for order in observations.open_orders]
    duplicate_count = sum(1 for order in open_orders if order.get("order_id") == "42")
    cancel_orders = [order for order in open_orders if order.get("order_id") == "43"]
    cancel_status = cancel_orders[0]["status"] if cancel_orders else None
    reason_codes = list(dict(observations.coverage).get("reason_codes") or ())
    has_fee = any("fee" in order or "commission" in order for order in open_orders)
    stream_missing = (
        observations.recent_executions_complete is False
        and observations.recent_executions == ()
        and _FILL_STREAM_REASON in reason_codes
    )
    classification = "unsupported" if stream_missing else "supported"
    return {
        "scenario_id": "partial_fill_cancel_late_duplicate_reports",
        "classification": classification,
        "call_path": [_qual(collect_read_only_reconciliation_observations)],
        "events": ["partial_fill_snapshot", "duplicate_fill_snapshot", "cancel_request", "reconcile"],
        "assertions": {
            "recent_executions_complete": observations.recent_executions_complete is True,
            "recent_executions_count": len(observations.recent_executions),
            "reason_codes": reason_codes,
            "cancel_request_status": cancel_status,
            "cancel_request_remains_open": cancel_status == "PENDING_CANCEL",
            "duplicate_open_order_count": duplicate_count,
            "exactly_once_fill_proven": False,
            "fee_exactly_once_proven": False,
            "normalized_order_has_fee": has_fee,
            "filled_quantity_is_timestamped_stream": False,
        },
        "limitations": [
            "固定 base 的 recent_executions_complete 恒为 false，filledQuantity 不是带时间的成交流。",
            "撤单请求状态保留为 PENDING_CANCEL，不能当成已撤。",
            "迟到或重复回报与费用的恰好一次无法证明，harness 不自行去重后宣称已支持。",
        ],
    }


def _scenario_restart(state_dir: Path) -> dict[str, Any]:
    store = ExecutionMarkerStore(local_dir=str(state_dir), cloud_prefix_uri=None)
    claim_execution_marker(store, RESTART_MARKER_KEY, metadata={"phase": "persisted"})
    restarted = ExecutionMarkerStore(local_dir=str(state_dir), cloud_prefix_uri=None)
    second_claim = claim_execution_marker(
        restarted,
        RESTART_MARKER_KEY,
        metadata={"phase": "after-restart"},
    )
    claim_path = Path(restarted._local_path(RESTART_MARKER_KEY))
    payload = json.loads(claim_path.read_text(encoding="utf-8"))
    observations = _collect_observations((), (), 0.0)
    events = ["claim", "restart", "reconcile", "continue_decision"]
    continue_calls: list[Any] = []
    continue_allowed = second_claim is True and observations.recent_executions_complete is True
    if continue_allowed:
        portfolio, allocation = _buy_book()
        _result, continue_calls = _run_cycle(portfolio, allocation, _accepted_report)
    claim_has_ttl = "ttl" in inspect.signature(claim_execution_marker).parameters or any(
        key in payload for key in ("ttl", "expires_at")
    )
    survived = second_claim is False and claim_path.is_file()
    classification = (
        "verified-safe-reject"
        if survived and not claim_has_ttl and observations.recent_executions_complete is False and not continue_calls
        else "unsupported"
    )
    return {
        "scenario_id": "restart_around_claim",
        "classification": classification,
        "call_path": [
            _qual(claim_execution_marker),
            _qual(collect_read_only_reconciliation_observations),
        ],
        "events": events,
        "assertions": {
            "restarted_claim_acquired": second_claim is True,
            "claim_has_ttl": claim_has_ttl,
            "claim_file_exists_after_reconcile": claim_path.is_file(),
            "recent_executions_complete": observations.recent_executions_complete is True,
            "continue_submit_count": len(continue_calls),
            "assumed_unfilled": False,
        },
        "limitations": [
            "未完成 claim 没有 TTL，重启后的第二次 claim 不会把它当成未成交并清除。",
            "对账看不到完整成交流，因此 continue 被拒绝；这不是同一订单已恢复成交。",
        ],
    }


def _status_error(status_code: int) -> RuntimeError:
    exc = RuntimeError(str(status_code))
    exc.status_code = status_code  # type: ignore[attr-defined]
    return exc


def _adapters(**kwargs) -> SchwabRuntimeBrokerAdapters:
    arguments = {
        "fetch_account_snapshot_fn": lambda _client, *, strategy_symbols: SimpleNamespace(),
        "fetch_quotes_fn": lambda _client, _symbols: {},
        "fetch_daily_price_history_fn": lambda _client, _symbol: [],
        "submit_equity_order_fn": lambda *_args, **_kwargs: None,
    }
    arguments.update(kwargs)
    return build_runtime_broker_adapters(managed_symbols=("SOXL",), **arguments)


def _scenario_query() -> dict[str, Any]:
    account_attempts: list[tuple[str, ...]] = []

    def fetch_account(_client, *, strategy_symbols):
        account_attempts.append(tuple(strategy_symbols))
        if len(account_attempts) < 3:
            raise _status_error(429)
        return SimpleNamespace(account_hash=_ACCOUNT_HASH, cash_balance=100.0)

    adapters = _adapters(fetch_account_snapshot_fn=fetch_account)
    adapters.fetch_managed_snapshot(object())

    disconnect_attempts: list[int] = []

    def fetch_disconnect(_client, *, strategy_symbols):
        del strategy_symbols
        disconnect_attempts.append(1)
        raise RuntimeError("temporary disconnect")

    disconnect_cash = None
    try:
        _adapters(fetch_account_snapshot_fn=fetch_disconnect).fetch_managed_snapshot(object())
        disconnect_cash = 0.0
    except RuntimeError:
        disconnect_cash = None

    order_attempts: list[str] = []

    def fetch_order_status(_client, _account_hash, order_id):
        order_attempts.append(str(order_id))
        if order_id == "rate-limit":
            raise _status_error(429)
        if order_id == "unknown":
            return {"status": "UNKNOWN"}
        raise AssertionError(order_id)

    fetcher = _adapters(fetch_order_status_fn=fetch_order_status).build_order_status_fetcher(
        object(),
        _ACCOUNT_HASH,
    )
    try:
        fetcher("rate-limit")
    except RuntimeError:
        pass
    unknown_payload = dict(fetcher("unknown") or {})
    unknown_filled = unknown_payload["filledQuantity"] if "filledQuantity" in unknown_payload else None
    portfolio = {
        "market_values": {"SOXL": 0.0},
        "quantities": {"SOXL": 0},
        "total_equity": 50000.0,
        "liquid_cash": 1500.0,
        "cash_sweep_symbol": None,
        "unknown_pending_orders": True,
    }
    allocation = {
        "target_mode": "value",
        "strategy_symbols": ("SOXL",),
        "risk_symbols": ("SOXL",),
        "income_symbols": (),
        "safe_haven_symbols": (),
        "targets": {"SOXL": 400.0},
    }
    _result, new_risk_calls = _run_cycle(portfolio, allocation, _accepted_report)
    order_retry = order_attempts.count("rate-limit") > 1
    safe = (
        len(account_attempts) == 3
        and len(disconnect_attempts) == 1
        and disconnect_cash is None
        and order_attempts.count("rate-limit") == 1
        and unknown_filled is None
        and unknown_filled != 0
        and len(new_risk_calls) == 0
        and order_retry is False
    )
    return {
        "scenario_id": "query_429_disconnect_unknown",
        "classification": "verified-safe-reject" if safe else "unsupported",
        "call_path": [
            _qual(SchwabRuntimeBrokerAdapters.fetch_managed_snapshot),
            _qual(SchwabRuntimeBrokerAdapters.build_order_status_fetcher),
            _qual(execute_rebalance_cycle),
        ],
        "events": ["account_429", "account_disconnect", "order_429", "order_unknown", "new_risk_cycle"],
        "assertions": {
            "account_query_attempts": len(account_attempts),
            "disconnect_query_attempts": len(disconnect_attempts),
            "disconnect_snapshot_cash": disconnect_cash,
            "order_query_attempts_on_429": order_attempts.count("rate-limit"),
            "order_query_bounded_retry": order_retry,
            "unknown_filled_quantity": unknown_filled,
            "unknown_treated_as_zero": unknown_filled == 0,
            "new_risk_submit_count": len(new_risk_calls),
        },
        "limitations": [
            "有界重试只存在于账户快照查询；订单查询没有重试循环。",
            "不能把账户 429 重试外推成订单查询重试。",
            "未知委托没有数量时保持空值，不用 0 代替，并冻结该周期新增风险提交。",
        ],
    }


def _scenario_multi_owner() -> dict[str, Any]:
    observations = _collect_observations(
        (),
        (SimpleNamespace(symbol="BOXX", quantity=25.0, market_value=2500.0),),
        100.0,
    )
    facts = [dict(item) for item in _position_accounting_facts(observations.positions)]
    cash_facts = dict(_cash_accounting_facts(observations.cash))
    owner_present = any("owner" in fact for fact in facts)
    net_quantity = facts[0]["quantity"] if facts else None
    return {
        "scenario_id": "same_boxx_multi_owner",
        "classification": "supported" if owner_present else "unsupported",
        "call_path": [
            _qual(collect_read_only_reconciliation_observations),
            _qual(_position_accounting_facts),
            _qual(_cash_accounting_facts),
        ],
        "events": [
            {"owner": "core", "symbol": "BOXX", "quantity": 10, "cash": 100.0},
            {"owner": "sleeve", "symbol": "BOXX", "quantity": 15, "cash": 100.0},
            "broker_net_position",
        ],
        "assertions": {
            "broker_net_quantity": net_quantity,
            "owner_attribution_supported": owner_present,
            "position_fact_keys": sorted(facts[0]) if facts else [],
            "aggregate_cash": cash_facts.get("cash_balance"),
            "cash_recorded_assignments": 1,
            "owner_cash_allocations": None,
        },
        "limitations": [
            "券商净持仓和现金事实只有合计，没有持久的 owner-order 或 fill 映射。",
            "不能把同一笔现金分给多个 owner；归属保持 unsupported。",
        ],
    }


def _scenario_risk_pause() -> dict[str, Any]:
    control_portfolio, control_allocation = _held_book(paused=False)
    _control, control_calls = _run_cycle(control_portfolio, control_allocation, _accepted_report)
    phases = {
        "request": {
            "seq": 1,
            "constraint": "durable_circuit_breaker_state=OPEN",
        }
    }
    paused_portfolio, paused_allocation = _held_book(paused=True)
    paused_result, paused_calls = _run_cycle(paused_portfolio, paused_allocation, _accepted_report)
    logs = "\n".join(str(line) for line in paused_result.trade_logs)
    phases["receive"] = {
        "seq": 2,
        "seen_in_execution_trade_log": "NEW_RISK_PROHIBITED" in logs,
    }
    paused_buys = _side_count(paused_calls, "buy")
    paused_sells = _side_count(paused_calls, "sell")
    phases["effective_prevention"] = {
        "seq": 3,
        "new_risk_submit_count": paused_buys,
        "market_liquidation_submit_count": paused_sells,
    }
    queries: list[str] = []

    def fetch_order_status(_client, _account_hash, order_id):
        queries.append(str(order_id))
        return {"status": "WORKING"}

    fetcher = _adapters(fetch_order_status_fn=fetch_order_status).build_order_status_fetcher(
        object(),
        _ACCOUNT_HASH,
    )
    fetcher("pause-status")
    unpaused_buys = _side_count(control_calls, "buy")
    prevented = (
        phases["receive"]["seen_in_execution_trade_log"] is True
        and paused_buys == 0
        and paused_sells == 0
        and unpaused_buys == 1
        and len(queries) == 1
    )
    return {
        "scenario_id": "risk_pause",
        "classification": "supported" if prevented else "unsupported",
        "call_path": [
            _qual(execute_rebalance_cycle),
            _qual(SchwabRuntimeBrokerAdapters.build_order_status_fetcher),
        ],
        "events": ["pause_request", "execute_rebalance_cycle", "effective_prevention", "safe_query"],
        "phases": phases,
        "assertions": {
            "paused_buy_submit_count": paused_buys,
            "paused_sell_submit_count": paused_sells,
            "unpaused_buy_submit_count": unpaused_buys,
            "safe_query_after_pause_count": len(queries),
            "cancel_all_invoked": False,
        },
        "limitations": [
            "阻止新增风险来自本进程执行路径上的既有 gate，不是已部署的生产暂停开关。",
            "暂停后仍可做只读订单查询；没有调用撤单，也没有市价清仓。",
        ],
    }


def run_offline_fault_acceptance(state_dir: str | os.PathLike[str]) -> dict[str, Any]:
    """Run the six synthetic scenarios and return a JSON-ready aggregate."""

    root = Path(state_dir)
    root.mkdir(parents=True, exist_ok=True)
    with _without_cloud_report_uri():
        scenarios = [
            _scenario_response_loss(root),
            _scenario_partial_fill(),
            _scenario_restart(root),
            _scenario_query(),
            _scenario_multi_owner(),
            _scenario_risk_pause(),
        ]
    return _json_ready(
        {
            "schema_version": "schwab_offline_fault_acceptance.v1",
            "base_commit": BASE_COMMIT,
            "research_only": True,
            "offline": True,
            "no_account_connection": True,
            "scenarios": scenarios,
        }
    )
