from __future__ import annotations

from types import SimpleNamespace

from application.runtime_report_summary import summarize_execution_cycle_result


def test_summarize_execution_cycle_result_counts_submitted_orders() -> None:
    result = SimpleNamespace(
        execution={
            "execution_status": "submitted",
            "signal_date": "2026-07-10",
            "effective_date": "2026-07-11",
            "execution_timing_contract": "next_trading_day",
        },
        submitted_orders=(
            {"symbol": "TQQQ", "side": "buy", "order_type": "limit", "status": "accepted"},
            {"symbol": "BOXX", "side": "sell", "order_type": "market", "status": "accepted"},
            {"symbol": "SPYI", "side": "buy", "order_type": "market", "status": "dry_run"},
        ),
        trade_logs=("first", "second"),
    )

    summary = summarize_execution_cycle_result(result, dry_run=False)

    assert summary["orders_submitted_count"] == 3
    assert summary["orders_previewed_count"] == 0
    assert summary["submitted_order_status_counts"] == {"accepted": 2, "dry_run": 1}
    assert summary["submitted_order_side_counts"] == {"buy": 2, "sell": 1}
    assert summary["submitted_order_type_counts"] == {"limit": 1, "market": 2}
    assert summary["notes_count"] == 2
    assert summary["signal_date"] == "2026-07-10"
    assert summary["effective_date"] == "2026-07-11"
    assert summary["execution_timing_contract"] == "next_trading_day"


def test_summarize_execution_cycle_result_marks_dry_run_preview() -> None:
    result = SimpleNamespace(
        execution={"execution_status": "dry_run"},
        submitted_orders=({"symbol": "TQQQ", "side": "buy", "order_type": "limit", "status": "dry_run"},),
        trade_logs=(),
    )

    summary = summarize_execution_cycle_result(result, dry_run=True)

    assert summary["orders_submitted_count"] == 1
    assert summary["orders_previewed_count"] == 1
    assert summary["dry_run_order_preview_available"] is True
    assert summary["dry_run_preview"] is True


def test_summarize_execution_cycle_result_handles_empty_result() -> None:
    result = SimpleNamespace(execution={"no_op_reason": "market_closed"}, submitted_orders=(), trade_logs=())

    summary = summarize_execution_cycle_result(result, dry_run=False)

    assert summary["orders_submitted_count"] == 0
    assert summary["submitted_order_status_counts"] == {}
    assert summary["submitted_order_side_counts"] == {}
    assert summary["submitted_order_type_counts"] == {}
    assert summary["no_op_reason"] == "market_closed"
