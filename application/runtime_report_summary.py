from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping


def _as_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_submitted_orders(result: object) -> tuple[dict[str, object], ...]:
    raw_orders = getattr(result, "submitted_orders", ()) or ()
    normalized: list[dict[str, object]] = []
    for raw in raw_orders:
        if isinstance(raw, Mapping):
            normalized.append(dict(raw))
    return tuple(normalized)


def _counter_by_key(items: Iterable[Mapping[str, object]], key: str) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for item in items:
        value = str(item.get(key) or "").strip().lower()
        if value:
            counter[value] += 1
    return dict(counter)


def summarize_execution_cycle_result(result: object, *, dry_run: bool) -> dict[str, object]:
    execution = _as_mapping(getattr(result, "execution", {}))
    submitted_orders = _normalize_submitted_orders(result)
    trade_logs = tuple(str(item) for item in (getattr(result, "trade_logs", ()) or ()) if str(item).strip())

    execution_status = str(execution.get("execution_status") or "").strip()
    no_op_reason = str(execution.get("no_op_reason") or "").strip()
    summary: dict[str, object] = {
        "result": execution_status or ("dry_run" if dry_run and submitted_orders else "ok"),
        "execution_status": execution_status or None,
        "no_op_reason": no_op_reason or None,
        "orders_submitted_count": len(submitted_orders),
        "orders_previewed_count": len(submitted_orders) if dry_run else 0,
        "dry_run_order_preview_available": bool(dry_run and submitted_orders),
        "dry_run_preview": bool(dry_run and submitted_orders),
        "notes_count": len(trade_logs),
        "submitted_order_status_counts": _counter_by_key(submitted_orders, "status"),
        "submitted_order_side_counts": _counter_by_key(submitted_orders, "side"),
        "submitted_order_type_counts": _counter_by_key(submitted_orders, "order_type"),
    }
    for field_name in ("signal_date", "effective_date", "execution_timing_contract"):
        value = execution.get(field_name)
        if value is not None and value != "":
            summary[field_name] = value
    return summary
