"""Project Schwab daily / account-facts evidence into QRS DIGEST_CANDIDATES JSON.

Pure adapter: no broker calls, no GCS, no QRS POST, no secret reads.
Missing fields stay null with explicit field_status / reason_code — never invent
fill/order counts (Schwab daily fills.source is not_connected).
Identity (opaque_account_uid / target_id) must be supplied by the caller from
protected configuration; this module does not mint them from thin air.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

PLATFORM_ID = "schwab"
SCHEMA_VERSION = "qsl.digest_candidates.v1"
DEFAULT_STRATEGY = "soxl_soxx_trend_income"
EVIDENCE_PROVENANCE = "candidates"

# Activities that mean a real cycle ran for the business day.
_RAN_ACTIVITIES = frozenset(
    {
        "no_signal",
        "no_rebalance",
        "no_submission",
        "submitted",
        "broker_acknowledged",
        "partially_filled",
        "filled",
        "previewed",
        "blocked",
        "failed",
        "unknown",
        "reconciliation_required",
    }
)
_ALERT_ACTIVITIES = frozenset(
    {"blocked", "failed", "unknown", "reconciliation_required"}
)
_SIGNAL_BY_ACTIVITY = {
    "no_signal": "no_signal",
    "no_rebalance": "no_rebalance",
    "no_submission": "no_action",
    "submitted": "order_submitted",
    "broker_acknowledged": "order_acknowledged",
    "partially_filled": "partially_filled",
    "filled": "filled",
    "previewed": "previewed",
    "blocked": "blocked",
    "failed": "failed",
    "unknown": "unknown",
    "reconciliation_required": "reconciliation_required",
}
_REBALANCE_BY_ACTIVITY = {
    "no_signal": "no_rebalance",
    "no_rebalance": "no_rebalance",
    "no_submission": "no_order",
    "submitted": "rebalance",
    "broker_acknowledged": "rebalance",
    "partially_filled": "rebalance",
    "filled": "rebalance",
    "previewed": "pending",
    "blocked": "pending",
    "failed": "pending",
    "unknown": "pending",
    "reconciliation_required": "pending",
}


def _as_mapping(value: object) -> dict[str, Any] | None:
    return dict(value) if isinstance(value, Mapping) else None


def _as_str(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _money_to_float(value: object) -> float | None:
    """Parse owner-confirmed decimal text; reject non-finite / non-numeric."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        amount = float(value)
        return amount if amount >= 0 and amount == amount else None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        amount = Decimal(value.strip())
    except (InvalidOperation, ValueError):
        return None
    if not amount.is_finite() or amount < 0:
        return None
    return float(amount)


def _unknown_counts(*, reason_code: str) -> dict[str, Any]:
    return {
        "fill_count": None,
        "order_count": None,
        "field_status": {
            "fill_count": "counts_unknown",
            "order_count": "counts_unknown",
        },
        "reason_code": reason_code,
    }


def _activities(runs: Sequence[Mapping[str, Any]]) -> list[str]:
    out: list[str] = []
    for run in runs:
        if not isinstance(run, Mapping):
            continue
        activity = run.get("activity")
        if isinstance(activity, str) and activity in _RAN_ACTIVITIES:
            out.append(activity)
    return out


def _pick_signal_and_rebalance(
    activities: Sequence[str],
) -> tuple[str, str, str]:
    """Return (signal_summary, rebalance_kind, rebalance_conclusion)."""
    if not activities:
        return "", "", ""
    # Prefer the strongest actionable activity when multiple runs exist.
    priority = (
        "failed",
        "blocked",
        "reconciliation_required",
        "unknown",
        "filled",
        "partially_filled",
        "broker_acknowledged",
        "submitted",
        "previewed",
        "no_rebalance",
        "no_signal",
        "no_submission",
    )
    chosen = next((name for name in priority if name in activities), activities[0])
    signal = _SIGNAL_BY_ACTIVITY.get(chosen, chosen)
    kind = _REBALANCE_BY_ACTIVITY.get(chosen, "")
    conclusion = ""
    if kind == "no_rebalance":
        conclusion = "no_rebalance"
    elif kind == "no_order":
        conclusion = "no_order"
    elif kind == "rebalance":
        conclusion = chosen
    elif kind == "pending":
        conclusion = chosen
    return signal, kind, conclusion


def _equity_from_account_facts(facts: Mapping[str, Any] | None) -> tuple[float | None, str, str]:
    """Return (equity, currency, reason_if_missing)."""
    if facts is None:
        return None, "USD", "account_facts_absent"
    if facts.get("status") == "skipped":
        return None, "USD", str(facts.get("reason") or "account_facts_skipped")
    balances = facts.get("broker_reported_balances")
    if not isinstance(balances, list) or not balances:
        # Some fixtures may expose net_assets at top level — only accept if
        # schema marks snapshot/history; otherwise unknown.
        direct = facts.get("net_assets")
        equity = _money_to_float(direct)
        if equity is not None and facts.get("net_assets_currency") == "USD":
            return equity, "USD", ""
        return None, "USD", "account_facts_equity_absent"
    for item in balances:
        if not isinstance(item, Mapping):
            continue
        if item.get("currency") != "USD":
            continue
        if item.get("currency_source") not in {None, "owner_confirmed"}:
            continue
        equity = _money_to_float(item.get("net_assets"))
        if equity is not None:
            return equity, "USD", ""
    return None, "USD", "account_facts_equity_absent"


def project_digest_candidates(
    *,
    daily_projection: Mapping[str, Any],
    opaque_account_uid: str = "",
    target_id: str = "",
    account_facts: Mapping[str, Any] | None = None,
    business_day: str | None = None,
) -> dict[str, Any]:
    """Build ``{"schema_version", "runs": [...]}`` from a daily projection.

    Schwab daily ``fills`` are not connected today: counts stay null +
    ``counts_unknown``. Rows are emitted only when at least one covering run
    actually ran (``actually_ran=true``).
    """
    if not isinstance(daily_projection, Mapping):
        return {
            "schema_version": SCHEMA_VERSION,
            "runs": [],
            "producer_status": "skipped",
            "producer_reason": "daily_projection_invalid",
        }

    records = daily_projection.get("records")
    if not isinstance(records, list):
        return {
            "schema_version": SCHEMA_VERSION,
            "runs": [],
            "producer_status": "skipped",
            "producer_reason": "daily_records_missing",
        }

    uid = _as_str(opaque_account_uid)
    tid = _as_str(target_id)
    equity, equity_currency, equity_reason = _equity_from_account_facts(account_facts)

    runs_out: list[dict[str, Any]] = []
    for record in records:
        mapping = _as_mapping(record)
        if mapping is None:
            continue
        if mapping.get("platform") not in {None, PLATFORM_ID, "charles_schwab"}:
            continue
        day = _as_str(mapping.get("business_date"))
        if business_day and day and day != business_day:
            continue
        target = _as_mapping(mapping.get("target")) or {}
        strategy = _as_str(target.get("strategy_profile")) or _as_str(
            mapping.get("strategy_profile")
        ) or DEFAULT_STRATEGY
        raw_runs = mapping.get("runs")
        run_list = raw_runs if isinstance(raw_runs, list) else []
        activities = _activities(
            [item for item in run_list if isinstance(item, Mapping)]
        )
        if not activities:
            continue

        fills = _as_mapping(mapping.get("fills")) or {}
        fills_source = _as_str(fills.get("source")) or "not_connected"
        fill_count_raw = fills.get("count")
        # Connected + explicit non-negative int → known; otherwise unknown.
        counts: dict[str, Any]
        if (
            fills_source not in {"", "not_connected"}
            and type(fill_count_raw) is int
            and fill_count_raw >= 0
        ):
            counts = {
                "fill_count": fill_count_raw,
                "order_count": None,
                "field_status": {
                    "fill_count": "known",
                    "order_count": "counts_unknown",
                },
                "reason_code": "schwab_fills_orders_unverified",
            }
        else:
            counts = _unknown_counts(reason_code="schwab_fills_not_connected")

        signal, rebalance_kind, rebalance_conclusion = _pick_signal_and_rebalance(
            activities
        )
        status = "alert" if any(item in _ALERT_ACTIVITIES for item in activities) else "ok"
        if _as_str(mapping.get("status")) in {
            "failed",
            "blocked",
            "reconciliation_required",
            "unknown",
            "conflict",
            "read_incomplete",
        }:
            status = "alert"

        identity_reasons: list[str] = []
        if not uid:
            identity_reasons.append("opaque_account_uid_absent")
        if not tid:
            identity_reasons.append("target_id_absent")
        reason_parts = [counts["reason_code"], *identity_reasons]
        if equity is None and equity_reason:
            reason_parts.append(equity_reason)

        cycle_count = len(activities)
        field_status = dict(counts["field_status"])
        field_status["cycle_count"] = "known"

        row: dict[str, Any] = {
            "platform_id": PLATFORM_ID,
            "strategy_profile": strategy,
            "opaque_account_uid": uid,
            "target_id": tid,
            "actually_ran": True,
            "fill_count": counts["fill_count"],
            "order_count": counts["order_count"],
            "cycle_count": cycle_count,
            "field_status": field_status,
            "evidence_provenance": EVIDENCE_PROVENANCE,
            "reason_code": "+".join(reason_parts),
            "status": status,
            "business_day": day or business_day or "",
        }
        if signal:
            row["signal_summary"] = signal
        if rebalance_kind:
            row["rebalance_kind"] = rebalance_kind
        if rebalance_conclusion:
            row["rebalance_conclusion"] = rebalance_conclusion
        if equity is not None:
            row["equity"] = equity
            row["equity_currency"] = equity_currency
            row["currency"] = equity_currency
        # Holdings are not present on the privacy-safe daily projection or the
        # account-facts history body used today — omit rather than invent.
        target_key = _as_str(mapping.get("target_key"))
        if target_key:
            row["note"] = f"target_key={target_key}"
        runs_out.append(row)

    # Equity must not require covering runs / fills. When daily is empty but
    # account-facts already carries owner-confirmed net_assets, emit one row.
    if not runs_out and equity is not None:
        reason_parts = ["schwab_fills_not_connected", "no_covering_runs"]
        if not uid:
            reason_parts.append("opaque_account_uid_absent")
        if not tid:
            reason_parts.append("target_id_absent")
        row = {
            "platform_id": PLATFORM_ID,
            "strategy_profile": DEFAULT_STRATEGY,
            "opaque_account_uid": uid,
            "target_id": tid,
            "actually_ran": False,
            "fill_count": None,
            "order_count": None,
            "cycle_count": 0,
            "field_status": {
                "fill_count": "counts_unknown",
                "order_count": "counts_unknown",
                "cycle_count": "known",
            },
            "evidence_provenance": EVIDENCE_PROVENANCE,
            "reason_code": "+".join(reason_parts),
            "status": "ok",
            "business_day": business_day or "",
            "equity": equity,
            "equity_currency": equity_currency,
            "currency": equity_currency,
        }
        runs_out.append(row)
        return {
            "schema_version": SCHEMA_VERSION,
            "runs": runs_out,
            "producer_status": "projected",
            "producer_reason": "equity_without_covering_runs",
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "runs": runs_out,
        "producer_status": "projected" if runs_out else "empty",
        "producer_reason": "" if runs_out else "no_covering_runs",
    }


def load_json_object(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("json_root_must_be_object")
    return raw


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Project Schwab daily evidence to QRS DIGEST_CANDIDATES JSON."
    )
    parser.add_argument(
        "--daily-projection",
        type=Path,
        required=True,
        help="Path to project_daily_runtime / prepared daily projection JSON.",
    )
    parser.add_argument(
        "--account-facts",
        type=Path,
        default=None,
        help="Optional account-facts history/snapshot JSON for equity only.",
    )
    parser.add_argument(
        "--opaque-account-uid",
        default="",
        help="Opaque account identity from protected config (not a raw account number).",
    )
    parser.add_argument(
        "--target-id",
        default="",
        help="Console/target identity from protected config (e.g. schwab/...).",
    )
    parser.add_argument(
        "--business-day",
        default=None,
        help="Optional YYYY-MM-DD filter matching record.business_date.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Where to write candidates JSON (keep ephemeral / private).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        daily = load_json_object(args.daily_projection)
        facts = load_json_object(args.account_facts) if args.account_facts else None
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"status": "skipped", "reason": "input_unreadable", "detail": type(exc).__name__}))
        return 2
    payload = project_digest_candidates(
        daily_projection=daily,
        opaque_account_uid=args.opaque_account_uid,
        target_id=args.target_id,
        account_facts=facts,
        business_day=args.business_day,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    # Safe summary only: no equity, no uid, no target_id values.
    print(
        json.dumps(
            {
                "status": payload.get("producer_status"),
                "reason": payload.get("producer_reason") or "ok",
                "runs": len(payload.get("runs") or []),
                "output_written": True,
            },
            sort_keys=True,
        )
    )
    return 0 if payload.get("producer_status") in {"projected", "empty"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
