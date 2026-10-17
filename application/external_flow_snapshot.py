"""Opt-in, read-only cash-flow snapshot for reports."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import re
from typing import Any


_IN = frozenset({"ACH_RECEIPT", "CASH_RECEIPT", "WIRE_IN"})
_OUT = frozenset({"ACH_DISBURSEMENT", "CASH_DISBURSEMENT", "WIRE_OUT"})
_INTERNAL = frozenset({"TRADE", "DIVIDEND_OR_INTEREST", "MONEY_MARKET"})
_MONEY = re.compile(r"^-?(?:0|[1-9]\d{0,11})(?:\.\d{1,8})?$")


def external_flow_enabled(config: Mapping[str, str]) -> bool:
    return config.get("SCHWAB_EXTERNAL_FLOW_EXPORT_ENABLED") == "true"


def _instant(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("external_flow_time_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("external_flow_time_invalid") from None
    if parsed.tzinfo is None:
        raise ValueError("external_flow_timezone_required")
    return parsed.astimezone(timezone.utc)


def _utc(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def project_external_flow_snapshot(
    transactions: Sequence[Mapping[str, Any]] | None,
    *,
    expected_account_hash: str,
    observed_account_hash: str,
    period_start: str,
    period_end: str,
    currency: str | None,
    source_complete: bool = False,
    enabled: bool = False,
) -> dict[str, Any] | None:
    """Project (start, end] already-read transactions; source completeness is explicit.

    Currency must be owner-confirmed exactly as in the existing USD account-facts
    contract. Unknown journals, non-cash transfers, reversals, missing timestamps,
    duplicate conflicts or unverified pagination prevent complete coverage.
    """
    if enabled is not True:
        return None
    if not expected_account_hash or observed_account_hash != expected_account_hash:
        raise ValueError("external_flow_account_mismatch")
    start, end = _instant(period_start), _instant(period_end)
    if end <= start or end - start > timedelta(days=2) or currency != "USD":
        raise ValueError("external_flow_window_or_currency_invalid")
    complete = source_complete is True and isinstance(transactions, (list, tuple))
    records: dict[str, dict[str, Any]] = {}
    for tx in transactions or []:
        if not isinstance(tx, Mapping):
            complete = False
            continue
        kind = tx.get("type")
        if kind in _INTERNAL:
            continue
        if kind not in _IN | _OUT:
            # Includes JOURNAL/ELECTRONIC_FUND/RECEIVE_AND_DELIVER. A cash-looking
            # label alone cannot prove an external movement or an in-kind value.
            complete = False
            continue
        try:
            when = _instant(tx.get("time"))
        except ValueError:
            complete = False
            continue
        if not start < when <= end:
            continue
        items = tx.get("transferItems")
        if (tx.get("status") != "VALID" or not isinstance(items, list) or not items
                or any(tx.get(field) not in (None, 0, "0", []) for field in ("fees", "commission", "transactionFee"))
                or any(not isinstance(i, Mapping) or not isinstance(i.get("instrument"), Mapping)
                       or i["instrument"].get("assetType") != "CURRENCY" or i.get("feeType") is not None for i in items)):
            complete = False
            continue
        native_id = tx.get("activityId")
        amount = tx.get("netAmount")
        try:
            if isinstance(amount, bool) or amount is None:
                raise ValueError
            value = Decimal(str(amount))
            text = format(value, "f")
            if (not value.is_finite() or not _MONEY.fullmatch(text) or value == 0
                    or (kind in _IN) != (value > 0) or isinstance(native_id, bool)
                    or not isinstance(native_id, (str, int)) or not str(native_id)):
                raise ValueError
        except (ValueError, InvalidOperation):
            complete = False
            continue
        settlement = tx.get("settlementDate")
        if settlement is not None:
            try:
                # Preserve the broker date without assigning it a timezone/time.
                if date.fromisoformat(settlement).isoformat() != settlement:
                    raise ValueError
            except (ValueError, TypeError):
                complete = False
                continue
        ref = hashlib.sha256(f"schwab_transactions\0{expected_account_hash}\0{native_id}".encode()).hexdigest()
        record = {"source_ref": ref, "type": "deposit" if kind in _IN else "withdrawal",
                  "amount": text, "effective_at": _utc(when), "settlement_date": settlement, "transfer_ref": None}
        if ref in records and records[ref] != record:
            raise ValueError("external_flow_conflict")
        records[ref] = record
    if len(records) > 256:
        raise ValueError("external_flow_size_limit")
    return {"schema_version": "external_flow.v1", "windows": [{
        "currency": currency, "date": end.date().isoformat(), "period_start": _utc(start), "period_end": _utc(end),
        "source": "schwab_transactions", "coverage": "complete" if complete else "incomplete",
        "records": sorted(records.values(), key=lambda r: r["source_ref"]),
    }]}


def attach_external_flow_snapshot(
    history: Mapping[str, Any], snapshot: Mapping[str, Any] | None, *, enabled: bool = False,
) -> dict[str, Any]:
    """Add an optional already-observed source to the existing trusted upload."""
    result = dict(history)
    if enabled is not True or snapshot is None:
        return result
    end = _instant(snapshot.get("period_end"))
    if end > _instant(history.get("observed_finished_at")):
        raise ValueError("external_flow_observation_mismatch")
    result["externalFlow"] = project_external_flow_snapshot(
        snapshot.get("transactions"), expected_account_hash=history.get("account_hash"),
        observed_account_hash=snapshot.get("account_hash"), period_start=snapshot.get("period_start"),
        period_end=snapshot.get("period_end"), currency=snapshot.get("currency"),
        source_complete=snapshot.get("source_complete") is True, enabled=True,
    )
    return result


def collect_external_flow_snapshot(
    client: Any, *, account_hash: str, period_start: str, period_end: str,
    currency: str | None, enabled: bool = False, source_complete: bool = False,
) -> dict[str, Any] | None:
    """Optional snapshot seam for an already-authenticated, account-bound reader.

    This does not construct clients or access credentials. Completeness must be
    established by the caller's approved source/backfill policy; HTTP 200 alone
    cannot prove it. The report publisher projects this raw envelope once.
    """
    if enabled is not True:
        return None
    start, end = _instant(period_start), _instant(period_end)
    if not account_hash or end <= start or end - start > timedelta(days=2):
        raise ValueError("external_flow_window_or_account_invalid")
    if currency != "USD":
        raise ValueError("external_flow_window_or_currency_invalid")
    response = client.get_transactions(account_hash, start_date=start, end_date=end)
    if response.status_code != 200:
        raise ValueError("external_flow_source_unavailable")
    transactions = response.json()
    if not isinstance(transactions, list):
        raise ValueError("external_flow_source_invalid")
    return {
        "account_hash": account_hash, "period_start": _utc(start), "period_end": _utc(end),
        "currency": currency, "source_complete": source_complete is True, "transactions": transactions,
    }
