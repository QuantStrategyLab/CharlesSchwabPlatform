"""Per-run, non-sensitive signal data provenance (RS-08 fix 4).

Records, per symbol, which source supplied the signal history, how many rows it
had, its first/last session date and the price-basis label. Never records
prices, quantities, account values or positions. Recording is best-effort and
never raises, so it cannot change trading behaviour.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable
from datetime import date, datetime, timezone
from typing import Any

SCHEMA_VERSION = "qsl.signal_data_provenance.v1"
SPLIT_ONLY = "split_only"
DIVIDEND_ADJUSTED = "dividend_adjusted"
UNKNOWN = "unknown"
PRICE_BASIS_LABELS = frozenset({SPLIT_ONLY, DIVIDEND_ADJUSTED, UNKNOWN})

_LOCK = threading.Lock()
_RECORDS: dict[str, dict[str, Any]] = {}


def reset() -> None:
    with _LOCK:
        _RECORDS.clear()


def _iso_date(value: Any) -> str | None:
    try:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime().date().isoformat()
        if isinstance(value, (int, float)):
            raw = float(value)
            if abs(raw) > 10_000_000_000:
                raw /= 1000
            return datetime.fromtimestamp(raw, tz=timezone.utc).date().isoformat()
        text = str(value or "").strip()
        return text[:10] or None
    except Exception:
        return None


def record(symbol: Any, *, source: str, price_basis: str, dates: Iterable[Any], note: str | None = None) -> None:
    """Record one history fetch. Best-effort: swallows every error."""
    try:
        normalized = str(symbol or "").strip().upper()
        if not normalized:
            return
        materialized = list(dates or ())
        entry: dict[str, Any] = {
            "source": str(source),
            "price_basis": price_basis if price_basis in PRICE_BASIS_LABELS else UNKNOWN,
            "rows": len(materialized),
            "first_date": _iso_date(materialized[0]) if materialized else None,
            "last_date": _iso_date(materialized[-1]) if materialized else None,
        }
        if note:
            entry["note"] = str(note)
        with _LOCK:
            previous = _RECORDS.get(normalized)
            entry["fetch_count"] = int(previous.get("fetch_count", 0)) + 1 if previous else 1
            _RECORDS[normalized] = entry
    except Exception:
        return


def snapshot() -> dict[str, Any]:
    """Return the run's provenance, or an empty dict when nothing was recorded."""
    try:
        with _LOCK:
            if not _RECORDS:
                return {}
            return {
                "schema_version": SCHEMA_VERSION,
                "symbols": {symbol: dict(entry) for symbol, entry in sorted(_RECORDS.items())},
            }
    except Exception:
        return {}
