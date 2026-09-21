"""Schwab daily-loss fact producer: baseline + verified external cash flow.

Produces an explicit ``daily_loss_usd`` for the existing NEW_RISK pass-through seam.
Never invents facts: missing baseline, unverified flow, or IO failure → omit.
"""

from __future__ import annotations

import math
import os
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

EXTERNAL_CASH_FLOW_TYPES = frozenset(
    {
        "CASH_RECEIPT",
        "CASH_DISBURSEMENT",
        "WIRE_IN",
        "WIRE_OUT",
        "ACH_RECEIPT",
        "ACH_DISBURSEMENT",
        "ELECTRONIC_FUND",
    }
)

DAILY_LOSS_FACT_ENABLED_ENV = "SCHWAB_DAILY_LOSS_FACT_ENABLED"
_NY = ZoneInfo("America/New_York")
_OFFSET_NO_COLON = re.compile(r"([+-]\d{2})(\d{2})$")


@dataclass(frozen=True)
class SessionBaseline:
    equity_usd: float
    as_of: datetime
    source: str = "runtime_report"


@dataclass(frozen=True)
class CashFlowSummary:
    status: str  # verified | unverified | unavailable
    net_flow: float | None
    event_count: int = 0
    reason: str | None = None


@dataclass(frozen=True)
class DailyLossFact:
    daily_loss_usd: float
    status: str
    baseline_equity_usd: float
    baseline_as_of: datetime
    verified_net_external_flow: float
    current_equity_usd: float


def is_daily_loss_fact_enabled() -> bool:
    """Default on; set SCHWAB_DAILY_LOSS_FACT_ENABLED=0 to disable."""
    return str(os.environ.get(DAILY_LOSS_FACT_ENABLED_ENV, "") or "").strip() != "0"


def _coerce_finite_float(value: object) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    return number


def parse_broker_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=_NY)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    text = _OFFSET_NO_COLON.sub(r"\1:\2", text)
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=_NY)


def _transfer_asset_types(tx: Mapping[str, Any]) -> list[str]:
    types: list[str] = []
    for item in tx.get("transferItems") or []:
        if not isinstance(item, Mapping):
            continue
        instrument = item.get("instrument")
        if not isinstance(instrument, Mapping):
            continue
        asset = instrument.get("assetType")
        if asset is not None and str(asset).strip():
            types.append(str(asset).strip().upper())
    return types


def _is_pure_currency(tx: Mapping[str, Any]) -> bool:
    assets = _transfer_asset_types(tx)
    if not assets:
        # No legs: treat as not verified cash (fail closed for whitelist inclusion).
        return False
    return all(asset == "CURRENCY" for asset in assets)


def summarize_verified_external_cash_flow(
    transactions: Sequence[Mapping[str, Any]] | None,
    *,
    window_start: datetime,
    window_end: datetime,
) -> CashFlowSummary:
    if transactions is None:
        return CashFlowSummary(status="unavailable", net_flow=None)
    if window_end < window_start:
        return CashFlowSummary(status="unavailable", net_flow=None)

    net = 0.0
    count = 0
    for raw in transactions:
        if not isinstance(raw, Mapping):
            continue
        tx_type = str(raw.get("type") or raw.get("transactionType") or "").strip().upper()
        if tx_type not in EXTERNAL_CASH_FLOW_TYPES:
            continue
        when = parse_broker_datetime(raw.get("time") or raw.get("transactionDate"))
        if when is None:
            # Identified external capital movement whose session membership cannot be
            # proven — never skip into verified net_flow=0.
            return CashFlowSummary(
                status="unverified",
                net_flow=None,
                event_count=count,
                reason="missing_or_invalid_time",
            )
        if when <= window_start or when > window_end:
            continue
        status = str(raw.get("status") or "").strip().upper()
        if status != "VALID":
            return CashFlowSummary(
                status="unverified",
                net_flow=None,
                event_count=count,
                reason="non_valid_status",
            )
        if not _is_pure_currency(raw):
            continue
        amount = _coerce_finite_float(raw.get("netAmount"))
        if amount is None:
            return CashFlowSummary(
                status="unverified",
                net_flow=None,
                event_count=count,
                reason="missing_or_invalid_amount",
            )
        net += amount
        count += 1
    return CashFlowSummary(status="verified", net_flow=net, event_count=count)


def derive_daily_loss_usd(
    baseline_equity_usd: float | None,
    verified_net_external_flow: float | None,
    current_equity_usd: float | None,
) -> float | None:
    baseline = _coerce_finite_float(baseline_equity_usd)
    flow = _coerce_finite_float(verified_net_external_flow)
    current = _coerce_finite_float(current_equity_usd)
    if baseline is None or flow is None or current is None:
        return None
    if baseline <= 0.0 or current < 0.0:
        return None
    return max(0.0, baseline + flow - current)


def select_session_baseline(
    reports: Iterable[Mapping[str, Any]] | None,
    *,
    session_open: datetime,
    prior_session_close: datetime,
) -> SessionBaseline | None:
    if reports is None:
        return None
    primary: list[SessionBaseline] = []
    fallback: list[SessionBaseline] = []
    fallback_floor = session_open - timedelta(hours=36)
    prior_session_day_start = prior_session_close.astimezone(_NY).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    for report in reports:
        if not isinstance(report, Mapping):
            continue
        status = str(report.get("status") or "").strip().lower()
        if status not in {"ok", "success"}:
            continue
        summary = report.get("summary")
        if not isinstance(summary, Mapping):
            continue
        equity = _coerce_finite_float(summary.get("total_equity"))
        if equity is None or equity <= 0.0:
            continue
        finished = parse_broker_datetime(report.get("finished_at") or report.get("started_at"))
        if finished is None:
            continue
        if prior_session_close <= finished < session_open:
            primary.append(SessionBaseline(equity_usd=equity, as_of=finished))
        elif prior_session_day_start <= finished < session_open:
            # A weekend or exchange holiday can make the previous session's
            # final report older than the short fallback window. Keep the
            # fallback bounded to the immediately preceding session day.
            fallback.append(
                SessionBaseline(
                    equity_usd=equity,
                    as_of=finished,
                    source="runtime_report_prior_session",
                )
            )
        elif fallback_floor <= finished < session_open:
            fallback.append(SessionBaseline(equity_usd=equity, as_of=finished, source="runtime_report_fallback"))
    pool = primary or fallback
    if not pool:
        return None
    return max(pool, key=lambda item: item.as_of)


def resolve_nasdaq_session_bounds(
    reference_now: datetime,
    *,
    calendar_name: str = "NASDAQ",
) -> tuple[datetime, datetime] | None:
    """Return (prior_session_close, session_open) for the session date of reference_now."""
    import pandas_market_calendars as mcal

    now_ny = reference_now.astimezone(_NY)
    calendar = mcal.get_calendar(calendar_name)
    # Look back enough to cover long weekends / holidays.
    start = (now_ny.date() - timedelta(days=14)).isoformat()
    end = now_ny.date().isoformat()
    schedule = calendar.schedule(start_date=start, end_date=end)
    if schedule is None or getattr(schedule, "empty", True):
        return None
    # schedule index are session dates; market open/close columns are UTC timestamps.
    opens = list(schedule["market_open"])
    closes = list(schedule["market_close"])
    if not opens:
        return None
    session_open = opens[-1].to_pydatetime().astimezone(_NY)
    if len(opens) < 2:
        # No prior session in window.
        return None
    prior_close = closes[-2].to_pydatetime().astimezone(_NY)
    # If reference is before today's open and today is a session, still use today's open
    # with prior close — matches pre-open probe baseline selection.
    return prior_close, session_open


@dataclass(frozen=True)
class DailyLossFactAttempt:
    """Outcome of one produce attempt; fact is set only when status=verified."""

    status: str
    reason: str
    fact: DailyLossFact | None = None


def produce_daily_loss_fact(
    *,
    current_equity_usd: float | None,
    reference_now: datetime,
    session_open: datetime | None = None,
    prior_session_close: datetime | None = None,
    reports_loader: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
    transactions_loader: Callable[..., Sequence[Mapping[str, Any]]] | None = None,
) -> DailyLossFact | None:
    """Compose baseline + flow + current equity into a verified daily_loss fact."""
    attempt = produce_daily_loss_fact_attempt(
        current_equity_usd=current_equity_usd,
        reference_now=reference_now,
        session_open=session_open,
        prior_session_close=prior_session_close,
        reports_loader=reports_loader,
        transactions_loader=transactions_loader,
    )
    return attempt.fact


def produce_daily_loss_fact_attempt(
    *,
    current_equity_usd: float | None,
    reference_now: datetime,
    session_open: datetime | None = None,
    prior_session_close: datetime | None = None,
    reports_loader: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
    transactions_loader: Callable[..., Sequence[Mapping[str, Any]]] | None = None,
) -> DailyLossFactAttempt:
    """Same as produce_daily_loss_fact, but always returns a status/reason for logs."""
    if _coerce_finite_float(current_equity_usd) is None:
        return DailyLossFactAttempt(status="omitted", reason="missing_current_equity")

    if session_open is None or prior_session_close is None:
        bounds = resolve_nasdaq_session_bounds(reference_now)
        if bounds is None:
            return DailyLossFactAttempt(status="omitted", reason="session_bounds_unavailable")
        prior_session_close, session_open = bounds

    if reports_loader is None or transactions_loader is None:
        return DailyLossFactAttempt(status="omitted", reason="loaders_unavailable")

    try:
        reports = reports_loader()
    except Exception:
        return DailyLossFactAttempt(status="omitted", reason="reports_load_failed")

    baseline = select_session_baseline(
        reports,
        session_open=session_open,
        prior_session_close=prior_session_close,
    )
    if baseline is None:
        return DailyLossFactAttempt(status="omitted", reason="baseline_unavailable")

    try:
        transactions = transactions_loader(start=baseline.as_of, end=reference_now)
    except Exception:
        return DailyLossFactAttempt(status="omitted", reason="transactions_load_failed")

    flow = summarize_verified_external_cash_flow(
        transactions,
        window_start=baseline.as_of,
        window_end=reference_now,
    )
    if flow.status == "unverified":
        reason = flow.reason or "flow_unverified"
        if reason != "flow_unverified" and not reason.startswith("flow_"):
            reason = f"flow_unverified:{reason}"
        return DailyLossFactAttempt(status="omitted", reason=reason)
    if flow.status != "verified" or flow.net_flow is None:
        return DailyLossFactAttempt(status="omitted", reason="flow_unavailable")

    loss = derive_daily_loss_usd(baseline.equity_usd, flow.net_flow, current_equity_usd)
    current = _coerce_finite_float(current_equity_usd)
    if loss is None or current is None:
        return DailyLossFactAttempt(status="omitted", reason="derive_failed")

    fact = DailyLossFact(
        daily_loss_usd=loss,
        status="verified",
        baseline_equity_usd=baseline.equity_usd,
        baseline_as_of=baseline.as_of,
        verified_net_external_flow=flow.net_flow,
        current_equity_usd=current,
    )
    return DailyLossFactAttempt(status="verified", reason="verified", fact=fact)


def _strategy_profile_from_portfolio(portfolio: Mapping[str, Any]) -> str:
    for source in (
        portfolio.get("account_new_risk_snapshot"),
        portfolio,
        portfolio.get("metadata"),
    ):
        if isinstance(source, Mapping):
            value = source.get("strategy_profile")
            if isinstance(value, str) and value.strip():
                return value.strip()
    return str(os.environ.get("STRATEGY_PROFILE") or "").strip() or "soxl_soxx_trend_income"


def load_recent_runtime_reports_from_gcs(
    *,
    gcs_uri: str | None = None,
    strategy_profile: str | None = None,
    limit: int = 40,
) -> list[dict[str, Any]]:
    """Load newest runtime report JSON objects for baseline selection."""
    import json
    from urllib.parse import urlparse

    from google.cloud import storage

    uri = (gcs_uri or os.environ.get("EXECUTION_REPORT_GCS_URI") or "").strip()
    if not uri:
        return []
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc:
        return []
    profile = strategy_profile or str(os.environ.get("STRATEGY_PROFILE") or "").strip() or "soxl_soxx_trend_income"
    prefix_root = parsed.path.lstrip("/")
    # URI may be gs://bucket/execution-reports or gs://bucket/execution-reports/
    base = f"{prefix_root.rstrip('/')}/charles_schwab/{profile}/"
    client = storage.Client()
    bucket = client.bucket(parsed.netloc)
    blobs = sorted(
        bucket.list_blobs(prefix=base),
        key=lambda blob: blob.updated or blob.name,
        reverse=True,
    )
    reports: list[dict[str, Any]] = []
    for blob in blobs[: max(1, limit)]:
        if not str(blob.name).endswith(".json"):
            continue
        try:
            payload = json.loads(blob.download_as_text())
        except Exception:
            continue
        if isinstance(payload, dict):
            reports.append(payload)
    return reports


def fetch_schwab_transactions(
    client: Any,
    *,
    start: datetime,
    end: datetime,
    expected_account_hash: str | None = None,
) -> list[Mapping[str, Any]]:
    """Load transactions for the bound account identity (never invent another).

    Reuses the same expected-hash selection rule as QPK schwab portfolio reads:
    single account may omit expected; multiple accounts require an explicit hash.
    """
    get_account_numbers = getattr(client, "get_account_numbers", None)
    get_transactions = getattr(client, "get_transactions", None)
    if not callable(get_account_numbers) or not callable(get_transactions):
        raise RuntimeError("schwab client missing transaction surfaces")
    numbers = get_account_numbers().json()
    if not isinstance(numbers, list) or not numbers:
        raise RuntimeError("no schwab account numbers")
    account_hashes: list[str] = []
    for row in numbers:
        if not isinstance(row, Mapping):
            continue
        value = row.get("hashValue") or row.get("accountHash")
        if value is not None and str(value).strip():
            account_hashes.append(str(value).strip())
    if not account_hashes:
        raise RuntimeError("missing schwab account hash")

    expected = str(expected_account_hash or "").strip() or None
    if expected is None:
        if len(account_hashes) != 1:
            raise RuntimeError("schwab transactions require explicit account hash for multiple accounts")
        account_hash = account_hashes[0]
    else:
        # Casefold match mirrors strategy_runtime account_hash binding.
        matched = [item for item in account_hashes if item.casefold() == expected.casefold()]
        if not matched:
            raise RuntimeError("expected schwab account hash unavailable for transactions")
        account_hash = matched[0]

    response = get_transactions(account_hash, start_date=start, end_date=end)
    status = getattr(response, "status_code", None)
    payload = response.json()
    if status is not None and int(status) >= 400:
        raise RuntimeError(f"get_transactions status={status}")
    if not isinstance(payload, list):
        raise RuntimeError("get_transactions payload is not a list")
    return [item for item in payload if isinstance(item, Mapping)]


def _expected_account_hash_from_portfolio(portfolio: Mapping[str, Any]) -> str | None:
    for source in (
        portfolio,
        portfolio.get("account_new_risk_snapshot"),
        portfolio.get("metadata"),
    ):
        if not isinstance(source, Mapping):
            continue
        for key in ("account_hash", "accountHash", "hashValue"):
            value = source.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
    env_hash = str(os.environ.get("SCHWAB_ACCOUNT_HASH") or "").strip()
    return env_hash or None


def attach_daily_loss_fact_to_portfolio(
    portfolio: Mapping[str, Any],
    *,
    client: Any | None = None,
    reference_now: datetime | None = None,
    reports_loader: Callable[[], Sequence[Mapping[str, Any]]] | None = None,
    transactions_loader: Callable[..., Sequence[Mapping[str, Any]]] | None = None,
    expected_account_hash: str | None = None,
) -> dict[str, Any]:
    """Inject verified daily_loss_usd when producible; never invent; never raise."""
    out = dict(portfolio or {})
    projection = dict(out.get("account_new_risk_snapshot") or {})
    if "daily_loss_usd" in projection or "daily_loss_usd" in out:
        print("[Daily loss fact] status=skipped reason=explicit_present", flush=True)
        return out
    if not is_daily_loss_fact_enabled():
        print("[Daily loss fact] status=omitted reason=disabled", flush=True)
        return out

    now = reference_now or datetime.now(tz=_NY)
    current = _coerce_finite_float(out.get("total_equity") or out.get("total_strategy_equity"))
    if current is None and isinstance(out.get("metadata"), Mapping):
        current = _coerce_finite_float(out.get("total_equity"))

    profile = _strategy_profile_from_portfolio(out)
    account_hash = str(expected_account_hash or "").strip() or _expected_account_hash_from_portfolio(out)

    def _default_reports_loader() -> Sequence[Mapping[str, Any]]:
        return load_recent_runtime_reports_from_gcs(strategy_profile=profile)

    def _default_transactions_loader(*, start: datetime, end: datetime) -> Sequence[Mapping[str, Any]]:
        if client is None:
            raise RuntimeError("no schwab client")
        return fetch_schwab_transactions(
            client,
            start=start,
            end=end,
            expected_account_hash=account_hash,
        )

    try:
        attempt = produce_daily_loss_fact_attempt(
            current_equity_usd=current,
            reference_now=now,
            reports_loader=reports_loader or _default_reports_loader,
            transactions_loader=transactions_loader or _default_transactions_loader,
        )
    except Exception as exc:
        print(
            f"[Daily loss fact] status=omitted reason=unexpected_error "
            f"error_type={type(exc).__name__}",
            flush=True,
        )
        return out

    if attempt.fact is None:
        print(f"[Daily loss fact] status=omitted reason={attempt.reason}", flush=True)
        projection["daily_loss_fact_status"] = "omitted"
        projection["daily_loss_fact_reason"] = attempt.reason
        out["account_new_risk_snapshot"] = projection
        return out

    fact = attempt.fact
    projection["daily_loss_usd"] = fact.daily_loss_usd
    projection["daily_loss_fact_status"] = fact.status
    projection["daily_loss_fact_reason"] = attempt.reason
    projection["daily_loss_baseline_equity_usd"] = fact.baseline_equity_usd
    projection["daily_loss_baseline_as_of"] = fact.baseline_as_of.isoformat()
    projection["daily_loss_verified_net_external_flow"] = fact.verified_net_external_flow
    out["account_new_risk_snapshot"] = projection
    out["daily_loss_usd"] = fact.daily_loss_usd
    print(
        "[Daily loss fact] "
        f"status=verified loss={fact.daily_loss_usd} "
        f"baseline={fact.baseline_equity_usd} "
        f"flow={fact.verified_net_external_flow} "
        f"current={fact.current_equity_usd}",
        flush=True,
    )
    return out
