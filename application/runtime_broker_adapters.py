"""Builder helpers for Schwab broker-side runtime adapters."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from quant_platform_kit.common.models import PricePoint, PriceSeries, QuoteSnapshot
from quant_platform_kit.common.port_adapters import (
    CallableExecutionPort,
    CallableMarketDataPort,
    CallablePortfolioPort,
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


_NEW_YORK_TZ = ZoneInfo("America/New_York")
_QUOTE_RATE_LIMIT_MAX_ATTEMPTS = 3
_QUOTE_RATE_LIMIT_BACKOFF_SECONDS = (0.5, 1.5)


def _market_date(value: datetime) -> date:
    normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return normalized.astimezone(_NEW_YORK_TZ).date()


def _is_quote_rate_limit_error(exc: Exception) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code == 429:
        return True
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) == 429:
        return True
    return "429" in str(exc)


@dataclass(frozen=True)
class SchwabRuntimeBrokerAdapters:
    managed_symbols: tuple[str, ...]
    fetch_account_snapshot_fn: Any
    fetch_quotes_fn: Any
    fetch_daily_price_history_fn: Any
    submit_equity_order_fn: Any
    fetch_order_status_fn: Any | None = None
    clock: Any = _utcnow

    def fetch_managed_snapshot(self, client):
        return self.fetch_account_snapshot_fn(client, strategy_symbols=list(self.managed_symbols))

    def build_market_data_port(self, client):
        quote_cache: dict[str, QuoteSnapshot] = {}
        price_series_cache: dict[str, PriceSeries] = {}

        def normalize_quote_symbol(symbol: str) -> str:
            return str(symbol).strip().upper()

        def build_quote_snapshot(symbol: str, raw_snapshot) -> QuoteSnapshot:
            return QuoteSnapshot(
                symbol=symbol,
                as_of=self.clock(),
                last_price=float(raw_snapshot.last_price),
                ask_price=(
                    float(raw_snapshot.ask_price)
                    if getattr(raw_snapshot, "ask_price", None) is not None
                    else None
                ),
                bid_price=(
                    float(raw_snapshot.bid_price)
                    if getattr(raw_snapshot, "bid_price", None) is not None
                    else None
                ),
            )

        def quote_batch_symbols(requested_symbol: str) -> tuple[str, ...]:
            symbols = [normalize_quote_symbol(symbol) for symbol in self.managed_symbols]
            symbols.append(requested_symbol)
            return tuple(dict.fromkeys(symbol for symbol in symbols if symbol))

        def fetch_and_cache_quotes(symbols: tuple[str, ...]) -> None:
            missing = tuple(symbol for symbol in symbols if symbol not in quote_cache)
            if not missing:
                return
            last_error: Exception | None = None
            for attempt in range(_QUOTE_RATE_LIMIT_MAX_ATTEMPTS):
                try:
                    raw_quotes = self.fetch_quotes_fn(client, list(missing))
                    break
                except Exception as exc:
                    last_error = exc
                    if (
                        attempt >= _QUOTE_RATE_LIMIT_MAX_ATTEMPTS - 1
                        or not _is_quote_rate_limit_error(exc)
                    ):
                        raise
                    time.sleep(
                        _QUOTE_RATE_LIMIT_BACKOFF_SECONDS[
                            min(attempt, len(_QUOTE_RATE_LIMIT_BACKOFF_SECONDS) - 1)
                        ]
                    )
            else:  # pragma: no cover - loop always exits through break or raise
                raise last_error or RuntimeError("Schwab quote fetch failed")
            for symbol in missing:
                raw_snapshot = raw_quotes.get(symbol)
                if raw_snapshot is not None:
                    quote_cache[symbol] = build_quote_snapshot(symbol, raw_snapshot)

        def load_quote(symbol: str) -> QuoteSnapshot:
            normalized_symbol = str(symbol).strip().upper()
            cached = quote_cache.get(normalized_symbol)
            if cached is not None:
                return cached
            fetch_and_cache_quotes(quote_batch_symbols(normalized_symbol))
            return quote_cache[normalized_symbol]

        def load_price_series(symbol: str) -> PriceSeries:
            normalized_symbol = str(symbol).strip().upper()
            cached = price_series_cache.get(normalized_symbol)
            if cached is not None:
                return cached
            candles = self.fetch_daily_price_history_fn(client, normalized_symbol)
            if not candles:
                raise ValueError(f"Price history unavailable for {normalized_symbol}")
            fallback_start = self.clock() - timedelta(days=max(0, len(candles) - 1))
            points = []
            for index, candle in enumerate(candles):
                if candle.get("datetime") is None:
                    as_of = fallback_start + timedelta(days=index)
                else:
                    raw_datetime = int(candle["datetime"])
                    if abs(raw_datetime) > 10_000_000_000:
                        raw_datetime /= 1000
                    as_of = datetime.fromtimestamp(raw_datetime, tz=timezone.utc)
                points.append(
                    PricePoint(
                        as_of=as_of,
                        close=float(candle["close"]),
                    )
                )
            series = PriceSeries(
                symbol=normalized_symbol,
                currency="USD",
                points=tuple(points),
            )
            try:
                quote = load_quote(normalized_symbol)
            except Exception:
                quote = None
            if quote is not None and quote.last_price > 0:
                quote_point = PricePoint(
                    as_of=quote.as_of,
                    close=float(quote.last_price),
                )
                latest_point = series.points[-1]
                latest_market_date = _market_date(latest_point.as_of)
                quote_market_date = _market_date(quote_point.as_of)
                if quote_market_date > latest_market_date:
                    series = PriceSeries(
                        symbol=series.symbol,
                        currency=series.currency,
                        points=(*series.points, quote_point),
                    )
                elif quote_market_date == latest_market_date:
                    series = PriceSeries(
                        symbol=series.symbol,
                        currency=series.currency,
                        points=(*series.points[:-1], quote_point),
                    )
            price_series_cache[normalized_symbol] = series
            return series

        return CallableMarketDataPort(
            quote_loader=load_quote,
            price_series_loader=load_price_series,
        )

    def build_price_history(self, market_data_port, symbol: str):
        series = market_data_port.get_price_series(symbol)
        return [
            {
                "datetime": int(point.as_of.timestamp() * 1000),
                "close": float(point.close),
                "high": float(point.close),
                "low": float(point.close),
            }
            for point in series.points
        ]

    def build_market_history_loader(self, market_data_port):
        def load_market_history(_broker_client, symbol, *_args, **_kwargs):
            series = market_data_port.get_price_series(str(symbol).strip().upper())
            if not series.points:
                return pd.Series(dtype=float)
            closes = [float(point.close) for point in series.points]
            index = [pd.Timestamp(point.as_of) for point in series.points]
            return pd.Series(closes, index=pd.DatetimeIndex(index), dtype=float)

        return load_market_history

    def build_portfolio_port(self, client):
        return CallablePortfolioPort(lambda: self.fetch_managed_snapshot(client))

    def build_execution_port(self, client, account_hash: str):
        return CallableExecutionPort(
            lambda order_intent: self.submit_equity_order_fn(client, account_hash, order_intent)
        )

    def build_order_status_fetcher(self, client, account_hash: str):
        if self.fetch_order_status_fn is None:
            return None
        return lambda broker_order_id: self.fetch_order_status_fn(
            client,
            account_hash,
            broker_order_id,
        )


def build_runtime_broker_adapters(
    *,
    managed_symbols: tuple[str, ...],
    fetch_account_snapshot_fn,
    fetch_quotes_fn,
    fetch_daily_price_history_fn,
    submit_equity_order_fn,
    fetch_order_status_fn=None,
    clock=_utcnow,
) -> SchwabRuntimeBrokerAdapters:
    return SchwabRuntimeBrokerAdapters(
        managed_symbols=tuple(managed_symbols),
        fetch_account_snapshot_fn=fetch_account_snapshot_fn,
        fetch_quotes_fn=fetch_quotes_fn,
        fetch_daily_price_history_fn=fetch_daily_price_history_fn,
        submit_equity_order_fn=submit_equity_order_fn,
        fetch_order_status_fn=fetch_order_status_fn,
        clock=clock,
    )
