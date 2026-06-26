"""Order execution helpers for CharlesSchwabPlatform."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

try:
    from quant_platform_kit.common.cash_sweep import should_sell_cash_sweep_to_fund_whole_share_buy
except ImportError:  # pragma: no cover - compatibility with older pinned shared wheels
    def should_sell_cash_sweep_to_fund_whole_share_buy(
        max_quantity,
        cash_sweep_price,
        base_buying_power,
        funding_needs,
    ):
        if max_quantity <= 0:
            return False
        sweep_price = float(cash_sweep_price or 0.0)
        if sweep_price <= 0.0:
            return False
        current_buying_power = max(0.0, float(base_buying_power or 0.0))
        sweep_capacity = float(max_quantity) * sweep_price
        if sweep_capacity <= 0.0:
            return False

        for underweight_value, ask_price in funding_needs:
            _ = underweight_value
            quote_price = float(ask_price or 0.0)
            if quote_price <= 0.0:
                continue
            if current_buying_power >= quote_price:
                return False
            if current_buying_power + sweep_capacity >= quote_price:
                return True
        return False
try:
    from quant_platform_kit.common.small_account_compatibility import (
        apply_small_account_cash_compatibility,
        build_small_account_allocation_drift_notes,
        format_small_account_allocation_drift_notes,
        format_small_account_cash_substitution_notes,
    )
except ImportError:  # pragma: no cover - compatibility with older pinned shared wheels
    @dataclass(frozen=True)
    class _SmallAccountCashCompatibilityResult:
        targets: dict
        whole_share_substituted_symbols: tuple[str, ...]
        safe_haven_cash_substituted_symbols: tuple[str, ...]
        cash_substitution_notes: tuple[dict, ...]

    def _project_unbuyable_value_targets_to_cash(
        target_values,
        prices,
        *,
        candidate_symbols=None,
        quantity_step=1.0,
    ):
        adjusted = {
            str(symbol or "").strip().upper(): float(value or 0.0)
            for symbol, value in dict(target_values or {}).items()
        }
        step = max(0.0, float(quantity_step or 0.0))
        if step <= 0.0:
            return adjusted, ()
        normalized_candidates = (
            tuple(adjusted)
            if candidate_symbols is None
            else tuple(dict.fromkeys(str(symbol or "").strip().upper() for symbol in candidate_symbols))
        )
        normalized_prices = {
            str(symbol or "").strip().upper(): float(price or 0.0)
            for symbol, price in dict(prices or {}).items()
        }
        substituted = []
        for symbol in normalized_candidates:
            target_value = max(0.0, float(adjusted.get(symbol, 0.0) or 0.0))
            price = max(0.0, float(normalized_prices.get(symbol, 0.0) or 0.0))
            if price > 0.0 and 0.0 < target_value < (price * step):
                adjusted[symbol] = 0.0
                substituted.append(symbol)
        return adjusted, tuple(dict.fromkeys(substituted))

    def apply_small_account_cash_compatibility(
        target_values,
        prices,
        *,
        candidate_symbols=None,
        safe_haven_cash_symbols=(),
        quantity_step=1.0,
        cash_substitute_limit_usd=2000.0,
    ):
        adjusted_targets, substituted = _project_unbuyable_value_targets_to_cash(
            target_values,
            prices,
            candidate_symbols=candidate_symbols,
            quantity_step=quantity_step,
        )
        normalized_candidates = (
            tuple(adjusted_targets)
            if candidate_symbols is None
            else tuple(dict.fromkeys(str(symbol or "").strip().upper() for symbol in candidate_symbols))
        )
        remaining_non_safe_targets = [
            symbol
            for symbol in normalized_candidates
            if float(adjusted_targets.get(str(symbol or "").strip().upper(), 0.0) or 0.0) > 0.0
        ]
        safe_haven_symbols = tuple(
            dict.fromkeys(
                str(symbol or "").strip().upper()
                for symbol in safe_haven_cash_symbols
                if str(symbol or "").strip()
            )
        )
        safe_haven_substituted = []
        if (
            substituted
            and not remaining_non_safe_targets
            and _positive_target_total(adjusted_targets) <= max(0.0, float(cash_substitute_limit_usd or 0.0))
        ):
            for symbol in safe_haven_symbols:
                if float(adjusted_targets.get(symbol, 0.0) or 0.0) > 0.0:
                    adjusted_targets[symbol] = 0.0
                    safe_haven_substituted.append(symbol)
        normalized_targets = {
            str(symbol or "").strip().upper(): float(value or 0.0)
            for symbol, value in dict(target_values or {}).items()
        }
        normalized_prices = {
            str(symbol or "").strip().upper(): float(price or 0.0)
            for symbol, price in dict(prices or {}).items()
        }
        notes = []
        if safe_haven_substituted:
            for symbol in substituted:
                target_value = max(0.0, float(normalized_targets.get(symbol, 0.0) or 0.0))
                price = max(0.0, float(normalized_prices.get(symbol, 0.0) or 0.0))
                if target_value <= 0.0 or price <= 0.0:
                    continue
                notes.append(
                    {
                        "symbol": symbol,
                        "target_value": target_value,
                        "price": price,
                        "cash_symbols": tuple(safe_haven_substituted),
                    }
                )
        return _SmallAccountCashCompatibilityResult(
            targets=adjusted_targets,
            whole_share_substituted_symbols=substituted,
            safe_haven_cash_substituted_symbols=tuple(safe_haven_substituted),
            cash_substitution_notes=tuple(notes),
        )

    def format_small_account_cash_substitution_notes(
        notes,
        *,
        translator,
        wrapper_key="buy_deferred",
        detail_key="buy_deferred_small_account_cash_substitution",
        cash_label_key="cash_label",
        symbol_suffix=".US",
    ):
        messages = []
        seen_keys = set()
        for note in tuple(notes or ()):
            if not isinstance(note, Mapping):
                continue
            symbol = str(note.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            target_value = max(0.0, float(note.get("target_value") or 0.0))
            price = max(0.0, float(note.get("price") or 0.0))
            if target_value <= 0.0 or price <= 0.0:
                continue
            cash_symbols = tuple(
                dict.fromkeys(
                    str(cash_symbol or "").strip().upper()
                    for cash_symbol in tuple(note.get("cash_symbols") or ())
                    if str(cash_symbol or "").strip()
                )
            )
            cash_symbols_text = ", ".join(f"{cash_symbol}{symbol_suffix}" for cash_symbol in cash_symbols)
            if not cash_symbols_text:
                cash_symbols_text = translator(cash_label_key)
            note_key = (symbol, f"{target_value:.2f}", cash_symbols_text)
            if note_key in seen_keys:
                continue
            seen_keys.add(note_key)
            detail = translator(
                detail_key,
                symbol=f"{symbol}{symbol_suffix}",
                diff=f"{target_value:.2f}",
                price=f"{price:.2f}",
                cash_symbols=cash_symbols_text,
            )
            messages.append(translator(wrapper_key, detail=detail))
        return tuple(messages)

    def build_small_account_allocation_drift_notes(**_kwargs):
        return ()

    def format_small_account_allocation_drift_notes(_notes, *, translator, **_kwargs):
        return ()
from quant_platform_kit.common.models import OrderIntent
from quant_platform_kit.common.quantity import format_quantity


@dataclass(frozen=True)
class ExecutionCycleResult:
    plan: dict
    portfolio: dict
    execution: dict
    allocation: dict
    trade_logs: tuple[str, ...]


DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD = 1000.0
SMALL_ACCOUNT_SAFE_HAVEN_CASH_SUBSTITUTE_LIMIT_USD = 2000.0
SMALL_ACCOUNT_EXISTING_WHOLE_SHARE_RETENTION_SYMBOLS = frozenset({"TQQQ", "SOXL"})
SMALL_ACCOUNT_EXISTING_WHOLE_SHARE_RETENTION_MIN_TARGET_SHARE_RATIO_BY_SYMBOL = {
    "SOXX": 0.90,
}
SMALL_ACCOUNT_WHOLE_SHARE_BOOTSTRAP_MIN_TARGET_SHARE_RATIO_BY_SYMBOL = {
    "TQQQ": 0.90,
    "SOXL": 0.90,
    "SOXX": 0.90,
}


def _limit_buy_premium_for_symbol(symbol, default_premium, premium_by_symbol=None) -> float:
    normalized_symbol = str(symbol or "").strip().upper()
    try:
        fallback = float(default_premium)
    except (TypeError, ValueError):
        fallback = 1.005
    if not isinstance(premium_by_symbol, dict):
        return fallback
    raw_value = premium_by_symbol.get(normalized_symbol)
    if raw_value is None:
        return fallback
    try:
        premium = float(raw_value)
    except (TypeError, ValueError):
        return fallback
    return premium if premium > 0.0 else fallback


def _limit_buy_price(symbol, price, default_premium, premium_by_symbol=None) -> float:
    return round(
        float(price) * _limit_buy_premium_for_symbol(symbol, default_premium, premium_by_symbol),
        2,
    )


def _noop_sleep(_seconds):
    return None


def _safe_haven_cash_symbols(*, portfolio: dict, allocation: dict) -> tuple[str, ...]:
    symbols: list[str] = []
    for symbol in allocation.get("safe_haven_symbols", ()):
        normalized = str(symbol or "").strip().upper()
        if normalized:
            symbols.append(normalized)
    cash_sweep_symbol = str(portfolio.get("cash_sweep_symbol") or "").strip().upper()
    if cash_sweep_symbol:
        symbols.append(cash_sweep_symbol)
    return tuple(dict.fromkeys(symbols))


def _small_account_drift_reference_targets(allocation: Mapping, *, portfolio: Mapping | None = None) -> dict:
    allocation = dict(allocation or {})
    targets = {
        str(symbol or "").strip().upper(): float(value or 0.0)
        for symbol, value in dict(allocation.get("targets") or {}).items()
    }
    candidate_symbols = tuple(
        dict.fromkeys(
            str(symbol or "").strip().upper()
            for symbol in tuple(allocation.get("risk_symbols", ()))
            + tuple(allocation.get("income_symbols", ()))
            if str(symbol or "").strip()
        )
    )
    if not candidate_symbols:
        safe_haven_symbols = set(_safe_haven_cash_symbols(portfolio=dict(portfolio or {}), allocation=allocation))
        candidate_symbols = tuple(symbol for symbol in targets if symbol not in safe_haven_symbols)
    return {symbol: targets.get(symbol, 0.0) for symbol in candidate_symbols if symbol in targets}


def _positive_target_total(targets: dict) -> float:
    total = 0.0
    for value in dict(targets or {}).values():
        try:
            total += max(0.0, float(value or 0.0))
        except (TypeError, ValueError):
            continue
    return total


def _apply_safe_haven_cash_substitution(
    *,
    plan,
    portfolio,
    allocation,
    threshold_usd,
) -> tuple[dict, dict]:
    threshold = max(0.0, float(threshold_usd or 0.0))
    target_values = {
        str(symbol).strip().upper(): float(value or 0.0)
        for symbol, value in dict(allocation.get("targets") or {}).items()
    }
    if threshold <= 0.0:
        return dict(plan or {}), {**dict(allocation or {}), "targets": target_values}

    changed = False
    for symbol in _safe_haven_cash_symbols(portfolio=portfolio, allocation=allocation):
        target_value = float(target_values.get(symbol, 0.0) or 0.0)
        if 0.0 < target_value < threshold:
            target_values[symbol] = 0.0
            changed = True
    adjusted_allocation = {**dict(allocation or {}), "targets": target_values}
    adjusted_plan = dict(plan or {})
    if changed:
        adjusted_plan["allocation"] = adjusted_allocation
    return adjusted_plan, adjusted_allocation


def _should_retain_existing_whole_share(symbol, *, target_value, price) -> bool:
    normalized_symbol = str(symbol or "").strip().upper()
    if normalized_symbol in SMALL_ACCOUNT_EXISTING_WHOLE_SHARE_RETENTION_SYMBOLS:
        return True

    min_target_share_ratio = (
        SMALL_ACCOUNT_EXISTING_WHOLE_SHARE_RETENTION_MIN_TARGET_SHARE_RATIO_BY_SYMBOL.get(normalized_symbol)
    )
    if min_target_share_ratio is None:
        return False
    quote_price = max(0.0, float(price or 0.0))
    if quote_price <= 0.0:
        return False
    return max(0.0, float(target_value or 0.0)) >= quote_price * float(min_target_share_ratio)


def _should_bootstrap_whole_share_buy(symbol, *, target_value, limit_price) -> bool:
    normalized_symbol = str(symbol or "").strip().upper()
    min_target_share_ratio = (
        SMALL_ACCOUNT_WHOLE_SHARE_BOOTSTRAP_MIN_TARGET_SHARE_RATIO_BY_SYMBOL.get(normalized_symbol)
    )
    if min_target_share_ratio is None:
        return False
    effective_limit_price = max(0.0, float(limit_price or 0.0))
    if effective_limit_price <= 0.0:
        return False
    return max(0.0, float(target_value or 0.0)) >= effective_limit_price * float(min_target_share_ratio)


def _format_symbol_with_suffix(symbol, *, suffix=".US") -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return normalized
    if "." in normalized:
        return normalized
    normalized_suffix = str(suffix or "").strip().upper()
    return f"{normalized}{normalized_suffix}" if normalized_suffix else normalized


def _format_small_account_whole_share_bootstrap_notes(
    symbols,
    *,
    translator,
    symbol_suffix=".US",
) -> tuple[str, ...]:
    normalized_symbols = tuple(
        dict.fromkeys(
            _format_symbol_with_suffix(symbol, suffix=symbol_suffix)
            for symbol in tuple(symbols or ())
            if str(symbol or "").strip()
        )
    )
    if not normalized_symbols:
        return ()
    try:
        message = translator(
            "buy_lifted_small_account_whole_share",
            symbols=", ".join(normalized_symbols),
        )
    except Exception:
        message = ""
    if not message or message == "buy_lifted_small_account_whole_share":
        message = (
            f"ℹ️ [买入说明] {', '.join(normalized_symbols)} 目标金额接近 1 股；"
            "小账户整数股兼容，本轮允许按 1 股下单"
        )
    return (message,)


def _apply_small_account_whole_share_compatibility(
    *,
    plan,
    allocation,
    quotes,
    limit_buy_premium=1.005,
    limit_buy_premium_by_symbol=None,
) -> tuple[dict, dict]:
    target_values = dict(allocation.get("targets") or {})
    candidate_symbols = tuple(
        dict.fromkeys(
            str(symbol or "").strip().upper()
            for symbol in tuple(allocation.get("risk_symbols", ()))
            + tuple(allocation.get("income_symbols", ()))
            if str(symbol or "").strip()
        )
    )
    if not candidate_symbols:
        safe_haven_symbols = set(
            _safe_haven_cash_symbols(portfolio=dict((plan or {}).get("portfolio") or {}), allocation=allocation)
        )
        candidate_symbols = tuple(
            str(symbol or "").strip().upper()
            for symbol in target_values
            if str(symbol or "").strip().upper() not in safe_haven_symbols
        )
    quote_prices = {
        str(symbol).strip().upper(): float(
            quote.get("askPrice") or quote.get("lastPrice") or 0.0
        )
        for symbol, quote in dict(quotes or {}).items()
    }
    retained_symbols = []
    bootstrap_symbols = []
    portfolio = dict((plan or {}).get("portfolio") or {})
    quantities = {
        str(symbol or "").strip().upper(): float(quantity or 0.0)
        for symbol, quantity in dict(portfolio.get("quantities") or {}).items()
    }
    compatibility_targets = {
        str(symbol or "").strip().upper(): float(value or 0.0)
        for symbol, value in target_values.items()
    }
    for symbol in candidate_symbols:
        target_value = max(0.0, float(compatibility_targets.get(symbol, 0.0) or 0.0))
        price = max(0.0, float(quote_prices.get(symbol, 0.0) or 0.0))
        limit_price = (
            _limit_buy_price(symbol, price, limit_buy_premium, limit_buy_premium_by_symbol)
            if price > 0.0
            else 0.0
        )
        if not _should_retain_existing_whole_share(symbol, target_value=target_value, price=price):
            if (
                quantities.get(symbol, 0.0) <= 0.0
                and 0.0 < target_value < limit_price
                and _should_bootstrap_whole_share_buy(symbol, target_value=target_value, limit_price=limit_price)
            ):
                compatibility_targets[symbol] = limit_price
                bootstrap_symbols.append(symbol)
            continue
        if price > 0.0 and 0.0 < target_value < price and quantities.get(symbol, 0.0) >= 1.0:
            compatibility_targets[symbol] = price
            retained_symbols.append(symbol)
            continue
        if (
            quantities.get(symbol, 0.0) <= 0.0
            and 0.0 < target_value < limit_price
            and _should_bootstrap_whole_share_buy(symbol, target_value=target_value, limit_price=limit_price)
        ):
            compatibility_targets[symbol] = limit_price
            bootstrap_symbols.append(symbol)
    safe_haven_symbols = _safe_haven_cash_symbols(
        portfolio=portfolio,
        allocation=allocation,
    )
    compatibility = apply_small_account_cash_compatibility(
        compatibility_targets,
        quote_prices,
        candidate_symbols=candidate_symbols,
        safe_haven_cash_symbols=safe_haven_symbols,
        quantity_step=1.0,
        cash_substitute_limit_usd=SMALL_ACCOUNT_SAFE_HAVEN_CASH_SUBSTITUTE_LIMIT_USD,
    )
    adjusted_targets = compatibility.targets
    substituted = compatibility.whole_share_substituted_symbols
    safe_haven_substituted = compatibility.safe_haven_cash_substituted_symbols
    adjusted_allocation = {**dict(allocation or {}), "targets": adjusted_targets}
    adjusted_allocation.pop("small_account_whole_share_cash_notes", None)
    if substituted:
        adjusted_allocation["small_account_whole_share_substituted_symbols"] = substituted
    if safe_haven_substituted:
        adjusted_allocation["small_account_safe_haven_cash_substituted_symbols"] = tuple(safe_haven_substituted)
    if retained_symbols:
        adjusted_allocation["small_account_existing_whole_share_retained_symbols"] = tuple(
            dict.fromkeys(retained_symbols)
        )
    if bootstrap_symbols:
        adjusted_allocation["small_account_whole_share_bootstrap_symbols"] = tuple(
            dict.fromkeys(bootstrap_symbols)
        )
    if compatibility.cash_substitution_notes:
        adjusted_allocation["small_account_whole_share_cash_notes"] = tuple(compatibility.cash_substitution_notes)
    adjusted_plan = dict(plan or {})
    if substituted or safe_haven_substituted or retained_symbols or bootstrap_symbols:
        adjusted_plan["allocation"] = adjusted_allocation
    return adjusted_plan, adjusted_allocation


def execute_rebalance_cycle(
    *,
    client,
    plan,
    portfolio,
    execution,
    allocation,
    fetch_managed_snapshot,
    market_data_port,
    load_plan,
    execution_port=None,
    submit_equity_order=None,
    translator,
    limit_buy_premium,
    sell_settle_delay_sec,
    limit_buy_premium_by_symbol=None,
    dry_run_only=False,
    post_sell_refresh_attempts=1,
    post_sell_refresh_interval_sec=0.0,
    sleeper=_noop_sleep,
    publish_order_issue,
    safe_haven_cash_substitute_threshold_usd=DEFAULT_SAFE_HAVEN_CASH_SUBSTITUTE_THRESHOLD_USD,
) -> ExecutionCycleResult:
    def load_quotes(symbols):
        quotes = {}
        for symbol in symbols:
            snapshot = market_data_port.get_quote(symbol)
            quotes[symbol] = {
                "lastPrice": snapshot.last_price,
                "askPrice": snapshot.ask_price or snapshot.last_price,
            }
        return quotes

    def buying_power_from_plan(current_portfolio, current_execution):
        current_liquid_cash = float(current_portfolio["liquid_cash"])
        current_reserved_cash = float(current_execution["reserved_cash"])
        return max(0.0, current_liquid_cash - current_reserved_cash)

    strategy_symbols = tuple(allocation["strategy_symbols"])
    quotes = load_quotes(strategy_symbols)
    trade_logs: list[str] = []
    submitted_orders: list[dict] = []
    small_account_cash_note_messages: set[str] = set()
    small_account_bootstrap_note_messages: set[str] = set()

    def append_small_account_cash_notes(current_allocation):
        for message in format_small_account_cash_substitution_notes(
            dict(current_allocation or {}).get("small_account_whole_share_cash_notes") or (),
            translator=translator,
        ):
            if message in small_account_cash_note_messages:
                continue
            small_account_cash_note_messages.add(message)
            trade_logs.append(message)

    def append_small_account_bootstrap_notes(current_allocation):
        for message in _format_small_account_whole_share_bootstrap_notes(
            dict(current_allocation or {}).get("small_account_whole_share_bootstrap_symbols") or (),
            translator=translator,
        ):
            if message in small_account_bootstrap_note_messages:
                continue
            small_account_bootstrap_note_messages.add(message)
            trade_logs.append(message)

    def sell_order_quantity(symbol, current_value, target_value, price):
        if price <= 0:
            return 0
        value_based_quantity = int((current_value - target_value) // price)
        held_quantity = int(quantities.get(symbol, 0) or 0)
        if held_quantity <= 0:
            return max(0, value_based_quantity)
        position_value = held_quantity * price
        quantity_from_position = int(max(0.0, position_value - target_value) / price + 1e-9)
        return min(held_quantity, max(quantity_from_position, value_based_quantity))

    def record_submitted_order(symbol, action_type, quantity, price=None, *, status, broker_order_id=None):
        side = "sell" if action_type == "SELL" else "buy"
        order_type = "limit" if action_type == "BUY_LIMIT" else "market"
        payload = {
            "symbol": str(symbol or "").strip().upper(),
            "side": side,
            "quantity": quantity,
            "order_type": order_type,
            "status": status,
        }
        if price:
            payload["price"] = round(float(price), 4)
            if order_type == "limit":
                payload["limit_price"] = round(float(price), 4)
        if broker_order_id:
            payload["broker_order_id"] = broker_order_id
        submitted_orders.append(payload)

    def execute_fire_forget(symbol, action_type, quantity, price=None):
        if quantity <= 0:
            return False
        try:
            price_text = "{:.2f}".format(price) if price else None
            if action_type == "SELL":
                order_intent = OrderIntent(symbol=symbol, side="sell", quantity=quantity)
            elif action_type == "BUY_LIMIT":
                order_intent = OrderIntent(
                    symbol=symbol,
                    side="buy",
                    quantity=quantity,
                    order_type="limit",
                    limit_price=float(price),
                )
            elif action_type == "BUY_MARKET":
                order_intent = OrderIntent(symbol=symbol, side="buy", quantity=quantity)
            else:
                return False

            if dry_run_only:
                if action_type == "SELL":
                    trade_logs.append(
                        translator(
                            "dry_run_trade_log",
                            command=translator("market_sell_cmd"),
                            symbol=symbol,
                            quantity=quantity,
                            shares=translator("shares"),
                        )
                    )
                elif action_type == "BUY_LIMIT":
                    trade_logs.append(
                        translator(
                            "dry_run_trade_log_with_price",
                            command=translator("limit_buy_cmd"),
                            symbol=symbol,
                            price=price_text,
                            quantity=quantity,
                            shares=translator("shares"),
                        )
                    )
                elif action_type == "BUY_MARKET":
                    trade_logs.append(
                        translator(
                            "dry_run_trade_log",
                            command=translator("market_buy_cmd"),
                            symbol=symbol,
                            quantity=quantity,
                            shares=translator("shares"),
                        )
                    )
                record_submitted_order(symbol, action_type, quantity, price, status="dry_run")
                return True

            if execution_port is not None:
                report = execution_port.submit_order(order_intent)
            elif submit_equity_order is not None:
                report = submit_equity_order(client, plan["account_hash"], order_intent)
            else:
                raise ValueError("Schwab execution requires execution_port or submit_equity_order")
            success = report.status == "accepted"
            info = report.broker_order_id if success else report.raw_payload.get("detail", report.status)
            order_id_suffix = str(translator("order_id_suffix", order_id=info)).strip()
            if not order_id_suffix or order_id_suffix == "order_id_suffix":
                if str(translator("shares")).strip() == "股":
                    order_id_suffix = f"（订单号: {info}）"
                else:
                    order_id_suffix = f"(ID: {info})"
            if success:
                record_submitted_order(
                    symbol,
                    action_type,
                    quantity,
                    price,
                    status=report.status,
                    broker_order_id=report.broker_order_id,
                )
                if action_type == "SELL":
                    trade_logs.append(
                        f"✅ 📉 {translator('market_sell_cmd')} {symbol}: {quantity}{translator('shares')} {order_id_suffix}"
                    )
                elif action_type == "BUY_LIMIT":
                    trade_logs.append(
                        f"✅ 💰 {translator('limit_buy_cmd')} {symbol} (${price_text}): {quantity}{translator('shares')} {translator('submitted')} {order_id_suffix}"
                    )
                elif action_type == "BUY_MARKET":
                    trade_logs.append(
                        f"✅ 📈 {translator('market_buy_cmd')} {symbol}: {quantity}{translator('shares')} {order_id_suffix}"
                    )
                return True

            if action_type == "SELL":
                message = f"❌ {translator('market_sell')} {symbol}: {quantity}{translator('shares')} {translator('failed')} - {info}"
            elif action_type == "BUY_LIMIT":
                message = f"❌ {translator('limit_buy')} {symbol}: {quantity}{translator('shares')} {translator('failed')} - {info}"
            else:
                message = f"❌ {translator('market_buy')} {symbol}: {quantity}{translator('shares')} {translator('failed')} - {info}"
            trade_logs.append(message)
            publish_order_issue(message)
            return False
        except Exception as exc:
            message = f"🚨 {symbol} {translator('buy_label')} {quantity}{translator('shares')} {translator('exception')}: {exc}"
            trade_logs.append(message)
            publish_order_issue(message)
            return False

    market_values = dict(portfolio["market_values"])
    quantities = dict(portfolio["quantities"])
    allocation_drift_base_market_values = dict(market_values)
    allocation_drift_base_quantities = dict(quantities)
    allocation_drift_base_cash = float(portfolio.get("liquid_cash", 0.0) or 0.0)
    plan, allocation = _apply_safe_haven_cash_substitution(
        plan=plan,
        portfolio=portfolio,
        allocation=allocation,
        threshold_usd=safe_haven_cash_substitute_threshold_usd,
    )
    small_account_reference_target_values = _small_account_drift_reference_targets(
        allocation,
        portfolio=portfolio,
    )
    plan, allocation = _apply_small_account_whole_share_compatibility(
        plan=plan,
        allocation=allocation,
        quotes=quotes,
        limit_buy_premium=limit_buy_premium,
        limit_buy_premium_by_symbol=limit_buy_premium_by_symbol,
    )
    append_small_account_cash_notes(allocation)
    append_small_account_bootstrap_notes(allocation)
    target_values = dict(allocation["targets"])
    threshold = float(execution["trade_threshold_value"])
    cash_sweep_symbol = str(portfolio["cash_sweep_symbol"])
    dry_run_sale_events = []
    post_sell_buying_power_released = None
    buy_order_symbols = tuple(
        allocation.get("income_symbols", ()) + allocation.get("risk_symbols", ())
    )
    funding_buy_candidates = [
        symbol
        for symbol in buy_order_symbols
        if symbol != cash_sweep_symbol and (target_values[symbol] - market_values[symbol]) > threshold
    ]

    def cash_sweep_sale_quantity_to_fund_buy(max_quantity, candidate_symbols):
        if max_quantity <= 0 or not cash_sweep_symbol:
            return 0
        cash_sweep_price = quotes[cash_sweep_symbol]["lastPrice"]
        base_buying_power = buying_power_from_plan(portfolio, execution)
        funding_needs = (
            (
                target_values[buy_symbol] - market_values[buy_symbol],
                quotes[buy_symbol]["askPrice"],
            )
            for buy_symbol in candidate_symbols
        )
        if should_sell_cash_sweep_to_fund_whole_share_buy(
            max_quantity,
            cash_sweep_price,
            base_buying_power,
            funding_needs,
        ):
            return int(max_quantity)
        return 0

    sell_order_symbols = tuple(
        allocation.get("risk_symbols", ())
        + allocation.get("income_symbols", ())
        + allocation.get("safe_haven_symbols", ())
    )
    sell_executed = False
    cash_sweep_sold_this_cycle = False
    pending_sell_release_symbols: list[str] = []
    for symbol in sell_order_symbols:
        current = market_values[symbol]
        target = target_values[symbol]
        if current > (target + threshold):
            quantity = sell_order_quantity(symbol, current, target, quotes[symbol]["lastPrice"])
            if symbol == cash_sweep_symbol:
                quantity = cash_sweep_sale_quantity_to_fund_buy(quantity, funding_buy_candidates)
                if quantity <= 0:
                    continue
            if quantity <= 0:
                pending_sell_release_symbols.append(symbol)
                trade_logs.append(translator("sell_deferred_whole_share", symbol=symbol))
                continue
            if execute_fire_forget(symbol, "SELL", quantity):
                sell_executed = True
                if symbol == cash_sweep_symbol:
                    cash_sweep_sold_this_cycle = True
                if dry_run_only:
                    dry_run_sale_events.append(
                        (symbol, quantity, quantity * quotes[symbol]["lastPrice"])
                    )

    if (
        not cash_sweep_sold_this_cycle
        and funding_buy_candidates
        and cash_sweep_symbol
        and quantities.get(cash_sweep_symbol, 0.0) > 0.0
    ):
        sweep_quantity = cash_sweep_sale_quantity_to_fund_buy(
            int(quantities[cash_sweep_symbol]),
            funding_buy_candidates,
        )
        if sweep_quantity > 0:
            sweep_price = round(float(quotes[cash_sweep_symbol]["lastPrice"]), 2)
            if dry_run_only:
                submitted = execute_fire_forget(
                    cash_sweep_symbol,
                    "SELL",
                    sweep_quantity,
                    sweep_price,
                )
            else:
                submitted = execute_fire_forget(
                    cash_sweep_symbol,
                    "SELL",
                    sweep_quantity,
                    sweep_price,
                )
            if submitted:
                sell_executed = True
                cash_sweep_sold_this_cycle = True
                if dry_run_only:
                    dry_run_sale_events.append(
                        (
                            cash_sweep_symbol,
                            sweep_quantity,
                            sweep_quantity * sweep_price,
                        )
                    )

    if sell_executed:
        if dry_run_only:
            virtual_market_values = dict(portfolio["market_values"])
            virtual_quantities = dict(portfolio["quantities"])
            virtual_sale_proceeds = 0.0
            for symbol, quantity, sale_value in dry_run_sale_events:
                virtual_sale_proceeds += sale_value
                virtual_market_values[symbol] = max(
                    0.0,
                    float(virtual_market_values.get(symbol, 0.0)) - sale_value,
                )
                virtual_quantities[symbol] = max(
                    0,
                    int(virtual_quantities.get(symbol, 0)) - quantity,
                )
            portfolio = dict(portfolio)
            portfolio["market_values"] = virtual_market_values
            portfolio["quantities"] = virtual_quantities
            portfolio["liquid_cash"] = float(portfolio["liquid_cash"]) + virtual_sale_proceeds
            market_values = dict(portfolio["market_values"])
        else:
            previous_buying_power = buying_power_from_plan(portfolio, execution)
            refresh_attempts = max(1, int(post_sell_refresh_attempts or 1))
            refresh_interval = max(0.0, float(post_sell_refresh_interval_sec or 0.0))
            best_refreshed_state = None
            best_buying_power = previous_buying_power
            for attempt in range(refresh_attempts):
                sleeper(sell_settle_delay_sec if attempt == 0 else refresh_interval)
                snapshot = fetch_managed_snapshot(client)
                refreshed_state = load_plan(snapshot)
                refreshed_buying_power = buying_power_from_plan(
                    refreshed_state[1],
                    refreshed_state[2],
                )
                if best_refreshed_state is None or refreshed_buying_power > best_buying_power:
                    best_refreshed_state = refreshed_state
                    best_buying_power = refreshed_buying_power
                if refreshed_buying_power > previous_buying_power:
                    best_refreshed_state = refreshed_state
                    break
            post_sell_buying_power_released = best_buying_power > previous_buying_power
            plan, portfolio, execution, allocation = best_refreshed_state
            plan, allocation = _apply_safe_haven_cash_substitution(
                plan=plan,
                portfolio=portfolio,
                allocation=allocation,
                threshold_usd=safe_haven_cash_substitute_threshold_usd,
            )
            small_account_reference_target_values = _small_account_drift_reference_targets(
                allocation,
                portfolio=portfolio,
            )
            strategy_symbols = tuple(allocation["strategy_symbols"])
            quotes = load_quotes(strategy_symbols)
            plan, allocation = _apply_small_account_whole_share_compatibility(
                plan=plan,
                allocation=allocation,
                quotes=quotes,
                limit_buy_premium=limit_buy_premium,
                limit_buy_premium_by_symbol=limit_buy_premium_by_symbol,
            )
            append_small_account_cash_notes(allocation)
            append_small_account_bootstrap_notes(allocation)
            market_values = dict(portfolio["market_values"])
            target_values = dict(allocation["targets"])
            threshold = float(execution["trade_threshold_value"])

    liquid_cash = float(portfolio["liquid_cash"])
    reserved_cash = float(execution["reserved_cash"])
    estimated_buying_power = max(0, liquid_cash - reserved_cash)
    pending_sell_release_symbols = list(dict.fromkeys(pending_sell_release_symbols))
    buy_needed_symbols = [
        symbol
        for symbol in buy_order_symbols
        if market_values[symbol] < (target_values[symbol] - threshold)
    ]
    buys_blocked_reason = None
    if pending_sell_release_symbols and buy_needed_symbols:
        estimated_buy_cost = 0.0
        for symbol in buy_needed_symbols:
            target_val = target_values[symbol]
            amount_to_spend = min(
                target_val - market_values[symbol],
                estimated_buying_power,
            )
            if amount_to_spend <= 0:
                continue
            ask = quotes[symbol]["askPrice"]
            limit_price = _limit_buy_price(
                symbol, ask, limit_buy_premium, limit_buy_premium_by_symbol
            )
            quantity = int(amount_to_spend // limit_price) if limit_price > 0 else 0
            if quantity > 0:
                estimated_buy_cost += quantity * limit_price
        if estimated_buy_cost > estimated_buying_power:
            buys_blocked_reason = "pending_sell_release"
            trade_logs.append(
                translator(
                    "buy_deferred_pending_sell_release",
                    symbols=", ".join(pending_sell_release_symbols),
                )
            )
    if buys_blocked_reason is None and liquid_cash < 0.0 and buy_needed_symbols:
        buys_blocked_reason = "negative_cash"
        trade_logs.append(
            translator(
                "buy_deferred_negative_cash",
                cash=f"{liquid_cash:,.2f}",
            )
        )
    buy_executed = False
    if not buys_blocked_reason:
        for symbol in buy_order_symbols:
            target_val = target_values[symbol]
            if market_values[symbol] < (target_val - threshold):
                amount_to_spend = min(target_val - market_values[symbol], estimated_buying_power)
                if amount_to_spend > 0:
                    ask = quotes[symbol]["askPrice"]
                    limit_price = _limit_buy_price(symbol, ask, limit_buy_premium, limit_buy_premium_by_symbol)
                    quantity = int(amount_to_spend // limit_price) if limit_price > 0 else 0
                    if quantity > 0:
                        order_cost = quantity * limit_price
                        if order_cost > estimated_buying_power:
                            quantity = int(estimated_buying_power // limit_price) if limit_price > 0 else 0
                            order_cost = quantity * limit_price
                        if quantity > 0 and order_cost <= estimated_buying_power:
                            if execute_fire_forget(symbol, "BUY_LIMIT", quantity, limit_price):
                                buy_executed = True
                                estimated_buying_power -= order_cost

    cash_sweep_substituted_to_cash = bool(
        allocation.get("small_account_safe_haven_cash_substituted_symbols")
    )
    if (
        not buys_blocked_reason
        and not cash_sweep_sold_this_cycle
        and cash_sweep_symbol
        and (
            float(target_values.get(cash_sweep_symbol, 0.0) or 0.0) > 0.0
            or not cash_sweep_substituted_to_cash
        )
        and estimated_buying_power > quotes[cash_sweep_symbol]["lastPrice"] * 2
        and (
            max(0.0, float(safe_haven_cash_substitute_threshold_usd or 0.0)) <= 0.0
            or estimated_buying_power >= max(
                0.0,
                float(safe_haven_cash_substitute_threshold_usd or 0.0),
            )
        )
    ):
        quantity = int(estimated_buying_power // quotes[cash_sweep_symbol]["lastPrice"])
        if quantity > 0:
            if execute_fire_forget(cash_sweep_symbol, "BUY_MARKET", quantity):
                trade_logs.append(
                    translator(
                        "cash_sweep_rebuy",
                        symbol=cash_sweep_symbol,
                        quantity=format_quantity(quantity),
                        shares=translator("shares"),
                        price=f"{quotes[cash_sweep_symbol]['lastPrice']:.2f}",
                    )
                )
                buy_executed = True

    if (
        sell_executed
        and not dry_run_only
        and post_sell_buying_power_released is False
        and not buy_executed
    ):
        trade_logs.append(translator("post_sell_buying_power_unreleased"))

    reference_prices = {
        symbol: float((quotes.get(symbol) or {}).get("lastPrice") or 0.0)
        for symbol in tuple(strategy_symbols)
    }
    total_value = float(portfolio.get("total_strategy_equity") or portfolio.get("total_equity") or 0.0)
    drift_notes = build_small_account_allocation_drift_notes(
        target_values=small_account_reference_target_values,
        current_values=allocation_drift_base_market_values,
        current_quantities=allocation_drift_base_quantities,
        prices=reference_prices,
        submitted_orders=submitted_orders,
        total_value=total_value,
        cash_value=allocation_drift_base_cash,
    )
    trade_logs.extend(format_small_account_allocation_drift_notes(drift_notes, translator=translator))

    return ExecutionCycleResult(
        plan=dict(plan or {}),
        portfolio=dict(portfolio or {}),
        execution=dict(execution or {}),
        allocation=dict(allocation or {}),
        trade_logs=tuple(trade_logs),
    )
