"""Schwab evidence adapter for the shared, paper-only command consumer.

The adapter converts a Schwab portfolio snapshot and quotes into reconciled
paper proposals.  It deliberately has no execution-port or order-submission
import; lifecycle, risk admission, command binding, and durable events stay in
``quant_platform_kit.common.paper_execution_command_consumer``.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from typing import Any

from quant_platform_kit.common.execution_commands import ExecutionCommand, ExecutionCommandStore
from quant_platform_kit.common.paper_execution_command_consumer import (
    PaperExecutionProposal,
    PaperExecutionReconciliation,
    consume_due_paper_execution_commands as consume_shared_paper_execution_commands,
)
from quant_platform_kit.common.runtime_command_gate import RuntimeCommandExposureEffect
from quant_platform_kit.common.strategy_release import StrategyReleaseIdentity


SCHWAB_PAPER_EXECUTION_INTENT_SCHEMA_VERSION = "schwab.paper-execution-intent.v1"
_NOTIONAL_TOLERANCE = 0.01


def _normalized_symbol(value: object) -> str:
    return str(value or "").strip().upper()


def _normalized_symbols(value: object) -> set[str]:
    if not isinstance(value, (list, tuple, set)):
        return set()
    return {_normalized_symbol(symbol) for symbol in value if _normalized_symbol(symbol)}


def _finite_number(value: object, *, field_name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be numeric") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field_name} must be finite")
    return number


def _build_reconciled_order_proposals(
    command: ExecutionCommand,
    *,
    portfolio: Any,
    market_data_port: Any,
    managed_symbols: Sequence[str],
) -> PaperExecutionReconciliation:
    """Build only reconciled paper proposals for Schwab's long-only contract."""

    intent = command.intent
    if str(intent.get("schema_version") or "") != SCHWAB_PAPER_EXECUTION_INTENT_SCHEMA_VERSION:
        return PaperExecutionReconciliation(proposals=(), integrity_findings=("data_artifact_invalid",))
    if str(intent.get("target_mode") or "") != "value":
        return PaperExecutionReconciliation(proposals=(), integrity_findings=("data_artifact_invalid",))
    raw_targets = intent.get("targets")
    if not isinstance(raw_targets, Mapping):
        return PaperExecutionReconciliation(proposals=(), integrity_findings=("data_artifact_invalid",))
    try:
        targets = {
            _normalized_symbol(symbol): _finite_number(target, field_name=f"targets[{symbol!r}]")
            for symbol, target in raw_targets.items()
            if _normalized_symbol(symbol)
        }
    except ValueError:
        return PaperExecutionReconciliation(proposals=(), integrity_findings=("data_artifact_invalid",))
    strategy_symbols = _normalized_symbols(intent.get("strategy_symbols"))
    expected_symbols = {
        _normalized_symbol(symbol)
        for symbol in managed_symbols
        if _normalized_symbol(symbol)
    }
    if (
        not strategy_symbols
        or strategy_symbols != expected_symbols
        or set(targets) != strategy_symbols
        or any(target < 0.0 for target in targets.values())
    ):
        return PaperExecutionReconciliation(proposals=(), integrity_findings=("data_artifact_invalid",))

    position_values: dict[str, float] = {}
    position_quantities: dict[str, float] = {}
    findings: list[str] = []
    for position in tuple(getattr(portfolio, "positions", ()) or ()):
        symbol = _normalized_symbol(getattr(position, "symbol", ""))
        if symbol not in strategy_symbols:
            findings.append("position_reconciliation_mismatch")
            continue
        try:
            market_value = _finite_number(
                getattr(position, "market_value", None),
                field_name=f"position[{symbol}].market_value",
            )
            quantity = _finite_number(
                getattr(position, "quantity", None),
                field_name=f"position[{symbol}].quantity",
            )
        except ValueError:
            findings.append("position_reconciliation_mismatch")
            continue
        # A cash-only consumer cannot safely model a short or an inconsistent
        # quantity/value pair as a paper order.
        if market_value < -_NOTIONAL_TOLERANCE or quantity < -_NOTIONAL_TOLERANCE:
            findings.append("position_reconciliation_mismatch")
        if abs(quantity) > _NOTIONAL_TOLERANCE and abs(market_value) <= _NOTIONAL_TOLERANCE:
            findings.append("position_reconciliation_mismatch")
        position_values[symbol] = position_values.get(symbol, 0.0) + market_value
        position_quantities[symbol] = position_quantities.get(symbol, 0.0) + quantity

    try:
        cash_balance = _finite_number(
            getattr(portfolio, "cash_balance", None),
            field_name="portfolio.cash_balance",
        )
        total_equity = _finite_number(
            getattr(portfolio, "total_equity", None),
            field_name="portfolio.total_equity",
        )
        tolerance = max(1.0, abs(total_equity) * 0.005)
        if abs(cash_balance + sum(position_values.values()) - total_equity) > tolerance:
            findings.append("position_reconciliation_mismatch")
    except ValueError:
        findings.append("position_reconciliation_mismatch")

    proposals: list[PaperExecutionProposal] = []
    for symbol in sorted(strategy_symbols):
        current_value = position_values.get(symbol, 0.0)
        target_value = targets[symbol]
        delta_value = target_value - current_value
        if abs(delta_value) <= _NOTIONAL_TOLERANCE:
            continue
        try:
            quote = market_data_port.get_quote(symbol)
            price = _finite_number(
                getattr(quote, "last_price", None),
                field_name=f"quote[{symbol}].last_price",
            )
            if price <= 0.0:
                raise ValueError("quote price must be positive")
        except Exception:
            findings.append("position_reconciliation_mismatch")
            continue
        before_exposure = abs(current_value)
        after_exposure = abs(target_value)
        if after_exposure < before_exposure - _NOTIONAL_TOLERANCE:
            exposure_effect = RuntimeCommandExposureEffect.REDUCES
        elif after_exposure > before_exposure + _NOTIONAL_TOLERANCE:
            exposure_effect = RuntimeCommandExposureEffect.INCREASES
        else:
            exposure_effect = RuntimeCommandExposureEffect.NEUTRAL
        proposals.append(
            PaperExecutionProposal(
                symbol=symbol,
                exposure_effect=exposure_effect,
                details={
                    "side": "buy" if delta_value > 0.0 else "sell",
                    "quantity": round(abs(delta_value) / price, 8),
                    "reference_price": round(price, 8),
                    "current_value": round(current_value, 8),
                    "target_value": round(target_value, 8),
                    "target_notional_delta": round(delta_value, 8),
                    "current_quantity": round(position_quantities.get(symbol, 0.0), 8),
                },
            )
        )
    return PaperExecutionReconciliation(
        proposals=tuple(proposals),
        integrity_findings=tuple(dict.fromkeys(findings)),
    )


def consume_due_paper_execution_commands(
    *,
    store: ExecutionCommandStore | None,
    as_of_session: date | str,
    claimant: str,
    portfolio_loader: Callable[[], Any],
    market_data_port_loader: Callable[[], Any],
    managed_symbols: Sequence[str],
    runtime_release_receipt: Mapping[str, Any] | None,
    expected_strategy_release: StrategyReleaseIdentity | Mapping[str, object] | None,
    expected_command_binding: Mapping[str, object] | None,
) -> dict[str, object]:
    """Consume matching paper commands, loading broker facts only after binding."""

    def reconcile_command(command: ExecutionCommand) -> PaperExecutionReconciliation:
        return _build_reconciled_order_proposals(
            command,
            portfolio=portfolio_loader(),
            market_data_port=market_data_port_loader(),
            managed_symbols=managed_symbols,
        )

    return consume_shared_paper_execution_commands(
        store=store,
        as_of_session=as_of_session,
        claimant=claimant,
        reconcile_command=reconcile_command,
        runtime_release_receipt=runtime_release_receipt,
        expected_strategy_release=expected_strategy_release,
        expected_command_binding=expected_command_binding,
    )


__all__ = (
    "SCHWAB_PAPER_EXECUTION_INTENT_SCHEMA_VERSION",
    "consume_due_paper_execution_commands",
)
