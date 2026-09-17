"""Align cash-only strategy sleeve equity to broker liquidation for RRL weights."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from quant_platform_kit.common.models import PortfolioSnapshot


def align_cash_only_sleeve_to_broker_liquidation(
    snapshot: PortfolioSnapshot,
    *,
    cash_only_execution: bool,
) -> PortfolioSnapshot:
    """Shrink cash when positions+cash exceed verified liquidation value.

    SOXL sizes value targets from managed positions + cash, while the RRL gate
    divides by capital_base NLV (``total_equity`` from ``liquidationValue``).
    When sleeve marks drift above liquidation, in-cap target dollars inflate
    into overweight ratios and fail closed. Mirror IBKR: rebase cash so
    strategy equity matches the verified liquidation denominator.
    """

    if not cash_only_execution:
        return snapshot
    metadata = getattr(snapshot, "metadata", None)
    if not isinstance(metadata, Mapping):
        return snapshot
    if metadata.get("total_equity_source") != "broker_liquidation_value":
        return snapshot

    try:
        nlv = float(snapshot.total_equity)
    except (TypeError, ValueError):
        return snapshot
    if not (nlv > 0.0):
        return snapshot

    position_mv_sum = sum(float(position.market_value) for position in snapshot.positions)
    raw_cash = snapshot.cash_balance
    if raw_cash is None:
        raw_cash = metadata.get("cash_available_for_trading")
    try:
        cash = float(raw_cash)
    except (TypeError, ValueError):
        return snapshot

    strategy_equity = position_mv_sum + cash
    if strategy_equity <= nlv + 1e-6:
        return snapshot

    aligned_cash = nlv - position_mv_sum
    updated_metadata = dict(metadata)
    updated_metadata["broker_liquidation_value"] = nlv
    updated_metadata["strategy_equity_before_nlv_align"] = strategy_equity
    updated_metadata["cash_available_for_trading"] = aligned_cash
    return PortfolioSnapshot(
        as_of=snapshot.as_of,
        total_equity=nlv,
        buying_power=aligned_cash,
        cash_balance=aligned_cash,
        positions=snapshot.positions,
        metadata=updated_metadata,
    )


__all__ = ["align_cash_only_sleeve_to_broker_liquidation"]
