"""Pure Phase 5 research target-to-funding conversion; never submits orders."""

from __future__ import annotations

import math
from datetime import date
from typing import Any, Mapping

SYMBOLS = ("TQQQ", "QQQM", "BOXX")
OWNERS = {"TQQQ": "tqqq_core", "QQQM": "outer", "BOXX": "outer"}
CANDIDATE_IDS = frozenset({
    "qqqm_tqqq_guard_boxx_fixed_research_v3",
    "qqqm_boxx_matched_defense_research_v3",
    "qqqm_tqqq_guard_boxx_fixed_scale_research_v1",
    "qqqm_boxx_matched_defense_scale_research_v1",
})
POLICY_VERSION = "fixed_45_05_49_01_vs_50_00_49_01_restricted_paid_cash_v3"
MONEY_TOLERANCE = 1e-6


def _amount(value: Any, field: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"{field} must be finite and nonnegative")
    return result


def _shares(value: Any, field: str) -> int:
    result = _amount(value, field)
    if result != int(result):
        raise ValueError(f"{field} must be whole shares")
    return int(result)


def _unsupported(reason: str, *, diagnostics: dict | None = None) -> dict:
    return {"status": "UNSUPPORTED", "reason": reason,
            "diagnostics": diagnostics or {}, "simulated_trades": ()}


def adapt_phase5_first_platform_offline(plan: Mapping[str, Any]) -> dict:
    """Project one frozen v3 open under its declared *research* fill contract.

    Input is the pre-open state after effective corporate actions. Dated pending
    sales are released here; dividend event creation/recognition remains with
    the upstream research producer. The result is a proposal plus hypothetical
    open-price post-state, not a Schwab order or actual broker fill.
    """
    if plan.get("candidate_id") not in CANDIDATE_IDS or plan.get("budget_policy_version") != POLICY_VERSION:
        raise ValueError("PHASE5_OFFLINE_IDENTITY_MISMATCH")
    signal_date = date.fromisoformat(str(plan["signal_date"]))
    execution_date = date.fromisoformat(str(plan["execution_date"]))
    quote_date = date.fromisoformat(str(plan["quote_date"]))
    if not signal_date < execution_date or quote_date != execution_date:
        raise ValueError("PHASE5_OFFLINE_TIME_CAUSALITY")
    index = plan["session_index"]
    if type(index) is not int or index < 0:
        raise ValueError("PHASE5_OFFLINE_SESSION_INDEX")
    if plan.get("company_actions_applied") is not True:
        return _unsupported("company_actions_not_applied")
    if _amount(plan.get("option_liability_usd", 0), "option_liability_usd") > 0:
        return _unsupported("option_liability")
    if _amount(plan.get("collateral_usd", 0), "collateral_usd") > 0:
        return _unsupported("collateral")
    if _amount(plan.get("external_cash_flow_usd", 0), "external_cash_flow_usd") > 0:
        return _unsupported("external_cash_flow")

    prices = {symbol: _amount(plan["open_prices_usd"][symbol], f"open_prices_usd.{symbol}")
              for symbol in SYMBOLS}
    if any(price <= 0 for price in prices.values()):
        raise ValueError("PHASE5_OFFLINE_PRICE")
    targets_by_owner = plan["target_usd_by_owner"]
    positions_by_owner = plan["positions_shares_by_owner"]
    owners = ("tqqq_core", "outer")
    targets = {owner: {symbol: _amount(targets_by_owner.get(owner, {}).get(symbol, 0),
                                       f"target_usd_by_owner.{owner}.{symbol}")
                       for symbol in SYMBOLS} for owner in owners}
    positions = {owner: {symbol: _shares(positions_by_owner.get(owner, {}).get(symbol, 0),
                                         f"positions_shares_by_owner.{owner}.{symbol}")
                         for symbol in SYMBOLS} for owner in owners}
    if set(targets_by_owner) != set(owners) or set(positions_by_owner) != set(owners):
        raise ValueError("PHASE5_OFFLINE_OWNER_SET")
    if any(set(targets_by_owner[owner]) - set(SYMBOLS) or
           set(positions_by_owner[owner]) - set(SYMBOLS) for owner in owners):
        raise ValueError("PHASE5_OFFLINE_UNKNOWN_SYMBOL")
    account_before = {symbol: _shares(plan["account_shares"][symbol], f"account_shares.{symbol}")
                      for symbol in SYMBOLS}
    if set(plan["account_shares"]) != set(SYMBOLS) or any(
            account_before[symbol] != sum(positions[owner][symbol] for owner in owners)
            for symbol in SYMBOLS):
        raise ValueError("PHASE5_OFFLINE_MEMBER_ACCOUNT_IDENTITY")
    target_shares = {owner: {symbol: math.floor(targets[owner][symbol] / prices[symbol])
                             for symbol in SYMBOLS} for owner in owners}
    target_total = {symbol: sum(target_shares[owner][symbol] for owner in owners)
                    for symbol in SYMBOLS}
    gross_deltas = {owner: {symbol: target_shares[owner][symbol] - positions[owner][symbol]
                            for symbol in SYMBOLS} for owner in owners}
    net_deltas = {symbol: target_total[symbol] - account_before[symbol] for symbol in SYMBOLS}
    if any(targets[owner][symbol] > 0 or positions[owner][symbol] > 0
           for owner in owners for symbol in SYMBOLS if owner != OWNERS[symbol]):
        return _unsupported("unfrozen_member_ownership", diagnostics={
            "gross_member_target_deltas": gross_deltas,
            "net_account_target_deltas": net_deltas,
        })

    fee_bps = _amount(plan["cost_bps_per_side"], "cost_bps_per_side")
    if fee_bps not in (5, 10, 15):
        return _unsupported("unfrozen_cost_scenario")
    fee_rate = fee_bps / 10_000.0
    budget = _amount(plan["tqqq_member_budget_usd"], "tqqq_member_budget_usd")
    cash_target = _amount(plan["outer_cash_target_usd"], "outer_cash_target_usd")
    if targets["tqqq_core"]["TQQQ"] > budget + MONEY_TOLERANCE:
        raise ValueError("PHASE5_OFFLINE_MEMBER_TARGET_EXCEEDS_BUDGET")
    cash = _amount(plan["settled_cash_usd"], "settled_cash_usd")
    restricted = _amount(plan["restricted_paid_cash_usd"], "restricted_paid_cash_usd")
    if restricted > cash + MONEY_TOLERANCE:
        raise ValueError("PHASE5_OFFLINE_RESTRICTED_CASH")
    pending = []
    for item in plan["pending_sales"]:
        symbol = str(item["symbol"])
        release_index = item["release_session_index"]
        if symbol not in SYMBOLS or type(release_index) is not int or release_index < 0:
            raise ValueError("PHASE5_OFFLINE_PENDING_SALE")
        pending.append({"symbol": symbol, "release_session_index": release_index,
                        "net_amount_usd": _amount(item["net_amount_usd"], "pending_sale.net_amount_usd")})
    receivables = []
    for item in plan["unpaid_receivables"]:
        symbol = str(item["symbol"])
        if symbol not in SYMBOLS or type(item["recognized_at_signal"]) is not bool:
            raise ValueError("PHASE5_OFFLINE_RECEIVABLE")
        receivables.append({"symbol": symbol,
                            "amount_usd": _amount(item["amount_usd"], "receivable.amount_usd"),
                            "recognized_at_signal": item["recognized_at_signal"]})
    prior_nav = math.fsum((cash, *(item["net_amount_usd"] for item in pending),
                           *(item["amount_usd"] for item in receivables),
                           *(account_before[symbol] * prices[symbol] for symbol in SYMBOLS)))
    released = math.fsum(item["net_amount_usd"] for item in pending
                         if item["release_session_index"] <= index)
    cash += released
    pending = [item for item in pending if item["release_session_index"] > index]
    shares = dict(account_before)
    trades = []
    costs = {symbol: 0.0 for symbol in SYMBOLS}

    # The frozen account contract sells first. Proceeds enter the dated queue.
    for symbol in SYMBOLS:
        quantity = max(0, shares[symbol] - target_total[symbol])
        if quantity:
            cost = quantity * prices[symbol] * fee_rate
            shares[symbol] -= quantity
            costs[symbol] += cost
            pending.append({"symbol": symbol, "release_session_index": index + 2,
                            "net_amount_usd": quantity * prices[symbol] - cost})
            trades.append({"symbol": symbol, "side": "sell", "shares": quantity,
                           "owner": OWNERS[symbol], "open_price_usd": prices[symbol],
                           "research_cost_usd": cost})

    def member_reserve() -> float:
        member_pending = math.fsum(item["net_amount_usd"] for item in pending
                                   if item["symbol"] == "TQQQ")
        member_receivable = math.fsum(item["amount_usd"] for item in receivables
                                      if item["symbol"] == "TQQQ" and item["recognized_at_signal"])
        return max(0.0, budget - shares["TQQQ"] * prices["TQQQ"]
                   - member_pending - member_receivable)

    shortfalls = {}
    for symbol in SYMBOLS:
        quantity = max(0, target_total[symbol] - shares[symbol])
        reserve = 0.0 if symbol == "TQQQ" else member_reserve()
        available = max(0.0, cash - restricted - cash_target - reserve)
        affordable = math.floor((available + 1e-9) / (prices[symbol] * (1.0 + fee_rate)))
        fill = min(quantity, affordable)
        if fill:
            cost = fill * prices[symbol] * fee_rate
            cash -= fill * prices[symbol] + cost
            shares[symbol] += fill
            costs[symbol] += cost
            trades.append({"symbol": symbol, "side": "buy", "shares": fill,
                           "owner": OWNERS[symbol], "open_price_usd": prices[symbol],
                           "research_cost_usd": cost})
        if fill < quantity:
            shortfalls[symbol] = {"target_shares": target_total[symbol],
                                  "proposed_shares": shares[symbol],
                                  "reason": "insufficient_free_settled_cash"}
    if cash < -MONEY_TOLERANCE:
        raise ValueError("PHASE5_OFFLINE_NEGATIVE_CASH")
    after_positions = {owner: {symbol: shares[symbol] if owner == OWNERS[symbol] else 0
                               for symbol in SYMBOLS} for owner in owners}
    after_nav = math.fsum((cash, *(item["net_amount_usd"] for item in pending),
                           *(item["amount_usd"] for item in receivables),
                           *(shares[symbol] * prices[symbol] for symbol in SYMBOLS)))
    total_cost = math.fsum(costs.values())
    identity_error = abs(prior_nav - total_cost - after_nav)
    if identity_error > MONEY_TOLERANCE:
        raise ValueError("PHASE5_OFFLINE_ACCOUNT_IDENTITY")
    return {
        "status": "FUNDED_SHORTFALL" if shortfalls else "FEASIBLE_RESEARCH_PROPOSAL",
        "candidate_id": plan["candidate_id"],
        "signal_date": str(signal_date), "execution_date": str(execution_date),
        "target_usd_by_owner": targets,
        "target_shares_by_owner": target_shares,
        "gross_member_target_deltas": gross_deltas,
        "net_account_target_deltas": net_deltas,
        "net_account_proposed_deltas": {symbol: shares[symbol] - account_before[symbol]
                                        for symbol in SYMBOLS},
        "simulated_trades": tuple(trades),
        "unfilled_target_shares": shortfalls,
        "research_cost_usd_by_symbol": costs,
        "research_total_cost_usd": total_cost,
        "member_reserved_cash_after_usd": member_reserve(),
        "outer_free_settled_cash_after_usd": max(0.0, cash - restricted - member_reserve()),
        "released_pending_sale_usd": released,
        "account_identity_error_usd": identity_error,
        "pre_open_account_nav_usd": prior_nav,
        "simulated_post_open_account_nav_usd": after_nav,
        "simulated_post_open_state": {
            "account_shares": shares,
            "positions_shares_by_owner": after_positions,
            "settled_cash_usd": cash,
            "restricted_paid_cash_usd": restricted,
            "pending_sales": tuple(pending),
            "unpaid_receivables": tuple(receivables),
        },
        "scope": "offline_research_open_price_simulation_only",
    }
