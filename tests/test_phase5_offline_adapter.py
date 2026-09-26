"""Direct contract checks for the isolated Phase 5 offline research adapter."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from research.phase5_offline_adapter import (POLICY_VERSION,
                                             adapt_phase5_first_platform_offline)


def _plan(**changes):
    plan = {
        "candidate_id": "qqqm_tqqq_guard_boxx_fixed_research_v3",
        "budget_policy_version": POLICY_VERSION,
        "signal_date": "2023-03-27", "execution_date": "2023-03-28",
        "quote_date": "2023-03-28", "session_index": 0,
        "company_actions_applied": True,
        "open_prices_usd": {"TQQQ": 100, "QQQM": 50, "BOXX": 100},
        "target_usd_by_owner": {
            "tqqq_core": {"TQQQ": 250},
            "outer": {"QQQM": 400, "BOXX": 300},
        },
        "positions_shares_by_owner": {"tqqq_core": {}, "outer": {}},
        "account_shares": {"TQQQ": 0, "QQQM": 0, "BOXX": 0},
        "settled_cash_usd": 1000,
        "restricted_paid_cash_usd": 0,
        "pending_sales": [], "unpaid_receivables": [],
        "tqqq_member_budget_usd": 300,
        "outer_cash_target_usd": 10,
        "cost_bps_per_side": 10,
        "option_liability_usd": 0, "collateral_usd": 0,
        "external_cash_flow_usd": 0,
    }
    plan.update(changes)
    return plan


def test_member_reserve_boxx_ceiling_and_account_identity():
    result = adapt_phase5_first_platform_offline(_plan())
    assert result["status"] == "FUNDED_SHORTFALL"
    assert result["target_shares_by_owner"]["outer"]["BOXX"] == 3
    assert result["simulated_post_open_state"]["account_shares"] == {
        "TQQQ": 2, "QQQM": 8, "BOXX": 2}
    assert result["member_reserved_cash_after_usd"] == 100
    assert result["unfilled_target_shares"]["BOXX"]["reason"] == "insufficient_free_settled_cash"
    assert result["research_total_cost_usd"] == pytest.approx(0.8000000000000002)
    assert result["simulated_post_open_state"]["settled_cash_usd"] == pytest.approx(199.2)
    assert result["account_identity_error_usd"] <= 1e-6
    assert [item["symbol"] for item in result["simulated_trades"]] == ["TQQQ", "QQQM", "BOXX"]


def test_one_share_boundary_and_repeated_pure_evaluation():
    inputs = _plan(
        target_usd_by_owner={"tqqq_core": {"TQQQ": 100}, "outer": {}},
        settled_cash_usd=100.1, tqqq_member_budget_usd=100,
        outer_cash_target_usd=0)
    first = adapt_phase5_first_platform_offline(inputs)
    assert first == adapt_phase5_first_platform_offline(inputs)
    assert first["net_account_proposed_deltas"]["TQQQ"] == 1
    assert first["simulated_post_open_state"]["settled_cash_usd"] == pytest.approx(0)
    underfunded = adapt_phase5_first_platform_offline({**inputs, "settled_cash_usd": 100.09})
    assert underfunded["status"] == "FUNDED_SHORTFALL"
    assert underfunded["simulated_trades"] == ()
    assert underfunded["research_total_cost_usd"] == 0
    after = first["simulated_post_open_state"]
    repeated = adapt_phase5_first_platform_offline({**inputs,
        "positions_shares_by_owner": after["positions_shares_by_owner"],
        "account_shares": after["account_shares"],
        "settled_cash_usd": after["settled_cash_usd"]})
    assert repeated["simulated_trades"] == ()
    assert repeated["research_total_cost_usd"] == 0


def test_sale_proceeds_release_only_at_second_subsequent_session():
    plan = _plan(
        open_prices_usd={"TQQQ": 100, "QQQM": 101, "BOXX": 100},
        target_usd_by_owner={"tqqq_core": {"TQQQ": 100}, "outer": {}},
        positions_shares_by_owner={"tqqq_core": {}, "outer": {"QQQM": 1}},
        account_shares={"TQQQ": 0, "QQQM": 1, "BOXX": 0},
        settled_cash_usd=0, tqqq_member_budget_usd=100,
        outer_cash_target_usd=0)
    first = adapt_phase5_first_platform_offline(plan)
    assert first["net_account_proposed_deltas"] == {"TQQQ": 0, "QQQM": -1, "BOXX": 0}
    assert first["simulated_post_open_state"]["settled_cash_usd"] == 0
    assert first["simulated_post_open_state"]["pending_sales"][0]["release_session_index"] == 2
    state = first["simulated_post_open_state"]
    second = adapt_phase5_first_platform_offline({**plan,
        "signal_date": "2023-03-28", "execution_date": "2023-03-29", "quote_date": "2023-03-29",
        "session_index": 1, "positions_shares_by_owner": state["positions_shares_by_owner"],
        "account_shares": state["account_shares"], "settled_cash_usd": state["settled_cash_usd"],
        "pending_sales": state["pending_sales"]})
    assert second["released_pending_sale_usd"] == 0
    assert second["net_account_proposed_deltas"]["TQQQ"] == 0
    state = second["simulated_post_open_state"]
    third = adapt_phase5_first_platform_offline({**plan,
        "signal_date": "2023-03-29", "execution_date": "2023-03-30", "quote_date": "2023-03-30",
        "session_index": 2, "positions_shares_by_owner": state["positions_shares_by_owner"],
        "account_shares": state["account_shares"], "settled_cash_usd": state["settled_cash_usd"],
        "pending_sales": state["pending_sales"]})
    assert third["released_pending_sale_usd"] == pytest.approx(100.899)
    assert third["net_account_proposed_deltas"]["TQQQ"] == 1
    assert third["simulated_post_open_state"]["settled_cash_usd"] == pytest.approx(0.799)


def test_opposing_member_ownership_has_no_implicit_netting_or_free_transfer():
    result = adapt_phase5_first_platform_offline(_plan(
        target_usd_by_owner={"tqqq_core": {"TQQQ": 100}, "outer": {}},
        positions_shares_by_owner={"tqqq_core": {}, "outer": {"TQQQ": 1}},
        account_shares={"TQQQ": 1, "QQQM": 0, "BOXX": 0},
        tqqq_member_budget_usd=100))
    assert result["status"] == "UNSUPPORTED"
    assert result["reason"] == "unfrozen_member_ownership"
    assert result["diagnostics"]["net_account_target_deltas"]["TQQQ"] == 0
    assert result["diagnostics"]["gross_member_target_deltas"]["tqqq_core"]["TQQQ"] == 1
    assert result["diagnostics"]["gross_member_target_deltas"]["outer"]["TQQQ"] == -1
    assert result["simulated_trades"] == ()


def test_restricted_cash_receivable_and_unsupported_liabilities():
    plan = _plan(
        target_usd_by_owner={"tqqq_core": {"TQQQ": 100}, "outer": {}},
        settled_cash_usd=150, restricted_paid_cash_usd=50,
        unpaid_receivables=[{"symbol": "TQQQ", "amount_usd": 20,
                             "recognized_at_signal": True}],
        tqqq_member_budget_usd=100, outer_cash_target_usd=0)
    result = adapt_phase5_first_platform_offline(plan)
    assert result["net_account_proposed_deltas"]["TQQQ"] == 0
    assert result["simulated_post_open_state"]["unpaid_receivables"][0]["amount_usd"] == 20
    assert adapt_phase5_first_platform_offline({**plan, "option_liability_usd": 1})["status"] == "UNSUPPORTED"
    assert adapt_phase5_first_platform_offline({**plan, "collateral_usd": 1})["reason"] == "collateral"


def test_signal_target_is_frozen_before_execution_quote():
    plan = _plan()
    first = adapt_phase5_first_platform_offline(plan)
    changed = adapt_phase5_first_platform_offline({**plan,
        "open_prices_usd": {**plan["open_prices_usd"], "QQQM": 60}})
    assert first["target_usd_by_owner"] == changed["target_usd_by_owner"]
    assert first["target_shares_by_owner"] != changed["target_shares_by_owner"]
    with pytest.raises(ValueError, match="TIME_CAUSALITY"):
        adapt_phase5_first_platform_offline({**plan, "quote_date": "2023-03-29"})


@pytest.mark.skipif(not os.environ.get("QSL_PHASE5_PRIVATE_ROOT"),
                    reason="approved local Phase 5 source not attached")
def test_eight_frozen_first_open_paths_against_private_ledger():
    private_root = Path(os.environ["QSL_PHASE5_PRIVATE_ROOT"])
    ues_root = Path(os.environ["QSL_PHASE5_UES_ROOT"])
    sys.path.insert(0, str(ues_root / "docs/research/first_compounding_20260925"))
    try:
        from boxx_outer_cash_compare import _load
        from phase5_locked_inventory_audit import (COST_BPS, PATHS, PRINCIPALS,
                                                   SCALE_SUMMARY_SHA, _ledger, _sha)
    finally:
        sys.path.pop(0)
    summary_path = private_root / "phase5_capital_scale_v1/phase5_capital_scale_summary.v1.json"
    assert _sha(summary_path) == SCALE_SUMMARY_SHA
    study = json.loads(summary_path.read_text())
    _, _, _, rows, _ = _load(private_root)
    first = next(row for row in rows if row["date"] == "2023-03-28")
    prices = {symbol: float(first[symbol.lower() + "_open"])
              for symbol in ("TQQQ", "QQQM", "BOXX")}
    count = 0
    max_cash_error = 0.0
    max_cost_error = 0.0
    max_member_reserve_error = 0.0
    funded_shortfalls = 0
    for principal in PRINCIPALS:
        for path_name in PATHS:
            day = _ledger(private_root, principal, path_name, study)[0]
            target = day["target_usd"]
            result = adapt_phase5_first_platform_offline(_plan(
                candidate_id=day["candidate_id"], signal_date=day["signal_date"],
                execution_date=day["date"], quote_date=day["date"],
                open_prices_usd=prices,
                target_usd_by_owner={"tqqq_core": {"TQQQ": target["TQQQ"]},
                                     "outer": {"QQQM": target["QQQM"], "BOXX": target["BOXX"]}},
                settled_cash_usd=principal,
                tqqq_member_budget_usd=day["member_budget_usd"],
                outer_cash_target_usd=day["cash_target_usd"],
                cost_bps_per_side=COST_BPS))
            assert result["status"] in ("FEASIBLE_RESEARCH_PROPOSAL", "FUNDED_SHORTFALL")
            funded_shortfalls += result["status"] == "FUNDED_SHORTFALL"
            assert result["net_account_proposed_deltas"] == {
                symbol: int(day["trade_shares"][symbol]) for symbol in ("TQQQ", "QQQM", "BOXX")}
            assert result["simulated_post_open_state"]["account_shares"] == {
                symbol: int(day["shares"][symbol]) for symbol in ("TQQQ", "QQQM", "BOXX")}
            assert result["simulated_post_open_state"]["settled_cash_usd"] == pytest.approx(
                day["settled_cash_usd"], abs=1e-6)
            assert result["research_total_cost_usd"] == pytest.approx(
                sum(day["trade_cost_usd"].values()), abs=1e-6)
            assert result["member_reserved_cash_after_usd"] == pytest.approx(
                day["member_reserved_cash_at_open_usd"], abs=1e-6)
            assert result["account_identity_error_usd"] <= 1e-6
            max_cash_error = max(max_cash_error, abs(
                result["simulated_post_open_state"]["settled_cash_usd"] - day["settled_cash_usd"]))
            max_cost_error = max(max_cost_error, abs(
                result["research_total_cost_usd"] - sum(day["trade_cost_usd"].values())))
            max_member_reserve_error = max(max_member_reserve_error, abs(
                result["member_reserved_cash_after_usd"] - day["member_reserved_cash_at_open_usd"]))
            count += 1
    assert count == 8
    print(f"phase5_first_open_paths={count} funded_shortfalls={funded_shortfalls} "
          f"max_cash_error_usd={max_cash_error:.12g} "
          f"max_cost_error_usd={max_cost_error:.12g} "
          f"max_member_reserve_error_usd={max_member_reserve_error:.12g}")
