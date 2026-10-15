"""Synthetic daily projection; empty unconnected fills never prove zero."""

import copy

import pytest

from scripts.project_digest_candidates import project_digest_candidates


def daily():
    return {"completeness": "complete", "read_errors": [], "records": [{
        "platform": "schwab", "business_date": "2026-10-14", "status": "no_signal",
        "completeness": "complete", "conflicts": [],
        "target": {"strategy_profile": "demo_strategy"},
        "fills": {"source": "not_connected", "count": None, "records": []},
        "runs": [{"run_id": "synthetic-run", "activity": "no_signal", "execution_lane": "live",
                  "evidence": {"receipt_state": "valid", "receipt_outcome": "no_signal",
                               "receipt_broker_confirmation": "not_applicable",
                               "broker_submission_done": False, "action_done": False,
                               "orders_pending_count": 0, "errors_present": False}}],
    }]}


def project(value):
    return project_digest_candidates(daily_projection=value, business_day="2026-10-14",
                                     opaque_account_uid="synthetic-account", target_id="demo/target")["runs"][0]


def test_complete_no_submission_evidence_projects_zero():
    row = project(daily())
    assert (row["fill_count"], row["order_count"]) == (0, 0)
    assert row["field_status"]["fill_count"] == "known"


@pytest.mark.parametrize("mutation", [
    lambda v: v.update(completeness="incomplete"),
    lambda v: v.update(read_errors=["synthetic failure"]),
    lambda v: v["records"][0].update(completeness="incomplete"),
    lambda v: v["records"][0].update(conflicts=["synthetic conflict"]),
    lambda v: v["records"][0].update(fills={"source": "connected", "count": "unreadable"}),
    lambda v: v["records"][0]["runs"][0].pop("evidence"),
    lambda v: v["records"][0]["runs"][0]["evidence"].update(receipt_state="missing"),
    lambda v: v["records"][0]["runs"][0]["evidence"].update(broker_submission_done=True),
    lambda v: v["records"][0]["runs"][0]["evidence"].update(orders_pending_count=False),
    lambda v: v["records"][0]["runs"][0]["evidence"].update(orders_pending_count=1),
    lambda v: v["records"][0]["runs"][0]["evidence"].update(errors_present=True),
    lambda v: v["records"][0]["runs"].append({"activity": "unknown"}),
    lambda v: v["records"][0]["runs"].append({"activity": "insufficient"}),
    lambda v: v["records"][0]["runs"][0].update(activity="blocked"),
])
def test_absent_incomplete_or_conflicting_evidence_stays_unknown(mutation):
    value = daily()
    mutation(value)
    row = project(value)
    assert row["fill_count"] is None
    assert row["order_count"] is None


def test_connected_nonzero_fills_override_quiet_signal():
    value = daily()
    value["records"][0]["fills"] = {"source": "synthetic_broker", "count": 3}
    row = project(value)
    assert row["fill_count"] == 3
    assert row["order_count"] is None


def test_stale_record_is_not_relabelled_as_current_zero():
    value = copy.deepcopy(daily())
    value["records"][0]["business_date"] = "2026-10-13"
    assert project_digest_candidates(daily_projection=value, business_day="2026-10-14")["runs"] == []


def test_blocked_with_explicit_no_submission_stays_alert_but_counts_zero():
    value = daily()
    record = value["records"][0]
    record["status"] = "blocked"
    record["runs"][0]["activity"] = "blocked"
    record["runs"][0]["evidence"]["receipt_outcome"] = "risk_blocked"
    row = project(value)
    assert (row["fill_count"], row["order_count"]) == (0, 0)
    assert row["status"] == "alert"
