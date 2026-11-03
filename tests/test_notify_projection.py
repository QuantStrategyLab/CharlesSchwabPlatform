"""Summary-only synthetic receipts and in-memory candidate storage."""
from copy import deepcopy
from datetime import datetime, timezone
import json

import pytest
from notifications import unified
from test_completed_window_stages import archive
from test_runtime_candidate_scope import payload, setup


NOW = datetime(2026, 10, 31, 1, tzinfo=timezone.utc)


def project(source, env):
    return unified._project_runs([source], expected_target=json.loads(env["RUNTIME_TARGET_JSON"]),
        opaque_account_uid="synthetic", target_id="synthetic", now=NOW)["runs"][0]


@pytest.mark.parametrize("status", ["risk_blocked", "blocked", "failed", "failure", "error", "strategy_risk_rejected"])
def test_outer_success_cannot_hide_inner_execution_attention(status):
    env, source, _, _ = archive()
    source["summary"]["execution_status"] = status
    before = deepcopy(source)
    row = project(source, env)
    assert row["status"] == "alert" and row["execution_status"] == status
    assert row["order_count"] == 0 and row["fill_count"] is None
    assert source == before


def test_receipt_after_schedule_uses_source_window_without_relaxing_acceptance():
    env, source, _, _ = archive()
    source.update(started_at="2026-10-30T21:59:00Z", finished_at="2026-10-30T22:00:15Z")
    source["summary"]["execution_status"] = "risk_blocked"
    # A receipt 70 seconds after the scheduled start crosses the digest cutoff.
    source["execution_receipt"] = {"observed_at": "2026-10-30T22:00:10Z"}
    row = project(source, env)
    assert row["business_day"] == "2026-10-30"
    assert row["execution_status"] == "risk_blocked" and row["status"] == "alert"


@pytest.mark.parametrize("observed", ["2026-10-30T19:46:10Z", "2026-10-30T19:44:59Z"])
def test_receipt_outside_recorded_run_is_unconfirmed_not_a_final_outcome(observed):
    env, source, _, _ = archive()
    source["finished_at"] = "2026-10-30T19:45:01Z"
    source["execution_receipt"] = {"observed_at": observed}
    row = project(source, env)
    assert row["status"] == "alert"
    assert "receipt_time_unconfirmed" in row["reason_code"].split("+")
    assert row["fill_count"] is None and not row.get("approved_gate_reasons")


def test_equivalent_optional_fields_and_timestamp_offsets_do_not_conflict(monkeypatch):
    client, store, env = setup(monkeypatch)
    first = payload()
    first["runs"][0]["as_of"] = "2026-10-27T20:00:00Z"
    unified.persist_projected_candidate(first, env=env, store=store)
    before = deepcopy(client.data)
    equivalent = deepcopy(first)
    equivalent["runs"][0].update(block_reason=None, routine_observation_counts=None,
        as_of="2026-10-28T04:00:00+08:00",
        field_status={"order_count": "counts_unknown", "fill_count": "unknown", "cycle_count": "known"})
    unified.persist_projected_candidate(equivalent, env=env, store=store)
    assert client.data == before


@pytest.mark.parametrize("field,value", [("order_count", 2), ("fill_count", 1),
    ("execution_status", "risk_blocked"), ("target_id", "synthetic-other")])
def test_fact_conflicts_still_stop_before_upload(monkeypatch, field, value):
    client, store, env = setup(monkeypatch)
    unified.persist_projected_candidate(payload(), env=env, store=store)
    before = deepcopy(client.data)
    changed = payload()
    changed["runs"][0][field] = value
    with pytest.raises(ValueError, match="notification_candidate_(source|scope)_conflict"):
        unified.persist_projected_candidate(changed, env=env, store=store)
    assert client.data == before
