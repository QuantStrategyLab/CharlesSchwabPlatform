"""Synthetic reports only; caller flags are not proofs of broker or source truth."""

import copy
import datetime as dt
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant_platform_kit.common.execution_receipts import build_execution_receipt  # noqa: E402
from scripts.runtime_daily_report_projection import project_daily_runtime  # noqa: E402

UTC = dt.timezone.utc
TARGET = {
    "service": "charles-schwab-quant-service",
    "strategy_profile": "soxl_soxx_trend_income",
    "account_scope": "live",
}
DAY = dt.date(2026, 10, 6)
NOW = dt.datetime(2026, 10, 6, 21, tzinfo=UTC)


def report(outcome="no_signal", **updates):
    value = {
        "schema_version": "runtime_report.v1",
        "platform": "charles_schwab",
        "project_id": "charlesschwabquant",
        "service_name": TARGET["service"],
        "strategy_profile": TARGET["strategy_profile"],
        "account_scope": "live",
        "run_id": "synthetic-run-1",
        "started_at": "2026-10-06T20:01:00Z",
        "finished_at": "2026-10-06T20:02:00Z",
        "status": "ok",
        "dry_run": False,
        "errors": [],
        "runtime_target": {
            "strategy_profile": TARGET["strategy_profile"],
            "account_scope": "live",
            "account_selector": ["live"],
            "execution_mode": "live",
        },
        "runtime_release_receipt": {
            "attestation_state": "self_attested",
            "strategy_release": {"strategy_revision": "a" * 40},
        },
        "summary": {
            "execution_status": "no_op",
            "broker_submission_done": False,
            "orders_pending_count": 0,
        },
    }
    value.update(updates)
    confirmation = {"failed": "not_observed"}.get(outcome)
    value["execution_receipt"] = build_execution_receipt(
        platform="schwab",
        strategy_profile=TARGET["strategy_profile"],
        strategy_revision="a" * 40,
        execution_mode="live",
        outcome=outcome,
        broker_confirmation=confirmation,
        observed_at=value["finished_at"],
    )
    return value


def schedule(state="due", day=DAY):
    return {
        "state": state,
        "business_date": day.isoformat(),
        "timezone": "America/New_York",
        "latest_due_at": day.isoformat() + "T20:00:00Z"
        if state in {"due", "within_grace", "market_closed"}
        else None,
        "next_due_at": day.isoformat() + "T21:30:00Z" if state == "not_due" else None,
        "grace_ends_at": day.isoformat() + "T20:30:00Z"
        if state in {"due", "within_grace"}
        else None,
        "publication_grace_ended": (state == "due")
        if state in {"due", "within_grace"}
        else None,
        "expected_window": "unspecified",
        "reason": {
            "due": "publication_grace_ended",
            "within_grace": "publication_grace_open",
            "not_due": "before_schedule",
            "market_closed": "market_closed",
        }[state],
    }


def project(reports=(), **kwargs):
    params = dict(
        target=TARGET,
        reports=reports,
        observed_at=NOW,
        business_date=DAY,
        schedule_facts=schedule(),
        coverage_complete=True,
    )
    params.update(kwargs)
    return project_daily_runtime(**params)


def record(reports=(), **kwargs):
    return project(reports, **kwargs)["records"][0]


def test_no_signal_keeps_existing_daily_shape_and_does_not_claim_fills():
    result = project([report()])
    item = result["records"][0]
    assert result["platform"] == item["platform"] == "schwab"
    assert item["target"] == TARGET
    assert (
        item["target_key"] == "charles-schwab-quant-service|soxl_soxx_trend_income|live"
    )
    assert item["business_date"] == "2026-10-06"
    assert item["status"] == "no_signal"
    assert item["completeness"] == result["completeness"] == "complete"
    assert item["fills"] == {"source": "not_connected", "records": [], "count": None}
    assert item["runs"][0]["execution_lane"] == "live"


@pytest.mark.parametrize(
    "outcome,expected",
    [
        ("no_rebalance", "no_rebalance"),
        ("no_action", "no_submission"),
        ("submitted", "submitted"),
        ("broker_acknowledged", "broker_acknowledged"),
        ("partially_filled", "partially_filled"),
        ("filled", "filled"),
        ("risk_blocked", "blocked"),
    ],
)
def test_only_valid_bound_receipts_promote_execution_facts(outcome, expected):
    value = report(outcome)
    if outcome in {"submitted", "broker_acknowledged", "partially_filled", "filled"}:
        value["summary"]["broker_submission_done"] = True
    item = record([value])
    assert item["status"] == expected
    assert item["fills"]["count"] is None


@pytest.mark.parametrize("state", ["not_due", "market_closed", "due"])
@pytest.mark.parametrize(
    "outcome,summary,expected",
    [
        (
            "reconciliation_required",
            {"execution_status": "pending_reconciliation", "orders_pending_count": 1},
            "reconciliation_required",
        ),
        ("failed", {}, "failed"),
        ("no_action", {"execution_status": "failed"}, "failed"),
    ],
)
def test_abnormal_runs_outrank_schedule(state, outcome, summary, expected):
    value = report(outcome)
    value["summary"].update(summary)
    assert record([value], schedule_facts=schedule(state))["status"] == expected


def test_accepted_order_details_never_become_fills():
    value = report("submitted")
    value["summary"].update(broker_submission_done=True, orders_submitted_count=1)
    value["orders"] = [
        {"status": "accepted", "symbol": "SECRET_SYMBOL", "amount": "99172"}
    ]
    assert record([value])["status"] == "submitted"
    value["summary"].update(
        execution_status="pending_reconciliation", orders_pending_count=1
    )
    assert record([value])["status"] == "reconciliation_required"
    assert "SECRET_SYMBOL" not in json.dumps(project([value]))


def test_dry_run_never_claims_live_order_activity():
    item = record([report("filled", dry_run=True)])
    assert item["status"] == item["execution_lane"] == "dry_run"
    assert item["runs"][0]["activity"] == "previewed"
    assert item["fills"]["records"] == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("service", "other-service"),
        ("strategy_profile", "other"),
        ("account_scope", "paper"),
        ("account_key", "PRIVATE_ACCOUNT"),
    ],
)
def test_caller_cannot_choose_another_target_or_bind_an_account(field, value):
    target = {**TARGET, field: value}
    with pytest.raises(ValueError, match="invalid_target"):
        project(target=target)


@pytest.mark.parametrize(
    "field,value",
    [
        ("service_name", "other"),
        ("strategy_profile", "other"),
        ("account_scope", "paper"),
        ("platform", "longbridge"),
        ("schema_version", "runtime_report.v2"),
    ],
)
def test_wrong_report_identity_is_unmatched(field, value):
    result = project([report(**{field: value})])
    assert result["unmatched_reports"]
    assert result["records"][0]["runs"] == []
    assert result["records"][0]["status"] == (
        "read_incomplete" if field == "schema_version" else "missing_report"
    )


def test_top_level_identity_cannot_hide_conflicting_nested_target():
    value = report()
    value["runtime_target"]["account_scope"] = "paper"
    assert project([value])["unmatched_reports"][0]["reason"] == "wrong_target"


@pytest.mark.parametrize(
    "change",
    [
        "missing",
        "digest",
        "wrong_revision",
        "wrong_profile",
        "wrong_mode",
        "future",
        "outside_run",
        "extra",
    ],
)
def test_missing_invalid_or_unbound_receipt_never_upgrades_success(change):
    value = report("filled")
    if change == "missing":
        del value["execution_receipt"]
    elif change == "digest":
        value["execution_receipt"]["outcome"] = "no_action"
    elif change == "wrong_revision":
        value["runtime_release_receipt"]["strategy_release"]["strategy_revision"] = (
            "b" * 40
        )
    elif change in {"wrong_profile", "wrong_mode", "future", "outside_run"}:
        value["execution_receipt"] = build_execution_receipt(
            platform="schwab",
            strategy_profile="different"
            if change == "wrong_profile"
            else TARGET["strategy_profile"],
            strategy_revision="a" * 40,
            execution_mode="paper" if change == "wrong_mode" else "live",
            outcome="filled",
            observed_at="2026-10-07T20:02:00Z"
            if change == "future"
            else "2026-10-06T19:00:00Z"
            if change == "outside_run"
            else value["finished_at"],
        )
    else:
        value["execution_receipt"]["account_key"] = "PRIVATE_ACCOUNT"
    item = record([value])
    assert item["status"] == "insufficient"
    assert item["completeness"] != "complete"
    assert item["runs"][0]["evidence"]["receipt_outcome"] is None


def test_missing_receipt_still_keeps_observed_failure():
    value = report(status="error", errors=["SECRET_ERROR"])
    del value["execution_receipt"]
    item = record([value])
    assert item["status"] == "failed"
    assert item["completeness"] == "insufficient"


def test_missing_schedule_or_default_coverage_never_proves_whole_day():
    item = record([report()], schedule_facts=None)
    assert item["status"] == "insufficient"
    assert len(item["runs"]) == 1
    assert item["completeness"] == "insufficient"
    item = project_daily_runtime(
        target=TARGET, reports=[report()], observed_at=NOW, schedule_facts=schedule()
    )["records"][0]
    assert item["status"] == "read_incomplete"
    assert item["completeness"] == "incomplete"


@pytest.mark.parametrize(
    "patch",
    [
        {"state": "looks_healthy"},
        {"timezone": "UTC"},
        {"business_date": "2026-10-05"},
        {"latest_due_at": "2026-10-06T22:00:00Z"},
        {"latest_due_at": "2026-10-05T20:00:00Z"},
        {"grace_ends_at": "2026-10-06T19:00:00Z"},
        {"publication_grace_ended": False},
        {"next_due_at": "2026-10-06T18:00:00Z"},
        {"account_key": "PRIVATE_ACCOUNT"},
    ],
)
def test_unknown_or_conflicting_schedule_facts_fail_closed(patch):
    item = record([report()], schedule_facts={**schedule(), **patch})
    assert item["status"] == "insufficient"
    assert item["completeness"] == "insufficient"


def test_read_error_truncation_is_redacted_and_cannot_claim_no_signal():
    result = project(
        [report()],
        read_errors=["gs://PRIVATE_BUCKET/ACCOUNT listing truncated SECRET_ERROR"],
    )
    assert result["read_errors"] == ["report_read_error"]
    assert result["records"][0]["status"] == "read_incomplete"
    assert result["completeness"] == "incomplete"
    assert "PRIVATE_BUCKET" not in json.dumps(result)
    assert record([report("failed")], read_errors=["truncated"])["status"] == "failed"


@pytest.mark.parametrize(
    "timestamp,day",
    [
        ("2026-10-07T03:59:00Z", DAY),
        ("2026-10-07T04:00:00Z", dt.date(2026, 10, 7)),
        ("2026-03-08T06:30:00Z", dt.date(2026, 3, 8)),
        ("2026-03-08T07:30:00Z", dt.date(2026, 3, 8)),
        ("2026-11-01T05:30:00Z", dt.date(2026, 11, 1)),
        ("2026-11-01T06:30:00Z", dt.date(2026, 11, 1)),
    ],
)
def test_new_york_midnight_and_dst_use_run_time_not_object_mtime(timestamp, day):
    instant = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    value = report(started_at=timestamp, finished_at=timestamp)
    item = record(
        [{"payload": value, "object_updated_at": "2099-01-01T00:00:00Z"}],
        observed_at=instant + dt.timedelta(hours=1),
        business_date=day,
        schedule_facts=None,
    )
    assert item["business_date"] == day.isoformat()
    assert len(item["runs"]) == 1


def test_cross_midnight_run_belongs_to_start_day():
    value = report(
        started_at="2026-10-07T03:59:00Z", finished_at="2026-10-07T04:01:00Z"
    )
    now = dt.datetime(2026, 10, 7, 5, tzinfo=UTC)
    assert len(record([value], observed_at=now, schedule_facts=None)["runs"]) == 1
    assert not record(
        [value],
        business_date=dt.date(2026, 10, 7),
        observed_at=now,
        schedule_facts=None,
    )["runs"]


def test_older_success_cannot_satisfy_latest_due_or_object_upload_time():
    value = report(
        started_at="2026-10-06T19:00:00Z", finished_at="2026-10-06T19:02:00Z"
    )
    item = record([{"payload": value, "object_updated_at": NOW}])
    assert item["status"] == "missing_report"
    assert len(item["runs"]) == 1


def test_prior_day_reconciliation_survives_current_success_and_market_closed():
    old = report(
        "reconciliation_required",
        run_id="older",
        started_at="2026-10-05T20:01:00Z",
        finished_at="2026-10-05T20:02:00Z",
    )
    item = record([old, report()], schedule_facts=schedule("market_closed"))
    assert item["status"] == "reconciliation_required"
    assert len(item["runs"]) == 2


@pytest.mark.parametrize(
    "updates,reason",
    [
        (
            {
                "started_at": "2026-10-06T22:01:00Z",
                "finished_at": "2026-10-06T22:02:00Z",
            },
            "future_run_time",
        ),
        ({"started_at": "2026-10-06T20:03:00Z"}, "inverted_run_time"),
        (
            {"started_at": "naive", "finished_at": "2026-10-06T20:02:00"},
            "invalid_run_time",
        ),
    ],
)
def test_invalid_report_times_cannot_prove_due(updates, reason):
    value = report()
    value.update(updates)
    item = record([value])
    assert item["status"] == "insufficient"
    assert item["excluded_reports"][0]["reason"] == reason


def test_identical_duplicate_collapses_but_conflicting_same_run_does_not():
    value = report()
    assert len(record([value, copy.deepcopy(value)])["runs"]) == 1
    conflict = report("no_rebalance")
    item = record([value, conflict])
    assert item["status"] == "conflict"
    assert item["completeness"] == "insufficient"
    assert item["conflicts"]
    changed_time = report(started_at="2026-10-06T20:00:30Z")
    assert record([value, changed_time])["status"] == "conflict"


def test_conflicting_failure_stays_visible_and_incomplete():
    item = record([report(), report("failed")])
    assert item["status"] == "failed"
    assert item["completeness"] == "insufficient"
    assert item["conflicts"]


def test_projection_does_not_mutate_inputs_or_return_shared_fills():
    value, facts = report(), schedule()
    original = copy.deepcopy((value, facts, TARGET))
    first = project([value], schedule_facts=facts)
    assert (value, facts, TARGET) == original
    first["records"][0]["fills"]["records"].append("mutated")
    assert record([value])["fills"]["records"] == []


def test_private_fields_never_escape_even_through_identifiers_and_unknown_status():
    value = report(run_id="PRIVATE_ACCOUNT", status="SECRET_ERROR")
    value["summary"].update(
        account_observation={"account_hash": "PRIVATE_ACCOUNT", "net_assets": "99172"},
        execution_status="SECRET_ERROR",
        orders=[{"symbol": "SECRET_SYMBOL"}],
    )
    value["account_key"] = "PRIVATE_ACCOUNT"
    result = project(
        [
            {
                "payload": value,
                "object_uri": "gs://PRIVATE_BUCKET/execution-reports/charles_schwab/soxl_soxx_trend_income/2026-10/20261006T200200Z.json",
            }
        ]
    )
    serialized = json.dumps(result)
    assert all(
        secret not in serialized
        for secret in (
            "PRIVATE_ACCOUNT",
            "SECRET_ERROR",
            "PRIVATE_BUCKET",
            "99172",
            "SECRET_SYMBOL",
            "account_key",
            "account_observation",
        )
    )


@pytest.mark.parametrize(
    "uri",
    [
        "gs://bucket/other/2026-10/20261006T200200Z.json",
        "gs://bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/2026-10/20260906T200200Z.json",
        "https://bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/2026-10/20261006T200200Z.json",
    ],
)
def test_supplied_source_uri_must_match_existing_report_path_contract(uri):
    result = project([{"payload": report(), "object_uri": uri}])
    assert result["unmatched_reports"][0]["reason"] == "invalid_source_object"
    assert result["records"][0]["runs"] == []


def test_observation_must_be_aware_and_business_date_cannot_be_future():
    with pytest.raises(ValueError, match="invalid_observed_at"):
        project(observed_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="invalid_business_date"):
        project(business_date=dt.date(2026, 10, 7))


@pytest.mark.parametrize("field", ["status", "platform", "schema_version"])
def test_malformed_report_enum_types_are_not_promoted(field):
    value = report()
    value[field] = {"PRIVATE_ACCOUNT": "SECRET_ERROR"}
    result = project([value])
    assert result["records"][0]["status"] in {
        "insufficient",
        "missing_report",
        "read_incomplete",
    }
    assert "PRIVATE_ACCOUNT" not in json.dumps(result)


@pytest.mark.parametrize("state", [[], {}, 42])
def test_malformed_schedule_state_is_insufficient(state):
    assert (
        record([report()], schedule_facts={**schedule(), "state": state})["status"]
        == "insufficient"
    )


@pytest.mark.parametrize(
    "execution_status",
    ["submitted", "filled", "partially_filled", "UNRECOGNIZED", {"SECRET": 1}],
)
def test_quiet_receipt_cannot_hide_conflicting_or_unknown_execution_status(
    execution_status,
):
    value = report()
    value["summary"]["execution_status"] = execution_status
    assert record([value])["status"] == "insufficient"


def test_pure_projection_never_calls_network_processes_or_modifies_report(monkeypatch):
    import socket
    import subprocess
    import urllib.request

    def forbidden(*args, **kwargs):
        pytest.fail("unexpected external operation")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(urllib.request, "urlopen", forbidden)
    value = report()
    original = copy.deepcopy(value)
    assert record([value])["status"] == "no_signal"
    assert value == original


def test_actual_runtime_report_null_aliases_do_not_conflict_with_nested_target():
    value = report(account_scope=None, account_group=None, account_region=None)
    assert record([value])["status"] == "no_signal"


def test_later_business_day_anomaly_is_not_carried_backwards():
    value = report("reconciliation_required")
    item = record([value], business_date=dt.date(2026, 10, 5), schedule_facts=None)
    assert not item["runs"]
    assert item["excluded_reports"][0]["reason"] == "other_business_date"


@pytest.mark.parametrize(
    "field,value,expected",
    [
        ("execution_lane", "shadow", "shadow"),
        ("run_kind", "validation", "validation"),
        ("validation_only", True, "validation"),
        ("run_source", "dry_run", "dry_run"),
    ],
)
def test_explicit_non_real_markers_never_become_live_execution(field, value, expected):
    item = record([report("filled", **{field: value})])
    assert item["execution_lane"] == item["status"] == expected
    assert item["runs"][0]["activity"] == "previewed"


@pytest.mark.parametrize(
    "state,latest,next_due,grace,ended,window,reason",
    [
        (
            "due",
            "2026-10-06T20:00:00Z",
            None,
            "2026-10-06T20:30:00Z",
            True,
            "unspecified",
            "publication_grace_ended",
        ),
        (
            "not_due",
            None,
            "2026-10-06T21:30:00Z",
            None,
            None,
            "inside",
            "before_schedule",
        ),
        ("not_due", None, None, None, None, "unspecified", "no_cron_on_business_date"),
        (
            "market_closed",
            "2026-10-06T20:00:00Z",
            None,
            None,
            None,
            "unspecified",
            "market_closed",
        ),
        (
            "outside_window",
            "2026-10-06T20:00:00Z",
            None,
            "2026-10-06T20:30:00Z",
            None,
            "outside",
            "outside_expected_window",
        ),
    ],
)
def test_real_existing_daily_schedule_shape_is_accepted(
    state, latest, next_due, grace, ended, window, reason
):
    facts = dict(
        state=state,
        business_date=DAY.isoformat(),
        timezone="America/New_York",
        latest_due_at=latest,
        next_due_at=next_due,
        grace_ends_at=grace,
        publication_grace_ended=ended,
        expected_window=window,
        reason=reason,
    )
    item = record(schedule_facts=facts)
    assert item["schedule"]["state"] == state
    assert item["status"] == ("missing_report" if state == "due" else state)


@pytest.mark.parametrize(
    "incomplete", ["submitted", "broker_acknowledged", "partially_filled"]
)
def test_one_filled_run_cannot_hide_another_unfinished_run(incomplete):
    filled = report("filled", run_id="filled-run")
    unfinished = report(incomplete, run_id="unfinished-run")
    for values in ([filled, unfinished], [unfinished, filled]):
        item = record(values)
        assert item["status"] == incomplete
        assert len(item["runs"]) == 2
        assert item["fills"]["count"] is None


def test_real_pinned_runtime_report_builder_shape_projects_without_schema_changes():
    from quant_platform_kit.common.runtime_reports import build_runtime_report_base

    sample = report()
    built = build_runtime_report_base(
        platform="charles_schwab",
        deploy_target="cloud_run",
        service_name=TARGET["service"],
        strategy_profile=TARGET["strategy_profile"],
        run_id="builder-run",
        run_source="cloud_run",
        runtime_target=sample["runtime_target"],
        project_id="charlesschwabquant",
        started_at=sample["started_at"],
        finished_at=sample["finished_at"],
        status="ok",
        summary=sample["summary"],
    )
    built["runtime_release_receipt"] = sample["runtime_release_receipt"]
    built["execution_receipt"] = sample["execution_receipt"]
    assert record([built])["status"] == "no_signal"


def test_within_grace_uses_existing_schedule_shape_and_explicit_run_evidence():
    now = dt.datetime(2026, 10, 6, 20, 15, tzinfo=UTC)
    assert (
        record(observed_at=now, schedule_facts=schedule("within_grace"))["status"]
        == "within_grace"
    )
    assert (
        record([report()], observed_at=now, schedule_facts=schedule("within_grace"))[
            "status"
        ]
        == "no_signal"
    )


@pytest.mark.parametrize("state", ["not_due", "market_closed"])
def test_malformed_candidate_is_not_a_complete_schedule_only_day(state):
    item = record([{"schema_version": "broken"}], schedule_facts=schedule(state))
    assert item["status"] == "read_incomplete"
    assert item["completeness"] == "incomplete"
