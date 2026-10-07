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


PRODUCER_HASH = "SYNTHETIC-NATIVE-BROKER-HASH-CaseExact"
PRODUCER_REVISION = "charles-schwab-quant-service-00001-synthetic"


def production_report(selector_kind="native"):
    """Use real production resolver/composer/serializer, never import live main."""
    from decimal import Decimal
    from types import SimpleNamespace

    from application.account_observation import build_account_observation
    from application.runtime_composer import SchwabRuntimeComposer
    from application.runtime_reporting_adapters import build_runtime_reporting_adapters
    from quant_platform_kit.common.runtime_reports import (
        build_runtime_report_base,
        finalize_runtime_report,
    )
    from quant_platform_kit.common.runtime_target import resolve_runtime_target_from_env
    from strategy_registry import SCHWAB_PLATFORM

    def forbidden(*_args, **_kwargs):
        raise AssertionError("No client, persistence or notification in this fixture")

    config = {
        "platform_id": SCHWAB_PLATFORM,
        "strategy_profile": TARGET["strategy_profile"],
        "service_name": TARGET["service"],
        "account_scope": "live",
        "dry_run_only": False,
        "execution_mode": "live",
    }
    if selector_kind != "omitted":
        config["account_selector"] = [
            PRODUCER_HASH if selector_kind == "native" else "live"
        ]
    runtime = resolve_runtime_target_from_env(
        env={"RUNTIME_TARGET_JSON": json.dumps(config)},
        expected_platform_id=SCHWAB_PLATFORM,
    )
    start = NOW - dt.timedelta(minutes=59)
    composer = SchwabRuntimeComposer(
        project_id="charlesschwabquant",
        service_name=TARGET["service"],
        secret_id="",
        app_key=None,
        app_secret=None,
        token_path="",
        strategy_profile=TARGET["strategy_profile"],
        strategy_domain="us_equity",
        strategy_display_name="Synthetic",
        strategy_display_name_localized="Synthetic",
        notify_lang="en",
        tg_token=None,
        tg_chat_id=None,
        managed_symbols=(),
        benchmark_symbol="",
        signal_effective_after_trading_days=1,
        dry_run_only=False,
        limit_buy_premium=1.0,
        sell_settle_delay_sec=0.0,
        post_sell_refresh_attempts=0,
        post_sell_refresh_interval_sec=0.0,
        safe_haven_cash_substitute_threshold_usd=0.0,
        broker_adapters=None,
        strategy_adapters=None,
        client_builder=forbidden,
        run_id_builder=lambda: start.strftime("%Y%m%dT%H%M%SZ"),
        event_logger=forbidden,
        report_builder=build_runtime_report_base,
        report_persister=forbidden,
        env_reader=lambda _name, default="": default,
        printer=forbidden,
        runtime_target=runtime,
        reporting_builder=lambda **kwargs: build_runtime_reporting_adapters(
            clock=lambda: start, **kwargs
        ),
    )
    _, value = composer.build_reporting_adapters().start_run()
    observation = build_account_observation(
        SimpleNamespace(
            metadata={
                "account_hash": PRODUCER_HASH,
                "total_equity_source": "broker_liquidation_value",
            },
            as_of=start + dt.timedelta(seconds=30),
            total_equity=Decimal("1.00"),
        ),
        net_assets_currency="USD",
    )
    assert observation is not None
    finalize_runtime_report(
        value,
        status="ok",
        finished_at=start + dt.timedelta(minutes=1),
        summary={"account_observation": observation},
        diagnostics={"runtime_revision": PRODUCER_REVISION},
    )
    return json.loads(json.dumps(value))


@pytest.mark.parametrize("selector_kind", ["native", "live", "omitted"])
def test_actual_producer_shapes_project_only_with_independent_identity_context(
    selector_kind,
):
    value = production_report(selector_kind)
    assert value["platform"] == "charles_schwab"
    assert value["runtime_target"]["platform_id"] == "schwab"
    result = project_daily_runtime(
        target=TARGET,
        reports=[value],
        observed_at=NOW,
        expected_account_hash=PRODUCER_HASH,
    )
    assert len(result["records"][0]["runs"]) == 1
    assert PRODUCER_HASH not in json.dumps(result)


@pytest.mark.parametrize("selector_kind", ["native", "omitted"])
def test_new_producer_selector_forms_are_not_admitted_by_unbound_direct_projector(
    selector_kind,
):
    result = project_daily_runtime(
        target=TARGET, reports=[production_report(selector_kind)], observed_at=NOW
    )
    assert result["records"][0]["runs"] == []


@pytest.mark.parametrize(
    "expected",
    ["", "OTHER-NATIVE", " SYNTHETIC-NATIVE-BROKER-HASH-CaseExact ", True, 1, []],
)
def test_direct_producer_projection_rejects_invalid_or_mismatched_binding(expected):
    result = project_daily_runtime(
        target=TARGET,
        reports=[production_report()],
        observed_at=NOW,
        expected_account_hash=expected,
    )
    assert result["records"][0]["runs"] == []


@pytest.mark.parametrize(
    "fault",
    [
        "outer_platform",
        "project",
        "nested_platform",
        "service_alias",
        "profile_alias",
        "scope_alias",
        "scope",
        "multi",
        "string",
        "null",
        "blank",
        "padded",
        "wrong_native",
        "wrong_observation",
        "missing_observation",
    ],
)
def test_producer_projection_keeps_namespace_scope_alias_and_identity_fail_closed(
    fault,
):
    value = production_report()
    runtime = value["runtime_target"]
    if fault == "outer_platform":
        value["platform"] = "schwab"
    elif fault == "project":
        value["project_id"] = "other"
    elif fault == "nested_platform":
        runtime["platform_id"] = "charles_schwab"
    elif fault == "service_alias":
        runtime["service"] = "other"
    elif fault == "profile_alias":
        runtime["profile"] = "other"
    elif fault == "scope_alias":
        runtime["account_group"] = "other"
    elif fault == "scope":
        runtime["account_scope"] = "paper"
    elif fault == "multi":
        runtime["account_selector"] = [PRODUCER_HASH, "other"]
    elif fault == "string":
        runtime["account_selector"] = PRODUCER_HASH
    elif fault == "null":
        runtime["account_selector"] = None
    elif fault == "blank":
        runtime["account_selector"] = [""]
    elif fault == "padded":
        runtime["account_selector"] = [" " + PRODUCER_HASH]
    elif fault == "wrong_native":
        runtime["account_selector"] = [PRODUCER_HASH.swapcase()]
    elif fault == "wrong_observation":
        value["summary"]["account_observation"]["account_hash"] = (
            PRODUCER_HASH.swapcase()
        )
    else:
        value["summary"].pop("account_observation")
    result = project_daily_runtime(
        target=TARGET,
        reports=[value],
        observed_at=NOW,
        expected_account_hash=PRODUCER_HASH,
    )
    assert result["records"][0]["runs"] == []


@pytest.mark.parametrize("selector_present", [False, True])
def test_default_direct_projector_preserves_only_baseline_legacy_selector_forms(
    selector_present,
):
    value = report()
    if not selector_present:
        value["runtime_target"].pop("account_selector")
    result = project_daily_runtime(target=TARGET, reports=[value], observed_at=NOW)
    assert len(result["records"][0]["runs"]) == 1
    # This baseline, unbound projection is not a source identity attestation.
    assert "account_hash" not in json.dumps(result)


@pytest.mark.parametrize("shape", [None, [], "PRIVATE"])
def test_bound_direct_projector_fails_closed_on_bad_observation_containers(shape):
    value = production_report()
    value["summary"]["account_observation"] = shape
    result = project_daily_runtime(
        target=TARGET,
        reports=[value],
        observed_at=NOW,
        expected_account_hash=PRODUCER_HASH,
    )
    assert result["records"][0]["runs"] == []


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


SCOPE_STAGE_FIELDS = (
    "outer_platform",
    "project",
    "runtime_map",
    "target_service",
    "target_profile",
    "target_scope",
    "required_profile",
    "required_scope",
    "selector_shape",
    "nested_platform",
    "detail_unevaluable",
    *(
        source + "_alias_" + field
        for source in ("top", "runtime")
        for field in (
            "service_name",
            "service",
            "cloud_run_service",
            "strategy_profile",
            "strategy",
            "profile",
            "account_scope",
            "account_group",
            "account_region",
        )
    ),
)


def scope_stage_case(field):
    value = production_report()
    runtime = value["runtime_target"]
    if field == "outer_platform":
        value["platform"] = "PRIVATE-PLATFORM"
    elif field == "project":
        value["project_id"] = "PRIVATE-PROJECT"
    elif field == "runtime_map":
        value["runtime_target"] = []
    elif field == "target_service":
        value["service_name"] = "PRIVATE-SERVICE"
    elif field == "target_profile":
        value["strategy_profile"] = "PRIVATE-PROFILE"
    elif field == "target_scope":
        value["account_scope"] = "PRIVATE-SCOPE"
    elif field == "required_profile":
        runtime.pop("strategy_profile")
    elif field == "required_scope":
        value["account_scope"] = "live"
        runtime.pop("account_scope")
    elif field == "selector_shape":
        runtime["account_selector"] = [PRODUCER_HASH, "PRIVATE-EXTRA"]
    elif field == "nested_platform":
        runtime["platform_id"] = "PRIVATE-PLATFORM"
    elif "_alias_" in field:
        source, alias = field.split("_alias_")
        value["account_scope"] = "live"
        destination = value if source == "top" else runtime
        expected = (
            TARGET["service"]
            if alias in {"service_name", "service", "cloud_run_service"}
            else TARGET["strategy_profile"]
            if alias in {"strategy_profile", "strategy", "profile"}
            else "live"
        )
        # Normalized primary fields pass the matcher but fail exact alias checks.
        destination[alias] = (
            expected.upper()
            if source == "top"
            and alias in {"service_name", "strategy_profile", "account_scope"}
            else "PRIVATE-ALIAS"
        )
    return value


@pytest.mark.parametrize(
    "field", [field for field in SCOPE_STAGE_FIELDS if field != "detail_unevaluable"]
)
def test_scope_detail_names_exact_first_field_without_report_values(field):
    from scripts import runtime_daily_report_projection as projection

    value = scope_stage_case(field)
    before = copy.deepcopy(value)
    assert projection._scope_failure_field(value) == field
    assert value == before
    assert "PRIVATE" not in projection._scope_failure_field(value)


@pytest.mark.parametrize("selector_kind", ["native", "live", "omitted"])
def test_scope_detail_never_turns_valid_forms_into_failures(selector_kind):
    from scripts import runtime_daily_report_projection as projection

    value = production_report(selector_kind)
    assert projection._scope_problem(value) is None
    assert projection._scope_problem_detail(value) == (None, None)
    assert projection._scope_failure_field(value) == "detail_unevaluable"


@pytest.mark.parametrize("fault", ["exception", "inconsistent_match", "unknown_stage"])
def test_scope_detail_unknowns_remain_explicitly_unevaluable(fault, monkeypatch):
    from scripts import runtime_daily_report_projection as projection

    value = production_report()
    if fault == "exception":

        def unavailable(_payload):
            raise ValueError("PRIVATE-ERROR")

        monkeypatch.setattr(projection, "_scope_problem_detail", unavailable)
    elif fault == "inconsistent_match":
        monkeypatch.setattr(
            projection,
            "match_payload_target",
            lambda *_args: (None, "PRIVATE-EXPLANATION"),
        )
    else:
        monkeypatch.setattr(
            projection,
            "_scope_problem_detail",
            lambda _payload: ("wrong_target", "PRIVATE-FIELD"),
        )
    assert projection._scope_failure_field(value) == "detail_unevaluable"


@pytest.mark.parametrize(
    "earlier,later",
    [
        ("outer_platform", "project"),
        ("project", "runtime_map"),
        ("target_service", "target_profile"),
        ("target_profile", "target_scope"),
        ("top_alias_service", "runtime_alias_profile"),
        ("runtime_alias_strategy", "selector_shape"),
        ("required_profile", "required_scope"),
        ("selector_shape", "nested_platform"),
    ],
)
def test_scope_subfield_priority_follows_original_predicate(earlier, later):
    from scripts import runtime_daily_report_projection as projection

    first, second, original = (
        scope_stage_case(earlier),
        scope_stage_case(later),
        production_report(),
    )
    for key, value in second.items():
        if (
            key == "runtime_target"
            and isinstance(value, dict)
            and isinstance(first.get(key), dict)
        ):
            for name, nested in value.items():
                if nested != original[key].get(name):
                    first[key][name] = nested
            for name in set(original[key]) - set(value):
                first[key].pop(name, None)
        elif value != original.get(key):
            first[key] = value
    assert projection._scope_failure_field(first) == earlier
