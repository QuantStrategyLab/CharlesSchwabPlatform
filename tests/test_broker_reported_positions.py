"""N12-D1 holdings: runtime → archive facts → digest candidates (synthetic only)."""

from __future__ import annotations

import json

import pytest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from application.account_observation import (
    POSITIONS_SCOPE,
    build_account_observation,
    build_broker_reported_positions,
)
from quant_platform_kit.common.models import PortfolioSnapshot, Position
from scripts import publish_account_facts_from_reports as publisher
from scripts.project_digest_candidates import project_digest_candidates



def _valid_report(
    *,
    observed_at="2026-09-30T22:21:00Z",
    run_id="20260930T222000Z",
    started_at="2026-09-30T22:20:00Z",
    finished_at="2026-09-30T22:25:00Z",
):
    return {
        "schema_version": "runtime_report.v1",
        "platform": "charles_schwab",
        "project_id": publisher.PROJECT_ID,
        "service_name": "synthetic-service",
        "strategy_profile": publisher.STRATEGY_PROFILE,
        "account_scope": None,
        "status": "ok",
        "errors": [],
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "runtime_target": {
            "strategy_profile": publisher.STRATEGY_PROFILE,
            "account_scope": "live",
            "account_selector": ["live"],
        },
        "diagnostics": {"runtime_revision": "service-00007-abc"},
        "summary": {
            "account_observation": {
                "account_hash": "synthetic-account-hash",
                "currency": None,
                "observed_at": observed_at,
                "net_assets": "123.45",
                "net_assets_source": "liquidationValue",
                "net_assets_currency": "USD",
                "net_assets_currency_source": "owner_confirmed",
            }
        },
    }

PREFIX = "gs://example-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
URI = PREFIX + "2026-09/20260930T222000Z.json"
NOW = datetime(2026, 10, 1, 1, 20, tzinfo=timezone.utc)


def _snapshot(positions=()):
    return PortfolioSnapshot(
        as_of=datetime(2026, 9, 30, 22, 21, tzinfo=timezone.utc),
        total_equity=1000.0,
        positions=tuple(positions),
        metadata={"account_hash": "synthetic-hash", "total_equity_source": "broker_liquidation_value"},
    )


# --- runtime ---------------------------------------------------------------


def test_runtime_positions_present_are_written_with_scope():
    observation = build_account_observation(
        _snapshot(
            [
                Position(symbol="soxx", quantity=1.0, market_value=250.5),
                Position(symbol="SOXL", quantity=4.0, market_value=635.92),
            ]
        )
    )
    assert observation is not None
    assert observation["broker_reported_positions_scope"] == POSITIONS_SCOPE == "strategy_symbols_only"
    assert observation["broker_reported_positions"] == [
        {"symbol": "SOXL", "quantity": "4", "market_value": "635.92", "currency": None, "avg_cost": None},
        {"symbol": "SOXX", "quantity": "1", "market_value": "250.5", "currency": None, "avg_cost": None},
    ]


def test_runtime_no_positions_omits_field_not_empty_list():
    observation = build_account_observation(_snapshot())
    assert observation is not None
    assert "broker_reported_positions" not in observation
    assert "broker_reported_positions_scope" not in observation


def test_runtime_malformed_position_omits_field_but_keeps_observation():
    bad = SimpleNamespace(symbol="SOXL", quantity=float("nan"), market_value=1.0)
    snapshot = SimpleNamespace(
        as_of=datetime(2026, 9, 30, 22, 21, tzinfo=timezone.utc),
        total_equity=1000.0,
        positions=(bad,),
        metadata={"account_hash": "synthetic-hash", "total_equity_source": "broker_liquidation_value"},
    )
    observation = build_account_observation(snapshot)
    assert observation is not None and observation["net_assets"] == "1000.0"
    assert "broker_reported_positions" not in observation


def test_runtime_positions_error_is_fail_soft():
    class Exploding:
        @property
        def positions(self):
            raise RuntimeError("boom")

    assert build_broker_reported_positions(Exploding()) is None
    assert build_broker_reported_positions(SimpleNamespace(positions=None)) is None
    dup = (Position("SOXL", 1.0, 1.0), Position("SOXL", 2.0, 2.0))
    assert build_broker_reported_positions(SimpleNamespace(positions=dup)) is None


# --- archive facts projection ----------------------------------------------


def _report_with_positions(positions, scope=POSITIONS_SCOPE):
    report = _valid_report()
    observation = report["summary"]["account_observation"]
    if positions is not None:
        observation["broker_reported_positions"] = positions
    if scope is not None:
        observation["broker_reported_positions_scope"] = scope
    return report


def _project(report):
    return publisher.project_schwab_account_facts_history(
        report,
        source_report_uri=URI,
        report_prefix=PREFIX,
        expected_service_name="synthetic-service",
        expected_runtime_revision="service-00007-abc",
        expected_target_id="synthetic-target",
        now=NOW,
    )


def test_facts_projection_passes_positions_through():
    body = _project(
        _report_with_positions(
            [{"symbol": "SOXL", "quantity": "4.0", "market_value": "635.92", "currency": None}]
        )
    )
    assert body["broker_reported_positions_scope"] == "strategy_symbols_only"
    assert body["broker_reported_positions"] == [
        {
            "symbol": "SOXL",
            "quantity": "4.0",
            "market_value": "635.92",
            "currency": "USD",
            "currency_source": "owner_confirmed",
        }
    ]


def test_facts_projection_absent_or_malformed_positions_omitted():
    for report in (
        _valid_report(),
        _report_with_positions([], scope=POSITIONS_SCOPE),
        _report_with_positions([{"symbol": "SOXL", "quantity": "x", "market_value": "1"}]),
        _report_with_positions([{"symbol": "SOXL", "quantity": "1", "market_value": "1"}], scope="stocks_only"),
        _report_with_positions([{"symbol": "SOXL", "quantity": "1", "market_value": "1", "extra": 1}]),
    ):
        body = _project(report)
        assert "status" not in body
        assert "broker_reported_positions" not in body
        assert "broker_reported_positions_scope" not in body


@pytest.mark.parametrize("switch", [None, "false", "1", "true"])
def test_publish_positions_are_opt_in_and_private_fields_are_omitted(capsys, switch):
    now = datetime.now(timezone.utc)
    archived_at = now.replace(second=0, microsecond=0)
    observed_at = archived_at + (now - archived_at) / 2
    run_id = archived_at.strftime("%Y%m%dT%H%M%SZ")
    uri = PREFIX + f"{archived_at:%Y-%m}/{run_id}.json"
    report = _valid_report(
        observed_at=observed_at.isoformat(),
        run_id=run_id,
        started_at=archived_at.isoformat(),
        finished_at=(archived_at + timedelta(minutes=1)).isoformat(),
    )
    report["summary"]["account_observation"].update(
        {
            "broker_reported_positions": [
                {"symbol": "SOXL", "quantity": "4", "market_value": "635.92", "currency": None, "avg_cost": "100"}
            ],
            "broker_reported_positions_scope": POSITIONS_SCOPE,
        }
    )
    report["summary"]["account_observation"]["private_note"] = "synthetic-private"
    report["summary"]["account_observation"]["account_number"] = "synthetic-private"
    env = {
        "GITHUB_ACTIONS": "true",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN": "synthetic-token",
        "ACCOUNT_FACTS_SYNC_URL": publisher.SYNC_URL,
        "SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX": PREFIX,
        "SCHWAB_ACCOUNT_FACTS_SERVICE_NAME": "synthetic-service",
        "SCHWAB_NET_ASSETS_CURRENCY": "USD",
        "SCHWAB_ACCOUNT_FACTS_TARGET_ID": "synthetic-target",
    }
    if switch is not None:
        env["SCHWAB_ACCOUNT_FACTS_INCLUDE_POSITIONS"] = switch
    with (
        patch.dict("os.environ", env, clear=True),
        patch.object(publisher, "_service_revision_matches", return_value=True),
        patch.object(publisher, "_read_report", return_value=report),
        patch.object(publisher, "_publish_once", return_value={"status": "published"}) as publish,
    ):
        assert publisher.main(["--report-uri", uri, "--expected-runtime-revision", "service-00007-abc"]) == 0
    posted = publish.call_args.args[0]
    if switch == "true":
        assert posted["broker_reported_positions"] == [{
            "symbol": "SOXL", "quantity": "4", "market_value": "635.92",
            "currency": "USD", "currency_source": "owner_confirmed", "avg_cost": "100",
        }]
        assert posted["broker_reported_positions_scope"] == POSITIONS_SCOPE
    else:
        assert "broker_reported_positions" not in posted
        assert "broker_reported_positions_scope" not in posted
    assert "synthetic-private" not in json.dumps(posted)
    assert posted["broker_reported_balances"]


# --- digest candidates ------------------------------------------------------


def _daily():
    return {
        "records": [
            {
                "platform": "schwab",
                "business_date": "2026-10-08",
                "status": "ok",
                "target": {"strategy_profile": "soxl_soxx_trend_income"},
                "runs": [{"activity": "no_signal"}],
                "fills": {"source": "not_connected", "records": [], "count": None},
            }
        ]
    }


def _facts(**extra):
    facts = {
        "schema_version": "schwab_account_snapshot_history.v1",
        "broker_reported_balances": [
            {"currency": "USD", "net_assets": "1000", "source_tag": "liquidationValue", "currency_source": "owner_confirmed"}
        ],
    }
    facts.update(extra)
    return facts


_POS = [{"symbol": "SOXL", "quantity": "4", "market_value": "635.92", "currency": "USD", "currency_source": "owner_confirmed"}]


def test_digest_maps_positions_to_holdings_with_scope():
    payload = project_digest_candidates(
        daily_projection=_daily(),
        account_facts=_facts(broker_reported_positions=_POS, broker_reported_positions_scope="strategy_symbols_only"),
    )
    row = payload["runs"][0]
    assert row["holdings"] == [{"symbol": "SOXL", "quantity": 4.0, "market_value": 635.92, "currency": "USD"}]
    assert row["holdings_scope"] == "strategy_symbols_only"
    json.dumps(payload)


def test_digest_equity_only_row_also_carries_holdings():
    payload = project_digest_candidates(
        daily_projection={"records": []},
        account_facts=_facts(broker_reported_positions=_POS, broker_reported_positions_scope="strategy_symbols_only"),
    )
    assert payload["runs"][0]["holdings_scope"] == "strategy_symbols_only"


def test_digest_absent_or_invalid_positions_omit_holdings():
    for facts in (
        _facts(),
        None,
        {"status": "skipped", "reason": "x"},
        _facts(broker_reported_positions=[], broker_reported_positions_scope="strategy_symbols_only"),
        _facts(broker_reported_positions=_POS),
        _facts(
            broker_reported_positions=[{"symbol": "SOXL", "quantity": "-1", "market_value": "1", "currency": "USD"}],
            broker_reported_positions_scope="strategy_symbols_only",
        ),
        _facts(
            broker_reported_positions=[{"symbol": "SOXL", "quantity": "1", "market_value": "1", "currency": None}],
            broker_reported_positions_scope="strategy_symbols_only",
        ),
    ):
        payload = project_digest_candidates(daily_projection=_daily(), account_facts=facts)
        for row in payload["runs"]:
            assert "holdings" not in row
            assert "holdings_scope" not in row


@pytest.mark.parametrize("cost, expected", [(125.25, "125.25"), (0, "0"), (None, None), (float("nan"), None), ("invalid", None), ("1e100", None)])
def test_cost_is_broker_reported_or_unknown(cost, expected):
    snapshot = _snapshot([Position("SOXL", 4.0, 635.92, average_cost=cost)])
    observation = build_account_observation(snapshot, net_assets_currency="USD")
    row = observation["broker_reported_positions"][0]
    assert row["avg_cost"] == expected
    report = _valid_report()
    report["summary"]["account_observation"].update(observation)
    body = _project(report)
    wire_row = body["broker_reported_positions"][0]
    assert wire_row.get("avg_cost") == expected
    if expected is None:
        assert "avg_cost" not in wire_row  # QRS wire schema uses optional decimal strings.
    assert snapshot.positions[0].average_cost is cost


@pytest.mark.parametrize("include_positions", [False, True])
def test_sync_payload_excludes_private_archive_fields(include_positions):
    projected = _project(_valid_report())
    projected.update({
        "source_report_uri": "synthetic-private-archive-location",
        "account_number": "synthetic-private-identifier",
        "orders": [{"internal_note": "synthetic-private"}],
        "status": "ok",
    })
    posted = publisher.prepare_sync_payload(projected, include_positions=include_positions)
    assert not {"source_report_uri", "account_number", "orders", "status"} & posted.keys()
    assert "synthetic-private" not in json.dumps(posted)


@pytest.mark.parametrize("cost", [None, "invalid", "1e3", True, "1.123456789", "1000000000000000"])
def test_invalid_archive_cost_retains_holding_without_inventing_cost(cost):
    body = _project(_report_with_positions([{
        "symbol": "SOXL", "quantity": "4", "market_value": "635.92",
        "currency": None, "avg_cost": cost,
    }]))
    assert body["broker_reported_positions"][0]["quantity"] == "4"
    assert "avg_cost" not in body["broker_reported_positions"][0]


def _native_stock_report(rows):
    report = _report_with_positions(rows, scope="stocks_only")
    report["summary"]["account_observation"].update({
        "broker_reported_positions_complete": True,
        "broker_reported_positions_observed_at": "2026-09-30T22:22:00Z",
    })
    return report


@pytest.mark.parametrize("rows", [[], [{"symbol": "OUTSIDE", "quantity": "-2",
    "market_value": None, "currency": "USD", "currency_source": "owner_confirmed"}]])
def test_native_inventory_projection_preserves_known_empty_missing_price_and_window(rows):
    body = _project(_native_stock_report(rows))
    assert body["broker_reported_positions"] == rows
    assert body["broker_reported_positions_scope"] == "stocks_only"
    assert body["broker_reported_positions_complete"] is True
    assert body["observed_started_at"] == "2026-09-30T22:21:00Z"
    assert body["observed_finished_at"] == "2026-09-30T22:22:00Z"
    assert body["snapshot_atomic"] is False
    assert publisher.prepare_sync_payload(body, include_positions=True)["broker_reported_positions_complete"] is True
    assert all(not key.startswith("broker_reported_positions") for key in publisher.prepare_sync_payload(body))
    assert "broker_reported_positions_observed_at" not in publisher.prepare_sync_payload(body, include_positions=True)


@pytest.mark.parametrize("patch", [
    {"broker_reported_positions_complete": False},
    {"broker_reported_positions_complete": None},
    {"broker_reported_positions_observed_at": None},
    {"broker_reported_positions_observed_at": "2026-09-30T22:20:59Z"},
    {"broker_reported_positions_observed_at": "2026-09-30T22:25:01Z"},
])
def test_bad_native_inventory_does_not_erase_valid_balances_or_claim_empty(patch):
    report = _native_stock_report([])
    report["summary"]["account_observation"].update(patch)
    body = _project(report)
    assert "status" not in body and body["broker_reported_balances"]
    assert "broker_reported_positions_complete" not in body
    assert "broker_reported_positions" not in body
    assert body["observed_finished_at"] == "2026-09-30T22:21:00Z"
