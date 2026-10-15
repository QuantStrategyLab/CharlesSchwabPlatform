"""N12-D1 holdings: runtime → archive facts → digest candidates (synthetic only)."""

from __future__ import annotations

import json
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
        {"symbol": "SOXL", "quantity": "4", "market_value": "635.92", "currency": None},
        {"symbol": "SOXX", "quantity": "1", "market_value": "250.5", "currency": None},
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


def test_publish_strips_archive_only_positions_before_post(capsys):
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
                {"symbol": "SOXL", "quantity": "4", "market_value": "635.92", "currency": None}
            ],
            "broker_reported_positions_scope": POSITIONS_SCOPE,
        }
    )
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
    with (
        patch.dict("os.environ", env, clear=True),
        patch.object(publisher, "_service_revision_matches", return_value=True),
        patch.object(publisher, "_read_report", return_value=report),
        patch.object(publisher, "_publish_once", return_value={"status": "published"}) as publish,
    ):
        assert publisher.main(["--report-uri", uri, "--expected-runtime-revision", "service-00007-abc"]) == 0
    posted = publish.call_args.args[0]
    assert "broker_reported_positions" not in posted
    assert "broker_reported_positions_scope" not in posted
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
