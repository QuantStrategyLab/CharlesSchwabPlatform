"""Emit path: synthetic prepare only; no cloud, no real accounts."""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import emit_digest_candidates as emit  # noqa: E402
from scripts.project_digest_candidates import SCHEMA_VERSION  # noqa: E402
from scripts.publish_account_facts_from_reports import (  # noqa: E402
    HISTORY_SCHEMA,
    PROJECT_ID,
    STRATEGY_PROFILE,
)
from scripts.publish_runtime_daily_from_reports import (  # noqa: E402
    PreparedDaily,
    ReadBatch,
    _canonical_body,
    _preparation_digest,
)

UTC = dt.timezone.utc
PREFIX = "gs://synthetic-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
REVISION = "charles-schwab-quant-service-00001-abc"
SERVICE = "charles-schwab-quant-service"


def _prepared() -> PreparedDaily:
    projection = {
        "platform": "schwab",
        "observed_at": "2026-10-08T21:00:00+00:00",
        "completeness": "incomplete",
        "read_errors": ["coverage_unconfirmed"],
        "records": [
            {
                "platform": "schwab",
                "target_key": "charles-schwab-quant-service|soxl_soxx_trend_income|live",
                "target": {
                    "service": "charles-schwab-quant-service",
                    "strategy_profile": "soxl_soxx_trend_income",
                    "account_scope": "live",
                },
                "business_date": "2026-10-08",
                "timezone": "America/New_York",
                "observed_at": "2026-10-08T21:00:00+00:00",
                "status": "ok",
                "kind": "quiet",
                "completeness": "incomplete",
                "execution_lane": "live",
                "schedule": {
                    "state": "unevaluable",
                    "business_date": "2026-10-08",
                    "timezone": "America/New_York",
                    "latest_due_at": None,
                    "next_due_at": None,
                    "grace_ends_at": None,
                    "publication_grace_ended": None,
                    "expected_window": "unspecified",
                    "reason": "schedule_missing",
                },
                "runs": [
                    {
                        "run_id": "run." + ("b" * 32),
                        "activity": "no_signal",
                        "execution_lane": "live",
                        "started_at": "2026-10-08T20:01:00+00:00",
                        "finished_at": "2026-10-08T20:02:00+00:00",
                        "run_time_known": True,
                        "report_status": "ok",
                        "source_object": None,
                        "source_objects": [],
                        "object_updated_at": None,
                        "evidence": {},
                    }
                ],
                "excluded_reports": [],
                "conflicts": [],
                "fills": {"source": "not_connected", "records": [], "count": None},
            }
        ],
        "unmatched_reports": [],
    }
    binding = "a" * 64
    body = _canonical_body(projection)
    return PreparedDaily(
        reason="prepared",
        projection=json.loads(body),
        _source_binding_id=binding,
        _preparation_digest=_preparation_digest(body, binding),
    )


def _base_environ(out: Path) -> dict[str, str]:
    return {
        "SCHWAB_DIGEST_CANDIDATES_OUTPUT_PATH": str(out),
        "SCHWAB_DIGEST_OPAQUE_ACCOUNT_UID": "acct_opaque_synthetic",
        "SCHWAB_DIGEST_TARGET_ID": "schwab/synthetic-target",
        "GCP_PROJECT_ID": "charlesschwabquant",
        "GCP_REGION": "us-central1",
        "SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX": PREFIX,
    }


def _fact_reader():
    return Mock(
        return_value=SimpleNamespace(
            reason="verified",
            runtime_revision=REVISION,
            scheduler_cron=None,
            scheduler_timezone=None,
        )
    )


def _patch_identity(monkeypatch):
    monkeypatch.setattr(
        emit.caller,
        "_select_identity",
        Mock(return_value=("SYNTHETIC-HASH-NOT-A-REAL-ACCOUNT", {"enabled": True})),
    )
    monkeypatch.setattr(emit.caller, "prepare_daily", Mock(return_value=_prepared()))


def _valid_report_payload(*, net_assets: str = "12345.67") -> dict:
    return {
        "schema_version": "runtime_report.v1",
        "platform": "charles_schwab",
        "project_id": PROJECT_ID,
        "service_name": SERVICE,
        "strategy_profile": STRATEGY_PROFILE,
        "account_scope": None,
        "status": "ok",
        "errors": [],
        "run_id": "20261008T200100Z",
        "started_at": "2026-10-08T20:01:00Z",
        "finished_at": "2026-10-08T20:05:00Z",
        "runtime_target": {
            "strategy_profile": STRATEGY_PROFILE,
            # Production archive often omits account_scope; native selector binds live.
            # Native pinned identity (post PR #487): selector is observation hash.
            "account_selector": ["SYNTHETIC-HASH-NOT-A-REAL-ACCOUNT"],
        },
        "diagnostics": {"runtime_revision": REVISION},
        "summary": {
            "account_observation": {
                "account_hash": "SYNTHETIC-HASH-NOT-A-REAL-ACCOUNT",
                "currency": None,
                "observed_at": "2026-10-08T20:03:00Z",
                "net_assets": net_assets,
                "net_assets_source": "liquidationValue",
                "net_assets_currency": "USD",
                "net_assets_currency_source": "owner_confirmed",
            }
        },
    }


def test_emit_writes_ephemeral_candidates(tmp_path, monkeypatch):
    out = tmp_path / "candidates.json"
    environ = _base_environ(out)
    _patch_identity(monkeypatch)
    result = emit.emit_digest_candidates(
        environ,
        observed_at=dt.datetime(2026, 10, 8, 21, tzinfo=UTC),
        fact_reader=_fact_reader(),
        archive_reader=Mock(return_value=ReadBatch()),
    )
    assert result["status"] == "candidates_written"
    assert result["runs"] == 1
    assert result["identity_uid_present"] is True
    assert result["identity_target_present"] is True
    assert result["equity_present"] is False
    assert result["fill_count_null"] is True
    assert result["holdings_omitted"] is True
    assert result["account_facts_source"] == "net_assets_currency_unconfirmed"
    dumped = json.dumps(result)
    assert "acct_opaque" not in dumped
    assert "12345" not in dumped
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["schema_version"] == SCHEMA_VERSION
    assert data["runs"][0]["fill_count"] is None
    assert data["runs"][0]["platform_id"] == "schwab"
    assert data["runs"][0]["opaque_account_uid"] == "acct_opaque_synthetic"
    assert "equity" not in data["runs"][0]
    assert "holdings" not in data["runs"][0]


def test_emit_projects_equity_from_archive_read_only(tmp_path, monkeypatch):
    out = tmp_path / "candidates.json"
    facts_out = tmp_path / "facts.json"
    environ = _base_environ(out)
    environ.update(
        {
            "SCHWAB_ACCOUNT_FACTS_SERVICE_NAME": SERVICE,
            "SCHWAB_NET_ASSETS_CURRENCY": "USD",
            "SCHWAB_CASH_CURRENCY": "USD",
            "SCHWAB_DIGEST_ACCOUNT_FACTS_OUTPUT_PATH": str(facts_out),
        }
    )
    _patch_identity(monkeypatch)
    uri = PREFIX + "2026-10/20261008T200100Z.json"
    batch = ReadBatch(
        entries=[{"payload": _valid_report_payload(), "object_uri": uri}],
        read_failed=False,
        truncated=False,
    )
    # Guard: archive path must never call publish/POST helpers.
    monkeypatch.setattr(
        emit,
        "project_schwab_account_facts_history",
        emit.project_schwab_account_facts_history,
    )
    publish_spy = Mock(side_effect=AssertionError("must not POST account-facts"))
    monkeypatch.setattr(
        "scripts.publish_account_facts_from_reports._publish_once",
        publish_spy,
    )

    result = emit.emit_digest_candidates(
        environ,
        observed_at=dt.datetime(2026, 10, 8, 21, tzinfo=UTC),
        fact_reader=_fact_reader(),
        archive_reader=Mock(return_value=batch),
    )
    assert result["status"] == "candidates_written"
    assert result["equity_present"] is True
    assert result["fill_count_null"] is True
    assert result["account_facts_source"] == "archive_projection"
    assert result["account_facts_ephemeral_written"] is True
    assert publish_spy.call_count == 0
    dumped = json.dumps(result)
    assert "12345" not in dumped
    data = json.loads(out.read_text(encoding="utf-8"))
    row = data["runs"][0]
    assert row["equity"] == 12345.67
    assert row["equity_currency"] == "USD"
    assert row["fill_count"] is None
    assert "holdings" not in row
    facts = json.loads(facts_out.read_text(encoding="utf-8"))
    assert facts["schema_version"] == HISTORY_SCHEMA
    assert facts["broker_reported_balances"][0]["net_assets"] == "12345.67"


def test_emit_explicit_facts_path_wins(tmp_path, monkeypatch):
    out = tmp_path / "candidates.json"
    facts_path = tmp_path / "given-facts.json"
    facts_path.write_text(
        json.dumps(
            {
                "schema_version": HISTORY_SCHEMA,
                "broker_reported_balances": [
                    {
                        "currency": "USD",
                        "net_assets": "99.50",
                        "source_tag": "liquidationValue",
                        "currency_source": "owner_confirmed",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    environ = _base_environ(out)
    environ["SCHWAB_DIGEST_ACCOUNT_FACTS_PATH"] = str(facts_path)
    # Even with service configured, explicit path must win and skip archive.
    environ["SCHWAB_ACCOUNT_FACTS_SERVICE_NAME"] = SERVICE
    environ["SCHWAB_NET_ASSETS_CURRENCY"] = "USD"
    _patch_identity(monkeypatch)
    archive = Mock(return_value=ReadBatch())
    result = emit.emit_digest_candidates(
        environ,
        observed_at=dt.datetime(2026, 10, 8, 21, tzinfo=UTC),
        fact_reader=_fact_reader(),
        archive_reader=archive,
    )
    assert result["status"] == "candidates_written"
    assert result["equity_present"] is True
    assert result["account_facts_source"] == "explicit_path"
    assert result["account_facts_ephemeral_written"] is False
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["runs"][0]["equity"] == 99.5


def test_emit_requires_output_path():
    assert emit.emit_digest_candidates({}) == {
        "status": "skipped",
        "reason": "output_path_missing",
    }


def test_emit_cli_rejects_args(capsys):
    assert emit.main(["--help"]) == 2
    assert "unsupported_arguments" in capsys.readouterr().out


def test_emit_equity_without_covering_runs_from_native_archive(tmp_path, monkeypatch):
    """Digest equity path must work when daily has zero covering runs."""
    out = tmp_path / "candidates.json"
    facts_out = tmp_path / "facts.json"
    environ = _base_environ(out)
    environ.update(
        {
            "SCHWAB_ACCOUNT_FACTS_SERVICE_NAME": SERVICE,
            "SCHWAB_NET_ASSETS_CURRENCY": "USD",
            "SCHWAB_DIGEST_ACCOUNT_FACTS_OUTPUT_PATH": str(facts_out),
        }
    )
    # Prepared daily with zero covering runs.
    empty = _prepared()
    empty.projection["records"][0]["runs"] = []
    # Re-seal digest after mutation.
    binding = empty._source_binding_id
    body = _canonical_body(empty.projection)
    empty = PreparedDaily(
        reason="prepared",
        projection=json.loads(body),
        _source_binding_id=binding,
        _preparation_digest=_preparation_digest(body, binding),
    )
    monkeypatch.setattr(
        emit.caller,
        "_select_identity",
        Mock(return_value=("SYNTHETIC-HASH-NOT-A-REAL-ACCOUNT", {"enabled": True})),
    )
    monkeypatch.setattr(emit.caller, "prepare_daily", Mock(return_value=empty))
    uri = PREFIX + "2026-10/20261008T200100Z.json"
    batch = ReadBatch(
        entries=[{"payload": _valid_report_payload(), "object_uri": uri}],
        read_failed=False,
        truncated=False,
    )
    publish_spy = Mock(side_effect=AssertionError("must not POST"))
    monkeypatch.setattr(
        "scripts.publish_account_facts_from_reports._publish_once",
        publish_spy,
    )
    result = emit.emit_digest_candidates(
        environ,
        observed_at=dt.datetime(2026, 10, 8, 21, tzinfo=UTC),
        fact_reader=_fact_reader(),
        archive_reader=Mock(return_value=batch),
    )
    assert result["status"] == "candidates_written"
    assert result["equity_present"] is True
    assert result["runs"] == 1
    assert result["account_facts_source"] == "archive_projection"
    assert result["account_facts_ephemeral_written"] is True
    assert publish_spy.call_count == 0
    data = json.loads(out.read_text(encoding="utf-8"))
    row = data["runs"][0]
    assert row["actually_ran"] is True
    assert row["equity"] == 12345.67
    assert row["fill_count"] is None
    assert "holdings" not in row
