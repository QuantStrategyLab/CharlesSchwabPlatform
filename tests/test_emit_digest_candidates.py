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
from scripts.publish_runtime_daily_from_reports import (  # noqa: E402
    PreparedDaily,
    _canonical_body,
    _preparation_digest,
)

UTC = dt.timezone.utc


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


def test_emit_writes_ephemeral_candidates(tmp_path, monkeypatch):
    out = tmp_path / "candidates.json"
    environ = {
        "SCHWAB_DIGEST_CANDIDATES_OUTPUT_PATH": str(out),
        "SCHWAB_DIGEST_OPAQUE_ACCOUNT_UID": "acct_opaque_synthetic",
        "SCHWAB_DIGEST_TARGET_ID": "schwab/synthetic-target",
        "GCP_PROJECT_ID": "charlesschwabquant",
        "GCP_REGION": "us-central1",
        "SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX": (
            "gs://synthetic-bucket/execution-reports/charles_schwab/soxl_soxx_trend_income/"
        ),
    }
    monkeypatch.setattr(
        emit.caller,
        "_select_identity",
        Mock(return_value=("SYNTHETIC-HASH-NOT-A-REAL-ACCOUNT", {"enabled": True})),
    )
    monkeypatch.setattr(emit.caller, "prepare_daily", Mock(return_value=_prepared()))
    result = emit.emit_digest_candidates(
        environ,
        observed_at=dt.datetime(2026, 10, 8, 21, tzinfo=UTC),
        fact_reader=Mock(
            return_value=SimpleNamespace(
                reason="verified",
                runtime_revision="charles-schwab-quant-service-00001-abc",
                scheduler_cron=None,
                scheduler_timezone=None,
            )
        ),
        archive_reader=Mock(return_value=emit.caller.ReadBatch()),
    )
    assert result["status"] == "candidates_written"
    assert result["runs"] == 1
    assert result["identity_uid_present"] is True
    assert result["identity_target_present"] is True
    dumped = json.dumps(result)
    assert "acct_opaque" not in dumped
    assert "12345" not in dumped
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["schema_version"] == SCHEMA_VERSION
    assert data["runs"][0]["fill_count"] is None
    assert data["runs"][0]["platform_id"] == "schwab"
    assert data["runs"][0]["opaque_account_uid"] == "acct_opaque_synthetic"


def test_emit_requires_output_path():
    assert emit.emit_digest_candidates({}) == {
        "status": "skipped",
        "reason": "output_path_missing",
    }


def test_emit_cli_rejects_args(capsys):
    assert emit.main(["--help"]) == 2
    assert "unsupported_arguments" in capsys.readouterr().out
