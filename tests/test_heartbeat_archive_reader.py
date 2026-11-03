"""Heartbeat uses bounded SDK archive reads, including empty month partitions."""
from datetime import datetime, timezone
import json
import subprocess

import pytest
from google.api_core.exceptions import Forbidden

from notifications import unified
from test_publish_digest_before_read import ENV, PLATFORM
import test_publish_digest_before_read as prepub

client = prepub.client

NOW = datetime(2026, 11, 2, 1, 35, tzinfo=timezone.utc)
SOURCE_PLATFORM = "charles_schwab" if PLATFORM == "schwab" else PLATFORM


def emit():
    return unified.emit_completed_candidates(ENV, now=NOW)


@pytest.fixture(autouse=True)
def no_cli(monkeypatch):
    def fail(*args, **kwargs):
        pytest.fail("heartbeat must use the Storage SDK")
    monkeypatch.setattr(subprocess, "run", fail)


def candidate(client):
    return client.blobs[f"gs://synthetic-candidates/digest/{PLATFORM}/2026-11-01/synthetic-slot.json"]


@pytest.mark.parametrize("other_month", [None, "2026-10", "2026-11"])
def test_cross_month_empty_partition_reaches_not_due_check(client, other_month):
    if other_month:
        target = {**json.loads(ENV["RUNTIME_TARGET_JSON"]), "service_name": "synthetic-other"}
        report = {"runtime_target": target, "started_at": "2026-11-01T20:00:00Z"}
        client.blob(f"gs://synthetic-archive/reports/{SOURCE_PLATFORM}/synthetic/{other_month}/other.json").upload_from_string(
            json.dumps(report), retry=None)
    result = emit()
    assert result["uploaded"] is True
    rows = json.loads(candidate(client).body)["runs"]
    assert len(rows) == 1 and rows[0]["actually_ran"] is False
    assert rows[0]["scheduled_idle"] is True
    assert rows[0]["business_day"] == "2026-11-01"
    assert rows[0].get("order_count") is None and rows[0].get("fill_count") is None
    assert [kwargs["match_glob"] for _, kwargs in client.lists] == [
        f"reports/{SOURCE_PLATFORM}/**/2026-10/*.json",
        f"reports/{SOURCE_PLATFORM}/**/2026-11/*.json",
    ]
    assert all(kwargs["retry"] is None and kwargs["timeout"] == 30 for _, kwargs in client.lists)


def test_cross_month_empty_due_window_still_fails_without_writing(client, monkeypatch):
    monkeypatch.setitem(ENV, "DIGEST_NOTIFICATION_SCHEDULE_JSON", json.dumps({"scheduler": {
        "main_time": "45 15 * * *", "timezone": "America/New_York"}}))
    with pytest.raises(ValueError, match="candidate_due_source_absent"):
        emit()
    assert not any(blob.writes for blob in client.blobs.values())


@pytest.mark.parametrize("error", [Forbidden("synthetic-private"), TimeoutError("synthetic-private")])
@pytest.mark.parametrize("failed_month", ["2026-10", "2026-11"])
def test_listing_failure_is_never_empty_even_after_an_empty_month(client, monkeypatch, error, failed_month):
    real_list = client.list_blobs
    def listing(*args, **kwargs):
        if failed_month in kwargs["match_glob"]:
            def failed_page():
                raise error
                yield
            return failed_page()
        return real_list(*args, **kwargs)
    monkeypatch.setattr(client, "list_blobs", listing)
    with pytest.raises(ValueError, match="candidate_archive_read_failed"):
        emit()
    assert not any(blob.writes for blob in client.blobs.values())


def test_invalid_archive_json_remains_a_read_failure(client):
    blob = client.blob(f"gs://synthetic-archive/reports/{SOURCE_PLATFORM}/synthetic/2026-10/run.json")
    blob.upload_from_string('{"synthetic-private":', retry=None)
    with pytest.raises(ValueError, match="candidate_archive_read_failed"):
        emit()
    assert all("/digest/" not in uri for uri in client.blobs)


def test_cross_month_report_in_october_is_preserved_when_november_is_empty(client):
    from notifications.digest_snapshot.digest_candidates import cycle_evidence
    target = json.loads(ENV["RUNTIME_TARGET_JSON"])
    report = {"schema_version": "runtime_report.v1", "platform": SOURCE_PLATFORM,
              "strategy_profile": target["strategy_profile"], "service_name": target["service_name"],
              "runtime_target": target, "run_id": "synthetic-run", "dry_run": False,
              "status": "ok", "started_at": "2026-10-31T23:00:00+00:00",
              "summary": {"notification_digest": cycle_evidence(
                  {"execution_status": "no_action", "broker_submission_done": False}, [])}}
    client.blob(f"gs://synthetic-archive/reports/{SOURCE_PLATFORM}/synthetic/2026-10/run.json").upload_from_string(
        json.dumps(report), retry=None)
    assert emit()["uploaded"] is True
    rows = json.loads(candidate(client).body)["runs"]
    assert len(rows) == 1 and rows[0]["actually_ran"] is True
    assert rows[0]["source_run_id"] == "synthetic-run"
    assert rows[0]["business_day"] == "2026-11-01"
