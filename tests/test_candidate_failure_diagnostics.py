"""Heartbeat errors use a fixed vocabulary and never echo provider details."""
import json

import pytest
from google.api_core.exceptions import DeadlineExceeded, Forbidden, PreconditionFailed

from notifications import unified
from scripts import emit_unified_digest_candidates as publisher
from scripts import execution_report_heartbeat as heartbeat
from test_heartbeat_archive_reader import NOW, candidate
from test_publish_digest_before_read import ENV
import test_publish_digest_before_read as prepub

client = prepub.client


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(heartbeat, "_send_telegram", lambda text: True)
    monkeypatch.setattr(publisher, "warn_coverage_unconfirmed", lambda send, locale: send("synthetic alert"))


def result(capsys):
    output = capsys.readouterr()
    assert "synthetic-private" not in output.out + output.err
    assert "gs://" not in output.out + output.err
    value = json.loads(output.out)
    assert value["status"] == "blocked" and value["uploaded"] is False
    assert value["reason"] == "candidate_projection_failed"
    return value


def real_emitter(monkeypatch):
    monkeypatch.setattr(publisher, "emit_completed_candidates", lambda _: unified.emit_completed_candidates(ENV, now=NOW))


@pytest.mark.parametrize("error", [Forbidden("synthetic-private"), TimeoutError("synthetic-private")])
def test_real_archive_errors_are_failures_with_fixed_diagnostics(client, monkeypatch, capsys, error):
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(client, "list_blobs", fail)
    real_emitter(monkeypatch)
    assert publisher.main() == 1
    value = result(capsys)
    assert value["stage"] == "archive_read" and value["error_code"] == "candidate_archive_read_failed"
    assert not any(blob.writes for blob in client.blobs.values())


@pytest.mark.parametrize("stage", ["candidate_read", "candidate_write"])
@pytest.mark.parametrize("error,code", [
    (Forbidden("synthetic-private"), "permission_denied"),
    (TimeoutError("synthetic-private"), "timeout_unknown"),
    (DeadlineExceeded("synthetic-private"), "timeout_unknown"),
    (PreconditionFailed("synthetic-private"), "generation_conflict"),
])
def test_real_candidate_io_has_stage_and_does_not_retry(client, monkeypatch, capsys, stage, error, code):
    blob = client.blob("gs://synthetic-candidates/digest/schwab/2026-11-01/synthetic-slot.json")
    calls = []
    def fail(*args, **kwargs):
        calls.append(kwargs)
        raise error
    monkeypatch.setattr(blob, "reload" if stage == "candidate_read" else "upload_from_string", fail)
    real_emitter(monkeypatch)
    assert publisher.main() == 1
    value = result(capsys)
    assert value["stage"] == stage and value["error_code"] == code
    assert len(calls) == 1 and calls[0]["retry"] is None
    assert blob.writes == 0


def test_malformed_candidate_json_is_a_read_failure(client, monkeypatch, capsys):
    client.blob("gs://synthetic-candidates/digest/schwab/2026-11-01/synthetic-slot.json").upload_from_string(
        '{"synthetic-private":', retry=None)
    real_emitter(monkeypatch)
    assert publisher.main() == 1
    value = result(capsys)
    assert value["stage"] == "candidate_read" and value["error_code"] == "json_invalid"
    assert candidate(client).writes == 1


def test_real_invalid_history_has_merge_stage(client, monkeypatch, capsys):
    client.blob("gs://synthetic-candidates/digest/schwab/2026-11-01/synthetic-slot.json").upload_from_string(
        '{"synthetic-private": true}', retry=None)
    real_emitter(monkeypatch)
    assert publisher.main() == 1
    value = result(capsys)
    assert value["stage"] == "candidate_merge" and value["error_code"] == "notification_candidate_history_unknown"
    assert candidate(client).writes == 1


@pytest.mark.parametrize("code,stage", [
    ("candidate_source_incomplete", "projection"),
    ("candidate_due_source_absent", "schedule_evaluation"),
    ("notification_candidate_scope_conflict", "candidate_merge"),
])
def test_exact_local_codes_are_whitelisted(code, stage, monkeypatch, capsys):
    def fail(env):
        raise ValueError(code)
    monkeypatch.setattr(publisher, "emit_completed_candidates", fail)
    assert publisher.main() == 1
    value = result(capsys)
    assert value["stage"] == stage and value["error_code"] == code


@pytest.mark.parametrize("error", [
    ValueError("candidate_archive_read_failed gs://synthetic-private/object"),
    KeyError("synthetic-private"), RuntimeError({"synthetic-private": "synthetic-value"}),
])
def test_unknown_errors_never_echo_or_partially_match(error, monkeypatch, capsys):
    def fail(env):
        raise error
    monkeypatch.setattr(publisher, "emit_completed_candidates", fail)
    assert publisher.main() == 1
    value = result(capsys)
    assert value["stage"] == "projection" and value["error_code"] == "unexpected_error"


def test_tampered_storage_metadata_cannot_escape_whitelist(monkeypatch, capsys):
    error = unified.CandidateStorageError("synthetic-private", RuntimeError())
    error.category = "synthetic-private"
    monkeypatch.setattr(publisher, "emit_completed_candidates", lambda env: (_ for _ in ()).throw(error))
    assert publisher.main() == 1
    value = result(capsys)
    assert value["stage"] == "projection" and value["error_code"] == "storage_unknown"


@pytest.mark.parametrize("throws", [False, True])
def test_alert_sender_cannot_leak_or_mask_original_error(throws, monkeypatch, capsys):
    def fail(env):
        raise ValueError("candidate_archive_read_failed")
    def send(text):
        import sys
        print("synthetic-private", file=sys.stderr)
        print("gs://synthetic-private/object")
        if throws:
            raise RuntimeError("synthetic-private")
        return False
    monkeypatch.setattr(publisher, "emit_completed_candidates", fail)
    monkeypatch.setattr(heartbeat, "_send_telegram", send)
    assert publisher.main() == 1
    assert result(capsys)["error_code"] == "candidate_archive_read_failed"
