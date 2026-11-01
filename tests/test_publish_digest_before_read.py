from datetime import datetime, timezone
import json
import fnmatch
from types import SimpleNamespace

import pytest
from google.api_core.exceptions import Forbidden, NotFound, PreconditionFailed
from google.cloud import storage

from scripts import publish_digest_before_read as publisher

NOW = datetime(2026, 10, 31, 22, 10, tzinfo=timezone.utc)
PLATFORM = "schwab"
ENV = {
    "DIGEST_PUBLISH_ENABLED": "true", "RUNTIME_TARGET_ENABLED": "true",
    "QSL_ROUTINE_NOTIFICATION_MODE": "daily_digest",
    "RUNTIME_TARGET_JSON": json.dumps({"platform_id": PLATFORM,
        "strategy_profile": "synthetic-strategy", "service_name": "synthetic-service",
        "account_scope": "synthetic-scope", "account_selector": ["synthetic-account"],
        "deployment_selector": "synthetic-deployment", "dry_run_only": False}),
    "DIGEST_CANDIDATES_GCS_URI": "gs://synthetic-candidates/digest",
    "EXECUTION_REPORT_GCS_URI": "gs://synthetic-archive/reports",
    "DIGEST_STORE_TARGET": "synthetic-slot", "DIGEST_OPAQUE_ACCOUNT_UID": "synthetic-uid",
    "DIGEST_TARGET_ID": "synthetic-target",
    "DIGEST_NOTIFICATION_SCHEDULE_JSON": json.dumps({"scheduler": {
        "main_time": "45 15 * * 1-5", "timezone": "America/New_York"}}),
}


class Blob:
    def __init__(self, name):
        self.name, self.body, self.generation, self.size = name, None, None, None
        self.metadata = {}
        self.updated = NOW
        self.writes = 0

    def reload(self, **kwargs):
        assert kwargs == {"retry": None, "timeout": 30}
        if self.body is None:
            raise NotFound("synthetic missing object")

    def download_as_bytes(self, **kwargs):
        self.reload(retry=None, timeout=30)
        assert kwargs["retry"] is None
        if "if_generation_match" in kwargs:
            assert kwargs["if_generation_match"] == self.generation
        return self.body.encode()

    def download_as_text(self, **kwargs):
        return self.download_as_bytes(**kwargs).decode()

    def upload_from_string(self, body, **kwargs):
        assert kwargs["retry"] is None
        if "if_generation_match" in kwargs and kwargs["if_generation_match"] != (self.generation or 0):
            raise PreconditionFailed("synthetic conflict")
        self.body, self.size = body, len(body.encode())
        self.generation = (self.generation or 0) + 1
        self.writes += 1


class Client:
    def __init__(self):
        self.blobs, self.lists = {}, []

    def bucket(self, name):
        return SimpleNamespace(blob=lambda key: self.blob(f"gs://{name}/{key}"))

    def blob(self, uri):
        return self.blobs.setdefault(uri, Blob(uri))

    def list_blobs(self, bucket, **kwargs):
        self.lists.append((bucket, kwargs))
        assert kwargs["retry"] is None and kwargs["max_results"] == 10001
        prefix = f"gs://{bucket}/"
        return [SimpleNamespace(name=uri[len(prefix):], updated=blob.updated)
                for uri, blob in self.blobs.items()
                if uri.startswith(prefix) and fnmatch.fnmatchcase(uri[len(prefix):], kwargs["match_glob"])]


@pytest.fixture
def client(monkeypatch):
    value = Client()
    monkeypatch.setattr(storage, "Client", lambda: value)
    monkeypatch.setattr(storage.Blob, "from_string", lambda uri, **kwargs: value.blob(uri))
    return value


def test_real_projector_publishes_completed_weekend_then_reads_exact_object(client):
    result = publisher.publish(ENV, now=lambda: NOW, client=client)
    assert result == {"status": "published", "business_day": "2026-10-31", "readback_verified": True}
    uri = f"gs://synthetic-candidates/digest/{PLATFORM}/2026-10-31/synthetic-slot.json"
    rows = json.loads(client.blobs[uri].body)["runs"]
    assert rows[0]["actually_ran"] is False
    assert rows[0].get("order_count") is None and rows[0].get("fill_count") is None
    assert client.lists


@pytest.mark.parametrize("hour,minute", [(21, 59), (22, 40), (23, 0), (1, 16)])
def test_early_or_delayed_start_never_calls_emitter(hour, minute):
    now = NOW.replace(hour=hour, minute=minute)
    with pytest.raises(ValueError, match="publication_window_closed"):
        publisher.publish(ENV, now=lambda: now,
                          emit=lambda *a, **kw: pytest.fail("producer must not run"))


@pytest.mark.parametrize("key,value", [
    ("DIGEST_PUBLISH_ENABLED", "false"), ("RUNTIME_TARGET_ENABLED", "false"),
    ("QSL_ROUTINE_NOTIFICATION_MODE", "immediate"),
    ("DIGEST_TARGET_BINDINGS_JSON", "[{}]"), ("DIGEST_STORE_TARGET", ""),
])
def test_configuration_failure_prevents_io(key, value):
    with pytest.raises(ValueError):
        publisher.publish({**ENV, key: value}, now=lambda: NOW,
                          emit=lambda *a, **kw: pytest.fail("producer must not run"))


def test_due_but_absent_does_not_publish_idle(client):
    env = {**ENV, "DIGEST_NOTIFICATION_SCHEDULE_JSON": json.dumps({"scheduler": {
        "main_time": "45 15 * * *", "timezone": "America/New_York"}})}
    with pytest.raises(ValueError, match="candidate_due_source_absent"):
        publisher.publish(env, now=lambda: NOW, client=client)
    assert not any(blob.writes for blob in client.blobs.values())


@pytest.mark.parametrize("body", [
    {"runs": []}, {"runs": [{"actually_ran": False, "business_day": "2026-10-30"}]},
    {"runs": [{"actually_ran": False, "business_day": "2026-10-31", "target_id": "wrong"}]},
])
def test_success_without_valid_readback_fails(client, body):
    def emit(*a, **kw):
        uri = f"gs://synthetic-candidates/digest/{PLATFORM}/2026-10-31/synthetic-slot.json"
        client.blob(uri).upload_from_string(json.dumps(body), retry=None)
        return {"status": "projected", "uploaded": True}
    with pytest.raises(ValueError, match="readback_invalid"):
        publisher.publish(ENV, now=lambda: NOW, client=client, emit=emit)


def test_unconfirmed_upload_never_reports_success(client):
    with pytest.raises(ValueError, match="publication_unconfirmed"):
        publisher.publish(ENV, now=lambda: NOW, client=client,
                          emit=lambda *a, **kw: {"status": "projected", "uploaded": False})


def test_unknown_write_is_not_retried(client):
    calls = []
    def emit(*a, **kw):
        calls.append(1)
        raise TimeoutError("synthetic sensitive transport detail")
    with pytest.raises(TimeoutError):
        publisher.publish(ENV, now=lambda: NOW, client=client, emit=emit)
    assert len(calls) == 1


def test_read_permission_denial_is_not_missing(client, monkeypatch):
    monkeypatch.setattr(client, "list_blobs", lambda *a, **kw: (_ for _ in ()).throw(Forbidden("private")))
    with pytest.raises(Forbidden):
        publisher.publish(ENV, now=lambda: NOW, client=client)
    assert not any(blob.writes for blob in client.blobs.values())


def test_late_finish_cannot_be_reported_as_timely(client):
    moments = iter([NOW, NOW.replace(hour=22, minute=55)])
    with pytest.raises(ValueError, match="deadline_missed"):
        publisher.publish(ENV, now=lambda: next(moments), client=client)


def test_main_redacts_exception_and_clears_watchdog(monkeypatch, capsys):
    alarms = []
    monkeypatch.setattr(publisher.signal, "signal", lambda *a: None)
    monkeypatch.setattr(publisher.signal, "alarm", alarms.append)
    monkeypatch.setattr(publisher, "publish", lambda *a: (_ for _ in ()).throw(RuntimeError("private-value")))
    assert publisher.main() == 1
    output = capsys.readouterr().out
    assert "private-value" not in output
    assert json.loads(output)["status"] == "failed"
    assert alarms == [600, 0]


def test_watchdog_escapes_producer_exception_handlers(monkeypatch, capsys):
    monkeypatch.setattr(publisher.signal, "signal", lambda *a: None)
    monkeypatch.setattr(publisher.signal, "alarm", lambda *a: None)
    def emit(*a):
        try:
            raise publisher.DeadlineExceeded()
        except Exception:
            pytest.fail("deadline must escape producer catch-all")
    monkeypatch.setattr(publisher, "publish", emit)
    assert publisher.main() == 1
    assert json.loads(capsys.readouterr().out)["status"] == "failed"


def test_existing_business_report_uses_native_archive_reader_and_verified_publication(client):
    from notifications.digest_snapshot.digest_candidates import cycle_evidence
    env = {**ENV, "SCHWAB_UNIFIED_NOTIFICATION_ENABLED": "true",
           "RUNTIME_HEARTBEAT_REPORT_PLATFORM": "charles_schwab",
           "IBKR_NOTIFICATION_MARKER_GCS_URI": "gs://synthetic-candidates/markers"}
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    report = {"schema_version": "runtime_report.v1", "platform": "charles_schwab",
              "strategy_profile": target["strategy_profile"], "service_name": target["service_name"],
              "runtime_target": target, "run_id": "synthetic-run", "dry_run": False,
              "status": "ok", "started_at": "2026-10-31T20:00:00+00:00",
              "summary": {"notification_digest": cycle_evidence(
                  {"execution_status": "no_action", "broker_submission_done": False}, [])}}
    archive = client.blob("gs://synthetic-archive/reports/charles_schwab/synthetic/2026-10/run.json")
    archive.upload_from_string(json.dumps(report), retry=None)
    result = publisher.publish(env, now=lambda: NOW, client=client)
    assert result["readback_verified"] is True
    uri = f"gs://synthetic-candidates/digest/{PLATFORM}/2026-10-31/synthetic-slot.json"
    rows = json.loads(client.blobs[uri].body)["runs"]
    assert rows and all(row["actually_ran"] is True for row in rows)
    assert all(row["business_day"] == "2026-10-31" for row in rows)


@pytest.mark.parametrize("key,value", [
    ("platform_id", "another-platform"), ("service_name", ""),
    ("account_scope", None), ("dry_run_only", "false"),
    ("account_selector", []), ("account_selector", "synthetic-account"),
])
def test_invalid_runtime_identity_is_rejected_before_idle_projection(key, value):
    target = {**json.loads(ENV["RUNTIME_TARGET_JSON"]), key: value}
    with pytest.raises(ValueError, match="target_invalid"):
        publisher.publish({**ENV, "RUNTIME_TARGET_JSON": json.dumps(target)}, now=lambda: NOW,
                          emit=lambda *a, **kw: pytest.fail("must not project an invalid target"))
