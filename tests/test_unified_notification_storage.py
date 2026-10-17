import copy
import json

import pytest
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage

from notifications import unified
from test_unified_notifications import NOW, event


@pytest.fixture
def fake_gcs(monkeypatch):
    objects, calls = {}, []
    failures = {"lose_marker_response": False}

    class Blob:
        def __init__(self, uri):
            self.uri = uri
            self.generation = None

        def reload(self, **kwargs):
            calls.append(("reload", self.uri, kwargs))
            if self.uri not in objects:
                raise NotFound("synthetic missing object")
            self.generation = objects[self.uri][0]

        def download_as_text(self, **kwargs):
            calls.append(("download", self.uri, kwargs))
            generation, body = objects[self.uri]
            if kwargs.get("if_generation_match", generation) != generation:
                raise PreconditionFailed("synthetic conflict")
            return body

        def upload_from_string(self, body, **kwargs):
            calls.append(("upload", self.uri, kwargs))
            current = objects.get(self.uri, (0, None))[0]
            if kwargs.get("if_generation_match", current) != current:
                raise PreconditionFailed("synthetic conflict")
            objects[self.uri] = (current + 1, body)
            if failures["lose_marker_response"] and "/notification-markers/" in self.uri:
                failures["lose_marker_response"] = False
                raise TimeoutError("synthetic lost write response")

    monkeypatch.setattr(storage.Blob, "from_string", lambda uri, client: Blob(uri))
    env = {unified.FLAG: "true", "DIGEST_CANDIDATES_GCS_URI": "gs://synthetic-store/candidates"}
    return objects, calls, failures, env


def test_real_gcs_adapter_uses_shared_atomic_claim_before_one_send(fake_gcs):
    objects, calls, _, env = fake_gcs
    sends = []
    for run in ("runtime-a", "monitor-b"):
        # Distinct adapters represent cold workers sharing the same cloud store.
        store = unified.GCSNotificationStore(env, client=object())
        unified.route(event(run=run), env=env, store=store, legacy=lambda: sends.append("alert") or True)
    assert sends == ["alert"]
    marker_writes = [kwargs for method, uri, kwargs in calls if method == "upload" and "/notification-markers/" in uri]
    assert marker_writes and all("if_generation_match" in kwargs for kwargs in marker_writes)
    assert all(kwargs.get("retry") is None for _, _, kwargs in calls)
    assert any(json.loads(body).get("delivery_status") == "delivered" for _, body in objects.values())


def test_unknown_claim_response_is_never_retried_or_sent(fake_gcs):
    _, _, failures, env = fake_gcs
    failures["lose_marker_response"] = True
    sends = []
    for run in ("a", "b"):
        unified.route(event(run=run), env=env, store=unified.GCSNotificationStore(env, client=object()),
            legacy=lambda: sends.append("unsafe") or True)
    assert not sends


def test_immutable_candidate_readback_proves_duplicates_and_rejects_conflict(fake_gcs):
    _, _, _, env = fake_gcs
    store = unified.GCSNotificationStore(env, client=object())
    fact = event("routine_cycle", order_count=0).to_candidate()
    assert store.persist(fact, {"runs": []})
    assert store.persist(fact, {"runs": []})
    changed = {**fact, "order_count": 1}
    with pytest.raises(ValueError, match="candidate_conflict"):
        store.persist(changed, {"runs": []})


@pytest.mark.parametrize("missing_source", [False, True])
@pytest.mark.parametrize("duplicate_source", [False, True, "conflict"])
def test_completed_archive_counts_runs_once_and_attaches_noncounting_facts(missing_source, duplicate_source):
    target = {"platform_id": "schwab", "strategy_profile": "strategy-a", "service_name": "synthetic-service",
        "account_scope": "synthetic", "account_selector": ["synthetic"], "deployment_selector": "synthetic-slot", "dry_run_only": False}
    env = {unified.FLAG: "true", "QSL_ROUTINE_NOTIFICATION_MODE": "immediate", "RUNTIME_TARGET_ENABLED": "true",
        "RUNTIME_TARGET_JSON": json.dumps(target), "DIGEST_STORE_TARGET": "synthetic-slot",
        "DIGEST_OPAQUE_ACCOUNT_UID": "opaque-a", "DIGEST_TARGET_ID": "target-a",
        "EXECUTION_REPORT_GCS_URI": "gs://synthetic-store/archive", "DIGEST_CANDIDATES_GCS_URI": "gs://synthetic-store/candidates"}
    source = {"schema_version": "runtime_report.v1", "platform": "schwab", "strategy_profile": "strategy-a",
        "service_name": "synthetic-service", "runtime_target": target, "dry_run": False, "status": "ok",
        "run_id": "source-run", "started_at": NOW.isoformat(), "summary": {"notification_digest": {
            "schema_version": "qsl.notification_cycle.v1", "order_count": 0, "fill_count": None, "routine": True, "dry_run": False}}}
    facts = [event("routine_cycle", run="missing-run" if missing_source else "source-run", order_count=0).to_candidate(),
             event("healthy_heartbeat", run="monitor-run").to_candidate()]
    def read(args):
        if args[0] == "ls":
            if "/notification-events/" in args[-1]:
                return [{"url": f"gs://synthetic-store/candidates/schwab/notification-events/2026-10-16/{i}.json"} for i in range(2)]
            return [{"url": "gs://synthetic-store/archive/schwab/strategy-a/2026-10/" + name + ".json", "metadata": {"updated": NOW.isoformat()}}
                for name in (["source", "copy"] if duplicate_source else ["source"])]
        if "/notification-events/" in args[-1]:
            return {"event": facts[int(args[-1].rsplit("/", 1)[-1].split(".")[0])]}
        payload = copy.deepcopy(source)
        if duplicate_source == "conflict" and args[-1].endswith("copy.json"):
            payload["status"] = "error"
        return payload
    uploads = []
    import datetime as dt
    if duplicate_source == "conflict":
        with pytest.raises(ValueError, match="source_run_conflict"):
            unified.emit_completed_candidates(env, now=NOW.replace(hour=23), read=read, upload=lambda *a: uploads.append(a))
        assert not uploads
        return
    result = unified.emit_completed_candidates(env, now=NOW.replace(hour=23), read=read, upload=lambda *a: uploads.append(a))
    assert result["uploaded"] and len(uploads) == 1
    row = json.loads(uploads[0][1])["runs"][0]
    assert row["cycle_count"] == 1 and row["order_count"] == (None if missing_source else 0) and row["fill_count"] is None
    assert row["notification_events"] == facts
    assert row["actually_ran"] is True
    assert row["source_run_ids"] == ["source-run"]
    if missing_source:
        assert row["coverage_status"] == "unknown"
        assert row["reason_code"] == "notification_source_report_missing"


def test_archive_off_delegates_without_any_added_event_reads():
    def forbidden(*a):
        pytest.fail("OFF must not do event/storage I/O")
    assert unified.emit_completed_candidates({}, read=forbidden, upload=forbidden) == {"status": "disabled", "uploaded": False}
