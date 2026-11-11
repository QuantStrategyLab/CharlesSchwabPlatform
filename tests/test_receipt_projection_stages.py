from types import SimpleNamespace

import pytest

from notifications import unified
from notifications.digest_snapshot.unified_contract import CandidatePersistenceError


def test_receipt_projection_stages_distinguish_failures_and_keep_business_run(monkeypatch):
    observed, logs = [], []
    fact = {"platform_id": "schwab", "opaque_account_uid": "opaque", "target_id": "target",
            "strategy_profile": "synthetic", "source_run_id": "business-run"}
    monkeypatch.setattr(unified, "_log_failure", logs.append)
    monkeypatch.setattr(unified, "_receipt_event", lambda report, kind, reason: SimpleNamespace(to_candidate=lambda: fact))
    monkeypatch.setattr(unified, "GCSNotificationStore", lambda: SimpleNamespace(persist=lambda value: observed.append(value) or True))
    def fail_projection(env, **kwargs):
        observed.append(kwargs)
        raise ValueError("synthetic")
    monkeypatch.setattr(unified, "emit_completed_candidates", fail_projection)
    with pytest.raises(CandidatePersistenceError):
        unified.publish_receipt(lambda: None, kind="healthy_heartbeat", reason="observed", source_report={"run_id": "business-run"})
    assert logs == ["receipt_archive_projection_unconfirmed"]
    assert observed[0]["source_run_id"] == "business-run"
    assert observed[1]["required_runs"][0][-1] == "business-run"
    assert observed[1]["observed_events"][0]["source_run_id"] == "business-run"


def test_receipt_source_identity_failure_is_distinct(monkeypatch):
    logs = []
    monkeypatch.setattr(unified, "_log_failure", logs.append)
    monkeypatch.setattr(unified, "_receipt_event", lambda *args: (_ for _ in ()).throw(ValueError("synthetic")))
    with pytest.raises(CandidatePersistenceError):
        unified.publish_receipt(lambda: None, kind="healthy_heartbeat", reason="observed", source_report={})
    assert logs == ["receipt_source_identity_unavailable"]


def test_uncovered_run_blocks_all_writes_across_target_bindings(monkeypatch):
    import json
    from notifications.digest_snapshot import digest_archive
    rows = [dict(platform_id="schwab", opaque_account_uid="other", target_id="other",
                 strategy_profile="synthetic", source_run_id="other-run", actually_ran=True)]
    def emit(_env, *, upload, **_kwargs):
        upload("gs://synthetic/one", json.dumps({"runs": rows}))
        return {"uploaded": True}
    monkeypatch.setattr(digest_archive, "emit", emit)
    uploads = []
    with pytest.raises(ValueError, match="notification_observed_run_not_covered"):
        unified.emit_completed_candidates({}, required_runs=[("schwab", "expected", "expected", "synthetic", "expected-run")],
            upload=lambda uri, body: uploads.append((uri, body)))
    assert uploads == []


def test_required_runs_across_targets_are_validated_before_upload(monkeypatch):
    import json
    from notifications.digest_snapshot import digest_archive
    rows = [dict(platform_id="schwab", opaque_account_uid=name, target_id=name,
                 strategy_profile="synthetic", source_run_id=name + "-run", actually_ran=True)
            for name in ("one", "two")]
    def emit(_env, *, upload, **_kwargs):
        for row in rows:
            upload("gs://synthetic/" + row["target_id"], json.dumps({"runs": [row]}))
        return {"uploaded": True}
    monkeypatch.setattr(digest_archive, "emit", emit)
    uploads = []
    required = [("schwab", name, name, "synthetic", name + "-run") for name in ("one", "two")]
    unified.emit_completed_candidates({}, required_runs=required, upload=lambda uri, body: uploads.append(uri))
    assert uploads == ["gs://synthetic/one", "gs://synthetic/two"]


def test_default_persistence_uses_the_matching_binding_for_each_target(monkeypatch):
    import json
    from notifications.digest_snapshot import digest_archive
    rows = [dict(platform_id="schwab", opaque_account_uid=name, target_id=name,
                 strategy_profile="synthetic", source_run_id=name + "-run", actually_ran=True,
                 business_day="2026-10-30") for name in ("one", "two")]
    def emit(_env, *, upload, **_kwargs):
        for row in rows:
            upload(f"gs://synthetic/candidates/schwab/2026-10-30/{row['target_id']}.json", json.dumps({"runs": [row]}))
        return {"uploaded": True}
    monkeypatch.setattr(digest_archive, "emit", emit)
    persisted = []
    monkeypatch.setattr(unified, "persist_projected_candidate", lambda payload, *, env: persisted.append((payload["runs"][0]["target_id"], env["DIGEST_STORE_TARGET"])))
    bindings = [{"RUNTIME_TARGET_JSON": json.dumps({"platform_id": "schwab", "strategy_profile": "synthetic"}),
                 "DIGEST_OPAQUE_ACCOUNT_UID": name, "DIGEST_TARGET_ID": name, "DIGEST_STORE_TARGET": name}
                for name in ("one", "two")]
    env = {"DIGEST_CANDIDATES_GCS_URI": "gs://synthetic/candidates", "DIGEST_TARGET_BINDINGS_JSON": json.dumps(bindings)}
    unified.emit_completed_candidates(env)
    assert persisted == [("one", "one"), ("two", "two")]


def test_ambiguous_default_binding_fails_before_any_write(monkeypatch):
    import json
    from notifications.digest_snapshot import digest_archive
    row = dict(platform_id="schwab", opaque_account_uid="one", target_id="one", strategy_profile="synthetic",
               source_run_id="one-run", actually_ran=True, business_day="2026-10-30")
    def emit(_env, *, upload, **_kwargs):
        upload("gs://synthetic/candidates/schwab/2026-10-30/one.json", json.dumps({"runs": [row]}))
        return {"uploaded": True}
    monkeypatch.setattr(digest_archive, "emit", emit)
    persisted = []
    monkeypatch.setattr(unified, "persist_projected_candidate", lambda *args, **kwargs: persisted.append(True))
    binding = {"RUNTIME_TARGET_JSON": json.dumps({"platform_id": "schwab", "strategy_profile": "synthetic"}),
               "DIGEST_OPAQUE_ACCOUNT_UID": "one", "DIGEST_TARGET_ID": "one", "DIGEST_STORE_TARGET": "one"}
    env = {"DIGEST_CANDIDATES_GCS_URI": "gs://synthetic/candidates", "DIGEST_TARGET_BINDINGS_JSON": json.dumps([binding, binding])}
    with pytest.raises(unified.ReceiptProjectionStageError, match="receipt_target_binding_unavailable"):
        unified.emit_completed_candidates(env)
    assert persisted == []
