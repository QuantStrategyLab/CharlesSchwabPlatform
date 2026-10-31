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
