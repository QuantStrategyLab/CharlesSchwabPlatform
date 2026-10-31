"""Completed live archive projection excludes identifiable non-execution stages."""
from copy import deepcopy
from datetime import datetime, timezone
import json

import pytest

from notifications import unified


def archive():
    target = dict(platform_id="schwab", strategy_profile="synthetic", service_name="synthetic-service",
                  account_scope="synthetic", account_selector=["synthetic"], deployment_selector="synthetic", dry_run_only=False)
    env = dict(RUNTIME_TARGET_ENABLED="true", RUNTIME_TARGET_JSON=json.dumps(target),
               EXECUTION_REPORT_GCS_URI="gs://synthetic/archive", DIGEST_CANDIDATES_GCS_URI="gs://synthetic/candidates",
               DIGEST_STORE_TARGET="synthetic", DIGEST_OPAQUE_ACCOUNT_UID="synthetic", DIGEST_TARGET_ID="synthetic")
    live = dict(schema_version="runtime_report.v1", platform="schwab", strategy_profile="synthetic",
                service_name="synthetic-service", runtime_target=target, dry_run=False, status="ok",
                run_id="live", started_at="2026-10-30T19:45:00+00:00", summary={"notification_digest": {
                    "schema_version": "qsl.notification_cycle.v1", "order_count": 0, "fill_count": None,
                    "routine": True, "dry_run": False}})
    preview = deepcopy(live)
    preview.update(run_id="preview", dry_run=True)
    preview["summary"]["notification_digest"].update(routine=False, dry_run=True)
    probe = deepcopy(live)
    probe.update(run_id="probe", summary={"account_observation": {}, "buying_power": 1, "total_equity": 1})
    return env, live, preview, probe


def export(env, reports, **kwargs):
    def read(args):
        if args[0] == "ls":
            return [{"url": f"gs://synthetic/archive/charles_schwab/synthetic/2026-10/{i}.json",
                     "metadata": {"updated": r["started_at"]}} for i, r in enumerate(reports)]
        return deepcopy(reports[int(args[-1].rsplit("/", 1)[1].split(".")[0])])
    uploads = []
    unified.emit_completed_candidates(env, now=datetime(2026, 10, 31, 1, tzinfo=timezone.utc),
        read=read, upload=lambda uri, body: uploads.append(json.loads(body)), **kwargs)
    return uploads


def test_live_window_with_preview_and_probes_covers_only_live_run():
    env, live, preview, probe = archive()
    uploads = export(env, [probe, preview, live])
    rows = uploads[0]["runs"]
    assert len(rows) == 1 and rows[0]["source_run_id"] == "live"
    assert rows[0]["cycle_count"] == 1 and rows[0]["fill_count"] is None


def test_missing_live_evidence_is_not_treated_as_a_probe():
    env, live, _, probe = archive()
    live["summary"] = {}
    rows = export(env, [probe, live])[0]["runs"]
    assert len(rows) == 1 and rows[0]["source_run_id"] == "live"
    assert rows[0]["status"] == "alert" and rows[0]["order_count"] is None


def test_excluded_preview_cannot_satisfy_required_live_coverage():
    env, live, preview, probe = archive()
    required = ("schwab", "synthetic", "synthetic", "synthetic", "preview")
    with pytest.raises(ValueError, match="notification_observed_run_not_covered"):
        export(env, [probe, preview, live], required_runs=[required])
