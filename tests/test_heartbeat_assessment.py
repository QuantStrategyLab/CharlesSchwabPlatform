"""Exercise machine-readable heartbeat facts without cloud or notification IO."""
from __future__ import annotations

import datetime as dt
import json
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from scripts import execution_report_heartbeat as heartbeat

NOW = dt.datetime(2026, 10, 5, 13, tzinfo=dt.timezone.utc)


@pytest.fixture
def assessment(monkeypatch, tmp_path):
    for key in list(os.environ):
        if key.startswith(("RUNTIME_HEARTBEAT_", "RUNTIME_TARGET_", "CLOUD_RUN_")):
            monkeypatch.delenv(key)
    path = tmp_path / "assessment.json"
    monkeypatch.setenv("RUNTIME_HEARTBEAT_ASSESSMENT_PATH", str(path))
    monkeypatch.setattr(heartbeat, "_runtime_target_enabled", lambda: True)
    monkeypatch.setattr(heartbeat, "runtime_target_configuration_present", lambda env: False)
    monkeypatch.setattr(heartbeat, "load_runtime_targets", lambda env: [])
    monkeypatch.setattr(heartbeat, "_hydrate_runtime_target_schedules", lambda targets, **kw: targets)
    monkeypatch.setattr(heartbeat, "filter_due_targets", lambda *args, **kw: ([], False))
    monkeypatch.setattr(heartbeat, "_heartbeat_skip_reason_for_schedule", lambda *args: None)
    monkeypatch.setattr(heartbeat, "_load_required_services", lambda: [])
    monkeypatch.setattr(heartbeat, "_report_globs", lambda *args: ["gs://example/reports"])
    monkeypatch.setattr(heartbeat, "_list_gcs_objects", lambda *args, **kw: [
        {"url": "gs://example/report.json", "metadata": {"updated": NOW.isoformat()}}
    ])
    monkeypatch.setattr(heartbeat, "_cat_gcs_json", lambda *args, **kw: {
        "started_at": "2026-10-05T12:00:00Z", "finished_at": "2026-10-05T12:01:00Z",
        "status": "ok", "dry_run": False, "errors": [],
        "summary": {"execution_status": "no_action", "orders_submitted_count": 0},
        "diagnostics": {},
    })
    monkeypatch.setattr(heartbeat, "_send_normal_heartbeat", lambda *args, **kw: 0)
    monkeypatch.setattr(heartbeat, "_send_telegram", lambda *args, **kw: True)
    monkeypatch.setattr(heartbeat, "publish_receipt", lambda callback, **kw: callback())
    monkeypatch.setattr(heartbeat, "unified_enabled", lambda: False)
    return path


@pytest.mark.parametrize("kind,expected", [
    ("healthy", ("healthy", "acceptable_report_observed")),
    ("disabled", ("not_applicable", "runtime_target_disabled")),
    ("not_due", ("not_due", "no_due_runtime_schedule")),
    ("pending", ("attention", "order_status_unknown")),
    ("unreadable", ("unavailable", "report_read_unavailable")),
    ("missing", ("attention", "acceptable_report_missing")),
])
def test_assessment_preserves_health_distinctions(monkeypatch, assessment, kind, expected):
    if kind == "disabled":
        monkeypatch.setattr(heartbeat, "_runtime_target_enabled", lambda: False)
    elif kind == "not_due":
        monkeypatch.setattr(heartbeat, "_heartbeat_skip_reason_for_schedule", lambda *args: "not due")
    elif kind == "pending":
        monkeypatch.setattr(heartbeat, "_cat_gcs_json", lambda *args, **kw: {
            "status": "ok", "summary": {"execution_status": "pending_reconciliation"},
        })
    elif kind == "unreadable":
        monkeypatch.setattr(heartbeat, "_cat_gcs_json", lambda *args, **kw: None)
    elif kind == "missing":
        monkeypatch.setattr(heartbeat, "_list_gcs_objects", lambda *args, **kw: [])
    heartbeat.main(NOW)
    assert json.loads(assessment.read_text()) == dict(zip(("status", "reason"), expected))


@pytest.mark.parametrize("content,exit_code,expected", [
    ('{"status":"healthy"}', 0, "pass"),
    ('{"status":"healthy"}', 1, "unavailable"),
    ('{"status":"not_applicable"}', 0, "not_applicable"),
    ('{"status":"not_due"}', 0, "not_due"),
    ('{"status":"attention"}', 0, "attention"),
    ('{"status":"unavailable"}', 1, "unavailable"),
    ('{"status":"unexpected"}', 0, "unavailable"),
    ("null", 0, "unavailable"),
    ("invalid", 0, "unavailable"),
    (None, 0, "unavailable"),
])
def test_lifecycle_reads_actual_assessment_without_parsing_logs(tmp_path, content, exit_code, expected):
    workflow = (Path(__file__).parents[1] / ".github/workflows/runtime-target-lifecycle.yml").read_text()
    script = re.search(r'status="\$\(python3 - "\$exit_code" <<\'PY\'\n(.*?)\n          PY', workflow, re.S)
    assert script is not None
    path = tmp_path / "assessment.json"
    if content is not None:
        path.write_text(content)
    result = subprocess.run(
        [sys.executable, "-", str(exit_code)], input=textwrap.dedent(script.group(1)),
        text=True, capture_output=True,
        env={**os.environ, "RUNTIME_HEARTBEAT_ASSESSMENT_PATH": str(path)},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected
