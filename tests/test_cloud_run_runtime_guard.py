from __future__ import annotations

import datetime as dt
import json
import re
import subprocess

from scripts import cloud_run_runtime_guard as guard


def test_scheduler_job_pattern_includes_service_alias():
    pattern = guard._scheduler_job_pattern_for_services(["charles-schwab-service"])

    assert re.search(pattern, "charles-schwab-service-scheduler")
    assert re.search(pattern, "charles-schwab-scheduler")
    assert not re.search(pattern, "other-platform-scheduler")


def test_telegram_token_falls_back_to_secret_manager(monkeypatch):
    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TG_TOKEN", raising=False)
    monkeypatch.setenv("TELEGRAM_TOKEN_SECRET_NAME", "platform-telegram-token")
    monkeypatch.setenv("GCP_PROJECT_ID", "charlesschwabquant")
    observed = {}

    def fake_run_gcloud(command):
        observed["command"] = command
        return subprocess.CompletedProcess(command, 0, stdout="secret-token\n", stderr="")

    monkeypatch.setattr(guard, "_run_gcloud", fake_run_gcloud)

    assert guard._telegram_token() == "secret-token"
    assert observed["command"] == [
        "gcloud",
        "secrets",
        "versions",
        "access",
        "latest",
        "--secret",
        "platform-telegram-token",
        "--project",
        "charlesschwabquant",
    ]


def test_cloud_run_log_since_uses_latest_ready_revision(monkeypatch):
    monkeypatch.setenv("CLOUD_RUN_REGION", "us-central1")
    observed = []

    def fake_run_gcloud(command):
        observed.append(command)
        if command[1:4] == ["run", "services", "describe"]:
            payload = {"status": {"latestReadyRevisionName": "charles-schwab-service-00002"}}
        else:
            payload = {"metadata": {"creationTimestamp": "2026-07-01T06:50:04.123Z"}}
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr(guard, "_run_gcloud", fake_run_gcloud)

    fallback = dt.datetime(2026, 7, 1, 6, 0, tzinfo=dt.timezone.utc)
    result = guard._cloud_run_log_since("charlesschwabquant", "charles-schwab-service", fallback)

    assert result == dt.datetime(2026, 7, 1, 6, 50, 4, 123000, tzinfo=dt.timezone.utc)
    assert observed[0] == [
        "gcloud",
        "run",
        "services",
        "describe",
        "charles-schwab-service",
        "--project",
        "charlesschwabquant",
        "--region",
        "us-central1",
        "--format=json",
    ]
    assert observed[1][1:5] == ["run", "revisions", "describe", "charles-schwab-service-00002"]


def test_region_for_service_prefers_target_region(monkeypatch):
    monkeypatch.setenv("CLOUD_RUN_REGION", "us-central1")
    monkeypatch.setenv(
        "CLOUD_RUN_SERVICE_TARGETS_JSON",
        json.dumps(
            {
                "targets": [
                    {"service": "charles-schwab-service", "region": "asia-east1"},
                ]
            }
        ),
    )

    assert guard._region_for_service("charles-schwab-service") == "asia-east1"


def test_load_services_ignores_disabled_runtime_targets(monkeypatch):
    monkeypatch.delenv("RUNTIME_GUARD_CLOUD_RUN_SERVICES", raising=False)
    monkeypatch.delenv("CLOUD_RUN_SERVICES", raising=False)
    monkeypatch.delenv("CLOUD_RUN_SERVICE", raising=False)
    monkeypatch.setenv(
        "CLOUD_RUN_SERVICE_TARGETS_JSON",
        json.dumps(
            {
                "targets": [
                    {"service": "enabled-service", "RUNTIME_TARGET_ENABLED": "true"},
                    {"service": "disabled-service", "RUNTIME_TARGET_ENABLED": "false"},
                    {"service": "disabled-lower-service", "runtime_target_enabled": "false"},
                ]
            }
        ),
    )

    assert guard._load_services() == ["enabled-service"]


def test_scheduler_entry_since_uses_matching_service_revision_window():
    fallback = dt.datetime(2026, 7, 1, 1, 0, tzinfo=dt.timezone.utc)
    service_since = dt.datetime(2026, 7, 1, 2, 0, tzinfo=dt.timezone.utc)
    entry = {"resource": {"labels": {"job_id": "enabled-service-scheduler"}}}

    assert (
        guard._scheduler_entry_since(entry, {"enabled-service": service_since}, fallback)
        == service_since
    )
    assert (
        guard._scheduler_entry_since(entry, {"other-service": service_since}, fallback)
        == fallback
    )
