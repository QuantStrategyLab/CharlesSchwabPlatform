from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import reconcile_cloud_runtime as runtime  # noqa: E402


def _completed(
    command: list[str], *, stdout: str = "", stderr: str = "", returncode: int = 0
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr=stderr)


class ReconcileCloudRuntimeTests(unittest.TestCase):
    def test_reconcile_traffic_updates_to_latest_ready_revision(self) -> None:
        service = "charles-schwab-service"
        revision = "charles-schwab-service-00002"
        target_sha = "abc123def456"
        env = {
            "SYNC_PLAN_JSON": json.dumps({"targets": [{"service_name": service}]}),
            "CLOUD_RUN_SERVICE": "ignored-by-plan",
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "GITHUB_SHA": target_sha,
        }

        service_payload_initial = {
            "status": {
                "latestReadyRevisionName": revision,
                "traffic": [{"revisionName": "charles-schwab-service-00001", "percent": 100}],
            }
        }
        service_payload_final = {
            "status": {
                "latestReadyRevisionName": revision,
                "traffic": [{"revisionName": revision, "percent": 100}],
            }
        }
        revision_payload = {"metadata": {"labels": {"commit-sha": target_sha}}}
        service_describes = [service_payload_initial, service_payload_initial, service_payload_final]
        commands: list[list[str]] = []

        def fake_run(command, text, capture_output, check):
            commands.append(command)
            if command[:4] == ["gcloud", "run", "services", "describe"]:
                payload = service_describes.pop(0)
                return _completed(command, stdout=json.dumps(payload))
            if command[:4] == ["gcloud", "run", "revisions", "describe"]:
                return _completed(command, stdout=json.dumps(revision_payload))
            if command[:4] == ["gcloud", "run", "services", "update-traffic"]:
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run), patch.object(runtime.time, "sleep"):
            runtime.reconcile_traffic(env)

        self.assertIn(
            [
                "gcloud",
                "run",
                "services",
                "update-traffic",
                service,
                "--project",
                "charlesschwabquant",
                "--region",
                "us-central1",
                "--to-latest",
                "--quiet",
            ],
            commands,
        )
        self.assertEqual(commands[0][:4], ["gcloud", "run", "services", "describe"])
        self.assertEqual(commands[1][:4], ["gcloud", "run", "revisions", "describe"])

    def test_cleanup_schedulers_deletes_only_whitelisted_legacy_jobs(self) -> None:
        service = "charles-schwab-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps({"targets": [{"service_name": service}]}),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
        }
        existing_jobs = {
            "charles-schwab-service-probe-scheduler",
            "charles-schwab-probe-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                job_name = command[4]
                if job_name in existing_jobs:
                    return _completed(command)
                return _completed(command, returncode=1, stderr="NOT_FOUND: job does not exist")
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertEqual(
            deleted_jobs,
            [
                "charles-schwab-service-probe-scheduler",
                "charles-schwab-probe-scheduler",
            ],
        )
