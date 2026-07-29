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
    def test_select_sync_target_matches_current_service_in_multi_target_plan(self) -> None:
        current_service = "charles-schwab-secondary-service"
        plan = {
            "targets": [
                {"service_name": "charles-schwab-primary-service", "env": {}},
                {"service_name": current_service, "env": {"TARGET": "current"}},
            ]
        }

        target = runtime.select_sync_target(plan, current_service)

        self.assertEqual(target["env"], {"TARGET": "current"})

    def test_select_sync_target_rejects_missing_service(self) -> None:
        plan = {
            "targets": [
                {"service_name": "charles-schwab-primary-service", "env": {}},
            ]
        }

        with self.assertRaisesRegex(RuntimeError, "Expected exactly one sync target"):
            runtime.select_sync_target(plan, "charles-schwab-missing-service")

    def test_select_sync_target_rejects_duplicate_service(self) -> None:
        service = "charles-schwab-service"
        plan = {
            "targets": [
                {"service_name": service, "env": {}},
                {"service_name": service, "env": {}},
            ]
        }

        with self.assertRaisesRegex(RuntimeError, "Expected exactly one sync target"):
            runtime.select_sync_target(plan, service)

    def test_build_monitor_targets_skips_disabled_services(self) -> None:
        services = (
            "charles-schwab-primary-service",
            "charles-schwab-secondary-service",
        )
        env = {
            "SYNC_PLAN_JSON": json.dumps(
                {
                    "targets": [
                        {
                            "service_name": services[0],
                            "strategy_profile": "primary",
                            "env": {
                                "RUNTIME_TARGET_JSON": json.dumps(
                                    {"account_scope": "primary"}
                                ),
                                "RUNTIME_TARGET_ENABLED": "true",
                            },
                        },
                        {
                            "service_name": services[1],
                            "strategy_profile": "secondary",
                            "env": {
                                "RUNTIME_TARGET_JSON": json.dumps(
                                    {"account_scope": "secondary"}
                                ),
                                "RUNTIME_TARGET_ENABLED": "false",
                            },
                        },
                    ]
                }
            ),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
        }

        described_services: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "run", "services", "describe"]:
                service = command[4]
                described_services.append(service)
                return _completed(
                    command,
                    stdout=json.dumps(
                        {"status": {"url": f"https://{service}.example.invalid"}}
                    ),
                )
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            payload = runtime.build_monitor_targets(env)

        self.assertEqual(
            [target["service_name"] for target in payload["targets"]],
            [services[0]],
        )
        self.assertEqual(described_services, [services[0]])

    def test_build_monitor_targets_uses_each_service_region(self) -> None:
        targets = (
            ("charles-schwab-primary-service", "us-east1"),
            ("charles-schwab-secondary-service", "europe-west1"),
        )
        env = {
            "SYNC_PLAN_JSON": json.dumps(
                {
                    "targets": [
                        {
                            "service_name": service,
                            "region": region,
                            "env": {"RUNTIME_TARGET_ENABLED": "true"},
                        }
                        for service, region in targets
                    ]
                }
            ),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
        }
        described_regions: dict[str, str] = {}

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "run", "services", "describe"]:
                service = command[4]
                described_regions[service] = command[command.index("--region") + 1]
                return _completed(
                    command,
                    stdout=json.dumps(
                        {"status": {"url": f"https://{service}.example.invalid"}}
                    ),
                )
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.build_monitor_targets(env)

        self.assertEqual(described_regions, dict(targets))

    def test_service_name_matches_current_service_in_multi_target_plan(self) -> None:
        current_service = "charles-schwab-secondary-service"
        env = {
            "CLOUD_RUN_SERVICE": current_service,
            "SYNC_PLAN_JSON": json.dumps(
                {
                    "targets": [
                        {"service_name": "charles-schwab-primary-service"},
                        {"service_name": current_service},
                    ]
                }
            ),
        }

        self.assertEqual(runtime._service_name(env), current_service)

    def test_service_name_rejects_configured_service_missing_from_plan(self) -> None:
        env = {
            "CLOUD_RUN_SERVICE": "charles-schwab-missing-service",
            "SYNC_PLAN_JSON": json.dumps(
                {"targets": [{"service_name": "charles-schwab-service"}]}
            ),
        }

        with self.assertRaisesRegex(RuntimeError, "does not match any SYNC_PLAN_JSON target"):
            runtime._service_name(env)

    def test_reconcile_traffic_updates_to_latest_ready_revision(self) -> None:
        service = "charles-schwab-service"
        revision = "charles-schwab-service-00002"
        target_sha = "abc123def456"
        env = {
            "SYNC_PLAN_JSON": json.dumps({"targets": [{"service_name": service}]}),
            "CLOUD_RUN_SERVICE": service,
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
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
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        existing_jobs = {
            "charles-schwab-service-probe-scheduler",
            "charles-schwab-service-precheck-scheduler",
            "charles-schwab-probe-scheduler",
            "charles-schwab-precheck-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        marked_jobs = {
            "charles-schwab-service-probe-scheduler",
            "charles-schwab-service-precheck-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                job_name = command[4]
                if job_name in existing_jobs:
                    return _completed(
                        command,
                        stdout=(
                            runtime.DIRECT_MONITOR_SCHEDULER_DESCRIPTION
                            if "--format=value(description)" in command
                            and job_name in marked_jobs
                            else "ENABLED"
                            if "--format=value(state)" in command
                            and job_name in marked_jobs
                            else ""
                        ),
                    )
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
                "charles-schwab-probe-scheduler",
                "charles-schwab-precheck-scheduler",
                "schwab-monitor-dispatcher-scheduler",
            ],
        )

    def test_cleanup_schedulers_keeps_dispatcher_until_direct_jobs_exist(self) -> None:
        service = "charles-schwab-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps({"targets": [{"service_name": service}]}),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        existing_jobs = {
            "charles-schwab-service-probe-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        marked_jobs = {"charles-schwab-service-probe-scheduler"}
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                stdout = (
                    runtime.DIRECT_MONITOR_SCHEDULER_DESCRIPTION
                    if "--format=value(description)" in command
                    and command[4] in marked_jobs
                    else "ENABLED"
                    if "--format=value(state)" in command
                    and command[4] in marked_jobs
                    else ""
                )
                return _completed(
                    command,
                    returncode=0 if command[4] in existing_jobs else 1,
                    stdout=stdout,
                    stderr="" if command[4] in existing_jobs else "NOT_FOUND: job does not exist",
                )
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertNotIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)

    def test_cleanup_schedulers_keeps_dispatcher_without_current_sync_proof(self) -> None:
        service = "charles-schwab-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps({"targets": [{"service_name": service}]}),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
        }
        existing_jobs = {
            "charles-schwab-service-probe-scheduler",
            "charles-schwab-service-precheck-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        marked_jobs = {
            "charles-schwab-service-probe-scheduler",
            "charles-schwab-service-precheck-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                stdout = (
                    runtime.DIRECT_MONITOR_SCHEDULER_DESCRIPTION
                    if "--format=value(description)" in command
                    and command[4] in marked_jobs
                    else "ENABLED"
                    if "--format=value(state)" in command
                    and command[4] in marked_jobs
                    else ""
                )
                return _completed(
                    command,
                    returncode=0 if command[4] in existing_jobs else 1,
                    stdout=stdout,
                    stderr="" if command[4] in existing_jobs else "NOT_FOUND: job does not exist",
                )
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertNotIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)

    def test_cleanup_schedulers_keeps_dispatcher_without_cutover_verification(
        self,
    ) -> None:
        service = "charles-schwab-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps({"targets": [{"service_name": service}]}),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        existing_jobs = {
            f"{service}-probe-scheduler",
            f"{service}-precheck-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                job_name = command[4]
                return _completed(
                    command,
                    returncode=0 if job_name in existing_jobs else 1,
                    stdout=(
                        runtime.DIRECT_MONITOR_SCHEDULER_DESCRIPTION
                        if "--format=value(description)" in command
                        and job_name != "schwab-monitor-dispatcher-scheduler"
                        else "ENABLED"
                        if "--format=value(state)" in command
                        and job_name != "schwab-monitor-dispatcher-scheduler"
                        else ""
                    ),
                    stderr="" if job_name in existing_jobs else "NOT_FOUND: job does not exist",
                )
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertNotIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)

    def test_cleanup_schedulers_keeps_dispatcher_without_migration_confirmation(self) -> None:
        service = "charles-schwab-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps({"targets": [{"service_name": service}]}),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
        }
        existing_jobs = {
            "charles-schwab-service-probe-scheduler",
            "charles-schwab-service-precheck-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                return _completed(
                    command,
                    returncode=0 if command[4] in existing_jobs else 1,
                    stderr="" if command[4] in existing_jobs else "NOT_FOUND: job does not exist",
                )
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertNotIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)
        self.assertIn("charles-schwab-service-probe-scheduler", deleted_jobs)
        self.assertIn("charles-schwab-service-precheck-scheduler", deleted_jobs)

    def test_cleanup_schedulers_requires_exact_lowercase_migration_confirmation(self) -> None:
        service = "charles-schwab-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps({"targets": [{"service_name": service}]}),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "TRUE",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        existing_jobs = {
            "charles-schwab-service-probe-scheduler",
            "charles-schwab-service-precheck-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                return _completed(
                    command,
                    returncode=0 if command[4] in existing_jobs else 1,
                    stderr="" if command[4] in existing_jobs else "NOT_FOUND: job does not exist",
                )
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertNotIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)
        self.assertIn("charles-schwab-service-probe-scheduler", deleted_jobs)
        self.assertIn("charles-schwab-service-precheck-scheduler", deleted_jobs)

    def test_cleanup_schedulers_keeps_dispatcher_for_partial_multi_target_migration(
        self,
    ) -> None:
        service = "charles-schwab-service"
        secondary_service = "charles-schwab-secondary-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps(
                {
                    "targets": [
                        {"service_name": service},
                        {"service_name": secondary_service},
                    ]
                }
            ),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        existing_jobs = {
            "charles-schwab-service-probe-scheduler",
            "charles-schwab-service-precheck-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        marked_jobs = {
            "charles-schwab-service-probe-scheduler",
            "charles-schwab-service-precheck-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                stdout = (
                    runtime.DIRECT_MONITOR_SCHEDULER_DESCRIPTION
                    if "--format=value(description)" in command
                    and command[4] in marked_jobs
                    else "ENABLED"
                    if "--format=value(state)" in command
                    and command[4] in marked_jobs
                    else ""
                )
                return _completed(
                    command,
                    returncode=0 if command[4] in existing_jobs else 1,
                    stdout=stdout,
                    stderr="" if command[4] in existing_jobs else "NOT_FOUND: job does not exist",
                )
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertNotIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)

    def test_cleanup_schedulers_deletes_dispatcher_after_all_targets_migrate(self) -> None:
        service = "charles-schwab-service"
        secondary_service = "charles-schwab-secondary-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps(
                {
                    "targets": [
                        {"service_name": service},
                        {"service_name": secondary_service},
                    ]
                }
            ),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        existing_jobs = {
            f"{service}-probe-scheduler",
            f"{service}-precheck-scheduler",
            f"{secondary_service}-probe-scheduler",
            f"{secondary_service}-precheck-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        marked_jobs = existing_jobs - {"schwab-monitor-dispatcher-scheduler"}
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                stdout = (
                    runtime.DIRECT_MONITOR_SCHEDULER_DESCRIPTION
                    if "--format=value(description)" in command
                    and command[4] in marked_jobs
                    else "ENABLED"
                    if "--format=value(state)" in command
                    and command[4] in marked_jobs
                    else ""
                )
                return _completed(
                    command,
                    returncode=0 if command[4] in existing_jobs else 1,
                    stdout=stdout,
                    stderr="" if command[4] in existing_jobs else "NOT_FOUND: job does not exist",
                )
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)

    def test_cleanup_schedulers_keeps_dispatcher_for_paused_enabled_target(self) -> None:
        service = "charles-schwab-service"
        secondary_service = "charles-schwab-secondary-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps(
                {
                    "targets": [
                        {"service_name": service},
                        {"service_name": secondary_service},
                    ]
                }
            ),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        direct_jobs = {
            f"{service}-probe-scheduler",
            f"{service}-precheck-scheduler",
            f"{secondary_service}-probe-scheduler",
            f"{secondary_service}-precheck-scheduler",
        }
        paused_jobs = {
            f"{secondary_service}-probe-scheduler",
            f"{secondary_service}-precheck-scheduler",
        }
        existing_jobs = {*direct_jobs, "schwab-monitor-dispatcher-scheduler"}
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                job_name = command[4]
                if job_name not in existing_jobs:
                    return _completed(
                        command,
                        returncode=1,
                        stderr="NOT_FOUND: job does not exist",
                    )
                if "--format=value(description)" in command:
                    return _completed(
                        command,
                        stdout=(
                            runtime.DIRECT_MONITOR_SCHEDULER_DESCRIPTION
                            if job_name in direct_jobs
                            else ""
                        ),
                    )
                if "--format=value(state)" in command:
                    return _completed(
                        command,
                        stdout="PAUSED" if job_name in paused_jobs else "ENABLED",
                    )
                return _completed(command)
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertNotIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)

    def test_cleanup_schedulers_ignores_disabled_targets_for_cutover(self) -> None:
        service = "charles-schwab-service"
        secondary_service = "charles-schwab-disabled-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps(
                {
                    "targets": [
                        {"service_name": service},
                        {
                            "service_name": secondary_service,
                            "env": {"RUNTIME_TARGET_ENABLED": "false"},
                        },
                    ]
                }
            ),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        existing_jobs = {
            f"{service}-probe-scheduler",
            f"{service}-precheck-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                job_name = command[4]
                return _completed(
                    command,
                    returncode=0 if job_name in existing_jobs else 1,
                    stdout=(
                        runtime.DIRECT_MONITOR_SCHEDULER_DESCRIPTION
                        if "--format=value(description)" in command
                        and job_name != "schwab-monitor-dispatcher-scheduler"
                        else "ENABLED"
                        if "--format=value(state)" in command
                        and job_name != "schwab-monitor-dispatcher-scheduler"
                        else ""
                    ),
                    stderr="" if job_name in existing_jobs else "NOT_FOUND: job does not exist",
                )
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)

    def test_cleanup_schedulers_keeps_dispatcher_for_active_disabled_target_jobs(
        self,
    ) -> None:
        service = "charles-schwab-service"
        disabled_service = "charles-schwab-disabled-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps(
                {
                    "targets": [
                        {"service_name": service},
                        {
                            "service_name": disabled_service,
                            "env": {"RUNTIME_TARGET_ENABLED": "false"},
                        },
                    ]
                }
            ),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        enabled_jobs = {
            f"{service}-probe-scheduler",
            f"{service}-precheck-scheduler",
        }
        disabled_jobs = {
            f"{disabled_service}-probe-scheduler",
            f"{disabled_service}-precheck-scheduler",
        }
        existing_jobs = {
            *enabled_jobs,
            *disabled_jobs,
            "schwab-monitor-dispatcher-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                job_name = command[4]
                if job_name not in existing_jobs:
                    return _completed(
                        command,
                        returncode=1,
                        stderr="NOT_FOUND: job does not exist",
                    )
                if "--format=value(description)" in command:
                    return _completed(
                        command,
                        stdout=(
                            runtime.DIRECT_MONITOR_SCHEDULER_DESCRIPTION
                            if job_name in enabled_jobs
                            else ""
                        ),
                    )
                if "--format=value(state)" in command:
                    return _completed(
                        command,
                        stdout="ENABLED" if job_name in enabled_jobs else "PAUSED",
                    )
                if "--format=json" in command:
                    return _completed(
                        command,
                        stdout=json.dumps({"description": "", "state": "ENABLED"}),
                    )
                return _completed(command)
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertNotIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)

    def test_cleanup_schedulers_keeps_dispatcher_for_unmarked_direct_jobs(self) -> None:
        service = "charles-schwab-service"
        secondary_service = "charles-schwab-secondary-service"
        env = {
            "SYNC_PLAN_JSON": json.dumps(
                {
                    "targets": [
                        {"service_name": service},
                        {"service_name": secondary_service},
                    ]
                }
            ),
            "GCP_PROJECT_ID": "charlesschwabquant",
            "CLOUD_RUN_REGION": "us-central1",
            "DIRECT_MONITOR_MIGRATION_COMPLETE": "true",
            "DIRECT_MONITOR_CUTOVER_VERIFIED": "true",
            "DIRECT_MONITOR_SCHEDULERS_RECONCILED": "true",
        }
        existing_jobs = {
            f"{service}-probe-scheduler",
            f"{service}-precheck-scheduler",
            f"{secondary_service}-probe-scheduler",
            f"{secondary_service}-precheck-scheduler",
            "schwab-monitor-dispatcher-scheduler",
        }
        deleted_jobs: list[str] = []

        def fake_run(command, text, capture_output, check):
            if command[:4] == ["gcloud", "scheduler", "jobs", "describe"]:
                return _completed(
                    command,
                    returncode=0 if command[4] in existing_jobs else 1,
                    stderr="" if command[4] in existing_jobs else "NOT_FOUND: job does not exist",
                )
            if command[:4] == ["gcloud", "scheduler", "jobs", "delete"]:
                deleted_jobs.append(command[4])
                return _completed(command)
            raise AssertionError(f"unexpected command: {command}")

        with patch.object(runtime.subprocess, "run", side_effect=fake_run):
            runtime.cleanup_schedulers(env)

        self.assertNotIn("schwab-monitor-dispatcher-scheduler", deleted_jobs)
