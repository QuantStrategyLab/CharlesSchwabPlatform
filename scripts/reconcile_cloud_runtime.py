#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from collections.abc import Mapping
from typing import Any


def _load_sync_plan(env: Mapping[str, str]) -> dict[str, Any]:
    raw = (env.get("SYNC_PLAN_JSON") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"SYNC_PLAN_JSON is invalid: {exc}") from exc
    return payload if isinstance(payload, dict) else {}


def _primary_target(env: Mapping[str, str]) -> dict[str, Any]:
    plan = _load_sync_plan(env)
    targets = plan.get("targets")
    if isinstance(targets, list) and targets:
        first = targets[0]
        if isinstance(first, dict):
            return first
    return {}


def _first_non_empty(*values: object) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _service_name(env: Mapping[str, str]) -> str:
    target = _primary_target(env)
    service = _first_non_empty(
        target.get("service_name"),
        target.get("service"),
        target.get("cloud_run_service"),
        env.get("CLOUD_RUN_SERVICE"),
    )
    if not service:
        raise RuntimeError("CLOUD_RUN_SERVICE or SYNC_PLAN_JSON.targets[0].service_name is required")
    return service


def _project_id(env: Mapping[str, str]) -> str:
    project = _first_non_empty(env.get("GCP_PROJECT_ID"), env.get("GOOGLE_CLOUD_PROJECT"))
    if not project:
        raise RuntimeError("GCP_PROJECT_ID or GOOGLE_CLOUD_PROJECT is required")
    return project


def _region(env: Mapping[str, str]) -> str:
    region = _first_non_empty(env.get("CLOUD_RUN_REGION"))
    if not region:
        raise RuntimeError("CLOUD_RUN_REGION is required")
    return region


def _scheduler_location(env: Mapping[str, str]) -> str:
    return _first_non_empty(env.get("CLOUD_SCHEDULER_LOCATION"), env.get("CLOUD_RUN_REGION"))


def _is_not_found(result: subprocess.CompletedProcess[str]) -> bool:
    detail = f"{result.stdout or ''}\n{result.stderr or ''}".lower()
    return (
        "not found" in detail
        or "not_found" in detail
        or "404" in detail
        or "code: 5" in detail
    )


def _gcloud(args: list[str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(["gcloud", *args], text=True, capture_output=True, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(detail or f"gcloud {' '.join(args[:3])} failed")
    return result


def _gcloud_json(args: list[str]) -> Any:
    result = _gcloud(args)
    payload = (result.stdout or "").strip()
    if not payload:
        return None
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"gcloud returned invalid JSON: {exc}") from exc


def _revision_commit_sha(
    *,
    project: str,
    region: str,
    revision: str,
) -> str:
    payload = _gcloud_json(
        [
            "run",
            "revisions",
            "describe",
            revision,
            "--project",
            project,
            "--region",
            region,
            "--format=json",
        ]
    )
    if not isinstance(payload, dict):
        return ""
    labels = payload.get("metadata", {}).get("labels", {})
    if not isinstance(labels, dict):
        return ""
    return str(labels.get("commit-sha") or "").strip()


def _service_status(*, project: str, region: str, service: str) -> dict[str, Any]:
    payload = _gcloud_json(
        [
            "run",
            "services",
            "describe",
            service,
            "--project",
            project,
            "--region",
            region,
            "--format=json",
        ]
    )
    return payload if isinstance(payload, dict) else {}


def _traffic_matches_latest(service_payload: Mapping[str, Any], revision: str) -> bool:
    traffic = service_payload.get("status", {}).get("traffic", [])
    if not isinstance(traffic, list):
        return False
    for item in traffic:
        if not isinstance(item, dict):
            continue
        percent = int(item.get("percent") or 0)
        if percent == 100 and (item.get("latestRevision") is True or item.get("revisionName") == revision):
            return True
    return False


def reconcile_traffic(env: Mapping[str, str] = os.environ) -> None:
    service = _service_name(env)
    project = _project_id(env)
    region = _region(env)
    target_sha = _first_non_empty(env.get("GITHUB_SHA"))
    if not target_sha:
        raise RuntimeError("GITHUB_SHA is required")

    deadline = time.monotonic() + 1800
    latest_revision = ""
    latest_sha = ""
    while True:
        payload = _service_status(project=project, region=region, service=service)
        latest_revision = str(payload.get("status", {}).get("latestReadyRevisionName") or "").strip()
        if latest_revision:
            latest_sha = _revision_commit_sha(project=project, region=region, revision=latest_revision)
            if latest_sha == target_sha:
                break
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "Timed out waiting for Cloud Run revision "
                f"{latest_revision or '<none>'} on {service} to match commit {target_sha}. "
                f"Last seen commit: {latest_sha or '<none>'}"
            )
        time.sleep(10)

    payload = _service_status(project=project, region=region, service=service)
    if not _traffic_matches_latest(payload, latest_revision):
        _gcloud(
            [
                "run",
                "services",
                "update-traffic",
                service,
                "--project",
                project,
                "--region",
                region,
                "--to-latest",
                "--quiet",
            ]
        )

    payload = _service_status(project=project, region=region, service=service)
    if not _traffic_matches_latest(payload, latest_revision):
        raise RuntimeError(
            f"Cloud Run service {service} is not routed 100% to latest ready revision {latest_revision}"
        )

    latest_sha = _revision_commit_sha(project=project, region=region, revision=latest_revision)
    if latest_sha != target_sha:
        raise RuntimeError(
            f"Cloud Run latest ready revision {latest_revision} on {service} has commit {latest_sha or '<none>'}, "
            f"expected {target_sha}"
        )

    print(f"Cloud Run service {service} is routed to latest ready revision {latest_revision}.")


def _legacy_scheduler_jobs(service: str) -> list[str]:
    service_name = service.strip()
    if not service_name:
        return []
    candidates = [
        f"{service_name}-probe-scheduler",
        f"{service_name}-precheck-scheduler",
    ]
    if service_name.endswith("-service"):
        base_service = service_name.removesuffix("-service")
        candidates.extend(
            [
                f"{base_service}-probe-scheduler",
                f"{base_service}-precheck-scheduler",
            ]
        )
    return list(dict.fromkeys(candidates))


def cleanup_schedulers(env: Mapping[str, str] = os.environ) -> None:
    service = _service_name(env)
    project = _project_id(env)
    location = _scheduler_location(env)
    if not location:
        raise RuntimeError("CLOUD_SCHEDULER_LOCATION or CLOUD_RUN_REGION is required")

    for job_name in _legacy_scheduler_jobs(service):
        result = subprocess.run(
            [
                "gcloud",
                "scheduler",
                "jobs",
                "describe",
                job_name,
                "--project",
                project,
                "--location",
                location,
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            if _is_not_found(result):
                continue
            detail = (result.stderr or result.stdout or "").strip()
            raise RuntimeError(detail or f"gcloud scheduler jobs describe {job_name} failed")
        print(f"Deleting legacy Cloud Scheduler job {job_name}.")
        _gcloud(
            [
                "scheduler",
                "jobs",
                "delete",
                job_name,
                "--project",
                project,
                "--location",
                location,
                "--quiet",
            ]
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reconcile Cloud Run runtime state.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("reconcile-traffic", help="Ensure latest Cloud Run revision receives traffic.")
    subparsers.add_parser("cleanup-schedulers", help="Delete whitelisted legacy Cloud Scheduler jobs.")
    args = parser.parse_args(argv)

    if args.command == "reconcile-traffic":
        reconcile_traffic()
    elif args.command == "cleanup-schedulers":
        cleanup_schedulers()
    else:
        parser.error(f"Unknown command: {args.command}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
