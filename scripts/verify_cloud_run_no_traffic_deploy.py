#!/usr/bin/env python3
"""Read back one no-traffic Cloud Run deploy without exposing secret values."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path


SERVICE_FORMAT = (
    "json(status.traffic,spec.template.spec.serviceAccountName,"
    "spec.template.spec.containerConcurrency,spec.template.spec.timeoutSeconds,"
    "spec.template.spec.containers[].resources,spec.template.spec.containers[].env[].name,"
    "spec.template.spec.containers[].env[].valueFrom.secretKeyRef)"
)
IAM_FORMAT = "json(bindings[].role,bindings[].members,bindings[].condition)"
SCHEDULER_FORMAT = "json(name,state,schedule,timeZone,httpTarget.uri,httpTarget.oidcToken)"
REVISION_FORMAT = "json(metadata.name,metadata.labels,spec.containers[].image)"


def _run_json(command: list[str]) -> object:
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError("read-only gcloud readback command failed")
    try:
        return json.loads(result.stdout or "null")
    except json.JSONDecodeError as exc:
        raise RuntimeError("gcloud readback returned invalid JSON") from exc


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _active_traffic(traffic: object) -> list[dict[str, object]]:
    if not isinstance(traffic, list):
        raise RuntimeError("Cloud Run traffic readback returned a non-list payload")
    active: list[dict[str, object]] = []
    for entry in traffic:
        if not isinstance(entry, dict):
            raise RuntimeError("Cloud Run traffic readback contained a non-object entry")
        try:
            percent = int(entry.get("percent", 0))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Cloud Run traffic readback contained an invalid percent") from exc
        if percent > 0:
            active.append({"revisionName": entry.get("revisionName"), "percent": percent})
    return sorted(active, key=_canonical)


def _snapshot(args: argparse.Namespace) -> dict[str, object]:
    service = _run_json([
        "gcloud", "run", "services", "describe", args.service,
        f"--project={args.project}", f"--region={args.region}", f"--format={SERVICE_FORMAT}",
    ])
    if not isinstance(service, dict):
        raise RuntimeError("Cloud Run service readback returned a non-object payload")
    iam = _run_json([
        "gcloud", "run", "services", "get-iam-policy", args.service,
        f"--project={args.project}", f"--region={args.region}", f"--format={IAM_FORMAT}",
    ])
    scheduler = _run_json([
        "gcloud", "scheduler", "jobs", "list", f"--project={args.project}",
        f"--location={args.scheduler_location}", f"--format={SCHEDULER_FORMAT}",
    ])
    status = service.get("status") if isinstance(service.get("status"), dict) else {}
    # Keep only digests in the on-runner baseline.  Service-account identities,
    # endpoint URIs, and secret-reference names are needed for comparison but
    # must not be persisted or printed by this verification helper.
    return {
        # A no-traffic revision is allowed to appear as a zero-percent status
        # entry.  Compare only effective traffic, otherwise a correct deploy
        # would fail its own readback merely because the new revision exists.
        "traffic": _digest(_active_traffic(status.get("traffic"))),
        "configuration": _digest(service.get("spec")),
        "iam": _digest(iam),
        "scheduler": _digest(scheduler),
    }


def _created_revision(args: argparse.Namespace) -> dict[str, object]:
    result = subprocess.run([
        "gcloud", "run", "services", "describe", args.service,
        f"--project={args.project}", f"--region={args.region}",
        "--format=value(status.latestCreatedRevisionName)",
    ], text=True, capture_output=True, check=False)
    revision_name = result.stdout.strip() if result.returncode == 0 else ""
    if not revision_name:
        raise RuntimeError("Cloud Run service did not report a created revision")
    revision = _run_json([
        "gcloud", "run", "revisions", "describe", revision_name,
        f"--project={args.project}", f"--region={args.region}", f"--format={REVISION_FORMAT}",
    ])
    if not isinstance(revision, dict):
        raise RuntimeError("Cloud Run revision readback returned a non-object payload")
    return revision


def _verify(args: argparse.Namespace) -> None:
    try:
        before = json.loads(args.before.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("deployment baseline is unreadable") from exc
    after = _snapshot(args)
    if not isinstance(before, dict):
        raise RuntimeError("deployment baseline is malformed")
    for key in ("traffic", "scheduler", "iam", "configuration"):
        if _canonical(before.get(key)) != _canonical(after.get(key)):
            raise RuntimeError(f"{key} changed during no-traffic deployment")

    revision = _created_revision(args)
    metadata = revision.get("metadata") if isinstance(revision.get("metadata"), dict) else {}
    labels = metadata.get("labels") if isinstance(metadata.get("labels"), dict) else {}
    containers = revision.get("spec", {}).get("containers", []) if isinstance(revision.get("spec"), dict) else []
    image = containers[0].get("image", "") if containers and isinstance(containers[0], dict) else ""
    if labels.get("commit-sha") != args.expected_sha:
        raise RuntimeError("created revision commit SHA does not match expected SHA")
    if f"@{args.expected_image_digest}" not in str(image):
        raise RuntimeError("created revision image digest does not match the pushed image")
    print(
        "Verified no-traffic deployment: commit SHA and image digest match; "
        "traffic, scheduler, IAM, and configuration digests are unchanged."
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("capture", "verify"):
        command = subparsers.add_parser(name)
        command.add_argument("--project", required=True)
        command.add_argument("--region", required=True)
        command.add_argument("--service", required=True)
        command.add_argument("--scheduler-location", required=True)
    subparsers.choices["capture"].add_argument("--output", required=True, type=Path)
    verify = subparsers.choices["verify"]
    verify.add_argument("--before", required=True, type=Path)
    verify.add_argument("--expected-sha", required=True)
    verify.add_argument("--expected-image-digest", required=True)
    args = parser.parse_args()
    try:
        if args.command == "capture":
            args.output.write_text(_canonical(_snapshot(args)), encoding="utf-8")
            print("Captured non-secret Cloud Run deployment baseline.")
        else:
            _verify(args)
    except RuntimeError as exc:
        print(f"No-traffic deployment readback failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
