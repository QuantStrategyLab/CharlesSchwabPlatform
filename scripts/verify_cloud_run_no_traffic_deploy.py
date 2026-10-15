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
CASH_CURRENCY_FORMAT = (
    'json(spec.template.spec.containers[].env.always().filter("name=SCHWAB_CASH_CURRENCY").map().extract(value))'
)
CASH_REVISION_FORMAT = (
    'json(spec.containers[].env.always().filter("name=SCHWAB_CASH_CURRENCY").map().extract(value))'
)


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


def _cash_values(value: object) -> list[object]:
    """Read only the explicitly projected non-secret cash-currency values."""
    if value is None:
        return []
    if isinstance(value, list):
        return [item for child in value for item in _cash_values(child)]
    if isinstance(value, dict):
        return [item for child in value.values() for item in _cash_values(child)]
    return [value]


def _cash_currency(args: argparse.Namespace, *, revision: bool = False) -> str | None:
    if not getattr(args, "cash_currency", None):
        return None
    if revision:
        payload = _run_json([
            "gcloud", "run", "revisions", "describe", args.revision_name,
            f"--project={args.project}", f"--region={args.region}",
            f"--format={CASH_REVISION_FORMAT}",
        ])
    else:
        payload = _run_json([
            "gcloud", "run", "services", "describe", args.service,
            f"--project={args.project}", f"--region={args.region}",
            f"--format={CASH_CURRENCY_FORMAT}",
        ])
    values = _cash_values(payload)
    if not values:
        return None
    if len(values) != 1 or not isinstance(values[0], str):
        raise RuntimeError("cash currency environment projection is ambiguous")
    return values[0]


def _configuration_projection(spec: object, *, allow_cash_currency: bool) -> object:
    if not allow_cash_currency or not isinstance(spec, dict):
        return spec
    # SERVICE_FORMAT returns only names and secret references, never plaintext
    # environment values. Remove the single explicitly allowed env name before
    # hashing so an absent-to-USD addition does not mask any other config drift.
    projected = json.loads(_canonical(spec))
    template = projected.get("template")
    template_spec = template.get("spec") if isinstance(template, dict) else None
    containers = template_spec.get("containers") if isinstance(template_spec, dict) else None
    if isinstance(containers, list):
        cash_entries = [
            entry
            for container in containers if isinstance(container, dict)
            for entry in (container.get("env") if isinstance(container.get("env"), list) else [])
            if isinstance(entry, dict) and entry.get("name") == "SCHWAB_CASH_CURRENCY"
        ]
        if len(cash_entries) > 1:
            raise RuntimeError("cash currency environment entry is duplicated")
        if cash_entries and "valueFrom" in cash_entries[0]:
            raise RuntimeError("cash currency environment entry must not use a secret reference")
        for container in containers:
            if not isinstance(container, dict) or not isinstance(container.get("env"), list):
                continue
            container["env"] = [
                entry for entry in container["env"]
                if not (isinstance(entry, dict) and entry.get("name") == "SCHWAB_CASH_CURRENCY")
            ]
    return projected


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
    cash_currency = _cash_currency(args)
    if getattr(args, "cash_currency", None) and cash_currency not in (None, args.cash_currency):
        raise RuntimeError("cash currency baseline does not match the approved declaration")
    return {
        # A no-traffic revision is allowed to appear as a zero-percent status
        # entry.  Compare only effective traffic, otherwise a correct deploy
        # would fail its own readback merely because the new revision exists.
        "traffic": _digest(_active_traffic(status.get("traffic"))),
        "configuration": _digest(_configuration_projection(
            service.get("spec"), allow_cash_currency=bool(getattr(args, "cash_currency", None))
        )),
        **({"cash_currency": cash_currency} if getattr(args, "cash_currency", None) else {}),
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
    if getattr(args, "cash_currency", None):
        before_currency = before.get("cash_currency")
        if before_currency not in (None, args.cash_currency):
            raise RuntimeError("deployment baseline cash currency is not an allowed value")
        # Re-read only the explicitly named, non-secret value from the service
        # and created revision. Neither full env values nor the projection are
        # written into the baseline or emitted in diagnostics.
        revision_name = metadata.get("name")
        if not isinstance(revision_name, str) or not revision_name:
            raise RuntimeError("created revision name is missing")
        args.revision_name = revision_name
        if after.get("cash_currency") != args.cash_currency or _cash_currency(args, revision=True) != args.cash_currency:
            raise RuntimeError("cash currency declaration did not reach the created revision")
    message = (
        "Verified no-traffic deployment: commit SHA and image digest match; "
        "traffic, scheduler, IAM, and configuration digests are unchanged."
    )
    if getattr(args, "cash_currency", None):
        message += " The explicitly approved cash currency matches."
    print(message)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("capture", "verify"):
        command = subparsers.add_parser(name)
        command.add_argument("--project", required=True)
        command.add_argument("--region", required=True)
        command.add_argument("--service", required=True)
        command.add_argument("--scheduler-location", required=True)
        command.add_argument("--cash-currency", choices=("USD",))
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
