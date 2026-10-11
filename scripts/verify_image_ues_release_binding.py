#!/usr/bin/env python3
"""Deploy guard (RS-08 fix 1): block an image whose installed UES revision differs
from the strategy release / runtime-risk binding it would run under.

The live runtime compares the installed UES revision with
``runtime_risk_limits.binding.ues_revision`` and rejects every SOXL rebalance on
mismatch (``rejected:runtime_risk_limits``). Rebuilding from a newer repo pin
without updating ``strategy_release.strategy_revision`` and the binding would
therefore silently stop trading. This check runs after ``docker build`` and before
``docker push`` / ``gcloud run deploy`` and fails the deploy instead.

Sources of the installed revision (all given must agree):
  --image    built image; reads /app/UES_REVISION or the UES direct_url.json
             inside the container (``--network none``, no environment)
  --lockfile uv.lock; reads the pinned us-equity-strategies git commit
  --installed-revision  explicit value (tests)

Expected revisions come from each planned target's RUNTIME_TARGET_JSON
(SYNC_PLAN_JSON targets[].env), else CLOUD_RUN_SERVICE_TARGETS_JSON runtime
targets, else RUNTIME_TARGET_JSON. A target without a strategy_release and
without a binding ues_revision is reported and not blocked. Prints only service
names, release ids and commit SHAs. Read-only; does not touch Cloud Run.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
UES_DIST = "us-equity-strategies"
_IMAGE_PROBE = (
    "import json,pathlib\n"
    "from importlib import metadata as m\n"
    "p=pathlib.Path('/app/UES_REVISION')\n"
    "s=p.read_text().strip() if p.exists() else ''\n"
    "d=json.loads(m.distribution('us-equity-strategies').read_text('direct_url.json') or '{}')\n"
    "print('UES_STAMP='+s)\n"
    "print('UES_DIRECT_URL='+str((d.get('vcs_info') or {}).get('commit_id') or ''))\n"
)


class GuardError(RuntimeError):
    pass


def revision_from_lockfile(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    for block in text.split("[[package]]"):
        if re.search(r'^name = "us-equity-strategies"\s*$', block, re.MULTILINE):
            match = re.search(r'^source = \{ git = "[^"]*#([0-9a-f]{40})"', block, re.MULTILINE)
            if match:
                return match.group(1)
    raise GuardError(f"{path}: no pinned git commit for {UES_DIST}")


def revision_from_image(image: str) -> str:
    result = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", "--entrypoint", "python", image, "-c", _IMAGE_PROBE],
        capture_output=True, text=True, timeout=300, check=False,
    )
    if result.returncode != 0:
        raise GuardError(f"image probe failed (exit {result.returncode})")
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    stamp, direct = values.get("UES_STAMP", "").strip(), values.get("UES_DIRECT_URL", "").strip()
    if stamp and direct and stamp != direct:
        raise GuardError(f"image UES stamp {stamp} != installed direct_url {direct}")
    revision = stamp or direct
    if not SHA_RE.match(revision):
        raise GuardError("image has no installed UES commit")
    return revision


def _load_json(raw: Any) -> Any:
    if isinstance(raw, str):
        raw = raw.strip()
        return json.loads(raw) if raw else None
    return raw


def runtime_targets_from_env(env: Mapping[str, str]) -> list[tuple[str, Mapping[str, Any]]]:
    plan = _load_json(env.get("SYNC_PLAN_JSON") or "")
    out: list[tuple[str, Mapping[str, Any]]] = []
    if isinstance(plan, Mapping) and plan.get("targets"):
        for target in plan["targets"]:
            runtime_target = _load_json(((target or {}).get("env") or {}).get("RUNTIME_TARGET_JSON") or "") or {}
            service = str(target.get("service_name") or runtime_target.get("service_name") or "").strip()
            out.append((service, runtime_target))
        return out
    targets = _load_json(env.get("CLOUD_RUN_SERVICE_TARGETS_JSON") or "")
    if targets:
        entries = targets.get("targets") if isinstance(targets, Mapping) else targets
        for entry in entries or ():
            if not isinstance(entry, Mapping):
                continue
            runtime_target = _load_json(entry.get("runtime_target") or entry.get("runtime_target_json")) or {}
            service = str(entry.get("service_name") or entry.get("service") or runtime_target.get("service_name") or "")
            out.append((service.strip(), runtime_target))
        return out
    single = _load_json(env.get("RUNTIME_TARGET_JSON") or "")
    if isinstance(single, Mapping):
        service = str(single.get("service_name") or env.get("CLOUD_RUN_SERVICE") or "").strip()
        out.append((service, single))
    return out


def expected_revisions(runtime_target: Mapping[str, Any]) -> dict[str, str]:
    expected: dict[str, str] = {}
    release = runtime_target.get("strategy_release")
    if isinstance(release, Mapping) and release.get("strategy_revision"):
        expected["strategy_release.strategy_revision"] = str(release["strategy_revision"]).strip()
    limits = runtime_target.get("runtime_risk_limits")
    binding = limits.get("binding") if isinstance(limits, Mapping) else None
    if isinstance(binding, Mapping) and binding.get("ues_revision"):
        expected["runtime_risk_limits.binding.ues_revision"] = str(binding["ues_revision"]).strip()
    return expected


def check(installed: Mapping[str, str], targets: Iterable[tuple[str, Mapping[str, Any]]]) -> list[str]:
    """Return failure lines; prints a non-sensitive report."""
    failures: list[str] = []
    values = set(installed.values())
    for label, rev in installed.items():
        print(f"installed_ues_revision[{label}]={rev}")
    if len(values) != 1:
        failures.append("installed UES revision sources disagree: "
                        + ", ".join(f"{k}={v}" for k, v in sorted(installed.items())))
        return failures
    (revision,) = values
    targets = list(targets)
    if not targets:
        failures.append("no runtime target resolved; cannot verify the strategy release binding")
    for service, runtime_target in targets:
        expected = expected_revisions(runtime_target or {})
        release_id = ((runtime_target or {}).get("strategy_release") or {}).get("release_id")
        if not expected:
            print(f"service={service or '?'} no strategy_release/binding ues_revision: not blocked")
            continue
        for field, value in sorted(expected.items()):
            ok = value == revision
            print(f"service={service or '?'} release_id={release_id} {field}={value} match={ok}")
            if not ok:
                failures.append(f"{service or '?'}: {field}={value} != installed {revision}")
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--image")
    parser.add_argument("--lockfile", type=Path)
    parser.add_argument("--installed-revision")
    args = parser.parse_args(argv)
    try:
        installed: dict[str, str] = {}
        if args.lockfile:
            installed["lockfile"] = revision_from_lockfile(args.lockfile)
        if args.image:
            installed["image"] = revision_from_image(args.image)
        if args.installed_revision:
            installed["explicit"] = args.installed_revision.strip()
        if not installed:
            raise GuardError("give --image, --lockfile or --installed-revision")
        failures = check(installed, runtime_targets_from_env(os.environ))
    except (GuardError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        print(f"::error::UES release binding guard could not verify: {exc}", file=sys.stderr)
        return 1
    if failures:
        for line in failures:
            print(f"::error::UES release binding mismatch: {line}", file=sys.stderr)
        print("Deploy blocked: update strategy_release.strategy_revision and runtime_risk_limits.binding."
              "ues_revision together with the UES pin (or keep the pin) before deploying.", file=sys.stderr)
        return 1
    print("UES release binding guard: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
