#!/usr/bin/env python3
"""Read-only drift check: what would a full ``sync-cloud-run-env`` env sync change?

Re-runs the exact "Sync Cloud Run environment" step script from
``.github/workflows/sync-cloud-run-env.yml`` (read from the checked-out ref)
with its single mutating ``gcloud "${gcloud_args[@]}"`` call replaced by a
capture, then diffs the captured env/secret/label arguments against the
serving (100% traffic) Cloud Run revision.

Prints KEY NAMES ONLY. Plain env values are compared in-process and never
printed; secret-backed keys print only the Secret Manager reference
(secret name + version), never a secret value. Changes nothing in Cloud Run,
Secret Manager, IAM, Scheduler, or traffic.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

WORKFLOW = Path(".github/workflows/sync-cloud-run-env.yml")
STEP_NAME = "Sync Cloud Run environment"
MUTATING_LINE = 'gcloud "${gcloud_args[@]}"'
CAPTURE_LINE = (
    'printf \'%s\\0\' "${gcloud_args[@]}" > "${DRIFT_CAPTURE_DIR}/$(printf %04d "${__drift_n}").args"; '
    "__drift_n=$((__drift_n + 1))"
)


def _extract_step_run(text: str, step_name: str) -> str:
    """Return the literal ``run: |`` block of the named step (no YAML dependency)."""
    lines = text.splitlines()
    starts = [i for i, l in enumerate(lines) if l.strip() == f"- name: {step_name}"]
    if len(starts) != 1:
        raise SystemExit(f"expected exactly one '{step_name}' step, found {len(starts)}")
    step_indent = len(lines[starts[0]]) - len(lines[starts[0]].lstrip())
    i = starts[0] + 1
    while i < len(lines):
        line = lines[i]
        indent = len(line) - len(line.lstrip())
        if line.strip() and indent <= step_indent:
            raise SystemExit(f"'{step_name}' step has no run block")
        if line.strip() == "run: |":
            run_indent = indent
            break
        i += 1
    else:
        raise SystemExit(f"'{step_name}' step has no run block")
    body: list[str] = []
    for line in lines[i + 1:]:
        if line.strip() and (len(line) - len(line.lstrip())) <= run_indent:
            break
        body.append(line)
    while body and not body[-1].strip():
        body.pop()
    nonblank = [l for l in body if l.strip()]
    cut = min(len(l) - len(l.lstrip()) for l in nonblank)
    return "\n".join(l[cut:] for l in body)


def load_step_script(workflow_path: Path = WORKFLOW) -> str:
    run = _extract_step_run(workflow_path.read_text(encoding="utf-8"), STEP_NAME)
    lines = run.splitlines()
    hits = [i for i, line in enumerate(lines) if line.strip() == MUTATING_LINE]
    if len(hits) != 1:
        raise SystemExit(f"expected exactly one mutating gcloud line, found {len(hits)}")
    indent = lines[hits[0]][: len(lines[hits[0]]) - len(lines[hits[0]].lstrip())]
    lines[hits[0]] = indent + CAPTURE_LINE
    script = "\n".join(lines)
    # Defense in depth: no other direct gcloud invocation may remain.
    for line in script.splitlines():
        stripped = line.strip()
        if stripped.startswith("gcloud ") and not stripped.startswith("gcloud_args"):
            raise SystemExit(f"unexpected gcloud call in step script: {stripped[:60]}")
    return script


def parse_args(argv: list[str]) -> dict[str, Any]:
    if argv[:3] != ["run", "services", "update"]:
        raise ValueError("captured args are not 'run services update'")
    out: dict[str, Any] = {
        "service": argv[3],
        "flags": {},
        "remove_env": [],
        "update_env": {},
        "remove_secrets": [],
        "update_secrets": {},
        "update_labels": {},
    }
    i = 4
    while i < len(argv):
        flag = argv[i]
        takes_value = flag in {
            "--region", "--concurrency", "--max-instances", "--remove-env-vars",
            "--update-env-vars", "--remove-secrets", "--update-secrets", "--update-labels",
        }
        value = argv[i + 1] if takes_value and i + 1 < len(argv) else None
        i += 2 if takes_value else 1
        if flag == "--remove-env-vars":
            out["remove_env"] = [k for k in (value or "").split(",") if k]
        elif flag == "--update-env-vars":
            raw = value or ""
            delim = ","
            if raw.startswith("^"):
                end = raw.index("^", 1)
                delim, raw = raw[1:end], raw[end + 1:]
            for pair in raw.split(delim) if raw else []:
                k, _, v = pair.partition("=")
                out["update_env"][k] = v
        elif flag == "--remove-secrets":
            out["remove_secrets"] = [k for k in (value or "").split(",") if k]
        elif flag == "--update-secrets":
            for pair in (value or "").split(","):
                if pair:
                    k, _, ref = pair.partition("=")
                    out["update_secrets"][k] = ref
        elif flag == "--update-labels":
            for pair in (value or "").split(","):
                if pair:
                    k, _, v = pair.partition("=")
                    out["update_labels"][k] = v
        else:
            out["flags"][flag] = value
    return out


def revision_state(revision: dict[str, Any]) -> dict[str, Any]:
    spec = revision.get("spec", {})
    container = (spec.get("containers") or [{}])[0]
    plain: dict[str, str] = {}
    secrets: dict[str, str] = {}
    for item in container.get("env") or []:
        name = item.get("name")
        ref = (item.get("valueFrom") or {}).get("secretKeyRef")
        if ref:
            secrets[name] = f"{ref.get('name')}:{ref.get('key')}"
        else:
            plain[name] = item.get("value", "")
    annotations = revision.get("metadata", {}).get("annotations", {}) or {}
    return {
        "plain": plain,
        "secrets": secrets,
        "labels": revision.get("metadata", {}).get("labels", {}) or {},
        "concurrency": spec.get("containerConcurrency"),
        "max_instances": annotations.get("autoscaling.knative.dev/maxScale"),
    }


def diff(current: dict[str, Any], planned: dict[str, Any]) -> dict[str, Any]:
    plain = dict(current["plain"])
    secrets = dict(current["secrets"])
    for k in planned["remove_env"]:
        plain.pop(k, None)
    for k in planned["remove_secrets"]:
        secrets.pop(k, None)
    for k, v in planned["update_env"].items():
        plain[k] = v
        secrets.pop(k, None)
    for k, ref in planned["update_secrets"].items():
        secrets[k] = ref
        plain.pop(k, None)

    before_keys = set(current["plain"]) | set(current["secrets"])
    after_keys = set(plain) | set(secrets)
    changed_plain = sorted(
        k for k in set(plain) & set(current["plain"]) if plain[k] != current["plain"][k]
    )
    changed_secret = [
        {"key": k, "before": current["secrets"][k], "after": secrets[k]}
        for k in sorted(set(secrets) & set(current["secrets"]))
        if secrets[k] != current["secrets"][k]
    ]
    kind_changed = sorted(
        k for k in before_keys & after_keys
        if (k in current["secrets"]) != (k in secrets)
    )
    label_changes = sorted(
        k for k, v in planned["update_labels"].items() if current["labels"].get(k) != v
    )
    flag_changes = []
    if "--concurrency" in planned["flags"] and str(current["concurrency"]) != str(planned["flags"]["--concurrency"]):
        flag_changes.append("concurrency")
    if "--max-instances" in planned["flags"] and str(current["max_instances"]) != str(planned["flags"]["--max-instances"]):
        flag_changes.append("max_instances")
    return {
        "added_keys": sorted(after_keys - before_keys),
        "removed_keys": sorted(before_keys - after_keys),
        "changed_plain_value_keys": changed_plain,
        "changed_secret_refs": changed_secret,
        "plain_secret_kind_changed_keys": kind_changed,
        "changed_label_keys": label_changes,
        "changed_runtime_flags": flag_changes,
        "unchanged_key_count": len(before_keys & after_keys) - len(changed_plain) - len(changed_secret) - len(kind_changed),
    }


def _gcloud_json(*args: str) -> Any:
    out = subprocess.run(["gcloud", *args, "--format=json"], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def serving_revision(service: str, project: str, region: str) -> str:
    svc = _gcloud_json("run", "services", "describe", service, f"--project={project}", f"--region={region}")
    full = [t["revisionName"] for t in svc.get("status", {}).get("traffic", []) if t.get("percent") == 100]
    if len(full) != 1:
        raise SystemExit(f"{service}: no unique 100% serving revision")
    return full[0]


def capture(script: str, env: dict[str, str]) -> list[list[str]]:
    with tempfile.TemporaryDirectory() as tmp:
        run_env = dict(env, DRIFT_CAPTURE_DIR=tmp)
        subprocess.run(["bash", "-c", "set -euo pipefail\n__drift_n=0\n" + script], check=True, env=run_env,
                       stdout=subprocess.DEVNULL)
        return [p.read_bytes().decode().split("\0")[:-1] for p in sorted(Path(tmp).glob("*.args"))]


def main() -> int:
    project = os.environ["GCP_PROJECT_ID"]
    region = os.environ["CLOUD_RUN_REGION"]
    plan = json.loads(os.environ["SYNC_PLAN_JSON"])
    per_target = os.environ.get("DRIFT_PER_TARGET") == "1"
    script = load_step_script()

    runs: list[dict[str, str]] = []
    if per_target:
        for target in plan["targets"]:
            name = target["service_name"]
            scoped = dict(plan, targets=[target])
            runs.append({
                "SYNC_PLAN_JSON": json.dumps(scoped),
                "CLOUD_RUN_SERVICE_TARGETS_JSON": json.dumps({"targets": [{"service_name": name}]}),
                "CLOUD_RUN_SERVICE": name,
                "CLOUD_RUN_SERVICES": name,
            })
    else:
        runs.append({"SYNC_PLAN_JSON": os.environ["SYNC_PLAN_JSON"]})

    report: list[dict[str, Any]] = []
    for overrides in runs:
        for argv in capture(script, {**os.environ, **overrides}):
            planned = parse_args(argv)
            rev = serving_revision(planned["service"], project, region)
            current = revision_state(_gcloud_json("run", "revisions", "describe", rev,
                                                  f"--project={project}", f"--region={region}"))
            entry = {"service": planned["service"], "serving_revision": rev, **diff(current, planned)}
            report.append(entry)

    print(json.dumps({"schema": "qsl.env_sync_drift.v1", "project": project, "services": report}, indent=2))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("## Env sync drift (key names only)\n\n")
            for e in report:
                fh.write(f"### {e['service']} @ {e['serving_revision']}\n")
                keys = ("added_keys", "removed_keys", "changed_plain_value_keys",
                        "plain_secret_kind_changed_keys", "changed_label_keys", "changed_runtime_flags")
                fh.writelines(f"- {key}: {', '.join(e[key]) or '-'}\n" for key in keys)
                fh.writelines(
                    f"- secret ref {c['key']}: {c['before']} -> {c['after']}\n" for c in e["changed_secret_refs"]
                )
                fh.write(f"- unchanged keys: {e['unchanged_key_count']}\n\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
