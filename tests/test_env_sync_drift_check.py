from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import env_sync_drift_check as drift

SYNC = ROOT / ".github/workflows/sync-cloud-run-env.yml"
DRIFT = ROOT / ".github/workflows/env-sync-drift-check.yml"


def _job_env(path: Path) -> dict[str, str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    jobs = next(i for i, l in enumerate(lines) if l.startswith("jobs:"))
    start = next(i for i in range(jobs, len(lines)) if lines[i] == "    env:")
    out: dict[str, str] = {}
    for line in lines[start + 1:]:
        if line.strip() and len(line) - len(line.lstrip()) < 6:
            break
        m = re.match(r"^      ([A-Z0-9_]+): (.*)$", line)
        if m:
            out[m.group(1)] = m.group(2)
    return out


def _blank_inputs(value: str) -> str:
    value = value.replace("${{ inputs.target || 'configured' }}", "configured")
    value = value.replace("${{ inputs.source_commit || github.sha }}", "${{ github.sha }}")
    return re.sub(r"\$\{\{ inputs\.[a-z_]+ \}\}", "''", value)


def test_drift_workflow_env_mirrors_sync_workflow():
    sync_env = {k: _blank_inputs(v) for k, v in _job_env(SYNC).items()}
    drift_env = _job_env(DRIFT)
    drift_env.pop("DRIFT_PER_TARGET")
    assert drift_env == sync_env


def test_drift_workflow_is_read_only():
    text = DRIFT.read_text(encoding="utf-8")
    assert "workflow_dispatch" in text
    for forbidden in ("services update", "update-traffic", "services replace", "add-iam-policy-binding",
                      "secrets versions add", "scheduler jobs", "curl "):
        assert forbidden not in text


def test_step_script_capture_replaces_only_mutation():
    script = drift.load_step_script(SYNC)
    assert drift.MUTATING_LINE not in script
    assert script.count("DRIFT_CAPTURE_DIR") == 1


def test_parse_args_and_diff_names_only():
    argv = [
        "run", "services", "update", "svc", "--region", "us-central1",
        "--concurrency", "1", "--max-instances", "1", "--no-traffic",
        "--remove-env-vars", "OLD,TELEGRAM_TOKEN",
        "--update-env-vars", "^|^A=1|B=new|C=x,y",
        "--remove-secrets", "TELEGRAM_TOKEN",
        "--update-secrets", "TELEGRAM_TOKEN=quant-sentinel-telegram-bot-token:latest",
        "--update-labels", "account=u1",
    ]
    planned = drift.parse_args(argv)
    assert planned["update_env"] == {"A": "1", "B": "new", "C": "x,y"}
    current = {
        "plain": {"A": "1", "B": "old", "OLD": "z", "KEEP": "k"},
        "secrets": {"TELEGRAM_TOKEN": "old-bot-token:latest"},
        "labels": {"account": "u1"},
        "concurrency": 1,
        "max_instances": "1",
    }
    d = drift.diff(current, planned)
    assert d["added_keys"] == ["C"]
    assert d["removed_keys"] == ["OLD"]
    assert d["changed_plain_value_keys"] == ["B"]
    assert d["changed_secret_refs"] == [
        {"key": "TELEGRAM_TOKEN", "before": "old-bot-token:latest",
         "after": "quant-sentinel-telegram-bot-token:latest"}
    ]
    assert d["changed_label_keys"] == [] and d["changed_runtime_flags"] == []
    assert "new" not in repr(d) and "z" not in d["removed_keys"][0].lower().replace("old", "")


def test_capture_collects_args_without_running_gcloud(tmp_path):
    wf = tmp_path / "wf.yml"
    wf.write_text(
        "jobs:\n  j:\n    steps:\n      - name: Sync Cloud Run environment\n        run: |\n"
        "          for s in a b; do\n"
        "            gcloud_args=(run services update \"$s\" --update-env-vars \"X=1\")\n"
        "            gcloud \"${gcloud_args[@]}\"\n"
        "          done\n",
        encoding="utf-8",
    )
    script = drift.load_step_script(wf)
    captured = drift.capture(script, {"PATH": "/usr/bin:/bin"})
    assert [c[3] for c in captured] == ["a", "b"]
