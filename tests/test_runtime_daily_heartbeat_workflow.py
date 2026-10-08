"""Targeted checks for the opt-in Schwab daily runtime projection step.

The step is default-off (missing/false ``vars.RUNTIME_DAILY_SYNC_ENABLED``) and
reuses the heartbeat run's existing WIF/uv flow. These tests read the workflow
text and evaluate its bounded Actions condition offline; they never run a
workflow, dispatch a run, or touch a real credential/endpoint.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/execution-report-heartbeat.yml"
STEP_NAME = "Publish daily runtime projection"
CLI = "uv run --no-sync python scripts/run_runtime_daily_from_reports.py --publish"


def _workflow_text() -> str:
    return WORKFLOW.read_text()


def _step_block(workflow: str, name: str) -> str:
    match = re.search(
        rf"(?ms)^      - name: {re.escape(name)}\n(.*?)(?=^      - name:|^  [a-z_]+:|\Z)",
        workflow,
    )
    assert match is not None, f"missing step {name!r}"
    return match.group(0)


def _heartbeat_job(workflow: str) -> str:
    return workflow.split("\n  heartbeat:\n", 1)[1].split("\n  account_facts:\n", 1)[0]


def _condition(step: str) -> str:
    match = re.search(r"if: \$\{\{ (.*?) \}\}", step)
    assert match is not None, "step has no bounded if-condition"
    return match.group(1)


def _evaluate(expression: str, enabled: str, primary: str, retry: str, *, installed: str = "success", cancelled: bool = False) -> bool:
    expr = expression.replace("!cancelled()", repr(not cancelled))
    expr = expr.replace("steps.install_runtime_deps.outcome", repr(installed))
    expr = expr.replace("vars.RUNTIME_DAILY_SYNC_ENABLED", repr(enabled))
    expr = expr.replace("steps.gcp_auth_primary.outcome", repr(primary))
    expr = expr.replace("steps.gcp_auth_retry.outcome", repr(retry))
    expr = expr.replace("&&", " and ").replace("||", " or ")
    parsed = ast.parse(expr, mode="eval")
    allowed = (
        ast.Expression,
        ast.BoolOp,
        ast.And,
        ast.Or,
        ast.Compare,
        ast.Eq,
        ast.NotEq,
        ast.Constant,
    )
    assert all(isinstance(node, allowed) for node in ast.walk(parsed)), expr
    return bool(eval(compile(parsed, "<offline Actions condition>", "eval"), {"__builtins__": {}}, {}))


def test_missing_or_false_switch_never_publishes() -> None:
    step = _step_block(_workflow_text(), STEP_NAME)
    condition = _condition(step)
    for enabled in ("", "false", "False", "TRUE"):
        for primary, retry in (
            ("success", "skipped"),
            ("failure", "success"),
        ):
            assert _evaluate(condition, enabled, primary, retry) is False, (enabled, primary, retry)


def test_true_switch_with_successful_authentication_publishes() -> None:
    step = _step_block(_workflow_text(), STEP_NAME)
    condition = _condition(step)
    assert _evaluate(condition, "true", "success", "skipped") is True
    assert _evaluate(condition, "true", "failure", "success") is True


def test_true_switch_after_failed_authentication_never_publishes() -> None:
    step = _step_block(_workflow_text(), STEP_NAME)
    condition = _condition(step)
    assert _evaluate(condition, "true", "failure", "failure") is False
    assert _evaluate(condition, "true", "failure", "skipped") is False


def test_true_switch_runs_the_existing_producer_cli() -> None:
    step = _step_block(_workflow_text(), STEP_NAME)
    assert f"run: {CLI}" in step
    runner = (ROOT / "scripts/run_runtime_daily_from_reports.py").read_text()
    assert '"--publish"' in runner


def test_step_scopes_the_sync_token_and_uses_protected_prefix_and_region() -> None:
    step = _step_block(_workflow_text(), STEP_NAME)
    assert "EXECUTION_EVIDENCE_SYNC_TOKEN: ${{ secrets.EXECUTION_EVIDENCE_SYNC_TOKEN }}" in step
    assert "SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX: ${{ secrets.SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX }}" in step
    assert "GCP_REGION: us-central1" in step
    assert (
        "RUNTIME_TARGET_JSON: ${{ secrets.SCHWAB_RUNTIME_DAILY_TARGET_JSON || "
        "vars.RUNTIME_TARGET_JSON || secrets.RUNTIME_TARGET_JSON }}"
    ) in step


def test_daily_target_override_is_step_scoped_and_preserves_other_heartbeat_steps() -> None:
    workflow = _workflow_text()
    daily_step = _step_block(workflow, STEP_NAME)
    heartbeat_job = _heartbeat_job(workflow)
    assignment = (
        "RUNTIME_TARGET_JSON: ${{ secrets.SCHWAB_RUNTIME_DAILY_TARGET_JSON || "
        "vars.RUNTIME_TARGET_JSON || secrets.RUNTIME_TARGET_JSON }}"
    )
    job_env = re.search(r"(?ms)^    env:\n(.*?)^    steps:", heartbeat_job)
    assert job_env is not None
    assert "RUNTIME_TARGET_JSON: ${{ vars.RUNTIME_TARGET_JSON || secrets.RUNTIME_TARGET_JSON }}" in job_env.group(1)
    assert assignment in daily_step
    assert workflow.count(assignment) == 1


def test_sync_token_is_not_exposed_job_wide() -> None:
    heartbeat_job = _heartbeat_job(_workflow_text())
    job_env = re.search(r"(?ms)^    env:\n(.*?)^    steps:", heartbeat_job)
    assert job_env is not None
    assert "EXECUTION_EVIDENCE_SYNC_TOKEN" not in job_env.group(1)
    # The token is injected only in the two existing step-level blocks.
    assert _workflow_text().count("EXECUTION_EVIDENCE_SYNC_TOKEN: ${{ secrets.EXECUTION_EVIDENCE_SYNC_TOKEN }}") == 2


def test_original_scheduling_notification_and_evidence_are_preserved() -> None:
    workflow = _workflow_text()
    assert workflow.count('cron: "20 22 * * *"') == 1
    assert workflow.count("cron:") == 1
    assert "RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS:" in workflow
    assert "Check recent execution report" in workflow
    assert "Publish read-only runtime execution evidence" in workflow
    assert "gcloud run deploy" not in workflow
    assert "gcloud scheduler jobs update" not in workflow


def test_retry_step_is_addressable_for_the_auth_condition() -> None:
    heartbeat_job = _heartbeat_job(_workflow_text())
    assert "id: gcp_auth_primary" in heartbeat_job
    assert "id: gcp_auth_retry" in heartbeat_job
    assert heartbeat_job.count("google-github-actions/auth@v3") == 2
    assert "steps.gcp_auth_primary.outcome == 'failure'" in heartbeat_job


def test_daily_step_appears_once_in_the_heartbeat_job() -> None:
    workflow = _workflow_text()
    assert workflow.count("- name: Publish daily runtime projection") == 1
    assert workflow.count(CLI) == 1
    heartbeat_job = _heartbeat_job(workflow)
    assert "- name: Publish daily runtime projection" in heartbeat_job
    assert "- name: Publish daily runtime projection" not in workflow.split("\n  account_facts:\n", 1)[1]


def test_daily_step_preserves_original_heartbeat_and_evidence_order() -> None:
    workflow = _workflow_text()
    install = workflow.index("- name: Install locked runtime dependencies")
    daily = workflow.index("- name: Publish daily runtime projection")
    check = workflow.index("- name: Check recent execution report")
    evidence = workflow.index("- name: Publish read-only runtime execution evidence")
    # A daily publisher failure cannot prevent the original notification steps.
    assert install < check < evidence < daily


def test_daily_publish_does_not_depend_on_heartbeat_or_evidence_success() -> None:
    workflow = _workflow_text()
    step = _step_block(workflow, STEP_NAME)
    condition = _condition(step)
    # A status function prevents Actions from implicitly gating on success().
    assert "!cancelled()" in condition
    assert workflow.index(CLI) > workflow.index("Publish read-only runtime execution evidence")
    assert "id: install_runtime_deps" in workflow
    assert _evaluate(condition, "true", "success", "skipped") is True
    for installed in ("failure", "skipped", "cancelled"):
        assert _evaluate(condition, "true", "success", "skipped", installed=installed) is False
    assert _evaluate(condition, "true", "success", "skipped", cancelled=True) is False
