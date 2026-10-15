from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_execution_report_heartbeat_has_market_neutral_daily_schedule() -> None:
    workflow = (ROOT / ".github/workflows/execution-report-heartbeat.yml").read_text()

    assert 'cron: "20 22 * * *"' in workflow
    assert 'cron: "20 22 * * 1-5"' not in workflow
    assert "RUNTIME_HEARTBEAT_MARKET_AWARE:" in workflow
    assert "RUNTIME_HEARTBEAT_PUBLICATION_GRACE_MINUTES:" in workflow
    assert "RUNTIME_HEARTBEAT_SCHEDULER_LOCATION:" in workflow
    assert "CLOUD_SCHEDULER_MAIN_TIME:" in workflow
    assert "pandas-market-calendars==5.4.0" not in workflow


def test_runtime_monitor_workflows_retry_gcp_authentication() -> None:
    heartbeat_workflow = (ROOT / ".github/workflows/execution-report-heartbeat.yml").read_text()
    heartbeat_job = heartbeat_workflow.split("\n  heartbeat:\n", 1)[1].split(
        "\n  account_facts:\n", 1
    )[0]
    publisher_job = heartbeat_workflow.split("\n  account_facts:\n", 1)[1]

    assert heartbeat_job.count("google-github-actions/auth@v3") == 2
    assert "id: gcp_auth_primary" in heartbeat_job
    assert "continue-on-error: true" in heartbeat_job
    assert "steps.gcp_auth_primary.outcome == 'failure'" in heartbeat_job
    assert publisher_job.count("google-github-actions/auth@v3") == 1
    assert "needs:" not in publisher_job

    runtime_guard = (ROOT / ".github/workflows/runtime-guard.yml").read_text()
    assert runtime_guard.count("google-github-actions/auth@v3") == 2
    assert "id: gcp_auth_primary" in runtime_guard
    assert "continue-on-error: true" in runtime_guard
    assert "steps.gcp_auth_primary.outcome == 'failure'" in runtime_guard


def test_runtime_guard_uses_locked_runtime_environment() -> None:
    setup_uv = "astral-sh/setup-uv@d0cc045d04ccac9d8b7881df0226f9e82c39688e"
    for name in ("runtime-guard.yml", "runtime-target-lifecycle.yml"):
        workflow = (ROOT / ".github/workflows" / name).read_text()

        assert workflow.count(setup_uv) == 1
        assert workflow.count("astral-sh/setup-uv@") == 1
        assert workflow.index(setup_uv) < workflow.index("- name: Authenticate to Google Cloud")
        assert "python -m pip install --upgrade pip uv" not in workflow
        assert "uv sync --frozen --no-dev" in workflow
        assert workflow.count("cloud_run_runtime_guard.py") == 1
        assert "uv run --no-sync python scripts/cloud_run_runtime_guard.py" in workflow
        assert workflow.index("uv sync --frozen --no-dev") < workflow.index(
            "uv run --no-sync python scripts/cloud_run_runtime_guard.py"
        )


def test_qpk_dependent_heartbeats_use_locked_uv_runtime() -> None:
    setup_uv = "astral-sh/setup-uv@d0cc045d04ccac9d8b7881df0226f9e82c39688e"
    for name in ("execution-report-heartbeat.yml", "runtime-target-lifecycle.yml"):
        workflow = (ROOT / ".github/workflows" / name).read_text()

        assert workflow.count(setup_uv) == 1
        assert workflow.count("astral-sh/setup-uv@") == 1
        assert workflow.count("uv sync --frozen --no-dev") == 1
        assert "python -m pip install" not in workflow
        assert "pandas-market-calendars==5.4.0" not in workflow
        assert "uv run --no-sync python scripts/execution_report_heartbeat.py" in workflow


def test_lifecycle_classifies_import_failures_as_unavailable() -> None:
    workflow = (ROOT / ".github/workflows/runtime-target-lifecycle.yml").read_text()

    assert workflow.count("status=unavailable") >= 2
    assert workflow.count("traceback|importerror|modulenotfounderror") == 2


def test_lifecycle_observes_production_drift_without_optimization() -> None:
    workflow = (ROOT / ".github/workflows/runtime-target-lifecycle.yml").read_text()

    assert "scripts/production_drift_health_observe.py" in workflow
    assert "id: production_drift" in workflow
    assert "LIFECYCLE_PERFORMANCE_BUCKET" in workflow
    assert "| Production drift |" in workflow
    assert "run_research_promotion_cycle" not in workflow


def test_lifecycle_uses_fail_closed_reconcile_only_state_resolver() -> None:
    workflow = (ROOT / ".github/workflows/runtime-target-lifecycle.yml").read_text()

    assert "python3 scripts/runtime_target_lifecycle_state.py" in workflow
    assert 'or "true"' not in workflow


def test_lifecycle_observes_completed_sync_regardless_of_conclusion() -> None:
    workflow = (ROOT / ".github/workflows/runtime-target-lifecycle.yml").read_text()
    sync = (ROOT / ".github/workflows/sync-cloud-run-env.yml").read_text()
    sync_name = sync.splitlines()[0].removeprefix("name: ")

    assert f'workflows: ["{sync_name}"]' in workflow
    assert "types: [completed]" in workflow
    assert "github.event.workflow_run.conclusion" not in workflow
    assert "github.event.workflow_run.head_sha" not in workflow


def test_lifecycle_publishes_read_only_observation_for_exact_service() -> None:
    workflow = (ROOT / ".github/workflows/runtime-target-lifecycle.yml").read_text()
    publisher = workflow.split("- name: Publish lifecycle to the unified control plane", 1)[1]
    publisher = publisher.split("\n      - name:", 1)[0]

    for line in (
        "observe-gcp: 'true'",
        "gcp-project: ${{ env.GCP_PROJECT_ID }}",
        "cloud-run-region: ${{ env.CLOUD_RUN_REGION }}",
        "cloud-run-service: ${{ env.CLOUD_RUN_SERVICE }}",
        "scheduler-location: ${{ env.RUNTIME_HEARTBEAT_SCHEDULER_LOCATION }}",
    ):
        assert line in publisher
    assert "CLOUD_RUN_SERVICE: ${{ secrets.CLOUD_RUN_SERVICE }}" in workflow
    assert "RUNTIME_HEARTBEAT_SCHEDULER_LOCATION: ${{ vars.RUNTIME_HEARTBEAT_SCHEDULER_LOCATION || vars.CLOUD_RUN_REGION || 'us-central1' }}" in workflow
    assert "CLOUD_RUN_SERVICES" not in publisher
    assert "gcloud scheduler jobs update" not in workflow
    assert "gcloud run deploy" not in workflow


def _manual_input_block(workflow: str, name: str) -> str:
    import re
    match = re.search(rf"(?ms)^      {name}:\n(.*?)(?=^      [a-z_]+:|^  [a-z_]+:)", workflow)
    assert match is not None, f"missing workflow_dispatch input {name}"
    return match.group(1)


def _evaluate_success_notify_expression(workflow: str, event: str, explicit_input, repository_value) -> str:
    """Evaluate this bounded Actions boolean/string expression without running a workflow."""
    import ast
    import re
    expression = re.search(r"RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS: \$\{\{ (.*?) \}\}", workflow).group(1)
    expression = expression.replace("github.event_name", repr(event))
    expression = expression.replace("inputs.notify_on_success", repr(False if explicit_input is None else explicit_input))
    expression = expression.replace("vars.RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS", repr(repository_value or ""))
    expression = expression.replace("&&", " and ").replace("||", " or ")
    parsed = ast.parse(expression, mode="eval")
    assert all(isinstance(node, (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.Compare, ast.Eq, ast.NotEq, ast.Constant)) for node in ast.walk(parsed))
    result = eval(compile(parsed, "<offline Actions expression>", "eval"), {"__builtins__": {}}, {})
    return str(result).lower() if isinstance(result, bool) else str(result)


def test_manual_healthy_notify_is_typed_and_explicitly_opt_in() -> None:
    workflow = (ROOT / ".github/workflows/execution-report-heartbeat.yml").read_text()
    block = _manual_input_block(workflow, "notify_on_success")
    assert "type: boolean" in block
    assert "default: false" in block
    assert "required: false" in block
    line = next(line for line in workflow.splitlines() if "RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS:" in line)
    assert "inputs.notify_on_success" in line
    assert "github.event.inputs" not in line
    assert "vars.RUNTIME_HEARTBEAT_NOTIFY_ON_SUCCESS" not in line
    assert "github.event_name == 'workflow_dispatch'" in line


def test_schedule_and_manual_default_never_inherit_legacy_success_variable() -> None:
    workflow = (ROOT / ".github/workflows/execution-report-heartbeat.yml").read_text()
    for event in ("schedule", "workflow_dispatch", "repository_dispatch"):
        for explicit_input in (None, False, True):
            for repository_value in (None, "false", "true"):
                actual = _evaluate_success_notify_expression(workflow, event, explicit_input, repository_value)
                expected = "true" if event == "workflow_dispatch" and explicit_input is True else "false"
                assert actual == expected, (event, explicit_input, repository_value, actual)


def test_manual_quiet_control_preserves_existing_alert_and_report_steps() -> None:
    workflow = (ROOT / ".github/workflows/execution-report-heartbeat.yml").read_text()
    alert_block = _manual_input_block(workflow, "fail_workflow_on_alert")
    assert 'default: "true"' in alert_block
    assert "RUNTIME_HEARTBEAT_FAIL_WORKFLOW_ON_ALERT: ${{ inputs.fail_workflow_on_alert || vars.RUNTIME_HEARTBEAT_FAIL_WORKFLOW_ON_ALERT || 'true' }}" in workflow
    assert "uv run --no-sync python scripts/execution_report_heartbeat.py" in workflow
    assert "Publish read-only runtime execution evidence" in workflow
    assert 'cron: "20 22 * * *"' in workflow
