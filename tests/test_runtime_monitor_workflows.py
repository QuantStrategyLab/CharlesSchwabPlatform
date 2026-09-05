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
    for name in ("execution-report-heartbeat.yml", "runtime-guard.yml"):
        workflow = (ROOT / ".github/workflows" / name).read_text()

        assert workflow.count("google-github-actions/auth@v3") == 2
        assert "id: gcp_auth_primary" in workflow
        assert "continue-on-error: true" in workflow
        assert "steps.gcp_auth_primary.outcome == 'failure'" in workflow


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
