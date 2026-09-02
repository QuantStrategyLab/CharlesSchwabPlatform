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
    assert "pandas-market-calendars==5.4.0" in workflow


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
