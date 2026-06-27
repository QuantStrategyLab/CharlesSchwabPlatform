import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
QPK_SRC = ROOT.parent / "QuantPlatformKit" / "src"
UES_SRC = ROOT.parent / "UsEquityStrategies" / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(QPK_SRC) not in sys.path:
    sys.path.insert(0, str(QPK_SRC))
if str(UES_SRC) not in sys.path:
    sys.path.insert(0, str(UES_SRC))

SYNC_PLAN_SCRIPT_PATH = ROOT / "scripts" / "build_cloud_run_env_sync_plan.py"


def runtime_target_json(
    strategy_profile: str,
    *,
    dry_run_only: bool = False,
    platform_id: str = "schwab",
    deployment_selector: str | None = "schwab",
    account_selector: list[str] | tuple[str, ...] | None = None,
    account_scope: str | None = "schwab",
    service_name: str | None = None,
) -> str:
    payload: dict[str, object] = {
        "platform_id": platform_id,
        "strategy_profile": strategy_profile,
        "dry_run_only": dry_run_only,
    }
    if deployment_selector is not None:
        payload["deployment_selector"] = deployment_selector
    if account_selector is not None:
        payload["account_selector"] = list(account_selector)
    if account_scope is not None:
        payload["account_scope"] = account_scope
    if service_name is not None:
        payload["service_name"] = service_name
    payload["execution_mode"] = "paper" if dry_run_only else "live"
    return json.dumps(payload, separators=(",", ":"))


def test_build_cloud_run_env_sync_plan_legacy_mode_tqqq_growth_income():
    env = {
        **os.environ,
        "CLOUD_RUN_SERVICE": "charles-schwab-service",
        "GLOBAL_TELEGRAM_CHAT_ID": "5992562050",
        "NOTIFY_LANG": "zh",
        "RUNTIME_TARGET_JSON": runtime_target_json(
            "tqqq_growth_income",
            service_name="charles-schwab-service",
        ),
        "EXECUTION_REPORT_GCS_URI": "gs://runtime/execution-reports",
        "CLOUD_SCHEDULER_MAIN_TIME": "10 16",
        "CLOUD_SCHEDULER_PROBE_TIME": "40 9,15",
    }

    result = subprocess.run(
        [sys.executable, str(SYNC_PLAN_SCRIPT_PATH), "--json"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    plan = json.loads(result.stdout)
    assert plan["mode"] == "legacy"
    assert len(plan["targets"]) == 1
    target = plan["targets"][0]
    assert target["service_name"] == "charles-schwab-service"
    assert target["strategy_profile"] == "tqqq_growth_income"
    assert target["env"]["GLOBAL_TELEGRAM_CHAT_ID"] == "5992562050"
    assert target["env"]["NOTIFY_LANG"] == "zh"
    assert target["env"]["STRATEGY_PROFILE"] == "tqqq_growth_income"
    assert target["env"]["EXECUTION_REPORT_GCS_URI"] == "gs://runtime/execution-reports"
    assert target["scheduler"] == {
        "timezone": "America/New_York",
        "main_time": "10 16",
        "probe_time": "40 9,15",
        "precheck_time": "45 9",
    }
    assert "SCHWAB_FEATURE_SNAPSHOT_PATH" not in target["env"]
    assert "SCHWAB_FEATURE_SNAPSHOT_PATH" in target["remove_env_vars"]


def test_build_cloud_run_env_sync_plan_requires_target_snapshot_in_per_service_mode():
    payload = {
        "defaults": {
            "GLOBAL_TELEGRAM_CHAT_ID": "5992562050",
            "NOTIFY_LANG": "zh",
        },
        "targets": [
            {
                "service": "charles-schwab-live-u7654-mega-service",
                "runtime_target": json.loads(
                    runtime_target_json(
                        "russell_top50_leader_rotation",
                        deployment_selector="live-u7654-mega",
                        account_selector=["U7654321"],
                        account_scope="live-u7654-mega",
                        service_name="charles-schwab-live-u7654-mega-service",
                    )
                ),
            }
        ],
    }
    env = {
        **os.environ,
        "CLOUD_RUN_SERVICE_TARGETS_JSON": json.dumps(payload),
        "SCHWAB_FEATURE_SNAPSHOT_PATH": "gs://stale-paper/snapshot.csv",
    }

    result = subprocess.run(
        [sys.executable, str(SYNC_PLAN_SCRIPT_PATH), "--json"],
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode != 0
    assert "charles-schwab-live-u7654-mega-service:SCHWAB_FEATURE_SNAPSHOT_PATH" in result.stderr
    assert "gs://stale-paper/snapshot.csv" not in result.stderr


def test_build_cloud_run_env_sync_plan_skips_snapshot_requirements_for_disabled_target():
    payload = {
        "defaults": {
            "GLOBAL_TELEGRAM_CHAT_ID": "5992562050",
            "NOTIFY_LANG": "zh",
        },
        "targets": [
            {
                "service": "charles-schwab-live-u7654-mega-service",
                "runtime_target_enabled": "false",
                "runtime_target": json.loads(
                    runtime_target_json(
                        "russell_top50_leader_rotation",
                        deployment_selector="live-u7654-mega",
                        account_selector=["U7654321"],
                        account_scope="live-u7654-mega",
                        service_name="charles-schwab-live-u7654-mega-service",
                    )
                ),
            }
        ],
    }
    env = {
        **os.environ,
        "CLOUD_RUN_SERVICE_TARGETS_JSON": json.dumps(payload),
        "SCHWAB_FEATURE_SNAPSHOT_PATH": "gs://stale-paper/snapshot.csv",
        "SCHWAB_FEATURE_SNAPSHOT_MANIFEST_PATH": "gs://stale-paper/snapshot.csv.manifest.json",
    }

    result = subprocess.run(
        [sys.executable, str(SYNC_PLAN_SCRIPT_PATH), "--json"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    plan = json.loads(result.stdout)
    target = plan["targets"][0]
    assert target["service_name"] == "charles-schwab-live-u7654-mega-service"
    assert target["env"]["RUNTIME_TARGET_ENABLED"] == "false"
    assert "SCHWAB_FEATURE_SNAPSHOT_PATH" not in target["env"]
    assert "SCHWAB_FEATURE_SNAPSHOT_MANIFEST_PATH" not in target["env"]
    assert "SCHWAB_FEATURE_SNAPSHOT_PATH" in target["remove_env_vars"]
    assert "SCHWAB_FEATURE_SNAPSHOT_MANIFEST_PATH" in target["remove_env_vars"]
    assert "gs://stale-paper/snapshot.csv" not in result.stdout
