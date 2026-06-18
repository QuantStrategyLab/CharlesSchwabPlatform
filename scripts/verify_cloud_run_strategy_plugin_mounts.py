#!/usr/bin/env python3
"""Verify Cloud Run strategy plugin mounts after env sync."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterable, Mapping
from typing import Any


DEFAULT_MOUNT_ENV_NAMES = (
    "IBKR_STRATEGY_PLUGIN_MOUNTS_JSON",
    "SCHWAB_STRATEGY_PLUGIN_MOUNTS_JSON",
    "LONGBRIDGE_STRATEGY_PLUGIN_MOUNTS_JSON",
    "FIRSTRADE_STRATEGY_PLUGIN_MOUNTS_JSON",
)
DEFAULT_ALLOWED_SIGNAL_PREFIXES = (
    "gs://qsl-runtime-logs-shared/",
    "gs://qsl-runtime-logs-interactivebrokersquant/",
)


def _split_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.replace(";", ",").split(",") if item.strip()]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _mount_env_names() -> list[str]:
    configured = _split_csv(os.environ.get("STRATEGY_PLUGIN_MOUNT_ENV_NAMES"))
    return configured or list(DEFAULT_MOUNT_ENV_NAMES)


def _allowed_signal_prefixes() -> tuple[str, ...]:
    configured = _split_csv(os.environ.get("STRATEGY_PLUGIN_ALLOWED_SIGNAL_PREFIXES"))
    return tuple(configured or DEFAULT_ALLOWED_SIGNAL_PREFIXES)


def _load_expected_targets(mount_env_names: Iterable[str]) -> list[dict[str, Any]]:
    raw_plan = (os.environ.get("SYNC_PLAN_JSON") or "").strip()
    if raw_plan:
        plan = json.loads(raw_plan)
        targets = plan.get("targets")
        if not isinstance(targets, list):
            raise ValueError("SYNC_PLAN_JSON.targets must be a list")
        expected_targets: list[dict[str, Any]] = []
        for target in targets:
            if not isinstance(target, Mapping):
                raise ValueError("Each SYNC_PLAN_JSON target must be an object")
            service = str(target.get("service_name") or "").strip()
            if not service:
                raise ValueError("Each SYNC_PLAN_JSON target requires service_name")
            env = target.get("env") if isinstance(target.get("env"), Mapping) else {}
            expected_targets.append(
                {
                    "service": service,
                    "expected": {
                        name: str(env.get(name) or "").strip()
                        for name in mount_env_names
                    },
                }
            )
        return expected_targets

    service = (os.environ.get("CLOUD_RUN_SERVICE") or "").strip()
    if not service:
        raise ValueError("CLOUD_RUN_SERVICE or SYNC_PLAN_JSON is required")
    return [
        {
            "service": service,
            "expected": {
                name: (os.environ.get(name) or "").strip()
                for name in mount_env_names
            },
        }
    ]


def _run(command: list[str]) -> str:
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(detail or f"Command failed: {' '.join(command)}")
    return result.stdout


def _describe_service(service: str, region: str, project: str | None) -> dict[str, Any]:
    command = ["gcloud", "run", "services", "describe", service, "--region", region, "--format=json"]
    if project:
        command.extend(["--project", project])
    payload = _run(command)
    loaded = json.loads(payload)
    if not isinstance(loaded, dict):
        raise RuntimeError(f"Cloud Run describe returned non-object JSON for {service}")
    return loaded


def _container_env(service_json: Mapping[str, Any]) -> dict[str, str]:
    containers = (
        service_json.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [])
    )
    if not containers:
        return {}
    values: dict[str, str] = {}
    for item in containers[0].get("env", []) or []:
        if not isinstance(item, Mapping):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        if "value" in item:
            values[name] = str(item.get("value") or "").strip()
    return values


def _load_mounts(raw: str, *, service: str, env_name: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{service}:{env_name} must be valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{service}:{env_name} must decode to a JSON object")
    plugins = payload.get("strategy_plugins")
    if not isinstance(plugins, list):
        raise ValueError(f"{service}:{env_name}.strategy_plugins must be a list")
    return payload


def _check_signal_path(
    *,
    service: str,
    env_name: str,
    plugin: Mapping[str, Any],
    allowed_prefixes: tuple[str, ...],
) -> None:
    enabled = plugin.get("enabled")
    if enabled is False or str(enabled).strip().lower() == "false":
        return

    plugin_name = str(plugin.get("plugin") or "").strip()
    strategy = str(plugin.get("strategy") or "").strip()
    signal_path = str(plugin.get("signal_path") or "").strip()
    expected_schema = str(plugin.get("expected_schema_version") or "").strip()

    if not strategy or not plugin_name or not signal_path:
        raise ValueError(
            f"{service}:{env_name} enabled plugin mounts require strategy, plugin, and signal_path"
        )
    if not signal_path.startswith("gs://"):
        raise ValueError(f"{service}:{env_name} signal_path must be a gs:// URI: {signal_path}")
    if allowed_prefixes and not signal_path.startswith(allowed_prefixes):
        raise ValueError(
            f"{service}:{env_name} signal_path is outside allowed prefixes: {signal_path}"
        )

    try:
        signal_raw = _run(["gcloud", "storage", "cat", signal_path])
    except RuntimeError as exc:
        print(
            f"Warning: {service}:{env_name} signal_path is not readable yet; "
            f"strategy runtime will ignore the plugin until a valid signal exists: "
            f"{signal_path} ({exc})"
        )
        return
    try:
        signal = json.loads(signal_raw)
    except json.JSONDecodeError as exc:
        print(
            f"Warning: {service}:{env_name} signal_path does not contain valid JSON; "
            f"strategy runtime will ignore the plugin until it is fixed: {signal_path} ({exc})"
        )
        return
    if not isinstance(signal, dict):
        print(
            f"Warning: {service}:{env_name} signal_path must contain a JSON object; "
            f"strategy runtime will ignore the plugin until it is fixed: {signal_path}"
        )
        return
    if expected_schema and str(signal.get("schema_version") or "").strip() != expected_schema:
        print(
            f"Warning: {service}:{env_name} expected schema {expected_schema}, "
            f"got {signal.get('schema_version')!r} at {signal_path}; "
            "strategy runtime will ignore the plugin until it is fixed."
        )
        return


def _verify_target(
    *,
    service: str,
    expected: Mapping[str, str],
    actual_env: Mapping[str, str],
    allowed_prefixes: tuple[str, ...],
) -> list[str]:
    checked: list[str] = []
    for env_name, expected_raw in expected.items():
        actual_raw = str(actual_env.get(env_name) or "").strip()
        if not expected_raw:
            if actual_raw:
                raise ValueError(f"{service}:{env_name} should be removed, but is still set")
            continue

        expected_mounts = _load_mounts(expected_raw, service=service, env_name=env_name)
        if not actual_raw:
            raise ValueError(f"{service}:{env_name} is missing from Cloud Run")
        actual_mounts = _load_mounts(actual_raw, service=service, env_name=env_name)
        if _canonical_json(actual_mounts) != _canonical_json(expected_mounts):
            raise ValueError(f"{service}:{env_name} does not match the configured deploy value")

        for plugin in expected_mounts["strategy_plugins"]:
            if not isinstance(plugin, Mapping):
                raise ValueError(f"{service}:{env_name}.strategy_plugins entries must be objects")
            _check_signal_path(
                service=service,
                env_name=env_name,
                plugin=plugin,
                allowed_prefixes=allowed_prefixes,
            )
        checked.append(env_name)
    return checked


def main() -> int:
    region = (os.environ.get("CLOUD_RUN_REGION") or "").strip()
    if not region:
        print("CLOUD_RUN_REGION is required", file=sys.stderr)
        return 1

    project = (os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "").strip()
    mount_env_names = _mount_env_names()
    allowed_prefixes = _allowed_signal_prefixes()

    try:
        targets = _load_expected_targets(mount_env_names)
        for target in targets:
            service = target["service"]
            service_json = _describe_service(service, region, project or None)
            actual_env = _container_env(service_json)
            checked = _verify_target(
                service=service,
                expected=target["expected"],
                actual_env=actual_env,
                allowed_prefixes=allowed_prefixes,
            )
            if checked:
                print(f"Verified strategy plugin mounts for {service}: {', '.join(checked)}")
            else:
                print(f"No strategy plugin mounts expected for {service}; verified none remain.")
    except Exception as exc:  # noqa: BLE001
        print(f"Strategy plugin mount verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
