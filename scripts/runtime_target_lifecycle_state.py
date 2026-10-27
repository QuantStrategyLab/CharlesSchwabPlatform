"""Resolve the no-order lifecycle state from a runtime target configuration."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass


class RuntimeTargetLifecycleStateError(ValueError):
    """Raised when the static lifecycle state is ambiguous."""


@dataclass(frozen=True)
class RuntimeTargetLifecycleState:
    configured_state: str
    execution_mode: str
    operator_state: str
    continuity_state: str


def _text(value: object) -> str:
    return str(value or "").strip()


def _target_from_env(environ: Mapping[str, str]) -> Mapping[str, object]:
    raw_target = _text(environ.get("RUNTIME_TARGET_JSON"))
    if not raw_target:
        return {}
    try:
        target = json.loads(raw_target)
    except json.JSONDecodeError as exc:
        raise RuntimeTargetLifecycleStateError("RUNTIME_TARGET_JSON is invalid") from exc
    if not isinstance(target, Mapping):
        raise RuntimeTargetLifecycleStateError("RUNTIME_TARGET_JSON is invalid")
    return target


def _enabled(environ: Mapping[str, str], target: Mapping[str, object]) -> bool:
    raw_enabled = _text(environ.get("RUNTIME_TARGET_ENABLED"))
    if not raw_enabled:
        raw_enabled = _text(
            target.get("runtime_target_enabled") or target.get("RUNTIME_TARGET_ENABLED")
        )
    if not raw_enabled:
        return False
    if raw_enabled not in {"true", "false"}:
        raise RuntimeTargetLifecycleStateError("RUNTIME_TARGET_ENABLED must be true or false")
    return raw_enabled == "true"


def _execution_mode(environ: Mapping[str, str], target: Mapping[str, object]) -> str:
    mode = _text(target.get("execution_mode")).lower()
    if not mode:
        return "dry_run" if _text(environ.get("SCHWAB_DRY_RUN_ONLY")).lower() == "true" else "live"
    if mode not in {"dry_run", "paper", "live"}:
        raise RuntimeTargetLifecycleStateError("runtime target execution_mode is invalid")
    return mode


def _permits_standard_execution(target: Mapping[str, object]) -> bool:
    continuity = target.get("live_continuity")
    if not isinstance(continuity, Mapping):
        return True
    state = _text(continuity.get("state")).upper()
    return not state or state in {"ACTIVE_LKG", "ROLLBACK_LKG"}


def resolve_runtime_target_lifecycle_state(
    environ: Mapping[str, str] | None = None,
) -> RuntimeTargetLifecycleState:
    """Return lifecycle fields without loading credentials or contacting a service."""

    environment = os.environ if environ is None else environ
    target = _target_from_env(environment)
    operator_enabled = _enabled(environment, target)
    continuity = target.get("live_continuity")
    continuity_state = (
        _text(continuity.get("state")).upper()
        if isinstance(continuity, Mapping) else ""
    ) or "NONE"
    if continuity_state not in {
        "NONE", "ACTIVE_LKG", "ROLLBACK_LKG", "PAUSED", "RECONCILE_ONLY",
    }:
        continuity_state = "UNKNOWN"
    configured_state = (
        "enabled"
        if operator_enabled and _permits_standard_execution(target)
        else "disabled"
    )
    return RuntimeTargetLifecycleState(
        configured_state=configured_state,
        execution_mode=_execution_mode(environment, target),
        operator_state="enabled" if operator_enabled else "disabled",
        continuity_state=continuity_state,
    )


def main() -> int:
    state = resolve_runtime_target_lifecycle_state()
    print(f"configured_state={state.configured_state}")
    print(f"execution_mode={state.execution_mode}")
    print(f"operator_state={state.operator_state}")
    print(f"continuity_state={state.continuity_state}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
