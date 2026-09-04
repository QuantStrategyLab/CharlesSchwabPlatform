from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.runtime_target_lifecycle_state import (
    RuntimeTargetLifecycleStateError,
    resolve_runtime_target_lifecycle_state,
)


def _target(*, continuity_state: str = "ACTIVE_LKG", execution_mode: str = "live") -> str:
    return json.dumps(
        {
            "execution_mode": execution_mode,
            "live_continuity": {"state": continuity_state},
        }
    )


def test_omitted_enablement_defaults_to_disabled() -> None:
    state = resolve_runtime_target_lifecycle_state(
        {"RUNTIME_TARGET_JSON": _target()}
    )

    assert state.configured_state == "disabled"
    assert state.execution_mode == "live"


def test_reconcile_only_target_is_disabled_for_standard_heartbeat() -> None:
    state = resolve_runtime_target_lifecycle_state(
        {
            "RUNTIME_TARGET_ENABLED": "true",
            "RUNTIME_TARGET_JSON": _target(continuity_state="RECONCILE_ONLY"),
        }
    )

    assert state.configured_state == "disabled"
    assert state.execution_mode == "live"


def test_active_lkg_target_requires_explicit_enablement() -> None:
    state = resolve_runtime_target_lifecycle_state(
        {
            "RUNTIME_TARGET_ENABLED": "true",
            "RUNTIME_TARGET_JSON": _target(),
        }
    )

    assert state.configured_state == "enabled"


def test_invalid_target_or_enablement_fails_closed() -> None:
    with pytest.raises(RuntimeTargetLifecycleStateError, match="invalid"):
        resolve_runtime_target_lifecycle_state({"RUNTIME_TARGET_JSON": "not-json"})

    with pytest.raises(RuntimeTargetLifecycleStateError, match="true or false"):
        resolve_runtime_target_lifecycle_state(
            {
                "RUNTIME_TARGET_ENABLED": "maybe",
                "RUNTIME_TARGET_JSON": _target(),
            }
        )
