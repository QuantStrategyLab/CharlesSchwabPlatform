"""Default-off release selector for the existing single notification mode."""
from __future__ import annotations

import os
from typing import Mapping
from notification_snapshot.unified_contract import unified_notification_enabled


def consolidation_enabled(env: Mapping[str, str] | None = None) -> bool:
    values = os.environ if env is None else env
    return unified_notification_enabled(values, "QRS_UNIFIED_NOTIFICATION_ENABLED")


def routine_mode(env: Mapping[str, str] | None = None) -> str:
    values = os.environ if env is None else env
    if consolidation_enabled(values):
        return "daily_digest"
    return values.get("QSL_ROUTINE_NOTIFICATION_MODE", "immediate")
