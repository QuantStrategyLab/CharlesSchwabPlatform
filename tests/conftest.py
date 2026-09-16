"""Prefer sibling risk-binding worktrees over the pinned site-packages QPK/UES.

Formal pin/lock of RuntimeRiskLimits still lands after QPK/UES merge; until then
local acceptance imports the uncommitted worktree sources for these tests only.
"""

from __future__ import annotations

import sys
from pathlib import Path

_WORKTREES = Path(__file__).resolve().parents[2]
_QPK_SRC = _WORKTREES / "qpk-runtime-risk-limits-20260917" / "src"
_UES_SRC = _WORKTREES / "ues-runtime-risk-limits-20260917" / "src"
_SCHWAB_ROOT = Path(__file__).resolve().parents[1]

for _path in (_SCHWAB_ROOT, _UES_SRC, _QPK_SRC):
    resolved = str(_path)
    if _path.is_dir() and resolved not in sys.path:
        sys.path.insert(0, resolved)
