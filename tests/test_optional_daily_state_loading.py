"""Optional strategy wiring must not require unpublished dependency APIs."""
from __future__ import annotations

import subprocess
import sys


def test_optional_daily_state_modules_load_without_candidate_packages():
    source = r'''
import importlib
import importlib.abc
import sys
from dataclasses import dataclass, field
from types import SimpleNamespace

class MissingCandidatePackages(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split(".")[0] in {"quant_platform_kit", "us_equity_strategies"}:
            raise ImportError("candidate package APIs unavailable")
        return None

sys.meta_path.insert(0, MissingCandidatePackages())
for name in ("application.daily_strategy_state", "application.decision_cycle",
             "application.joint_strategy_execution", "application.joint_account_facts",
             "application.execution_evidence"):
    importlib.import_module(name)

from application.daily_strategy_state import DailyStateBinding, evaluate_with_daily_state

@dataclass(frozen=True)
class Context:
    state: dict = field(default_factory=lambda: {"unrelated_state": "preserved"})
    capabilities: dict = field(default_factory=dict)

context = Context()
calls = []
legacy_decision = SimpleNamespace(risk_increase_allowed=True)
entrypoint = SimpleNamespace(manifest=SimpleNamespace(profile="tqqq_growth_income"),
    evaluate=lambda value: (calls.append(value), legacy_decision)[1])
assert evaluate_with_daily_state(entrypoint, context, None) is legacy_decision
for joint in (None, object()):
    binding = DailyStateBinding(store=object(), identity=object(),
        effective_session="2026-10-30", input_sha256="a" * 64,
        expected_previous_session=None, joint_planning_session=joint)
    try:
        evaluate_with_daily_state(entrypoint, context, binding)
    except RuntimeError as exc:
        assert "supported daily state strategy contract required" in str(exc)
    else:
        raise AssertionError("unsupported binding reached a legacy strategy that ignores daily state")
assert len(calls) == 1
'''
    result = subprocess.run([sys.executable, "-c", source], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
