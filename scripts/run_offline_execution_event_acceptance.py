#!/usr/bin/env python3
"""Run E01-E06 without leaving acceptance state or output files behind."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from charles_schwab_platform.application.offline_execution_event_acceptance import run_offline_execution_event_acceptance  # noqa: E402


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="schwab-execution-event-acceptance-") as state_dir:
        result = run_offline_execution_event_acceptance(state_dir)
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
