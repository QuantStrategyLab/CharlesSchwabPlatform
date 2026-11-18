#!/usr/bin/env python3
"""Run the local synthetic D1 no-order acceptance, including a new process."""

from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from charles_schwab_platform.entrypoints.d1_no_order_acceptance import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
