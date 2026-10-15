"""Project existing private reports; no service or broker invocation."""
from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from notifications.digest_snapshot.digest_archive import main  # noqa: E402
if __name__ == "__main__":
    raise SystemExit(main())
