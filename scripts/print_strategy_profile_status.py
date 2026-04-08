from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QPK_SRC = ROOT.parent / "QuantPlatformKit" / "src"
UES_SRC = ROOT.parent / "UsEquityStrategies" / "src"

for candidate in (ROOT, QPK_SRC, UES_SRC):
    candidate_str = str(candidate)
    if candidate_str not in sys.path:
        sys.path.insert(0, candidate_str)

from strategy_registry import get_platform_profile_status_matrix  # noqa: E402


def _print_table(rows: list[dict[str, object]]) -> None:
    headers = (
        "canonical_profile",
        "display_name",
        "eligible",
        "enabled",
        "is_default",
        "is_rollback",
        "domain",
    )
    widths = {
        header: max(len(header), *(len(str(row.get(header, ""))) for row in rows))
        for header in headers
    }
    print("  ".join(header.ljust(widths[header]) for header in headers))
    print("  ".join("-" * widths[header] for header in headers))
    for row in rows:
        print("  ".join(str(row.get(header, "")).ljust(widths[header]) for header in headers))


def main() -> int:
    rows = get_platform_profile_status_matrix()
    if "--json" in sys.argv:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    _print_table(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
