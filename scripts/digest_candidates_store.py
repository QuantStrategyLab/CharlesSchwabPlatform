#!/usr/bin/env python3
"""Helpers for the NOTIFY-04 B2 private digest-candidate store.

``summarize`` prints a redacted summary (counts / booleans / business days),
never equity, account ids or target ids. ``object-uri`` prints the object path
``<uri>/<platform>/<business_day>/<target>.json`` for the single business day
carried by the rows (empty output when rows are undated or span several days).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any

_SAFE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,40}$")


def _rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("runs") if isinstance(payload, dict) else payload
    return [row for row in (rows or []) if isinstance(row, dict)]


def business_days(rows: list[dict[str, Any]]) -> list[str]:
    days = set()
    for row in rows:
        raw = str(row.get("business_day") or "").strip()[:10]
        try:
            days.add(dt.date.fromisoformat(raw).isoformat())
        except ValueError:
            continue
    return sorted(days)


def summarize(path: Path) -> dict[str, Any]:
    rows = _rows(path)
    return {
        "runs": len(rows),
        "business_days": business_days(rows),
        "undated_rows": sum(1 for row in rows if not str(row.get("business_day") or "").strip()),
        "equity_present": any(row.get("equity") is not None for row in rows),
        "fill_count_null": all(row.get("fill_count") is None for row in rows),
    }


def object_uri(path: Path, *, platform: str, target: str, uri: str) -> str:
    if not _SAFE.match(platform) or not _SAFE.match(target):
        raise ValueError("platform/target must be short lowercase identifiers")
    if not uri.startswith("gs://"):
        raise ValueError("uri must be a gs:// prefix")
    days = business_days(_rows(path))
    if len(days) != 1:
        return ""
    return f"{uri.rstrip('/')}/{platform}/{days[0]}/{target}.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("summarize")
    s.add_argument("--path", type=Path, required=True)
    o = sub.add_parser("object-uri")
    o.add_argument("--path", type=Path, required=True)
    o.add_argument("--platform", required=True)
    o.add_argument("--target", required=True)
    o.add_argument("--uri", required=True)
    args = parser.parse_args(argv)
    if args.command == "summarize":
        print(json.dumps(summarize(args.path), sort_keys=True))
    else:
        print(object_uri(args.path, platform=args.platform, target=args.target, uri=args.uri))
    return 0


if __name__ == "__main__":
    sys.exit(main())
