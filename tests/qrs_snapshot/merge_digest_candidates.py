#!/usr/bin/env python3
"""Merge producer DIGEST_CANDIDATES files into one candidates JSON (NOTIFY-04 B2).

Inputs: an optional base file (e.g. materialized from the legacy Environment
secret) plus every ``*.json`` under ``--input-dir`` (files fetched from the
private producer store, one per platform/target). Output: a single
``qsl.digest_candidates.v1`` envelope with concatenated ``runs``.

Configured inputs must all be readable candidate envelopes; failed/skipped
producer output is not evidence of an empty day. Any invalid input aborts the
merge before writing output, rather than claiming complete partial coverage.
Freshness is not decided here: the aggregator drops rows whose business day
differs from the digest business day (NOTIFY-04 B0). This script never prints
row contents — only counts — because rows may carry account equity.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "qsl.digest_candidates.v1"


def _rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        raw = payload
    elif isinstance(payload, dict):
        if payload.get("schema_version", SCHEMA_VERSION) != SCHEMA_VERSION:
            raise ValueError("candidate_schema_unsupported")
        if "producer_status" in payload and (
            not isinstance(payload["producer_status"], str)
            or payload["producer_status"] not in {"projected", "empty", "skipped"}
        ):
            raise ValueError("candidate_producer_status_invalid")
        if payload.get("producer_status") == "skipped":
            raise ValueError("candidate_producer_skipped")
        if "runs" not in payload and "candidates" not in payload:
            raise ValueError("candidate_rows_missing")
        raw = payload.get("runs", payload.get("candidates"))
    else:
        raise ValueError("candidates payload must be an object or list")
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise ValueError("candidates runs must be a list of objects")
    for item in raw:
        for name, limit in (("platform_id", 64), ("strategy_profile", 128)):
            value = item.get(name)
            if not isinstance(value, str) or not value.strip() or len(value) > limit:
                raise ValueError("candidate_required_fields_invalid")
        if type(item.get("actually_ran")) is not bool:
            raise ValueError("candidate_required_fields_invalid")
    from notification_mode import consolidation_enabled
    if consolidation_enabled():
        from notification_candidate_adapter import validate_run_row
        if isinstance(payload, dict) and any(
            name in payload for name in ("source_runs", "source_run_ids", "events_by_run", "notification_source_runs", "notification_events_by_run", "notification_events")
        ):
            raise ValueError("notification_candidate_per_run_required")
        raw = [validate_run_row(item) for item in raw]
    return [dict(item, legacy=True) if isinstance(payload, dict) and payload.get("legacy") is True
            else dict(item) for item in raw]


def _read_rows(path: Path) -> list[dict[str, Any]]:
    try:
        return _rows(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        # Paths and parser details can contain private target/account material.
        raise ValueError("candidate_input_unavailable_or_invalid") from None


def _walk_error(_error: OSError) -> None:
    raise ValueError("candidate_input_directory_unavailable") from None


def merge(base: Path | None, input_dir: Path | None) -> tuple[dict[str, Any], dict[str, int]]:
    if base is None and input_dir is None:
        raise ValueError("candidate_source_unconfigured")
    runs: list[dict[str, Any]] = []
    stats = {"base_rows": 0, "files": 0, "file_rows": 0, "invalid_files": 0}
    if base is not None:
        rows = _read_rows(base)
        stats["base_rows"] = len(rows)
        runs.extend(rows)
    if input_dir is not None:
        if not input_dir.is_dir():
            raise ValueError("candidate_input_directory_unavailable")
        # pathlib glob can silently suppress directory-read failures.
        paths = [Path(root) / name for root, _dirs, names in os.walk(input_dir, onerror=_walk_error)
                 for name in names if name.endswith(".json")]
        for path in sorted(paths):
            rows = _read_rows(path)
            stats["files"] += 1
            stats["file_rows"] += len(rows)
            runs.extend(rows)
    return {"schema_version": SCHEMA_VERSION, "runs": runs}, stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, default=None)
    parser.add_argument("--input-dir", type=Path, default=None)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        payload, stats = merge(args.base, args.input_dir)
    except (OSError, ValueError):
        print(json.dumps({"status": "failed", "reason_code": "candidate_input_unavailable_or_invalid"}), file=sys.stderr)
        return 2
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(json.dumps({"merged_rows": len(payload["runs"]), **stats}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
