"""Inspect bounded local declarations without inventing trusted provenance.

This standalone adapter returns BLOCKED (exit 2) or FAIL (exit 1), never
authenticated acceptance. CI tests this contract with synthetic inputs. The
native Gitleaks job remains separate and unchanged; no real review evidence is
fabricated or fetched. This module performs no scanner, Git, API or runtime call.

Usage: python -m scripts.governance_evidence_gate --help
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat

from scripts import ai_review_evidence, gitleaks_report_evidence


_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_EXIT = re.compile(rb"(?:0|[1-9][0-9]{0,2}|-[1-9][0-9]{0,2})\n?\Z")
_EXIT_BYTES = 8


class _InputError(ValueError):
    def __init__(self, code, *, blocked=False):
        self.code = code
        self.blocked = blocked
        super().__init__(code)


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        raise _InputError("ARGUMENTS_INVALID")


def _revision(value):
    return (type(value) is str and _REVISION.fullmatch(value) is not None
            and value != "0" * 40)


def _read_exit(path):
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_NONBLOCK"):
        raise _InputError("SAFE_FILE_OPEN_UNAVAILABLE", blocked=True)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            details = os.fstat(stream.fileno())
            if not stat.S_ISREG(details.st_mode) or details.st_size > _EXIT_BYTES:
                raise _InputError("SCANNER_EXIT_CAPTURE_INVALID")
            raw = stream.read(_EXIT_BYTES + 1)
        if len(raw) > _EXIT_BYTES or _EXIT.fullmatch(raw) is None:
            raise _InputError("SCANNER_EXIT_CAPTURE_INVALID")
        value = int(raw)
        if not -255 <= value <= 255:
            raise _InputError("SCANNER_EXIT_CAPTURE_INVALID")
        return value
    except FileNotFoundError:
        raise _InputError("SCANNER_EXIT_CAPTURE_MISSING", blocked=True) from None
    except (OSError, TypeError, ValueError) as error:
        if isinstance(error, _InputError):
            raise
        raise _InputError("SCANNER_EXIT_CAPTURE_UNREADABLE") from None


def _empty_result():
    return {
        "schema_version": "qsl.governance_evidence_gate.v1",
        "status": "BLOCKED",
        "integration_state": "INCOMPLETE_TRUSTED_PROVENANCE",
        "gitleaks": {"status": "BLOCKED", "metadata_consistency": "UNVERIFIED",
                     "report_state": "MISSING", "reported_finding_count": None},
        "ai_review": {"status": "BLOCKED", "structural_status": "UNVERIFIED",
                      "risk_tier": None},
        "reason_codes": [],
        "execution_authorized": False,
        "production_ready": False,
        "merge_authorized": False,
        "owner_approval_granted": False,
    }


def inspect_gate(*, report_path, exit_path, review_path, base, head):
    """Read each bounded evidence file once and retain every failure boundary."""
    result = _empty_result()
    reasons = set()
    invalid_range = not (_revision(base) and _revision(head) and base != head)
    if invalid_range:
        reasons.add("EVENT_REVISION_RANGE_INVALID")
    scanner_exit = None
    capture_failed = False
    try:
        scanner_exit = _read_exit(exit_path)
    except _InputError as error:
        reasons.add(error.code)
        capture_failed = not error.blocked

    scanner = gitleaks_report_evidence.validate_report(
        None, scanner_exit_code=scanner_exit, base=base, head=head)
    try:
        raw = gitleaks_report_evidence._read_local_report(report_path)
        scanner = gitleaks_report_evidence.validate_report(
            raw, scanner_exit_code=scanner_exit, base=base, head=head)
    except gitleaks_report_evidence._Issue as error:
        # Keep an already-observed nonzero exit visible even if the report is
        # missing or unreadable. No source path or exception text is emitted.
        scanner["report_state"] = "UNREADABLE"
        gitleaks_report_evidence._add_issue(scanner, error.code, fail=not error.blocked)
    review = ai_review_evidence.load_and_validate(
        review_path, expected_base=base, expected_head=head)

    if (scanner.get("status") not in {"BLOCKED", "FAIL"}
            or review.get("status") not in {"BLOCKED", "FAIL"}):
        result["status"] = "FAIL"
        result["reason_codes"] = ["INSPECTOR_RESULT_CONTRACT_INVALID"]
        return result
    result["gitleaks"] = {key: scanner[key] for key in result["gitleaks"]}
    result["ai_review"] = {key: review[key] for key in result["ai_review"]}
    reasons.update("GITLEAKS_" + code for code in scanner["issues"])
    reasons.update("GITLEAKS_" + code for code in scanner["gaps"])
    reasons.update("AI_REVIEW_" + code.upper() for code in review["reason_codes"])
    if invalid_range or capture_failed or "FAIL" in (scanner["status"], review["status"]):
        result["status"] = "FAIL"
    result["reason_codes"] = sorted(reasons)
    return result


def main(arguments=None):
    parser = _Parser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--gitleaks-report", required=True, type=Path)
    parser.add_argument("--scanner-exit-file", required=True, type=Path)
    parser.add_argument("--ai-review", required=True, type=Path)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    try:
        args = parser.parse_args(arguments)
        result = inspect_gate(report_path=args.gitleaks_report,
                              exit_path=args.scanner_exit_file,
                              review_path=args.ai_review, base=args.base, head=args.head)
    except _InputError as error:
        result = _empty_result()
        result["status"] = "BLOCKED" if error.blocked else "FAIL"
        result["reason_codes"] = [error.code]
    except Exception:
        result = _empty_result()
        result["status"] = "FAIL"
        result["reason_codes"] = ["INTERNAL_GATE_ERROR"]
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 1 if result["status"] == "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
