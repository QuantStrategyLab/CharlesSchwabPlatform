"""Validate supplied Gitleaks 8.30.1 report metadata without running a scanner.

Raw, redacted scanner bytes remain private. Only bounded finding metadata is
emitted; it still requires privacy review before sharing. This is not an
anonymizer. The report and separately captured exit/range cannot authenticate
scanner execution, version, redaction, provenance, or coverage. No result grants
execution or production authority, and this helper does not alter any CI gate.

The CLI reads one regular local file, never stdin, Git, or a remote resource.
It exits 2 for BLOCKED evidence and 1 for FAIL; evidence is never accepted here.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import stat


MAX_REPORT_BYTES = 1_000_000
MAX_JSON_DEPTH = 16
MAX_FINDINGS = 1_000
_REVISION = re.compile(r"[0-9a-f]{40}")
_RULE = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*")
_PATH_PART = re.compile(r"[A-Za-z0-9_.-]{1,128}")
_SENSITIVE_NAME = re.compile(r"\d{6,}|[A-Za-z0-9]{32,}|gh[pousr]_|github_pat_|AKIA|ASIA")
_GAPS = (
    "SCANNER_EXECUTION_UNVERIFIED", "SCANNER_VERSION_UNVERIFIED",
    "REDACTION_UNVERIFIED", "REPORT_PROVENANCE_UNVERIFIED",
    "SCAN_RANGE_BINDING_UNVERIFIED", "SCAN_COVERAGE_UNVERIFIED",
)


class _Issue(ValueError):
    def __init__(self, code, *, blocked=False):
        super().__init__(code)
        self.code = code
        self.blocked = blocked


def _require(condition, code):
    if not condition:
        raise _Issue(code)


def _result():
    return {
        "schema_version": "qsl.gitleaks_report_evidence.v1",
        "scanner_contract": "gitleaks-8.30.1",
        "status": "BLOCKED", "metadata_consistency": "CONSISTENT",
        "scanner": {"exit_code": None, "outcome": "UNKNOWN", "authenticated": False},
        "scan_range": {"base": None, "head": None, "authenticated": False},
        "report_state": "MISSING", "reported_finding_count": None,
        "finding_metadata": [], "issues": [], "gaps": list(_GAPS),
        "execution_authorized": False, "production_ready": False,
    }


def _add_issue(result, code, *, fail=False, metadata=True):
    if code not in result["issues"]:
        result["issues"].append(code)
    if fail:
        result["status"] = "FAIL"
    if metadata and (fail or result["metadata_consistency"] == "CONSISTENT"):
        result["metadata_consistency"] = "FAIL" if fail else "BLOCKED"


def _is_revision(value):
    return type(value) is str and _REVISION.fullmatch(value) is not None and value != "0" * 40


def _check_depth(text):
    depth = 0
    quoted = escaped = False
    for character in text:
        if quoted:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                quoted = False
        elif character == '"':
            quoted = True
        elif character in "[{":
            depth += 1
            _require(depth <= MAX_JSON_DEPTH, "JSON_DEPTH_EXCEEDED")
        elif character in "]}":
            depth -= 1


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        _require(key not in value, "JSON_DUPLICATE_KEY")
        value[key] = item
    return value


def _nonfinite(_value):
    raise _Issue("JSON_NONFINITE_NUMBER")


def _finite_float(value):
    number = float(value)
    _require(math.isfinite(number), "JSON_NONFINITE_NUMBER")
    return number


def _parse_report(raw):
    _require(type(raw) is bytes, "REPORT_BYTES_REQUIRED")
    _require(len(raw) <= MAX_REPORT_BYTES, "REPORT_SIZE_EXCEEDED")
    try:
        text = raw.decode("utf-8")
        _check_depth(text)
        rows = json.loads(text, object_pairs_hook=_pairs,
                          parse_constant=_nonfinite, parse_float=_finite_float)
    except _Issue:
        raise
    except (UnicodeError, ValueError, RecursionError):
        raise _Issue("JSON_INVALID") from None
    _require(type(rows) is list, "REPORT_ARRAY_REQUIRED")
    _require(len(rows) <= MAX_FINDINGS, "FINDING_COUNT_EXCEEDED")
    return rows


def _safe_path(value):
    if type(value) is not str or not 1 <= len(value) <= 512 or _SENSITIVE_NAME.search(value):
        return False
    return all(part not in {".", ".."} and _PATH_PART.fullmatch(part)
               for part in value.split("/"))


def _finding_metadata(row):
    _require(type(row) is dict, "FINDING_OBJECT_REQUIRED")
    _require({"RuleID", "File", "StartLine", "EndLine"} <= row.keys(), "FINDING_METADATA_REQUIRED")
    rule = row["RuleID"]
    _require(type(rule) is str and 1 <= len(rule) <= 64 and _RULE.fullmatch(rule)
             and not _SENSITIVE_NAME.search(rule), "RULE_ID_INVALID")
    _require(_safe_path(row["File"]), "FILE_INVALID")
    start, end = row["StartLine"], row["EndLine"]
    _require(type(start) is int and type(end) is int and 1 <= start <= end < 2 ** 31,
             "LINE_RANGE_INVALID")
    metadata = {"RuleID": rule, "File": row["File"], "StartLine": start, "EndLine": end}
    commit = row.get("Commit")
    missing_commit = commit is None or commit == ""
    if not missing_commit:
        _require(_is_revision(commit), "COMMIT_INVALID")
        metadata["Commit"] = commit
    return metadata, missing_commit


def validate_report(report_bytes, *, scanner_exit_code=None, base=None, head=None):
    """Return only BLOCKED/FAIL evidence from bytes and unauthenticated captures.

    ``None`` means a missing report; ``b"[]"`` is an explicit empty report.
    A supplied report cannot establish that the scanner ran or was redacted.
    Nonzero scanner outcomes and metadata failures cannot be promoted to success.
    This function performs no I/O and never copies freeform errors into results.
    """
    result = _result()
    if scanner_exit_code is None:
        _add_issue(result, "SCANNER_EXIT_MISSING")
    elif type(scanner_exit_code) is not int or not -255 <= scanner_exit_code <= 255:
        _add_issue(result, "SCANNER_EXIT_INVALID", fail=True)
    else:
        result["scanner"]["exit_code"] = scanner_exit_code
        result["scanner"]["outcome"] = "EXIT_ZERO" if scanner_exit_code == 0 else "EXIT_NONZERO"
        if scanner_exit_code != 0:
            _add_issue(result, "SCANNER_EXIT_NONZERO", fail=True, metadata=False)
    for name, value in (("base", base), ("head", head)):
        if value is None:
            _add_issue(result, name.upper() + "_MISSING")
        elif not _is_revision(value):
            _add_issue(result, name.upper() + "_INVALID", fail=True)
        else:
            result["scan_range"][name] = value
    if report_bytes is None:
        _add_issue(result, "REPORT_MISSING")
        return result
    try:
        rows = _parse_report(report_bytes)
        result["reported_finding_count"] = len(rows)
        result["report_state"] = "FINDINGS" if rows else "EXPLICIT_EMPTY"
        observed_exit = result["scanner"]["exit_code"]
        if rows and observed_exit == 0:
            _add_issue(result, "ZERO_EXIT_WITH_FINDINGS", fail=True)
        if not rows and observed_exit is not None and observed_exit != 0:
            _add_issue(result, "NONZERO_EXIT_WITH_EMPTY_REPORT", fail=True)
        metadata = []
        for row in rows:
            item, missing_commit = _finding_metadata(row)
            metadata.append(item)
            if missing_commit:
                _add_issue(result, "COMMIT_MISSING")
            elif item["Commit"] == result["scan_range"]["base"]:
                _add_issue(result, "COMMIT_EQUALS_EXCLUDED_BASE", fail=True)
        result["finding_metadata"] = metadata
    except _Issue as error:
        result["report_state"] = "INVALID"
        _add_issue(result, error.code, fail=True)
    return result


def _read_local_report(path):
    if path is None:
        return None
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_NONBLOCK"):
        raise _Issue("SAFE_LOCAL_OPEN_UNSUPPORTED", blocked=True)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(descriptor)
            _require(stat.S_ISREG(info.st_mode), "REGULAR_REPORT_REQUIRED")
            _require(info.st_size <= MAX_REPORT_BYTES, "REPORT_SIZE_EXCEEDED")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                raw = stream.read(MAX_REPORT_BYTES + 1)
            _require(len(raw) <= MAX_REPORT_BYTES, "REPORT_SIZE_EXCEEDED")
            return raw
        finally:
            os.close(descriptor)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as error:
        if isinstance(error, _Issue):
            raise
        raise _Issue("REPORT_UNREADABLE", blocked=True) from None


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        raise _Issue("CLI_ARGUMENTS_INVALID")


def main(argv=None):
    """Read bounded private local bytes and print metadata without error details."""
    result = _result()
    try:
        parser = _Parser(description=__doc__, allow_abbrev=False)
        parser.add_argument("--report", help="Path to a private, redacted local JSON report")
        parser.add_argument("--scanner-exit-code", type=int, help="Separately captured scanner exit code")
        parser.add_argument("--base", help="Captured base revision, 40 lowercase hexadecimal characters")
        parser.add_argument("--head", help="Captured head revision, 40 lowercase hexadecimal characters")
        args = parser.parse_args(argv)
        result = validate_report(None, scanner_exit_code=args.scanner_exit_code,
                                 base=args.base, head=args.head)
        raw = _read_local_report(args.report)
        result = validate_report(raw, scanner_exit_code=args.scanner_exit_code,
                                 base=args.base, head=args.head)
    except _Issue as error:
        result["report_state"] = "UNREADABLE"
        _add_issue(result, error.code, fail=not error.blocked)
    except Exception:
        result["report_state"] = "UNREADABLE"
        result["finding_metadata"] = []
        _add_issue(result, "INTERNAL_VALIDATOR_ERROR", fail=True)
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 1 if result["status"] == "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
