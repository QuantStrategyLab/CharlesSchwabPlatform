"""Retain native redacted Gitleaks outputs without authenticating acceptance.

The scanner still decides its own result. Its original process return code is
retained separately, including nonzero exits. Collection errors cannot turn a
failed scan into success; a successful scan with incomplete evidence also fails.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys


MAX_REPORT_BYTES = 1_000_000
REVISION = re.compile(r"[0-9a-f]{40}\Z")


def _write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("nonfinite JSON number")
    return number


def _invalid_constant(_value):
    raise ValueError("nonfinite JSON constant")


def _report_identity(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        details = os.fstat(stream.fileno())
        if not stat.S_ISREG(details.st_mode) or details.st_size > MAX_REPORT_BYTES:
            raise ValueError("report is not a bounded regular file")
        raw = stream.read(MAX_REPORT_BYTES + 1)
    if len(raw) > MAX_REPORT_BYTES or type(json.loads(
            raw, parse_float=_finite_float, parse_constant=_invalid_constant)) is not list:
        raise ValueError("report is not a bounded JSON array")
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def _revision_preflight(base, head):
    for name, revision in (("base", base), ("head", head)):
        if not REVISION.fullmatch(revision) or revision == "0" * 40:
            return {"status": "failed", "reason": "invalid_revision", "revision": name}
        try:
            result = subprocess.run(["git", "cat-file", "-t", revision],
                                    capture_output=True, text=True, check=False)
        except OSError:
            return {"status": "failed", "reason": "git_unavailable", "revision": name}
        if result.returncode or result.stdout.strip() != "commit":
            return {"status": "failed", "reason": "commit_unavailable", "revision": name}
    return {"status": "passed", "base_is_commit": True, "head_is_commit": True}


def retain_scan(binary, output, base, head, *, scope):
    """Run one native scan and retain observations, never a trusted verdict."""
    output = Path(output)
    binary = Path(binary).absolute()
    report = output / "report.json"
    arguments = [str(binary), "git", ".", "--redact", "--no-banner",
                 "--log-opts=" + base + ".." + head, "--report-format=json",
                 "--report-path=" + str(report)]
    invocation = {
        "schema_version": 1,
        "scope": scope,
        "base_sha": base,
        "head_sha": head,
        "configured_version": "8.30.1",
        "binary_sha256": None,
        "arguments": arguments,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "github": {name: os.environ.get(name) for name in (
            "GITHUB_REPOSITORY", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_WORKFLOW_REF")},
        "authentication": "UNVERIFIED",
        "coverage": "UNVERIFIED",
    }
    try:
        output.mkdir(parents=True, exist_ok=False)
        if binary.is_file():
            invocation["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
        invocation["revision_preflight"] = _revision_preflight(base, head)
        _write_json(output / "invocation.json", invocation)
    except OSError:
        print("Native scan evidence directory could not be initialized", file=sys.stderr)
        return 1
    if invocation["revision_preflight"]["status"] != "passed":
        print("Revision preflight failed; native scanner was not started", file=sys.stderr)
        return 1
    try:
        completed = subprocess.run(arguments, check=False)
    except OSError as error:
        print("Native scanner could not be started; no process exit was fabricated", file=sys.stderr)
        return 127 if isinstance(error, FileNotFoundError) else 126

    raw_exit = completed.returncode
    shell_exit = 128 - raw_exit if raw_exit < 0 else raw_exit
    capture_complete = True
    try:
        (output / "scanner-exit.txt").write_text(str(raw_exit) + "\n", encoding="utf-8")
    except OSError:
        capture_complete = False
    identity = None
    try:
        identity = _report_identity(report)
    except (OSError, ValueError, UnicodeError, RecursionError):
        capture_complete = False
    receipt = {
        "schema_version": 1,
        "native_process_started": True,
        "native_returncode": raw_exit,
        "shell_exit": shell_exit,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "report": identity,
        "capture_complete": capture_complete,
        "authentication": "UNVERIFIED",
        "coverage": "UNVERIFIED",
    }
    try:
        _write_json(output / "capture.json", receipt)
    except OSError:
        capture_complete = False
    print("Native scan returned %d; evidence capture complete: %s" % (raw_exit, capture_complete))
    if shell_exit:
        return shell_exit
    return 0 if capture_complete else 1


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--scope", choices=("event-range", "retrospective-review-range"), required=True)
    args = parser.parse_args(arguments)
    return retain_scan(args.binary, args.output, args.base, args.head, scope=args.scope)


if __name__ == "__main__":
    raise SystemExit(main())
