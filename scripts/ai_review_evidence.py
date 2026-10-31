"""Validate bounded local AI-review evidence without authenticating it.

The version-1 contract binds the full file inventory, check observations and
review scope to exact revisions. ``scope_sha256`` is SHA-256 of compact,
sorted-key JSON containing base_sha, head_sha, changed_files,
inventory_complete and files sorted by path. File records include rename or
copy source paths. This digest binds declarations; it does not authenticate
their origin, completeness, review independence or required-check coverage.

Usage: python scripts/ai_review_evidence.py LOCAL_JSON
The CLI reads one regular UTF-8 JSON file, at most 256 KiB. It prints only a
bounded JSON result: BLOCKED (exit 2) or FAIL (exit 1). No input can produce
authenticated PASS, approval, or execution authority. There are no network,
Git, scanner, workflow, or trading calls.
"""

import errno
import hashlib
import json
import math
import os
import re
import stat
import sys
import unicodedata


MAX_BYTES = 262144
MAX_DEPTH = 16
MAX_NODES = 30000
TIERS = ("low", "standard", "high")
TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
PATH = re.compile(r"[A-Za-z0-9_.@+/-]{1,512}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"[0-9a-f]{64}\Z")
CHECK_STATES = {"passed", "failed", "pending", "error", "skipped", "missing"}
REVIEW_STATES = {"completed", "failed", "pending", "error", "skipped", "missing"}
FILE_STATES = {"added", "modified", "removed", "renamed", "copied", "changed"}
REVIEW_DEPTH = {
    "low": "independent_ai_review",
    "standard": "independent_ai_review_with_test_analysis",
    "high": "independent_ai_review_with_boundary_and_failure_analysis",
}


class _Invalid(ValueError):
    """Contains a fixed reason code, never an input value."""


def _require(condition, reason):
    if not condition:
        raise _Invalid(reason)


def _keys(value, required, optional=()):
    _require(type(value) is dict, "invalid_schema")
    _require(set(required) <= set(value) <= set(required) | set(optional), "invalid_schema")


def _matches(pattern, value):
    return type(value) is str and pattern.fullmatch(value) is not None


def _revision(value):
    return _matches(SHA, value) and value != "0" * 40


def _bounded(value):
    pending = [(value, 0)]
    nodes = text_bytes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        _require(depth <= MAX_DEPTH and nodes <= MAX_NODES, "input_limits_exceeded")
        if type(item) is dict:
            _require(len(item) <= MAX_NODES, "input_limits_exceeded")
            _require(all(type(key) is str for key in item), "invalid_json_value")
            pending.extend((child, depth + 1) for pair in item.items() for child in pair)
        elif type(item) is list:
            _require(len(item) <= MAX_NODES, "input_limits_exceeded")
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is str:
            _require(len(item) <= 4096, "input_limits_exceeded")
            _require(not any(unicodedata.category(char).startswith("C") for char in item),
                     "unsafe_text")
            text_bytes += len(item.encode("utf-8"))
            _require(text_bytes <= MAX_BYTES, "input_limits_exceeded")
        elif type(item) is float:
            _require(math.isfinite(item), "nonfinite_json_number")
        elif type(item) is int:
            _require(item.bit_length() <= 64, "input_limits_exceeded")
        else:
            _require(item is None or type(item) is bool, "invalid_json_value")
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    _require(len(encoded.encode("utf-8")) <= MAX_BYTES, "input_limits_exceeded")


def _safe_path(value):
    return (_matches(PATH, value) and not value.startswith("/")
            and all(part not in {"", ".", ".."} for part in value.split("/")))


def _path_tier(path):
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", path).lower()
    if (normalized.startswith((".github/workflows/", ".github/actions/"))
            or normalized in {"main.py", "gunicorn.conf.py"}
            or normalized.rsplit("/", 1)[-1] == "codeowners"
            or normalized == ".github/codex_auto_merge_policy.json"
            or normalized.endswith((".tf", ".tfvars"))):
        return "high"
    if normalized.startswith(("docs/", "tests/")):
        return "low"
    root = normalized.split("/", 1)[0]
    if root in {"application", "entrypoints", "strategy", "strategies", "infra", "terraform"}:
        return "high"
    if re.search(r"(?:^|[/_.-])(?:auth|accounts?|bindings?|brokers?|credentials?|deploy(?:ment)?|"
                 r"docker(?:file)?|compose|env|execution|executors?|iam|keys?|orders?|"
                 r"permissions?|risk|runtime|secrets?|strateg(?:y|ies)|tokens?|"
                 r"trad(?:e|es|ing))(?:$|[/_.-])", normalized):
        return "high"
    return "standard"


def _inventory(document):
    count, files = document["changed_files"], document["files"]
    _require(type(count) is int and 1 <= count <= 3000, "invalid_inventory")
    _require(type(files) is list and len(files) == count, "invalid_inventory")
    _require(type(document["inventory_complete"]) is bool, "invalid_inventory")
    paths, rename_sources, tiers = set(), set(), []
    for item in files:
        _require(type(item) is dict, "invalid_inventory")
        state = item.get("status")
        _require(type(state) is str and state in FILE_STATES, "invalid_inventory")
        names = {"path", "status"}
        if state in {"renamed", "copied"}:
            names.add("previous_path")
        _require(set(item) == names, "invalid_inventory")
        path = item["path"]
        _require(_safe_path(path) and path not in paths, "invalid_inventory")
        paths.add(path)
        tiers.append(_path_tier(path))
        if "previous_path" in item:
            previous = item["previous_path"]
            _require(_safe_path(previous) and previous != path, "invalid_inventory")
            if state == "renamed":
                _require(previous not in rename_sources, "invalid_inventory")
                rename_sources.add(previous)
            tiers.append(_path_tier(previous))
    return max(tiers, key=TIERS.index)


def _scope_digest(document):
    scope = {key: document[key]
             for key in ("base_sha", "head_sha", "changed_files", "inventory_complete")}
    scope["files"] = sorted(document["files"], key=lambda item: item["path"])
    serialized = json.dumps(scope, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _check_observations(document, failures, blocked):
    required, checks = document["required_checks"], document["checks"]
    _require(type(required) is list and len(required) <= 256, "invalid_checks")
    _require(all(_matches(TOKEN, name) for name in required), "invalid_checks")
    _require(len(set(required)) == len(required), "invalid_checks")
    if not required:
        blocked.add("required_checks_missing")
    _require(type(checks) is list and len(checks) <= 256, "invalid_checks")
    observations, states = set(), {}
    for check in checks:
        _keys(check, ("observation_id", "name", "head_sha", "status"))
        _require(_matches(TOKEN, check["name"])
                 and _matches(TOKEN, check["observation_id"])
                 and _revision(check["head_sha"]), "invalid_checks")
        _require(type(check["status"]) is str and check["status"] in CHECK_STATES,
                 "invalid_checks")
        _require(check["observation_id"] not in observations, "invalid_checks")
        observations.add(check["observation_id"])
        _require(check["head_sha"] == document["head_sha"], "check_head_mismatch")
        states.setdefault(check["name"], set()).add(check["status"])
        if check["status"] == "failed":
            failures.add("check_failed")
        elif check["status"] != "passed":
            blocked.add("check_not_successful")
    if any(len(values) > 1 for values in states.values()):
        failures.add("contradictory_check_observations")
    if set(required) - states.keys():
        blocked.add("required_check_missing")
    return states


def _review(document, states, failures, blocked):
    review = document["review"]
    _keys(review, ("reviewer_id", "base_sha", "head_sha", "scope_sha256", "status", "findings"))
    _require(_matches(TOKEN, review["reviewer_id"]), "invalid_review")
    _require(review["reviewer_id"].casefold() != document["author_id"].casefold(),
             "reviewer_matches_author")
    _require(_revision(review["base_sha"]) and _revision(review["head_sha"]),
             "invalid_review")
    _require(review["base_sha"] == document["base_sha"]
             and review["head_sha"] == document["head_sha"], "review_revision_mismatch")
    _require(_matches(DIGEST, review["scope_sha256"]), "invalid_review")
    _require(review["scope_sha256"] == _scope_digest(document), "review_scope_mismatch")
    _require(type(review["status"]) is str and review["status"] in REVIEW_STATES,
             "invalid_review")
    if review["status"] == "failed":
        failures.add("review_failed")
    elif review["status"] != "completed":
        blocked.add("review_not_completed")
    _require(type(review["findings"]) is list and len(review["findings"]) <= 256,
             "invalid_findings")
    finding_ids = set()
    for finding in review["findings"]:
        _keys(finding, ("id", "severity", "disposition", "head_sha"), ("verification_check",))
        _require(_matches(TOKEN, finding["id"]) and finding["id"] not in finding_ids,
                 "invalid_findings")
        finding_ids.add(finding["id"])
        _require(type(finding["severity"]) is str
                 and finding["severity"] in {"low", "medium", "high", "critical"},
                 "invalid_findings")
        _require(type(finding["disposition"]) is str
                 and finding["disposition"] in {"open", "resolved"}, "invalid_findings")
        _require(_revision(finding["head_sha"]), "invalid_findings")
        _require(finding["head_sha"] == document["head_sha"], "finding_revision_mismatch")
        if finding["disposition"] == "open":
            _require("verification_check" not in finding, "invalid_findings")
            failures.add("finding_open")
        else:
            name = finding.get("verification_check")
            if (not _matches(TOKEN, name) or name not in document["required_checks"]
                    or states.get(name) != {"passed"}):
                failures.add("finding_not_verified")


def _result(status, structure, reasons, tier=None):
    return {
        "schema_version": 1,
        "status": status,
        "structural_status": structure,
        "authentication": "UNVERIFIED",
        "required_check_coverage": "UNVERIFIED",
        "risk_tier": tier,
        "required_review_depth": REVIEW_DEPTH.get(tier),
        "reason_codes": sorted(reasons),
        "operational_authority": "NONE",
    }


def validate_evidence(document, *, expected_base=None, expected_head=None):
    """Check structure and optional event revisions without authenticating them."""
    try:
        _bounded(document)
        _keys(document, ("schema_version", "base_sha", "head_sha", "changed_files",
                         "inventory_complete", "files", "author_id", "required_checks",
                         "checks", "review"), ("declared_risk_tier",))
        _require(type(document["schema_version"]) is int and document["schema_version"] == 1,
                 "invalid_schema_version")
        _require(_revision(document["base_sha"]) and _revision(document["head_sha"]),
                 "invalid_revision")
        _require(document["base_sha"] != document["head_sha"], "invalid_revision_range")
        if expected_base is not None or expected_head is not None:
            _require(_revision(expected_base) and _revision(expected_head)
                     and expected_base != expected_head, "invalid_expected_revision_range")
            _require(document["base_sha"] == expected_base
                     and document["head_sha"] == expected_head, "expected_revision_mismatch")
        _require(_matches(TOKEN, document["author_id"]), "invalid_author")
        tier = _inventory(document)
        failures = set()
        blocked = {"trusted_provenance_unavailable", "required_check_policy_unverified"}
        if not document["inventory_complete"]:
            blocked.add("inventory_incomplete")
        if "declared_risk_tier" in document:
            declared = document["declared_risk_tier"]
            _require(type(declared) is str and declared in TIERS, "invalid_risk_tier")
            if TIERS.index(declared) < TIERS.index(tier):
                failures.add("risk_tier_understated")
            tier = max((tier, declared), key=TIERS.index)
        states = _check_observations(document, failures, blocked)
        _review(document, states, failures, blocked)
        return _result("FAIL" if failures else "BLOCKED", "CONSISTENT", failures | blocked, tier)
    except _Invalid as error:
        return _result("FAIL", "INCONSISTENT", {str(error)})


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def _nonfinite(_value):
    raise _Invalid("nonfinite_json_number")


def load_and_validate(path, *, expected_base=None, expected_head=None):
    """Read once without following a final symlink; bind optional event revisions."""
    try:
        path = os.fspath(path)
        _require(type(path) is str and path and path != "-"
                 and not any(unicodedata.category(char).startswith("C") for char in path),
                 "invalid_input_path")
        if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_NONBLOCK"):
            return _result("BLOCKED", "UNVERIFIED", {"safe_file_open_unavailable"})
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            details = os.fstat(stream.fileno())
            _require(stat.S_ISREG(details.st_mode), "input_not_regular_file")
            _require(details.st_size <= MAX_BYTES, "input_limits_exceeded")
            raw = stream.read(MAX_BYTES + 1)
        _require(len(raw) <= MAX_BYTES, "input_limits_exceeded")
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                              parse_constant=_nonfinite)
        return validate_evidence(document, expected_base=expected_base, expected_head=expected_head)
    except _Invalid as error:
        return _result("FAIL", "INCONSISTENT", {str(error)})
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.EISDIR}:
            return _result("FAIL", "INCONSISTENT", {"input_not_regular_file"})
        return _result("BLOCKED", "UNVERIFIED", {"input_file_unavailable"})
    except TypeError:
        return _result("FAIL", "INCONSISTENT", {"invalid_input_path"})
    except (UnicodeError, ValueError, RecursionError):
        return _result("FAIL", "INCONSISTENT", {"invalid_json"})


def main(arguments=None):
    arguments = sys.argv[1:] if arguments is None else arguments
    if arguments in (["-h"], ["--help"]):
        print(__doc__)
        return 0
    if len(arguments) != 1 or arguments[0].startswith("-"):
        result = _result("FAIL", "INCONSISTENT", {"invalid_arguments"})
    else:
        result = load_and_validate(arguments[0])
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 2 if result["status"] == "BLOCKED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
