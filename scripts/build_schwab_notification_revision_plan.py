#!/usr/bin/env python3
"""Build a review-only synthetic revision; never render an API Service or apply it.

The closed model is deliberately smaller than Cloud Run. Literal env payloads
are opaque, byte-preserved strings, not business admission evidence. The sole
excluded_desired_template slot is never accessed, even for hashing or copying.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import sys

if __package__:
    from . import verify_cloud_run_no_traffic_deploy as a
else:
    import verify_cloud_run_no_traffic_deploy as a


MODEL = "schwab-local-review-v1"
_IDENTITY = {"project": str, "region": str, "service": str}
_LABELS = {"commit-sha": str, "run-id": str, "purpose": str}
_PROVENANCE = {"commit": str, "run": str}
_METADATA = {"name": str, "uid": str, "labels": _LABELS}
_SERVICE = {
    "identity": _IDENTITY,
    "metadata": _METADATA | {"resourceVersion": str},
    "provenance": _PROVENANCE,
    "traffic": a._PROJECTION["traffic"],
}
_REVISION = {
    "identity": _IDENTITY | {"revision": str}, "metadata": _METADATA,
    "provenance": _PROVENANCE, "runtime": a._RUNTIME,
}
_SNAPSHOT = {"service": _SERVICE, "revision": _REVISION}
_EXPECTED = dict.fromkeys((
    "project", "region", "service", "baseline_revision", "candidate_revision",
    "candidate_image", "resource_version",
), str)
_ALIASES = {"latest", "latest-created", "latest-ready"}


class PlanError(ValueError):
    """Only fixed, non-sensitive reason codes leave this module."""


def _stop(reason: str) -> None:
    raise PlanError(reason)


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical(value).encode("ascii")).hexdigest()


def _required(value: object, schema: dict) -> None:
    a._keys(value, schema, schema)


def _nonempty(value: object) -> bool:
    return type(value) is str and bool(value.strip())


def _name(value: object, limit: int = 63) -> bool:
    return (type(value) is str and len(value) <= limit
            and re.fullmatch(r"[a-z](?:[a-z0-9-]*[a-z0-9])?", value) is not None)


def _revision_name(value: object, service: str) -> bool:
    return (_name(value) and value not in _ALIASES
            and value.startswith(service + "-")
            and value[len(service) + 1:] not in _ALIASES)


def _image(value: object) -> bool:
    return (type(value) is str and re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9./_-]*@sha256:[0-9a-f]{64}", value) is not None)


def _expected(expected: object) -> None:
    _required(expected, _EXPECTED)
    a._shape(expected, _EXPECTED)
    if (not all(_name(expected[key]) for key in _IDENTITY)
            or not _name(expected["service"], 49)):
        _stop("INVALID_EXPECTED_IDENTITY")
    for key in ("baseline_revision", "candidate_revision"):
        if not _revision_name(expected[key], expected["service"]):
            _stop("INVALID_EXPECTED_REVISION")
    if expected["baseline_revision"] == expected["candidate_revision"]:
        _stop("AMBIGUOUS_REVISION_IDENTITY")
    if not _image(expected["candidate_image"]):
        _stop("INVALID_IMMUTABLE_IMAGE")
    if not _nonempty(expected["resource_version"]):
        _stop("INVALID_RESOURCE_VERSION")


def _projection(snapshot: dict) -> dict:
    revision = snapshot["revision"]
    return {"identity": revision["identity"], "provenance": revision["provenance"],
            "runtime": revision["runtime"], "traffic": snapshot["service"]["traffic"]}


def _inventory(snapshot: object) -> None:
    # A never reads literal values during inventory. Inventory BOTH snapshots
    # before any subsequent equality, hash, deepcopy or literal type check.
    a._shape(snapshot, _SNAPSHOT)
    _required(snapshot, _SNAPSHOT)
    for key, schema in _SNAPSHOT.items():
        part = snapshot[key]
        _required(part, schema)
        for field in ("identity", "metadata", "provenance"):
            _required(part[field], schema[field])
        _required(part["metadata"]["labels"], _LABELS)
    a._inventory(_projection(snapshot))


def _traffic(rows: list, expected: dict) -> None:
    seen, tags = set(), set()
    total, baseline_percent = 0, 0
    for row in rows:
        a._keys(row, {"revisionName", "percent", "tag"}, {"revisionName", "percent"})
        revision, percent = row["revisionName"], row["percent"]
        tag = row.get("tag")
        if (not _revision_name(revision, expected["service"]) or type(percent) is not int
                or not 0 <= percent <= 100 or (tag is not None and not _name(tag))):
            _stop("INVALID_TRAFFIC")
        if (revision, tag) in seen or (tag is not None and tag in tags):
            _stop("AMBIGUOUS_TRAFFIC")
        seen.add((revision, tag))
        if tag is not None:
            tags.add(tag)
        if revision == expected["candidate_revision"]:
            _stop("CANDIDATE_ROUTE_PRESENT")
        total += percent
        if revision == expected["baseline_revision"]:
            baseline_percent += percent
    if total != 100 or baseline_percent <= 0:
        _stop("BASELINE_NOT_SERVING")


def _snapshot_values(snapshot: dict, expected: dict, *, candidate: bool = False) -> None:
    revision_name = expected["candidate_revision" if candidate else "baseline_revision"]
    if snapshot["revision"]["identity"]["revision"] != revision_name:
        _stop("BASELINE_IDENTITY_MISMATCH" if not candidate else "CANDIDATE_IDENTITY_MISMATCH")
    for part_name in ("service", "revision"):
        part = snapshot[part_name]
        if any(part["identity"][key] != expected[key] for key in _IDENTITY):
            _stop("IDENTITY_MISMATCH")
        if part["metadata"]["name"] != (expected["service"] if part_name == "service" else revision_name):
            _stop("IDENTITY_MISMATCH")
        if not _nonempty(part["metadata"]["uid"]):
            _stop("INVALID_UID")
        if any(not _nonempty(value) for value in part["metadata"]["labels"].values()):
            _stop("INVALID_METADATA")
        if (not re.fullmatch(r"[0-9a-f]{40}", part["provenance"]["commit"])
                or not re.fullmatch(r"[A-Za-z0-9_-]+", part["provenance"]["run"])):
            _stop("INVALID_PROVENANCE")
    version = snapshot["service"]["metadata"]["resourceVersion"]
    if not _nonempty(version):
        _stop("INVALID_RESOURCE_VERSION")
    if version != expected["resource_version"]:
        _stop("RESOURCE_VERSION_MISMATCH")
    _traffic(snapshot["service"]["traffic"], expected)
    runtime = snapshot["revision"]["runtime"]
    if (not re.fullmatch(r"[A-Za-z0-9._-]+@[A-Za-z0-9.-]+", runtime["serviceAccountName"])
            or runtime["ingress"] not in {"all", "internal", "internal-and-cloud-load-balancing"}
            or runtime["executionEnvironment"] not in {"gen1", "gen2"}
            or not 1 <= runtime["containerConcurrency"] <= 1000 or runtime["timeoutSeconds"] <= 0
            or runtime["scaling"]["minInstanceCount"] < 0
            or runtime["scaling"]["maxInstanceCount"] < max(1, runtime["scaling"]["minInstanceCount"])):
        _stop("INVALID_RUNTIME")
    for container in runtime["containers"]:
        if not _image(container["image"]):
            _stop("INVALID_IMMUTABLE_IMAGE")
        if any(not 1 <= port["containerPort"] <= 65535 for port in container["ports"]):
            _stop("INVALID_PORT")
        for entry in container["env"]:
            if "value" in entry and type(entry["value"]) is not str:
                _stop("INVALID_ENV_LITERAL")
    if 8080 not in [port["containerPort"] for port in runtime["containers"][0]["ports"]]:
        _stop("HTTP_STARTUP_GUARD_REQUIRED")


def _review_candidate(before: dict, candidate: dict, expected: dict) -> dict:
    _expected(expected)
    _inventory(before)
    _inventory(candidate)
    _snapshot_values(before, expected)
    _snapshot_values(candidate, expected, candidate=True)
    old_image = before["revision"]["runtime"]["containers"][0]["image"]
    new_image = candidate["revision"]["runtime"]["containers"][0]["image"]
    if new_image != expected["candidate_image"]:
        _stop("CANDIDATE_IMAGE_MISMATCH")
    if new_image == old_image:
        _stop("IMAGE_UNCHANGED")
    restored = copy.deepcopy(candidate)
    restored["revision"]["identity"]["revision"] = expected["baseline_revision"]
    restored["revision"]["metadata"]["name"] = expected["baseline_revision"]
    restored["revision"]["runtime"]["containers"][0]["image"] = old_image
    if restored != before:
        _stop("PROTECTED_CONFIGURATION_CHANGED")
    changes = []
    for path, old, new in (
        ("revision.identity.revision", expected["baseline_revision"], expected["candidate_revision"]),
        ("revision.metadata.name", expected["baseline_revision"], expected["candidate_revision"]),
        ("revision.runtime.containers[0].image", old_image, new_image),
    ):
        changes.append({"path": path, "comparison": "changed", "before_sha256": _hash(old), "after_sha256": _hash(new)})
    return {
        "model": MODEL, "mode": "review_only", "deployment_ready": False,
        "old_installation": "UNKNOWN", "startup_side_effects": "UNVERIFIED",
        "source": "NOT_VERIFIED", "admission": "NOT_VERIFIED", "cloud_configuration": "NOT_VERIFIED",
        "concurrency": "MATCHED_LOCAL_SNAPSHOTS_NO_CAS",
        "excluded_desired_template": "NOT_ACCESSED", "changes": changes,
        "preserved": [{"path": "service", "comparison": "equal", "sha256": _hash(before["service"])},
                      {"path": "revision_except_image_and_names", "comparison": "equal", "sha256": _hash(restored["revision"])}],
        "coverage": "CLOSED_SYNTHETIC_ENVELOPE_ONLY_OPAQUE_ENV_STRINGS",
    }


def review_candidate(before: dict, candidate: dict, expected: dict) -> dict:
    """Review an in-memory candidate without weakening probe or preservation guards."""
    try:
        return _review_candidate(before, candidate, expected)
    except PlanError:
        raise
    except a.PreflightError as exc:
        raise PlanError(str(exc)) from None
    except (ValueError, TypeError, KeyError, RecursionError):
        _stop("INVALID_INPUT")


def build_plan(request: dict) -> tuple[dict, dict]:
    """Return a detached candidate and redacted plan; never inspect desired data."""
    try:
        fields = {"model", "expected", "before", "current", "excluded_desired_template"}
        a._keys(request, fields, fields)
        if request["model"] != MODEL:
            _stop("INVALID_MODEL")
        expected = request["expected"]
        _expected(expected)
        before, current = request["before"], request["current"]
        if before is current:
            _stop("INDEPENDENT_SNAPSHOTS_REQUIRED")
        _inventory(before)
        _inventory(current)
        _snapshot_values(before, expected)
        _snapshot_values(current, expected)
        for key in ("service", "revision"):
            if before[key]["metadata"]["uid"] != current[key]["metadata"]["uid"]:
                _stop("OBJECT_RECREATED")
        if before != current:
            _stop("CONCURRENT_SNAPSHOT_CHANGED")
        candidate = {"service": copy.deepcopy(current["service"]), "revision": copy.deepcopy(before["revision"])}
        candidate["revision"]["identity"]["revision"] = expected["candidate_revision"]
        candidate["revision"]["metadata"]["name"] = expected["candidate_revision"]
        candidate["revision"]["runtime"]["containers"][0]["image"] = expected["candidate_image"]
        return candidate, _review_candidate(before, candidate, expected)
    except PlanError:
        raise
    except a.PreflightError as exc:
        raise PlanError(str(exc)) from None
    except (ValueError, TypeError, KeyError, RecursionError):
        _stop("INVALID_INPUT")


def _pairs(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            _stop("DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _invalid_constant(_value: str) -> None:
    _stop("INVALID_JSON")


def load_request(path: Path) -> object:
    """Read one explicitly named local JSON file, with fixed diagnostics only."""
    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_pairs, parse_constant=_invalid_constant)
    except PlanError:
        raise
    except (OSError, ValueError, RecursionError):
        _stop("INVALID_INPUT_FILE")


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        _stop("INVALID_ARGUMENTS")


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(description="Review a closed local synthetic snapshot; emits hashes only.")
    parser.add_argument("--input", required=True, type=Path)
    try:
        args = parser.parse_args(argv)
        _candidate, review = build_plan(load_request(args.input))
        print(_canonical(review))
    except PlanError as exc:
        print("Review stopped: " + str(exc), file=sys.stderr)
        return 1
    except (ValueError, TypeError, KeyError, RecursionError, OSError):
        print("Review stopped: INVALID_INPUT", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
