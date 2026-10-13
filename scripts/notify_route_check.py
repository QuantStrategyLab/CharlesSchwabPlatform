#!/usr/bin/env python3
"""Observe serving Telegram bindings without disclosing private route material.

Only a sole serving revision with one container and a pinned token binding can
be compared with this project's current contract bot. This is not central
canonical route approval. Cloud commands are read-only; Telegram uses getMe
only. No route values, identifiers, token masks or exception text are printed.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.request

CONTRACT_SECRET = "quant-sentinel-telegram-bot-token"
_PROJECT = r"(?:[a-z][a-z0-9-]{4,28}[a-z0-9]|[1-9][0-9]{5,19})"
_SECRET = r"[A-Za-z0-9_-]{1,255}"
_SERVICE = r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?"
_MAX_GETME_BYTES = 64 * 1024


def _services() -> list[str]:
    names: list[str] = []
    for key in ("CLOUD_RUN_SERVICE", "CLOUD_RUN_SERVICES", "RUNTIME_GUARD_CLOUD_RUN_SERVICES"):
        names.extend(item.strip() for item in (os.environ.get(key) or "").replace("\n", ",").split(",")
                     if item.strip())
    raw = (os.environ.get("CLOUD_RUN_SERVICE_TARGETS_JSON") or "").strip()
    if raw:
        data = json.loads(raw)
        items = data if isinstance(data, list) else data.get("targets") if isinstance(data, dict) else None
        if not isinstance(items, list):
            raise ValueError("invalid_targets")
        for item in items:
            if isinstance(item, str):
                candidates = [item.strip()]
            elif isinstance(item, dict):
                nested = item.get("runtime_target") or {}
                if not isinstance(nested, dict):
                    raise ValueError("invalid_targets")
                candidates = [obj[key].strip() for obj in (item, nested)
                              for key in ("service", "service_name", "cloud_run_service")
                              if isinstance(obj.get(key), str) and obj[key].strip()]
            else:
                raise ValueError("invalid_targets")
            if len(set(candidates)) != 1:
                raise ValueError("invalid_targets")
            names.append(candidates[0])
    if not names or any(not re.fullmatch(_SERVICE, name) for name in names):
        raise ValueError("invalid_targets")
    return list(dict.fromkeys(names))


def _gcloud(*args: str) -> str:
    return subprocess.run(["gcloud", *args], check=True, capture_output=True, text=True, timeout=30).stdout


def _bot_identity(token: str) -> tuple[int | None, str]:
    if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]+", token):
        return None, "token_invalid"
    try:
        request = urllib.request.Request(f"https://api.telegram.org/bot{token}/getMe", method="GET")
        with urllib.request.urlopen(request, timeout=15) as response:
            raw = response.read(_MAX_GETME_BYTES + 1)
        if len(raw) > _MAX_GETME_BYTES:
            return None, "getme_invalid"
        body = json.loads(raw)
        result = body.get("result") if isinstance(body, dict) and body.get("ok") is True else None
        bot_id = result.get("id") if isinstance(result, dict) else None
        if type(bot_id) is not int or bot_id <= 0 or result.get("is_bot") is not True:
            return None, "getme_invalid"
        return bot_id, "known"
    except Exception:  # noqa: BLE001 - never disclose URLs, response bodies or exceptions
        return None, "getme_failed"


def _secret_token(project: str, secret: str, version: str) -> tuple[str | None, str]:
    try:
        token = _gcloud("secrets", "versions", "access", version, f"--secret={secret}", f"--project={project}")
    except Exception:  # noqa: BLE001 - private command streams and arguments stay in memory
        return None, "secret_read_failed"
    if not token:
        return None, "token_missing"
    return token, "known"


def _secret_reference(name: str, revision: dict, project: str) -> tuple[str, str] | None:
    metadata = revision.get("metadata") or {}
    annotations = metadata.get("annotations") or {} if isinstance(metadata, dict) else None
    if not isinstance(annotations, dict):
        return None
    aliases = annotations.get("run.googleapis.com/secrets")
    aliased = False
    if aliases is not None:
        if not isinstance(aliases, str):
            return None
        matches = []
        for declaration in aliases.split(","):
            alias, separator, reference = declaration.strip().partition(":")
            if not separator or not alias or not reference:
                return None
            if alias == name:
                matches.append(reference)
        if len(matches) > 1:
            return None
        if matches:
            name = matches[0]
            aliased = True
    qualified = re.fullmatch(r"projects/(" + _PROJECT + r")/secrets/(" + _SECRET + r")", name)
    if qualified:
        return qualified.group(1), qualified.group(2)
    if not aliased and re.fullmatch(_SECRET, name):
        return project, name
    return None


def _binding_token(revision: dict, project: str) -> tuple[str | None, str, str]:
    spec = revision.get("spec")
    containers = spec.get("containers") if isinstance(spec, dict) else None
    if not isinstance(containers, list) or not containers:
        return None, "unknown", "containers_missing"
    if len(containers) != 1 or not isinstance(containers[0], dict):
        return None, "unknown", "containers_ambiguous"
    envs = containers[0].get("env")
    if not isinstance(envs, list) or any(not isinstance(item, dict) for item in envs):
        return None, "unknown", "token_environment_invalid"
    winners = [item for item in envs if item.get("name") == "TELEGRAM_TOKEN"]
    if not winners:
        return None, "unknown", "token_binding_missing"
    if len(winners) != 1:
        return None, "unknown", "token_binding_ambiguous"
    winner = winners[0]
    if "value" in winner and "valueFrom" in winner:
        return None, "unknown", "token_binding_ambiguous"
    if "value" in winner:
        token = winner["value"]
        if not isinstance(token, str) or not token:
            return None, "plain_env", "token_missing"
        return token, "plain_env", "known"
    value_from = winner.get("valueFrom")
    ref = value_from.get("secretKeyRef") if isinstance(value_from, dict) else None
    if not isinstance(ref, dict) or not isinstance(ref.get("name"), str):
        return None, "unknown", "token_binding_invalid"
    version = ref.get("key")
    # Alias/latest cannot identify the immutable token loaded by a running process.
    if not isinstance(version, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", version):
        return None, "secret_version", "token_version_unpinned"
    reference = _secret_reference(ref["name"], revision, project)
    if reference is None:
        return None, "secret_version", "token_binding_invalid"
    token, status = _secret_token(*reference, version)
    return token, "secret_version", status


def _traffic(service: dict) -> tuple[list[str], str, str]:
    status = service.get("status")
    rows = status.get("traffic") if isinstance(status, dict) else None
    if not isinstance(rows, list):
        return [], "unknown", "traffic_invalid"
    serving = []
    total = 0
    for row in rows:
        percent = row.get("percent") if isinstance(row, dict) else None
        if type(percent) is not int or not 0 <= percent <= 100:
            return [], "unknown", "traffic_invalid"
        if percent == 0:
            continue
        name = row.get("revisionName")
        if not isinstance(name, str) or not re.fullmatch(_SERVICE, name) or name in serving:
            return [], "unknown", "traffic_ambiguous"
        serving.append(name)
        total += percent
    if not serving:
        return [], "none", "no_positive_traffic"
    if total != 100:
        return [], "unknown", "traffic_incomplete"
    return serving, "single" if len(serving) == 1 else "split", "none"


def _unknown_comparison() -> dict:
    return {
        "bot_status": "unknown", "same_bot_as_contract": None,
        "chat_status": "unknown", "same_chat_as_canonical": None,
        "topic_status": "unknown", "same_topic_as_canonical": None,
    }


def _observe_service(service: str, ordinal: int, project: str, region: str, contract_id: int | None) -> dict:
    out = {"service_label": f"service#{ordinal}", "traffic_status": "unknown",
           "reason_code": "service_describe_failed", **_unknown_comparison(), "revisions": []}
    try:
        spec = json.loads(_gcloud("run", "services", "describe", service, f"--project={project}",
                                 f"--region={region}", "--format=json"))
    except Exception:  # noqa: BLE001 - fixed reason only
        return out
    if not isinstance(spec, dict):
        out["reason_code"] = "service_metadata_invalid"
        return out
    serving, traffic_status, reason = _traffic(spec)
    out.update(traffic_status=traffic_status, reason_code=reason)
    if not serving:
        return out
    for index, name in enumerate(serving, start=1):
        observed = {"revision_label": f"revision#{index}", "token_source": "unknown",
                    "token_status": "revision_describe_failed", "reason_code": "revision_describe_failed",
                    **_unknown_comparison()}
        try:
            revision = json.loads(_gcloud("run", "revisions", "describe", name, f"--project={project}",
                                         f"--region={region}", "--format=json"))
            if not isinstance(revision, dict):
                observed.update(token_status="revision_metadata_invalid", reason_code="revision_metadata_invalid")
            else:
                token, source, token_status = _binding_token(revision, project)
                observed.update(token_source=source, token_status=token_status, reason_code=token_status)
                if token is not None:
                    bot_id, bot_status = _bot_identity(token)
                    token = None
                    observed.update(token_status=bot_status, reason_code=bot_status)
                    if traffic_status == "single" and bot_id is not None and contract_id is not None:
                        same = bot_id == contract_id
                        observed.update(same_bot_as_contract=same, bot_status="match" if same else "mismatch",
                                        reason_code="project_contract_compared")
                    elif bot_id is not None:
                        observed["reason_code"] = "split_traffic" if traffic_status == "split" else "contract_unavailable"
        except Exception:  # noqa: BLE001 - never emit metadata, private values or tracebacks
            pass
        out["revisions"].append(observed)
    if traffic_status == "single":
        only = out["revisions"][0]
        out.update(bot_status=only["bot_status"], same_bot_as_contract=only["same_bot_as_contract"],
                   reason_code=only["reason_code"])
    else:
        out["reason_code"] = "split_traffic"
    return out


def _report() -> dict:
    source = os.environ.get("SOURCE_SHA") or ""
    report = {
        "schema_version": "qsl.notify_route_identity.v1",
        "source_sha": source if re.fullmatch(r"[0-9a-f]{40}", source) else None,
        "status": "config_unknown", "reference_scope": "project_contract_only",
        "canonical_route_status": "unknown", "contract_bot_status": "unavailable", "services": [],
    }
    project = os.environ.get("GCP_PROJECT_ID") or ""
    region = os.environ.get("CLOUD_RUN_REGION") or "us-central1"
    if not re.fullmatch(_PROJECT, project):
        return report
    try:
        services = _services()
    except Exception:  # noqa: BLE001 - do not report invalid private configuration
        return report
    token, status = _secret_token(project, CONTRACT_SECRET, "latest")
    contract_id = None
    if token is not None:
        contract_id, status = _bot_identity(token)
        token = None
    report.update(status="ok", contract_bot_status=status)
    report["services"] = [_observe_service(name, index, project, region, contract_id)
                          for index, name in enumerate(services, start=1)]
    return report


def main() -> int:
    report = _report()
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as stream:
            stream.write("## Telegram route identity (read-only)\n\n```json\n" + text + "\n```\n")
    # Diagnosis completion is not route readiness; nullable result fields carry unknown.
    return 0


if __name__ == "__main__":
    sys.exit(main())
