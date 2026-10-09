#!/usr/bin/env python3
"""Read-only Telegram route check for Cloud Run services (no token/chat printed).

For each configured Cloud Run service, report which Secret Manager secret the
live service binds to TELEGRAM_TOKEN, then resolve each distinct secret (plus
the QuantSentinel contract secret) to its public bot identity via Telegram
``getMe``. Only bot id / username are printed; token values are masked and
never written. Does not change Cloud Run, secrets, or trading state.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request

CONTRACT_SECRET = "quant-sentinel-telegram-bot-token"


def _services() -> list[str]:
    raw: list[str] = []
    for key in ("CLOUD_RUN_SERVICE", "CLOUD_RUN_SERVICES", "RUNTIME_GUARD_CLOUD_RUN_SERVICES"):
        raw.extend(p.strip() for p in (os.environ.get(key) or "").replace("\n", ",").split(","))
    targets = (os.environ.get("CLOUD_RUN_SERVICE_TARGETS_JSON") or "").strip()
    if targets:
        try:
            data = json.loads(targets)
            items = data if isinstance(data, list) else data.get("targets", []) if isinstance(data, dict) else []
            for item in items:
                if isinstance(item, dict):
                    for key in ("service", "service_name", "cloud_run_service"):
                        value = item.get(key) or (item.get("runtime_target") or {}).get(key)
                        if isinstance(value, str) and value.strip():
                            raw.append(value.strip())
                            break
                elif isinstance(item, str):
                    raw.append(item.strip())
        except json.JSONDecodeError:
            pass
    seen: list[str] = []
    for name in raw:
        if name and name not in seen:
            seen.append(name)
    return seen


def _gcloud(*args: str) -> str:
    return subprocess.run(["gcloud", *args], check=True, capture_output=True, text=True).stdout


def _telegram_binding(service: str, project: str, region: str) -> dict:
    spec = json.loads(_gcloud("run", "services", "describe", service, f"--project={project}",
                              f"--region={region}", "--format=json"))
    containers = spec.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    status = spec.get("status", {})
    out = {
        "latest_ready_revision": status.get("latestReadyRevisionName"),
        "traffic": [
            {"revision": t.get("revisionName"), "percent": t.get("percent")}
            for t in status.get("traffic", []) if t.get("percent")
        ],
        "telegram_token_source": "missing",
        "telegram_token_secret": None,
        "telegram_token_secret_version": None,
    }
    for container in containers:
        for env in container.get("env", []) or []:
            if env.get("name") != "TELEGRAM_TOKEN":
                continue
            ref = (env.get("valueFrom") or {}).get("secretKeyRef")
            if ref:
                out["telegram_token_source"] = "secret_ref"
                out["telegram_token_secret"] = ref.get("name")
                out["telegram_token_secret_version"] = ref.get("key")
            elif env.get("value"):
                out["telegram_token_source"] = "plain_env_value"
    return out


def _bot_identity(secret: str, project: str) -> dict:
    try:
        token = _gcloud("secrets", "versions", "access", "latest", f"--secret={secret}",
                        f"--project={project}").strip()
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").upper()
        reason = "unknown"
        for marker in ("NOT_FOUND", "PERMISSION_DENIED", "FAILED_PRECONDITION", "UNAUTHENTICATED"):
            if marker in stderr:
                reason = marker.lower()
                break
        return {"secret": secret, "status": "secret_inaccessible", "reason": reason}
    if not token:
        return {"secret": secret, "status": "secret_empty"}
    print(f"::add-mask::{token}")
    try:
        with urllib.request.urlopen(f"https://api.telegram.org/bot{token}/getMe", timeout=15) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - report class only, never the URL
        return {"secret": secret, "status": f"getme_failed:{type(exc).__name__}"}
    finally:
        token = ""
    result = body.get("result") or {}
    return {
        "secret": secret,
        "status": "ok" if body.get("ok") else "getme_not_ok",
        "bot_id": result.get("id"),
        "bot_username": result.get("username"),
        "bot_first_name": result.get("first_name"),
    }


def main() -> int:
    project = os.environ["GCP_PROJECT_ID"]
    region = os.environ.get("CLOUD_RUN_REGION") or "us-central1"
    services = _services()
    report: dict = {"project": project, "region": region, "services": [], "bots": []}
    secrets = {CONTRACT_SECRET}
    gh_secret_name = (os.environ.get("TELEGRAM_TOKEN_SECRET_NAME") or "").strip()
    if gh_secret_name:
        secrets.add(gh_secret_name)
    report["github_variable_TELEGRAM_TOKEN_SECRET_NAME"] = gh_secret_name or None
    for index, service in enumerate(services, start=1):
        print(f"::add-mask::{service}")
        try:
            binding = _telegram_binding(service, project, region)
        except subprocess.CalledProcessError:
            binding = {"telegram_token_source": "describe_failed"}
        binding["service_label"] = f"service#{index}"
        if binding.get("telegram_token_secret"):
            secrets.add(binding["telegram_token_secret"])
        report["services"].append(binding)
    for secret in sorted(secrets):
        report["bots"].append(_bot_identity(secret, project))
    contract_bot = next((b for b in report["bots"] if b["secret"] == CONTRACT_SECRET), {})
    for svc in report["services"]:
        bound = next((b for b in report["bots"] if b["secret"] == svc.get("telegram_token_secret")), {})
        svc["same_bot_as_contract"] = (
            bool(bound.get("bot_id")) and bound.get("bot_id") == contract_bot.get("bot_id")
        )
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("## Telegram route check (read-only)\n\n```json\n" + text + "\n```\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
