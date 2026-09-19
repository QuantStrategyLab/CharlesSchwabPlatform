#!/usr/bin/env python3
"""Send one manual CharlesSchwabPlatform Telegram notification-delivery canary.

Does not trade, deploy, invoke Cloud Run, or change configuration.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from notifications.telegram import build_sender

_CANARY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")

_FIXED_MESSAGE_LINES = (
    "🧪 【TEST】通知投递测试 / notification delivery test",
    "未提交任何订单。No order was submitted.",
    "未更改策略、仓位或配置。No strategy, position, or configuration was changed.",
)


def normalize_canary_id(raw: str | None) -> str:
    value = str(raw or "").strip()
    if not value:
        raise ValueError("canary_id is required and must be nonempty")
    if not _CANARY_ID_RE.fullmatch(value):
        raise ValueError(
            "canary_id must be 1-64 chars of [A-Za-z0-9._:-] and start with alphanumeric"
        )
    return value


def build_canary_message(canary_id: str) -> str:
    normalized = normalize_canary_id(canary_id)
    return "\n".join((*_FIXED_MESSAGE_LINES, f"canary_id={normalized}"))


def _split_chat_ids(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [part.strip() for part in str(raw).replace(";", ",").split(",") if part.strip()]


def _telegram_secret_project() -> str | None:
    return (
        os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or None
    )


def _load_telegram_token_from_secret() -> str:
    secret_name = (os.environ.get("TELEGRAM_TOKEN_SECRET_NAME") or "").strip()
    if not secret_name:
        return ""
    command = [
        "gcloud",
        "secrets",
        "versions",
        "access",
        "latest",
        "--secret",
        secret_name,
    ]
    project = _telegram_secret_project()
    if project:
        command.extend(["--project", project])
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        print("Unable to read Telegram token from Secret Manager.", file=sys.stderr)
        return ""
    if result.returncode != 0:
        print("Unable to read Telegram token from Secret Manager.", file=sys.stderr)
        return ""
    return (result.stdout or "").strip()


def resolve_telegram_token() -> str:
    direct = (os.environ.get("TELEGRAM_TOKEN") or os.environ.get("TG_TOKEN") or "").strip()
    if direct:
        return direct
    return _load_telegram_token_from_secret()


def resolve_telegram_chat_id() -> str:
    chat_ids = _split_chat_ids(os.environ.get("GLOBAL_TELEGRAM_CHAT_ID"))
    return chat_ids[0] if chat_ids else ""


def send_canary(*, canary_id: str, requests_module=None) -> bool:
    message = build_canary_message(canary_id)
    token = resolve_telegram_token()
    chat_id = resolve_telegram_chat_id()
    if not token or not chat_id:
        print(
            "Notification canary not sent: Telegram target is not configured.",
            file=sys.stderr,
        )
        return False
    sender = build_sender(token, chat_id, requests_module=requests_module)
    return bool(sender(message))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Send one CharlesSchwabPlatform Telegram notification canary."
    )
    parser.add_argument(
        "--canary-id",
        default=os.environ.get("CANARY_ID"),
        help="Required nonempty operator-supplied canary id for this manual send.",
    )
    args = parser.parse_args(argv)
    try:
        canary_id = normalize_canary_id(args.canary_id)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    delivered = send_canary(canary_id=canary_id)
    if not delivered:
        print("Notification canary delivery failed.", file=sys.stderr)
        return 1
    print(f"Notification canary delivered once for canary_id={canary_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
