"""Adapt existing injected HTTP clients to the shared QPK Telegram sender."""
from __future__ import annotations

import hashlib
import io
import json
import os

from quant_platform_kit.notifications import (
    NotificationIntent,
    redact_sensitive_text,
    send_notification_intent,
    send_telegram_message,
)


def send_intent(text, *, chat_id, token, token_env="TELEGRAM_TOKEN", requests_module,
                timeout=15, payload_key="json", request_options=None, printer=None):
    """Keep credentials outside intents, reports and provider error receipts.

    Environment-backed runtime routes use the intent sender. Existing callers
    supplying an in-memory credential retain QPK's explicit-credential transport
    until they can provide a credential reference; no process environment is mutated.
    """
    message = redact_sensitive_text(str(text))
    intent = NotificationIntent(text=message, run_id="runtime", chat_ids=(str(chat_id),))
    provider = {}
    statuses = []

    def opener(request, *, timeout):
        payload = json.loads(request.data)
        # Preserve existing consumer wire options; QPK still builds/validates the request.
        payload.pop("disable_web_page_preview", None)
        try:
            response = requests_module.post(
                request.full_url, **{payload_key: payload}, timeout=timeout,
                **(request_options or {}),
            )
        except Exception as exc:
            error_type = type(exc).__name__
            provider["error_type"] = error_type if error_type.isidentifier() else "transport_error"
            raise
        status = int(getattr(response, "status_code", 200) or 200)
        statuses.append(status)
        if printer is not None and not 200 <= status < 300:
            detail = redact_sensitive_text(str(getattr(response, "text", "")))
            if token:
                detail = detail.replace(str(token), "<redacted>")
            printer(f"Telegram send failed with status {status}: {detail[:300]}", flush=True)
        load = getattr(response, "json", None)
        body = load() if callable(load) else None
        if isinstance(body, dict):
            # Retain only scalar delivery metadata needed by existing consumers.
            result = body.get("result")
            if body.get("ok") is True and isinstance(result, dict):
                message_id = result.get("message_id")
                if type(message_id) is int and message_id > 0:
                    provider["message_id"] = message_id
                    chat = result.get("chat")
                    if isinstance(chat, dict) and str(chat.get("id")) == str(chat_id):
                        provider["transport_receipt_id"] = hashlib.sha256(
                            json.dumps([str(chat_id), message_id], separators=(",", ":")).encode()
                        ).hexdigest()
            parameters = body.get("parameters")
            delay = parameters.get("retry_after") if isinstance(parameters, dict) else None
            if type(delay) is int and delay > 0:
                provider["retry_after"] = delay
        wrapped = io.BytesIO(json.dumps(body).encode())
        wrapped.status = status
        return wrapped

    if token == os.environ.get(token_env):
        receipt = send_notification_intent(intent, bot_token_env=token_env, opener=opener, timeout=timeout)
    else:
        # Compatibility for existing dependency-injected and SecretStore callers.
        # QPK remains the sole URL/HTTP implementation, with the same strict ack.
        ok = send_telegram_message(
            bot_token=token, chat_ids=intent.chat_ids, text=intent.text,
            opener=opener, timeout=timeout, printer=lambda *_args, **_kwargs: None,
        )
        receipt = {
            "ok": ok, "status": "sent" if ok else "failed",
            "http_status": statuses[-1] if statuses else None,
            "sha256": hashlib.sha256(message.encode()).hexdigest(),
            "length": len(message), "run_id": intent.run_id,
        }
    return {**receipt, **provider}
