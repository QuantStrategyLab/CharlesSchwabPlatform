#!/usr/bin/env python3
"""Send a bounded CharlesSchwabPlatform PAPER Telegram notification preview pack.

Renders synthetic compact messages via existing notification renderers,
translator, and Telegram sender. Does not trade, read Schwab accounts,
positions, or quotes; does not import or call Schwab broker/order/Cloud Run
production interfaces; and does not change production configuration.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from notifications.renderers import render_heartbeat_notification, render_trade_notification
from notifications.telegram import (
    build_sender,
    build_strategy_display_name,
    build_translator,
)

_MAX_PREVIEW_MESSAGES = 6
_PREVIEW_STRATEGY_PROFILE = "us_equity_combo"
_PREVIEW_EXTRA_LINES = (
    "🧪 【PREVIEW】PAPER notification preview",
    "synthetic / 合成样例 · 不会下单 · No order will be placed",
)
_SYNTHETIC_SEPARATOR = "━━━━━━━━━━━━━━━━━━"
_SYNTHETIC_SYMBOL = "PREVIEW"


def _resolve_locale(raw: str | None = None) -> str:
    value = str(raw or os.environ.get("QSL_NOTIFY_LANG") or os.environ.get("NOTIFY_LANG") or "zh")
    value = value.strip().lower()
    return "en" if value.startswith("en") else "zh"


def _split_chat_ids(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [
        part.strip()
        for part in str(raw).replace(";", ",").replace("\n", ",").split(",")
        if part.strip()
    ]


def resolve_telegram_token() -> str:
    return (os.environ.get("TELEGRAM_TOKEN") or os.environ.get("TG_TOKEN") or "").strip()


def resolve_telegram_chat_id() -> str:
    chats = _split_chat_ids(
        os.environ.get("QSL_GLOBAL_TELEGRAM_CHAT_ID")
        or os.environ.get("GLOBAL_TELEGRAM_CHAT_ID")
    )
    return chats[0] if chats else ""


def _with_preview_markers(body: str) -> str:
    return "\n".join(("[PAPER]", body, *_PREVIEW_EXTRA_LINES))


def _synthetic_execution(*, signal_key: str = "signal_idle") -> dict:
    return {
        "dashboard_text": "",
        "separator": _SYNTHETIC_SEPARATOR,
        "signal_display": signal_key,
        "status_display": "",
        "cash_only_execution": True,
    }


def build_preview_messages(*, locale: str | None = None) -> list[str]:
    """Build at most six synthetic compact PAPER preview messages."""

    resolved_locale = _resolve_locale(locale)
    translator = build_translator(resolved_locale)
    strategy_name = build_strategy_display_name(translator)(
        _PREVIEW_STRATEGY_PROFILE,
        fallback_name="US Equity Combo",
    )
    account_line = "🆔 Account: PAPER" if resolved_locale == "en" else "🆔 账户: PAPER"
    signal_idle = translator("signal_idle")

    heartbeat = render_heartbeat_notification(
        translator=translator,
        strategy_display_name=strategy_name,
        dry_run_only=True,
        extra_notification_lines=(),
        execution={
            **_synthetic_execution(signal_key=signal_idle),
            "execution_status": "ok",
        },
        portfolio={
            "total_equity": 0.0,
            "portfolio_rows": (),
            "market_values": {},
        },
        account_label="PAPER",
    ).compact_text

    dry_run_log = translator(
        "dry_run_trade_log",
        command=translator("limit_buy_cmd"),
        symbol=_SYNTHETIC_SYMBOL,
        quantity=0,
        shares=translator("shares"),
    )
    rebalance = render_trade_notification(
        translator=translator,
        strategy_display_name=strategy_name,
        dry_run_only=True,
        extra_notification_lines=(),
        execution=_synthetic_execution(signal_key=translator("signal_entry")),
        trade_logs=(dry_run_log,),
        account_label="PAPER",
    ).compact_text

    pending = "\n".join(
        (
            translator("rebalance_title"),
            translator("strategy_label", name=strategy_name),
            account_line,
            translator("dry_run_banner"),
            (
                f"✅ 💰 {translator('limit_buy_cmd')} {_SYNTHETIC_SYMBOL} ($0.00): "
                f"0{translator('shares')} {translator('submitted')} "
                f"{translator('order_id_suffix', order_id='preview-synthetic-pending')}"
            ),
            "synthetic PREVIEW pending confirmation / 订单待确认",
        )
    )

    filled = "\n".join(
        (
            translator("rebalance_title"),
            translator("strategy_label", name=strategy_name),
            account_line,
            translator("dry_run_banner"),
            (
                f"✅ 📈 {translator('market_buy_cmd')} {_SYNTHETIC_SYMBOL}: "
                f"0{translator('shares')} "
                f"{translator('order_id_suffix', order_id='preview-synthetic-filled')}"
            ),
            "synthetic PREVIEW filled / 成交确认",
        )
    )

    rejected = "\n".join(
        (
            translator("error_header"),
            translator("strategy_label", name=strategy_name),
            account_line,
            translator("dry_run_banner"),
            (
                f"❌ {translator('limit_buy')} {_SYNTHETIC_SYMBOL}: "
                f"0{translator('shares')} {translator('failed')} - preview-synthetic-reject"
            ),
            f"❌ {translator('exception')}: synthetic PREVIEW reject / 拒单异常",
        )
    )

    unknown_status = "\n".join(
        (
            translator("error_header"),
            translator("strategy_label", name=strategy_name),
            account_line,
            translator("dry_run_banner"),
            f"status={translator('strategy_plugin_route_unknown_route')}",
            "synthetic PREVIEW unknown status / 未知状态",
        )
    )

    messages = [
        _with_preview_markers(heartbeat),
        _with_preview_markers(rebalance),
        _with_preview_markers(pending),
        _with_preview_markers(filled),
        _with_preview_markers(rejected),
        _with_preview_markers(unknown_status),
    ]
    if len(messages) > _MAX_PREVIEW_MESSAGES:
        raise RuntimeError(
            f"preview message count {len(messages)} exceeds cap {_MAX_PREVIEW_MESSAGES}"
        )
    return messages


def send_preview(*, locale: str | None = None, send_fn=None, requests_module=None) -> bool:
    messages = build_preview_messages(locale=locale)
    token = resolve_telegram_token()
    chat_id = resolve_telegram_chat_id()
    if not token or not chat_id:
        print(
            "Notification preview not sent: Telegram target is not configured.",
            file=sys.stderr,
        )
        return False

    sender = send_fn or build_sender(token, chat_id, requests_module=requests_module)
    for message in messages:
        if not sender(message):
            print("Notification preview delivery failed.", file=sys.stderr)
            return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Send a bounded CharlesSchwabPlatform PAPER Telegram notification preview pack."
    )
    parser.add_argument(
        "--locale",
        default=os.environ.get("NOTIFY_LANG"),
        help="Optional notification locale override (zh/en). Defaults to NOTIFY_LANG.",
    )
    args = parser.parse_args(argv)

    # Fail closed: this path never enables a production runtime target.
    if (os.environ.get("RUNTIME_TARGET_ENABLED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }:
        print(
            "Notification preview refused: RUNTIME_TARGET_ENABLED must stay disabled.",
            file=sys.stderr,
        )
        return 1

    delivered = send_preview(locale=args.locale)
    if not delivered:
        return 1
    print(
        "Notification preview delivered bounded synthetic PAPER pack "
        f"(at most {_MAX_PREVIEW_MESSAGES} messages; no orders)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
