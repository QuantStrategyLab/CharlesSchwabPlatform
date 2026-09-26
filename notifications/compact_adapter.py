"""Adapter for compact, user-facing notification sections."""

from __future__ import annotations

import re
from collections.abc import Iterable

_HOLDINGS_HEADERS = {
    "💼 持仓",
    "💼 策略持仓",
    "💼 当前持仓",
    "💼 Holdings",
    "💼 Current Holdings",
    "💼 Strategy Holdings",
    "💼 Strategy holdings",
}
_NUMBER_RE = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?")


def _contains_nonzero_number(text: str) -> bool:
    for match in _NUMBER_RE.finditer(text):
        try:
            if abs(float(match.group(0).replace(",", ""))) > 1e-12:
                return True
        except ValueError:
            continue
    return False


def _localize_holding_detail(detail: str, *, locale: str) -> str:
    if str(locale).lower().startswith("zh"):
        return re.sub(r"\s+shares?\b", "股", detail, flags=re.IGNORECASE)

    def replace_share(match: re.Match[str]) -> str:
        quantity = match.group(1)
        try:
            unit = "share" if abs(float(quantity.replace(",", ""))) == 1 else "shares"
        except ValueError:
            unit = "shares"
        return f"{quantity} {unit}"

    return re.sub(r"([-+]?\d[\d,]*(?:\.\d+)?)\s*股", replace_share, detail)


def adapt_compact_sections(
    dashboard_text: str,
    *,
    locale: str,
    supplemental_lines: Iterable[object] = (),
) -> tuple[str, ...]:
    """Return normalized non-zero holdings followed by explicit supplements.

    Holdings are read from the already-rendered same-cycle dashboard so each
    platform keeps ownership of broker-specific valuation and quantity rules.
    Supplemental lines must already be localized by the platform translator.
    """
    holdings: list[str] = []
    in_holdings = False
    for raw_line in str(dashboard_text or "").splitlines():
        line = raw_line.strip()
        if line in _HOLDINGS_HEADERS:
            in_holdings = True
            continue
        if not in_holdings or not line:
            continue
        if line.startswith("━") or line.startswith(("📌", "💵", "📊", "🎯", "🧾", "⏱", "🧩")):
            break
        normalized = line.lstrip("-• ").strip()
        if ":" not in normalized and "：" not in normalized:
            continue
        separator = "：" if "：" in normalized else ":"
        symbol, detail = (part.strip() for part in normalized.split(separator, 1))
        if not symbol or not detail or not _contains_nonzero_number(detail):
            continue
        detail = _localize_holding_detail(detail, locale=locale)
        holdings.append(f"- {symbol}: {detail}")

    lines: list[str] = []
    if holdings:
        lines.append("💼 持仓" if str(locale).lower().startswith("zh") else "💼 Holdings")
        lines.extend(holdings)

    for raw_line in supplemental_lines:
        line = str(raw_line or "").strip()
        if line and line not in lines:
            lines.append(line)
    return tuple(lines)
