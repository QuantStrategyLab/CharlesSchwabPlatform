"""Display-only resolver; generated defaults come from QRS platform-config.json.

Never use resolved names for execution identity, bindings, keys or deduplication.
QSL_DISPLAY_NAMES_JSON accepts an optional JSON object with per-section overrides.
Invalid overrides fall back to bundled names so an urgent alert can still render.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def _defaults():
    return json.loads(Path(__file__).with_suffix(".json").read_text(encoding="utf-8"))


@lru_cache(maxsize=8)
def _overrides(raw):
    try:
        value = json.loads(raw or "{}")
    except (ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def _text(value, locale):
    if isinstance(value, dict):
        language = "zh" if str(locale).lower().startswith("zh") else "en"
        value = value.get(language) or value.get("en")
    return value.strip() if isinstance(value, str) else ""


def display_name(kind, key, locale="en", *, fallback="", platform=""):
    """Resolve an explicit display key, retaining unknown operator labels."""
    key = str(key or "").strip()
    defaults = _defaults()
    overrides = _overrides(os.environ.get("QSL_DISPLAY_NAMES_JSON", ""))
    aliases = {**defaults.get("strategy_aliases", {}), **(overrides.get("strategy_aliases", {}) if isinstance(overrides.get("strategy_aliases"), dict) else {})}
    canonical = aliases.get(key, key) if kind == "strategies" else key
    if not isinstance(canonical, str):
        canonical = key
    for source in (overrides, defaults):
        section = source.get(kind, {})
        if kind == "accounts" and isinstance(section, dict):
            section = section.get(str(platform).lower(), {})
        if isinstance(section, dict):
            first, second = (key, canonical) if source is overrides else (canonical, key)
            result = _text(section.get(first), locale) or _text(section.get(second), locale)
            if result:
                return result
    return str(fallback or key)
