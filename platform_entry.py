"""Late-bound process entry for monkeypatch-friendly platform modules."""

from __future__ import annotations

import sys
from types import ModuleType

_ENTRY_MODULE = "main"
_entry_ref: ModuleType | None = None


def bind(module: ModuleType) -> ModuleType:
    """Keep a strong reference to the process entry module."""
    global _entry_ref
    _entry_ref = module
    return module


def entry() -> ModuleType:
    """Return the process entry module for patchable symbol lookup."""
    if _entry_ref is not None:
        return _entry_ref
    return sys.modules[_ENTRY_MODULE]
