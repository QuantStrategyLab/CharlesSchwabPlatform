"""Allocation and plan helpers for CharlesSchwabPlatform."""

import os

from strategy_loader import load_allocation_module

_ALLOCATION_MODULE = load_allocation_module(os.getenv("STRATEGY_PROFILE"))

build_rebalance_plan = _ALLOCATION_MODULE.build_rebalance_plan
get_hybrid_allocation = _ALLOCATION_MODULE.get_hybrid_allocation
get_income_ratio = _ALLOCATION_MODULE.get_income_ratio

__all__ = [
    "build_rebalance_plan",
    "get_hybrid_allocation",
    "get_income_ratio",
]
