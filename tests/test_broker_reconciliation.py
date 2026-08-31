from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from application.broker_reconciliation import (
    SchwabReconciliationReadError,
    build_reconciliation_candidate,
    collect_read_only_reconciliation_observations,
)
from quant_platform_kit.common.live_continuity import runtime_target_fingerprint
from quant_platform_kit.common.runtime_target import build_runtime_target


class _Response:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


class _Client:
    def get_account_numbers(self):
        return _Response([{"hashValue": "acct-hash"}])

    def get_orders_for_account(self, account_hash, **kwargs):
        assert account_hash == "acct-hash"
        assert kwargs["from_entered_datetime"] < kwargs["to_entered_datetime"]
        return _Response(
            [
                {
                    "orderId": 1,
                    "status": "WORKING",
                    "orderType": "LIMIT",
                    "orderStrategyType": "SINGLE",
                    "enteredTime": "2026-08-31T00:00:00Z",
                    "filledQuantity": 0,
                    "remainingQuantity": 2,
                    "orderLegCollection": [
                        {
                            "instruction": "BUY",
                            "quantity": 2,
                            "instrument": {"symbol": "SOXL", "assetType": "EQUITY"},
                        }
                    ],
                },
                {
                    "orderId": 2,
                    "status": "FILLED",
                    "orderType": "MARKET",
                    "orderStrategyType": "SINGLE",
                    "enteredTime": "2026-08-30T00:00:00Z",
                    "filledQuantity": 3,
                    "remainingQuantity": 0,
                    "orderLegCollection": [
                        {
                            "instruction": "SELL",
                            "quantity": 3,
                            "instrument": {"symbol": "SOXL", "assetType": "EQUITY"},
                        }
                    ],
                },
            ]
        )


def _snapshot(_client, *, strategy_symbols=()):
    assert strategy_symbols == ()
    return SimpleNamespace(
        cash_balance=100.0,
        buying_power=100.0,
        total_equity=350.0,
        positions=(SimpleNamespace(symbol="SOXL", quantity=10.0, market_value=250.0),),
        metadata={"account_hash": "acct-hash"},
    )


def _target():
    payload = {
        "platform_id": "schwab",
        "strategy_profile": "soxl_soxx_trend_income",
        "dry_run_only": False,
        "deployment_selector": "live",
        "account_selector": ["live"],
        "account_scope": "live",
        "service_name": "schwab-live",
    }
    return build_runtime_target(
        **payload,
        live_continuity={
            "state": "RECONCILE_ONLY",
            "baseline_kind": "legacy_authorized",
            "baseline_id": "schwab-soxl-lkg-20260830",
            "baseline_target_sha256": runtime_target_fingerprint(payload),
            "captured_at": "2026-08-30",
        },
        continuity_fingerprint_payload=payload,
    )


def test_collects_all_required_read_only_surfaces():
    observations = collect_read_only_reconciliation_observations(
        _Client(), fetch_account_snapshot=_snapshot
    )

    assert observations.account_identity_match is True
    assert observations.account_scope == {"account_hash": "acct-hash"}
    assert len(observations.positions) == 1
    assert len(observations.open_orders) == 1
    assert len(observations.recent_executions) == 1


def test_missing_order_history_support_fails_closed():
    class MissingOrders(_Client):
        get_orders_for_account = None

    with pytest.raises(SchwabReconciliationReadError, match="order-history"):
        collect_read_only_reconciliation_observations(
            MissingOrders(), fetch_account_snapshot=_snapshot
        )


def test_candidate_stays_frozen_without_private_expected_digests(tmp_path):
    observations = collect_read_only_reconciliation_observations(
        _Client(), fetch_account_snapshot=_snapshot
    )
    candidate = build_reconciliation_candidate(
        observations=observations,
        runtime_target=_target(),
        project_id=None,
        env_reader=lambda name, default=None: str(tmp_path)
        if name == "SCHWAB_EXECUTION_STATE_DIR"
        else default,
    )

    assert candidate.permits_active_lkg is False
    assert candidate.expected_digests_configured is False
    assert candidate.to_safe_dict()["evidence"]["positions_sha256"]


def test_candidate_can_pass_only_with_all_matching_private_digests(tmp_path):
    observations = collect_read_only_reconciliation_observations(
        _Client(), fetch_account_snapshot=_snapshot
    )

    def empty_env(name, default=None):
        return str(tmp_path) if name == "SCHWAB_EXECUTION_STATE_DIR" else default

    seed = build_reconciliation_candidate(
        observations=observations,
        runtime_target=_target(),
        project_id=None,
        env_reader=empty_env,
    )
    expected = {
        key: seed.evidence.to_dict()[key]
        for key in (
            "positions_sha256",
            "cash_sha256",
            "open_orders_sha256",
            "recent_executions_sha256",
            "local_execution_ledger_sha256",
        )
    }

    def configured_env(name, default=None):
        if name == "SCHWAB_EXECUTION_STATE_DIR":
            return str(tmp_path)
        if name == "SCHWAB_RECONCILIATION_EXPECTED_DIGESTS_JSON":
            return json.dumps(expected)
        return default

    candidate = build_reconciliation_candidate(
        observations=observations,
        runtime_target=_target(),
        project_id=None,
        env_reader=configured_env,
    )
    assert candidate.permits_active_lkg is True
