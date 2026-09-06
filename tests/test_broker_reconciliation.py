from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from application.broker_reconciliation import (
    SchwabReconciliationReadError,
    build_reconciliation_candidate,
    collect_read_only_reconciliation_observations,
    validate_reconciliation_candidate,
    validate_reconciliation_preconditions,
)
from quant_platform_kit.common.broker_reconciliation import (
    BrokerReconciliationFinding,
    calculate_broker_observation_sha256,
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


def test_collects_partial_read_only_surfaces_without_claiming_completeness():
    observations = collect_read_only_reconciliation_observations(
        _Client(), fetch_account_snapshot=_snapshot
    )

    assert observations.account_identity_match is True
    assert observations.account_scope == {"account_hash": "acct-hash"}
    assert len(observations.positions) == 1
    assert len(observations.open_orders) == 1
    assert observations.recent_executions == ()
    assert observations.open_orders_complete is False
    assert observations.recent_executions_complete is False
    assert observations.coverage["open_orders_complete"] is False
    assert observations.coverage["recent_executions_complete"] is False
    assert observations.coverage["order_lookback_days"] == 7
    assert "entered_time_window_may_miss_older_gtc_open_orders" in observations.coverage["reason_codes"]


def test_missing_order_history_support_fails_closed():
    class MissingOrders(_Client):
        get_orders_for_account = None

    with pytest.raises(SchwabReconciliationReadError, match="order-history"):
        collect_read_only_reconciliation_observations(
            MissingOrders(), fetch_account_snapshot=_snapshot
        )


def test_reconciliation_preconditions_default_off_and_require_collector():
    with pytest.raises(SchwabReconciliationReadError, match="disabled"):
        validate_reconciliation_preconditions(
            runtime_target=_target(),
            collector=collect_read_only_reconciliation_observations,
            env_reader=lambda _name, default=None: default,
        )

    with pytest.raises(SchwabReconciliationReadError, match="collector"):
        validate_reconciliation_preconditions(
            runtime_target=_target(),
            collector=None,
            env_reader=lambda _name, default=None: "true",
        )

    with pytest.raises(SchwabReconciliationReadError, match="frozen"):
        validate_reconciliation_preconditions(
            runtime_target=SimpleNamespace(
                live_continuity=SimpleNamespace(state="ACTIVE_LKG")
            ),
            collector=collect_read_only_reconciliation_observations,
            env_reader=lambda _name, default=None: "true",
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


def test_missing_private_baseline_is_a_single_stable_blocker(tmp_path):
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

    assert candidate.recovery_blockers == (BrokerReconciliationFinding.BASELINE_UNAVAILABLE,)


def _synthetic_complete_observations():
    # Test-only complete facts for comparison logic; not proof of broker API coverage.
    return replace(
        collect_read_only_reconciliation_observations(_Client(), fetch_account_snapshot=_snapshot),
        open_orders_complete=True,
        recent_executions_complete=True,
        recent_executions=({"execution_id": "synthetic-fill", "quantity": 3.0},),
    )


def test_candidate_can_pass_only_with_all_matching_private_digests(tmp_path):
    observations = _synthetic_complete_observations()

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
            "account_scope_sha256",
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
    assert (
        validate_reconciliation_candidate(candidate)["evidence"]["schema_version"]
        == "broker_reconciliation_evidence.v1"
    )
    unsafe_payload = candidate.to_safe_dict()
    unsafe_payload["unexpected_detail"] = "must-not-propagate"
    with pytest.raises(SchwabReconciliationReadError, match="receipt"):
        validate_reconciliation_candidate(
            SimpleNamespace(to_safe_dict=lambda: unsafe_payload)
        )


def test_candidate_ignores_valuation_only_changes_to_a_matching_baseline(tmp_path):
    observations = _synthetic_complete_observations()

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
            "account_scope_sha256",
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

    valuation_only_change = replace(
        observations,
        positions=({"symbol": "SOXL", "quantity": 10.0, "market_value": 300.0},),
        cash={"cash_balance": 100.0, "buying_power": 150.0, "total_equity": 400.0},
    )
    candidate = build_reconciliation_candidate(
        observations=valuation_only_change,
        runtime_target=_target(),
        project_id=None,
        env_reader=configured_env,
    )

    assert candidate.permits_active_lkg is True


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("market_value", None),
        ("market_value", float("nan")),
        ("buying_power", float("inf")),
        ("total_equity", float("nan")),
    ),
)
def test_collection_rejects_missing_or_non_finite_valuation_inputs(field_name, value):
    def invalid_snapshot(client, *, strategy_symbols=()):
        snapshot = _snapshot(client, strategy_symbols=strategy_symbols)
        if field_name == "market_value":
            return SimpleNamespace(
                cash_balance=snapshot.cash_balance,
                buying_power=snapshot.buying_power,
                total_equity=snapshot.total_equity,
                positions=(
                    SimpleNamespace(
                        symbol="SOXL", quantity=10.0, market_value=value
                    ),
                ),
                metadata=snapshot.metadata,
            )
        return SimpleNamespace(
            cash_balance=snapshot.cash_balance,
            buying_power=value if field_name == "buying_power" else snapshot.buying_power,
            total_equity=value if field_name == "total_equity" else snapshot.total_equity,
            positions=snapshot.positions,
            metadata=snapshot.metadata,
        )

    with pytest.raises(SchwabReconciliationReadError):
        collect_read_only_reconciliation_observations(
            _Client(), fetch_account_snapshot=invalid_snapshot
        )


def test_candidate_still_blocks_quantity_cash_order_and_ledger_changes(tmp_path):
    observations = _synthetic_complete_observations()

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
            "account_scope_sha256",
            "positions_sha256",
            "cash_sha256",
            "open_orders_sha256",
            "recent_executions_sha256",
            "local_execution_ledger_sha256",
        )
    }

    def candidate_for(changed_observations, configured_expected=expected):
        def configured_env(name, default=None):
            if name == "SCHWAB_EXECUTION_STATE_DIR":
                return str(tmp_path)
            if name == "SCHWAB_RECONCILIATION_EXPECTED_DIGESTS_JSON":
                return json.dumps(configured_expected)
            return default

        return build_reconciliation_candidate(
            observations=changed_observations,
            runtime_target=_target(),
            project_id=None,
            env_reader=configured_env,
        )

    quantity_change = candidate_for(
        replace(
            observations,
            positions=(
                {"symbol": "SOXL", "quantity": 11.0, "market_value": 250.0},
            ),
        )
    )
    cash_change = candidate_for(
        replace(
            observations,
            cash={"cash_balance": 99.0, "buying_power": 100.0, "total_equity": 350.0},
        )
    )
    order_change = candidate_for(replace(observations, open_orders=()))
    ledger_expected = dict(expected)
    ledger_expected["local_execution_ledger_sha256"] = "0" * 64
    ledger_change = candidate_for(observations, ledger_expected)

    assert quantity_change.recovery_blockers == (
        BrokerReconciliationFinding.POSITIONS_MISMATCH,
    )
    assert cash_change.recovery_blockers == (BrokerReconciliationFinding.CASH_MISMATCH,)
    assert order_change.recovery_blockers == (
        BrokerReconciliationFinding.OPEN_ORDERS_MISMATCH,
    )
    assert ledger_change.recovery_blockers == (
        BrokerReconciliationFinding.LOCAL_EXECUTION_LEDGER_MISMATCH,
    )


def test_candidate_rejects_a_different_account_scope_despite_matching_other_digests(tmp_path):
    observations = _synthetic_complete_observations()

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
            "account_scope_sha256",
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
        observations=replace(observations, account_scope={"account_hash": "other-account"}),
        runtime_target=_target(),
        project_id=None,
        env_reader=configured_env,
    )

    assert candidate.permits_active_lkg is False


def test_old_full_observation_baseline_does_not_silently_match_new_accounting_digest(tmp_path):
    observations = _synthetic_complete_observations()

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
            "account_scope_sha256",
            "positions_sha256",
            "cash_sha256",
            "open_orders_sha256",
            "recent_executions_sha256",
            "local_execution_ledger_sha256",
        )
    }
    expected["positions_sha256"] = calculate_broker_observation_sha256(
        observations.positions
    )
    expected["cash_sha256"] = calculate_broker_observation_sha256(observations.cash)

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

    assert candidate.permits_active_lkg is False
    assert BrokerReconciliationFinding.POSITIONS_MISMATCH in candidate.recovery_blockers
    assert BrokerReconciliationFinding.CASH_MISMATCH in candidate.recovery_blockers


def test_reconciliation_candidate_requires_canonical_receipt_schema():
    candidate = SimpleNamespace(
        to_safe_dict=lambda: {
            "schema_version": "schwab_reconciliation_candidate.v1",
            "evidence": {"schema_version": "unexpected.v1"},
        }
    )

    with pytest.raises(SchwabReconciliationReadError, match="receipt"):
        validate_reconciliation_candidate(candidate)


@pytest.mark.parametrize("outside_window_status", ["WORKING", "FILLED"])
def test_entered_window_cannot_authorize_recovery_even_when_all_digests_match(tmp_path, outside_window_status):
    from datetime import datetime, timezone
    from copy import deepcopy
    now = datetime(2026, 9, 5, tzinfo=timezone.utc)

    class WindowedClient(_Client):
        def get_orders_for_account(self, account_hash, **kwargs):
            visible = super().get_orders_for_account(account_hash, **kwargs).json()
            hidden = deepcopy(visible[0])
            hidden.update(orderId=99, status=outside_window_status,
                          enteredTime="2026-07-01T00:00:00Z",
                          closeTime="2026-09-04T12:00:00Z" if outside_window_status == "FILLED" else "",
                          filledQuantity=2 if outside_window_status == "FILLED" else 0)
            # The broker really has an old GTC, or an old order filled yesterday.
            # Filtering by entry date excludes it in both cases.
            return _Response([order for order in [*visible, hidden]
                              if kwargs["from_entered_datetime"] <= datetime.fromisoformat(order["enteredTime"].replace("Z", "+00:00")) <= kwargs["to_entered_datetime"]])

    observations = collect_read_only_reconciliation_observations(
        WindowedClient(), fetch_account_snapshot=_snapshot, now=now,
    )
    assert all(order["order_id"] != "99" for order in observations.open_orders)
    def empty_env(name, default=None):
        return str(tmp_path) if name == "SCHWAB_EXECUTION_STATE_DIR" else default
    seed = build_reconciliation_candidate(
        observations=observations, runtime_target=_target(), project_id=None,
        env_reader=empty_env, observed_at=now,
    )
    expected = {key: seed.evidence.to_dict()[key] for key in (
        "account_scope_sha256", "positions_sha256", "cash_sha256", "open_orders_sha256",
        "recent_executions_sha256", "local_execution_ledger_sha256",
    )}
    def configured_env(name, default=None):
        if name == "SCHWAB_RECONCILIATION_EXPECTED_DIGESTS_JSON":
            return json.dumps(expected)
        return empty_env(name, default)
    candidate = build_reconciliation_candidate(
        observations=observations, runtime_target=_target(), project_id=None,
        env_reader=configured_env, observed_at=now,
    )
    assert candidate.permits_active_lkg is False
    assert BrokerReconciliationFinding.OPEN_ORDERS_MISMATCH in candidate.recovery_blockers
    assert BrokerReconciliationFinding.RECENT_EXECUTIONS_MISMATCH in candidate.recovery_blockers


@pytest.mark.parametrize("field,finding", [
    ("open_orders_complete", BrokerReconciliationFinding.OPEN_ORDERS_MISMATCH),
    ("recent_executions_complete", BrokerReconciliationFinding.RECENT_EXECUTIONS_MISMATCH),
])
@pytest.mark.parametrize("incomplete", [False, None, "false"])
def test_each_surface_requires_explicit_completeness(tmp_path, field, finding, incomplete):
    observations = _synthetic_complete_observations()
    def empty_env(name, default=None):
        return str(tmp_path) if name == "SCHWAB_EXECUTION_STATE_DIR" else default
    seed = build_reconciliation_candidate(
        observations=observations, runtime_target=_target(), project_id=None, env_reader=empty_env,
    )
    expected = {key: seed.evidence.to_dict()[key] for key in (
        "account_scope_sha256", "positions_sha256", "cash_sha256", "open_orders_sha256",
        "recent_executions_sha256", "local_execution_ledger_sha256",
    )}
    def configured_env(name, default=None):
        if name == "SCHWAB_RECONCILIATION_EXPECTED_DIGESTS_JSON":
            return json.dumps(expected)
        return empty_env(name, default)
    candidate = build_reconciliation_candidate(
        observations=replace(observations, **{field: incomplete}), runtime_target=_target(),
        project_id=None, env_reader=configured_env,
    )
    assert candidate.permits_active_lkg is False
    assert candidate.recovery_blockers == (finding,)
