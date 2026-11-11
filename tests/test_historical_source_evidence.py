"""Synthetic source-contract observations; no broker or historical data access."""

from copy import deepcopy
from decimal import getcontext
import hashlib
import json

import pytest

from scripts.historical_source_evidence import validate_source_evidence


def digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def record(document, kind):
    return next(row for row in document["records"] if row["kind"] == kind)


def refresh(document, *, receipt=True, transition=True):
    if receipt:
        payload = record(document, "execution_receipt")["payload"]
        payload["receipt_id"] = "execution-receipt." + digest({key: value for key, value in payload.items()
                                                              if key != "receipt_id"})[:32]
    if transition:
        payload = record(document, "daily_state_transition")["payload"]
        payload["transition_sha256"] = digest({key: value for key, value in payload.items()
                                               if key != "transition_sha256"})
    for row in document["records"]:
        row["payload_sha256"] = digest(row["payload"])
    return document


def document():
    release = {"release_id": "synthetic-release", "strategy_revision": "a" * 40,
               "effective_session": "2026-10-30"}
    release.update({key: "b" * 64 for key in ("manifest_sha256", "config_sha256", "risk_policy_sha256",
                                             "evidence_sha256", "plugin_bundle_sha256")})
    association = {
        "run_id": "synthetic-run", "strategy_profile": "tqqq_growth_income",
        "account_scope": "synthetic-scope", "platform": "charles_schwab",
        "execution_environment": "paper", "execution_mode": "live", "dry_run": False,
        "record_kind": "execution", "server": "synthetic-server", "lifecycle_id": "synthetic-lifecycle",
        "account_alias": "acct_" + "0" * 16, "release_id": "synthetic-release",
    }
    binding = {**association, "release": release, "candidate_id": "synthetic-candidate",
               "input_sha256": "c" * 64, "as_of": "2026-10-30T20:01:00Z"}
    runtime = {
        "schema_version": "runtime_report.v1", "platform": "charles_schwab",
        "run_id": "synthetic-run", "strategy_profile": "tqqq_growth_income",
        "account_scope": "synthetic-scope", "dry_run": False,
        "runtime_target": {"execution_mode": "live", "execution_environment": "paper", "dry_run_only": False},
        "runtime_release_receipt": {"schema_version": "runtime_loaded_receipt.v1",
                                    "loaded_at": "2026-10-30T20:00:00Z",
                                    "attestation_state": "self_attested",
                                    "release_id": "synthetic-release", "strategy_release": release.copy()},
        "started_at": "2026-10-30T20:00:00Z", "decision_at": "2026-10-30T20:00:01Z",
        "finished_at": "2026-10-30T20:00:10Z", "status": "ok",
    }
    receipt = {"schema_version": "qsl_execution_receipt.v1", "platform": "schwab",
               "strategy_profile": "tqqq_growth_income", "strategy_revision": "a" * 40,
               "execution_mode": "live", "outcome": "reconciliation_required",
               "broker_confirmation": "reconciliation_required", "observed_at": "2026-10-30T20:00:09Z"}
    request = {"orderType": "MARKET", "session": "NORMAL", "duration": "DAY",
               "orderStrategyType": "SINGLE", "orderLegCollection": [
                   {"instruction": "BUY", "quantity": 50.0, "quantityType": "DOLLARS",
                    "instrument": {"symbol": "TQQQ", "assetType": "EQUITY"}}]}
    response = {"symbol": "TQQQ", "side": "buy", "quantity": 50.0, "status": "accepted",
                "broker_order_id": "17", "raw_payload": {"status_code": 201}}
    submitted = [{"symbol": "TQQQ", "side": "buy", "quantity": 50.0, "notional_usd": 50.0,
                  "order_type": "market", "status": "accepted", "broker_order_id": 17}]
    transition = {
        "schema_version": "strategy_risk_state_transition.v1",
        "identity": {"strategy_profile": "tqqq_growth_income", "account_scope": "synthetic-scope",
                     "candidate_id": "synthetic-candidate", "config_sha256": "b" * 64},
        "effective_session": "2026-10-30", "input_sha256": "c" * 64,
        "previous_transition_sha256": None,
        "state": {"schema_version": "tqqq_daily_state.v1", "session": "2026-10-30",
                  "alpha_trend_active": True, "alpha_epoch": 1, "volatility_latch_active": False,
                  "pending_intent": None, "intent_invalidation_reason": None},
    }
    values = (("runtime_report", runtime, 10, {}), ("execution_receipt", receipt, 9, {}),
              ("order_request", request, 2, {"quantity_unit": "DOLLARS", "quantity": "50", "symbol": "TQQQ", "side": "buy"}),
              ("submission_response", response, 3, {"outcome": "accepted", "quantity_unit": "DOLLARS",
                                                    "quantity": "50", "broker_order_id": "17", "reservation_released": False}),
              ("submitted_orders", submitted, 4, {"quantity_unit": "DOLLARS", "quantity": "50", "broker_order_id": "17"}),
              ("daily_state_transition", transition, 1, {}))
    result = {"schema_version": "qsl.schwab.source_evidence.v1", "binding": binding,
              "redaction": {"order_id_representation": "synthetic", "original_order_id_state": "unknown"},
              "source_hashes": {kind: "d" * 64 for kind, *_ in values}, "records": []}
    for kind, payload, second, normalized in values:
        event = f"2026-10-30T20:00:{second:02d}Z"
        result["records"].append({"kind": kind, "source_id": kind, "runtime_source_id": "runtime_report",
                                  "association": association.copy(), "producer_sha256": "d" * 64,
                                  "event_at": event, "available_at": event, "observed_at": "2026-10-30T20:00:20Z",
                                  "payload": payload, "normalized": normalized})
    return refresh(result)


def check(document, local, code=None):
    result = validate_source_evidence(document)
    assert result["status"] == ("FAIL" if local == "FAIL" else "BLOCKED")
    assert result["local_consistency"] == local
    for key in ("execution_authorized", "production_ready", "historical_parity_established"):
        assert result[key] is False
    if code is not None:
        assert code in {item["code"] for item in result["findings"]}
    return result


def test_declared_source_fields_can_agree_without_authenticating_history():
    value = document()
    value.update({"passed": True, "verified": True, "complete": True, "execution_validated": True})
    value["binding"]["account_verified"] = True
    before = deepcopy(value)
    result = check(value, "CONSISTENT")
    assert value == before
    assert result["source_authentication"] == "UNVERIFIED"
    assert set(result["unresolved"]) >= {"REAL_ACCOUNT_BINDING_UNVERIFIED", "DEPLOYED_RELEASE_UNVERIFIED",
                                        "ORIGINAL_SOURCE_BYTES_UNVERIFIED", "HISTORICAL_REPLAY_NOT_RUN",
                                        "POINT_IN_TIME_AVAILABILITY_UNVERIFIED", "INDIVIDUAL_FILLS_UNAVAILABLE"}


@pytest.mark.parametrize("representation", ["replaced", "unknown"])
@pytest.mark.parametrize("claim", ["unknown", "canonical"])
def test_canonical_surrogate_cannot_prove_original_order_id(representation, claim):
    value = document()
    value["redaction"] = {"order_id_representation": representation, "original_order_id_state": claim}
    check(value, "BLOCKED", "ORIGINAL_ORDER_ID_UNVERIFIED")


def test_replacement_cannot_erase_observed_invalid_original_id():
    value = document()
    value["redaction"] = {"order_id_representation": "replaced", "original_order_id_state": "invalid"}
    check(value, "FAIL", "ORIGINAL_ORDER_ID_INVALID")


@pytest.mark.parametrize("field", ["run_id", "strategy_profile", "account_scope"])
def test_observed_runtime_binding_conflict_fails(field):
    value = document()
    record(value, "runtime_report")["payload"][field] = "different"
    check(refresh(value), "FAIL", "NATIVE_BINDING_MISMATCH")


@pytest.mark.parametrize("field", ["run_id", "execution_environment", "server", "lifecycle_id", "account_alias",
                                   "record_kind", "release_id", "strategy_profile"])
def test_normalized_association_conflict_is_not_authenticated_by_an_alias(field):
    value = document()
    record(value, "submission_response")["association"][field] = "different"
    check(value, "FAIL", "ASSOCIATION_MISMATCH")


@pytest.mark.parametrize("mutation,code", [
    (lambda value: record(value, "submission_response").update(producer_sha256="e" * 64), "PRODUCER_DIGEST_MISMATCH"),
    (lambda value: record(value, "submission_response").update(runtime_source_id="absent"), "RUNTIME_REFERENCE_MISMATCH"),
    (lambda value: record(value, "submission_response").update(payload_sha256="e" * 64), "PAYLOAD_DIGEST_MISMATCH"),
    (lambda value: record(value, "execution_receipt")["payload"].update(run_id="invented"), "NATIVE_FIELD_SET_MISMATCH"),
])
def test_explicit_source_conflicts_fail(mutation, code):
    value = document()
    mutation(value)
    if code == "NATIVE_FIELD_SET_MISMATCH":
        refresh(value)
    check(value, "FAIL", code)


def test_release_hash_conflict_fails():
    value = document()
    record(value, "runtime_report")["payload"]["runtime_release_receipt"]["strategy_release"]["config_sha256"] = "e" * 64
    check(refresh(value), "FAIL", "RELEASE_BINDING_MISMATCH")


@pytest.mark.parametrize("kind,key", [("runtime_report", "run_id"), ("submission_response", "raw_payload"),
                                      ("execution_receipt", "observed_at")])
def test_missing_native_evidence_is_blocked_not_manufactured_from_binding(kind, key):
    value = document()
    del record(value, kind)["payload"][key]
    check(refresh(value), "BLOCKED", "MISSING_EVIDENCE")


@pytest.mark.parametrize("kind,schema", [("runtime_report", "runtime_report.v999"),
                                        ("execution_receipt", "qsl_execution_receipt.v999"),
                                        ("daily_state_transition", "soxl_state.v1")])
def test_unknown_source_schema_remains_blocked(kind, schema):
    value = document()
    record(value, kind)["payload"]["schema_version"] = schema
    check(refresh(value), "BLOCKED", "SOURCE_SCHEMA_UNSUPPORTED")


def test_unknown_source_kind_is_blocked():
    value = document()
    record(value, "order_request")["kind"] = "broker_fill"
    check(value, "BLOCKED", "SOURCE_KIND_UNSUPPORTED")


@pytest.mark.parametrize("kind,field,time,code", [
    ("submission_response", "available_at", "2026-10-30T20:00:02Z", "AVAILABILITY_PRECEDES_EVENT"),
    ("submission_response", "observed_at", "2026-10-30T20:00:02Z", "OBSERVATION_PRECEDES_AVAILABILITY"),
    ("submission_response", "observed_at", "2026-10-30T20:02:00Z", "OBSERVATION_AFTER_CUTOFF"),
    ("submission_response", "event_at", "2026-10-30T20:00:01Z", "ORDER_EVENT_SEQUENCE_CONFLICT"),
    ("runtime_report", "event_at", "2026-10-30T20:00:09Z", "NATIVE_CLOCK_MISMATCH"),
])
def test_event_availability_and_native_clocks_are_separate(kind, field, time, code):
    value = document()
    record(value, kind)[field] = time
    check(value, "FAIL", code)


def test_naive_or_missing_timestamp_cannot_establish_availability():
    value = document()
    record(value, "submission_response")["available_at"] = "2026-10-30T20:00:03"
    check(value, "FAIL", "AWARE_TIMESTAMP_REQUIRED")
    del record(value, "submission_response")["available_at"]
    check(value, "BLOCKED", "MISSING_EVIDENCE")


@pytest.mark.parametrize("bad_id", [True, False, 0, -1, 1.0, "017", " 17", "１７", "1.7", "+17", "", {}, []])
def test_noncanonical_numeric_order_id_fails_without_echoing_input(bad_id):
    value = document()
    record(value, "submission_response")["payload"]["broker_order_id"] = bad_id
    check(refresh(value), "FAIL", "INVALID_ORDER_ID")


def test_integer_and_string_order_id_duplicates_fail():
    value = document()
    row = deepcopy(record(value, "submitted_orders")["payload"][0])
    row["broker_order_id"] = "17"
    record(value, "submitted_orders")["payload"].append(row)
    check(refresh(value), "FAIL", "DUPLICATE_ORDER_ID")


def test_cross_source_order_id_conflict_fails():
    value = document()
    record(value, "submission_response")["payload"]["broker_order_id"] = "18"
    record(value, "submission_response")["normalized"]["broker_order_id"] = "18"
    check(refresh(value), "FAIL", "CROSS_SOURCE_ORDER_ID_MISMATCH")


@pytest.mark.parametrize("kind", ["order_request", "submission_response", "submitted_orders"])
def test_dollar_quantity_is_not_shares(kind):
    value = document()
    record(value, kind)["normalized"]["quantity_unit"] = "shares"
    check(value, "FAIL", "QUANTITY_UNIT_MISMATCH")


def test_unknown_request_quantity_unit_blocks():
    value = document()
    record(value, "order_request")["payload"]["orderLegCollection"][0]["quantityType"] = "UNKNOWN"
    check(refresh(value), "BLOCKED", "QUANTITY_UNIT_UNSUPPORTED")


def test_known_cross_source_quantity_conflict_fails():
    value = document()
    record(value, "order_request")["payload"]["orderLegCollection"][0]["quantity"] = 51
    record(value, "order_request")["normalized"]["quantity"] = "51"
    check(refresh(value), "FAIL", "CROSS_SOURCE_QUANTITY_MISMATCH")


def test_reported_notional_and_transmitted_rounded_dollars_are_distinct():
    value = document()
    for kind in ("submission_response", "submitted_orders"):
        payload = record(value, kind)["payload"]
        (payload[0] if type(payload) is list else payload)["quantity"] = 50.004
        record(value, kind)["normalized"]["quantity"] = "50.004"
    check(refresh(value), "CONSISTENT")


@pytest.mark.parametrize("value", [True, "NaN", "Infinity", "1e999999", "-1", "1000000000000000001"])
def test_normalized_decimal_amounts_are_bounded(value):
    candidate = document()
    record(candidate, "order_request")["normalized"]["quantity"] = value
    check(candidate, "FAIL", "INVALID_QUANTITY")


@pytest.mark.parametrize("status_code", [408, 500, 503, None])
def test_unknown_response_cannot_become_rejection_or_release(status_code):
    value = document()
    response = record(value, "submission_response")
    response["payload"].update(status="unknown", broker_order_id=None)
    response["payload"]["raw_payload"]["status_code"] = status_code
    response["normalized"].update(outcome="unknown", broker_order_id=None)
    check(refresh(value), "BLOCKED", "SUBMISSION_OUTCOME_UNKNOWN")
    response["normalized"]["outcome"] = "rejected"
    check(value, "FAIL", "OUTCOME_CONTRADICTION")
    response["normalized"].update(outcome="unknown", reservation_released=True)
    check(value, "FAIL", "UNKNOWN_RESERVATION_RELEASE")


def test_source_adapter_rejection_of_server_error_is_an_observed_conflict():
    value = document()
    response = record(value, "submission_response")
    response["payload"].update(status="rejected", broker_order_id=None)
    response["payload"]["raw_payload"]["status_code"] = 503
    response["normalized"].update(outcome="rejected", broker_order_id=None)
    check(refresh(value), "FAIL", "OUTCOME_CONTRADICTION")


def test_http_acknowledgement_does_not_supply_a_fill():
    value = document()
    record(value, "submission_response")["normalized"]["outcome"] = "filled"
    check(value, "FAIL", "ACK_IS_NOT_FILL")


def test_filled_receipt_label_without_individual_executions_remains_blocked():
    value = document()
    record(value, "execution_receipt")["payload"].update(outcome="filled", broker_confirmation="filled")
    check(refresh(value), "BLOCKED", "FILL_RECORDS_UNAVAILABLE")


def test_receipt_and_state_native_digest_mismatch_fails():
    value = document()
    record(value, "execution_receipt")["payload"]["receipt_id"] = "execution-receipt." + "0" * 32
    check(refresh(value, receipt=False), "FAIL", "NATIVE_DIGEST_MISMATCH")
    value = document()
    record(value, "daily_state_transition")["payload"]["transition_sha256"] = "0" * 64
    check(refresh(value, transition=False), "FAIL", "NATIVE_DIGEST_MISMATCH")


def test_state_identity_conflict_and_unknown_soxl_state_are_distinct():
    value = document()
    record(value, "daily_state_transition")["payload"]["identity"]["account_scope"] = "different"
    check(refresh(value), "FAIL", "STATE_IDENTITY_MISMATCH")
    value = document()
    value["binding"]["strategy_profile"] = "soxl_soxx_trend_income"
    for row in value["records"]:
        row["association"]["strategy_profile"] = "soxl_soxx_trend_income"
    for kind in ("runtime_report", "execution_receipt"):
        record(value, kind)["payload"]["strategy_profile"] = "soxl_soxx_trend_income"
    record(value, "daily_state_transition")["payload"]["identity"]["strategy_profile"] = "soxl_soxx_trend_income"
    check(refresh(value), "BLOCKED", "STATE_ADAPTER_UNAVAILABLE")


@pytest.mark.parametrize("changes", [
    {"pending_intent": "not-a-state-intent"},
    {"intent_invalidation_reason": "approved"},
    {"extra_state_authority": True},
    {"pending_intent": {"id": "synthetic-intent", "alpha_epoch": 2, "signal_session": "2026-10-30"}},
    {"pending_intent": {"id": "synthetic-intent", "alpha_epoch": 1, "signal_session": "2026-10-30"},
     "volatility_latch_active": True},
])
def test_native_state_wire_constraints_are_observations_not_risk_authority(changes):
    value = document()
    record(value, "daily_state_transition")["payload"]["state"].update(changes)
    check(refresh(value), "FAIL", "INVALID_STATE_FIELD")


@pytest.mark.parametrize("quantity", [0.5, 50.004])
def test_transmitted_dollar_request_respects_observed_builder_field_constraints(quantity):
    value = document()
    record(value, "order_request")["payload"]["orderLegCollection"][0]["quantity"] = quantity
    record(value, "order_request")["normalized"]["quantity"] = str(quantity)
    record(value, "submitted_orders")["payload"][0]["notional_usd"] = quantity
    check(refresh(value), "FAIL", "DOLLAR_REQUEST_FIELD_CONFLICT")


def test_oversized_metadata_container_fails_before_expanding_the_work_stack():
    value = document()
    value["metadata"] = [None] * 10001
    check(value, "FAIL", "JSON_SHAPE_LIMIT_EXCEEDED")


def test_native_state_identity_is_canonical_even_when_declared_bindings_agree():
    value = document()
    value["binding"]["candidate_id"] = "UPPERCASE"
    record(value, "daily_state_transition")["payload"]["identity"]["candidate_id"] = "UPPERCASE"
    check(refresh(value), "FAIL", "INVALID_STATE_IDENTITY")


def test_missing_environment_is_not_reconstructed_from_legacy_execution_mode():
    value = document()
    del record(value, "runtime_report")["payload"]["runtime_target"]["execution_environment"]
    check(refresh(value), "BLOCKED", "MISSING_EVIDENCE")


def test_native_environment_conflict_is_not_overridden_by_normalized_binding():
    value = document()
    record(value, "runtime_report")["payload"]["runtime_target"]["execution_environment"] = "live"
    check(refresh(value), "FAIL", "NATIVE_BINDING_MISMATCH")


def test_legacy_mode_and_target_dry_run_flag_observed_contradiction_fails():
    value = document()
    record(value, "runtime_report")["payload"]["runtime_target"]["dry_run_only"] = True
    check(refresh(value), "FAIL", "RUNTIME_TARGET_MODE_CONFLICT")


def test_runtime_mode_dry_run_and_account_environment_are_not_conflated():
    value = document()
    value["binding"].update(execution_environment="live", dry_run=True, record_kind="diagnostic")
    for row in value["records"]:
        row["association"].update(execution_environment="live", dry_run=True, record_kind="diagnostic")
    record(value, "runtime_report")["payload"]["dry_run"] = True
    record(value, "runtime_report")["payload"]["runtime_target"]["execution_environment"] = "live"
    check(refresh(value), "BLOCKED", "DRY_RUN_IS_NOT_BROKER_EVIDENCE")


@pytest.mark.parametrize("value", [None, [], "private-account-marker", {"records": {}}, {1: "private-account-marker"}])
def test_malformed_document_returns_fixed_codes_without_input_text(value):
    result = validate_source_evidence(value)
    assert result["local_consistency"] in {"FAIL", "BLOCKED"}
    assert "private-account-marker" not in json.dumps(result)


@pytest.mark.parametrize("number", [float("nan"), float("inf"), float("-inf"), 10 ** 400])
def test_nonfinite_and_out_of_range_native_numbers_are_structured_failures(number):
    value = document()
    value["untrusted_metadata"] = number
    check(value, "FAIL", "INVALID_JSON_NUMBER")


def test_deep_or_cyclic_metadata_fails_before_recursive_digest():
    value = document()
    current = value
    for _ in range(1500):
        current["nested"] = {}
        current = current["nested"]
    check(value, "FAIL", "JSON_SHAPE_LIMIT_EXCEEDED")
    value = document()
    value["cycle"] = value
    check(value, "FAIL", "JSON_SHAPE_LIMIT_EXCEEDED")


def test_validation_preserves_decimal_context_and_document_on_failure():
    value = document()
    record(value, "order_request")["normalized"]["quantity"] = "0.000000000001"
    before = deepcopy(value)
    context = getcontext().copy()
    check(value, "FAIL", "QUANTITY_MISMATCH")
    assert value == before
    assert getcontext().prec == context.prec
    assert getcontext().rounding == context.rounding
    assert getcontext().flags == context.flags


@pytest.mark.parametrize("started_at", ["2026-10-30T20:00:00.500000Z", "2026-10-30T20:00:01.500000Z"])
def test_source_cycle_clock_can_precede_report_construction(started_at):
    value = document()
    payload = record(value, "runtime_report")["payload"]
    payload.update(started_at=started_at, decision_at="2026-10-30T20:00:00Z")
    payload["runtime_release_receipt"]["loaded_at"] = started_at
    check(refresh(value), "CONSISTENT")


def test_terminal_receipt_after_finish_keeps_timing_association_unknown():
    value = document()
    value["records"] = [row for row in value["records"] if row["kind"] not in
                        {"order_request", "submission_response", "submitted_orders"}]
    record(value, "runtime_report")["payload"]["status"] = "error"
    receipt = record(value, "execution_receipt")
    receipt.update(event_at="2026-10-30T20:00:11Z", available_at="2026-10-30T20:00:11Z")
    receipt["payload"].update(observed_at="2026-10-30T20:00:11Z", outcome="failed", broker_confirmation="not_observed")
    receipt["normalized"].update(verified=True, terminal_fallback=True)
    check(refresh(value), "BLOCKED", "RECEIPT_TIMING_ASSOCIATION_UNVERIFIED")


def test_observed_embedded_receipt_cannot_conflict_with_same_run_standalone_receipt():
    value = document()
    receipt = deepcopy(record(value, "execution_receipt")["payload"])
    receipt.update(outcome="no_action", broker_confirmation="not_applicable")
    receipt["receipt_id"] = "execution-receipt." + digest({key: item for key, item in receipt.items() if key != "receipt_id"})[:32]
    record(value, "runtime_report")["payload"]["execution_receipt"] = receipt
    check(refresh(value), "FAIL", "EMBEDDED_RECEIPT_MISMATCH")


@pytest.mark.parametrize("field,time,code", [
    ("started_at", "2026-10-30T20:00:11Z", "NATIVE_CLOCK_SEQUENCE_CONFLICT"),
    ("decision_at", "2026-10-30T20:00:11Z", "NATIVE_CLOCK_SEQUENCE_CONFLICT"),
    ("loaded_at", "2026-10-30T20:00:11Z", "RELEASE_LOADED_AFTER_FINISH"),
    ("decision_at", "2026-10-30T20:00:00.500000Z", "DECISION_CLOCK_PRECISION_CONFLICT"),
])
def test_source_clock_fix_preserves_observed_upper_bounds_and_precision(field, time, code):
    value = document()
    payload = record(value, "runtime_report")["payload"]
    (payload["runtime_release_receipt"] if field == "loaded_at" else payload)[field] = time
    check(refresh(value), "FAIL", code)
