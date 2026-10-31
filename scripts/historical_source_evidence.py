"""Inspect supplied Schwab source fields without reading or authorizing a runtime.

Payload digests cover the supplied, possibly redacted JSON. Associations, source
hashes and release identities are declarations, not authentication of originals.
This module has no broker, filesystem, network or runtime imports.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import re

SCHEMA = "qsl.schwab.source_evidence.v1"
_KINDS = ("runtime_report", "execution_receipt", "order_request", "submission_response",
          "submitted_orders", "daily_state_transition")
_ASSOCIATION = ("run_id", "strategy_profile", "account_scope", "platform", "execution_environment",
                "execution_mode", "dry_run", "record_kind", "server", "lifecycle_id", "account_alias", "release_id")
_RELEASE_HASHES = ("manifest_sha256", "config_sha256", "risk_policy_sha256", "evidence_sha256", "plugin_bundle_sha256")
_UNRESOLVED = ("ORIGINAL_SOURCE_BYTES_UNVERIFIED", "ORIGINAL_ORDER_IDENTITY_UNVERIFIED",
               "REAL_ACCOUNT_BINDING_UNVERIFIED", "DEPLOYED_RELEASE_UNVERIFIED",
               "PRODUCER_SOURCE_AUTHENTICITY_UNVERIFIED", "POINT_IN_TIME_AVAILABILITY_UNVERIFIED",
               "RUN_COMPLETENESS_UNVERIFIED", "INDIVIDUAL_FILLS_UNAVAILABLE", "STATE_CONTINUITY_UNVERIFIED",
               "ORDER_MAPPING_UNVERIFIED", "EXECUTION_AUTHORITY_UNVERIFIED", "HISTORICAL_REPLAY_NOT_RUN")
_CONFIRMATIONS = {
    "not_due": ("not_applicable",), "no_action": ("not_applicable",), "no_signal": ("not_applicable",),
    "no_rebalance": ("not_applicable",), "risk_blocked": ("not_applicable",), "submitted": ("not_observed",),
    "broker_acknowledged": ("acknowledged",), "partially_filled": ("partially_filled",), "filled": ("filled",),
    "reconciliation_required": ("reconciliation_required",),
    "failed": ("not_applicable", "not_observed", "reconciliation_required"),
}
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}")
_HASH = re.compile(r"[0-9a-f]{64}")
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]{1,12})?")


class _Problem(ValueError):
    def __init__(self, code, scope, *, blocked=False):
        super().__init__(code)
        self.code, self.scope, self.blocked = code, scope, blocked


def _require(condition, code, scope, *, blocked=False):
    if not condition:
        raise _Problem(code, scope, blocked=blocked)


def _object(value, scope):
    _require(type(value) is dict, "INVALID_STRUCTURE", scope)
    return value


def _need(value, key, scope):
    _require(key in _object(value, scope), "MISSING_EVIDENCE", scope, blocked=True)
    return value[key]


def _text(value, scope):
    _require(type(value) is str and _ID.fullmatch(value) is not None, "INVALID_IDENTIFIER", scope)
    return value


def _hash(value, scope):
    _require(type(value) is str and _HASH.fullmatch(value) is not None, "INVALID_DIGEST", scope)
    return value


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _shape(document):
    """Bound every supplied field before a recursive JSON encoder is reached."""
    stack, count = [(document, 0)], 0
    while stack:
        value, depth = stack.pop()
        count += 1
        _require(depth <= 48 and count <= 10000, "JSON_SHAPE_LIMIT_EXCEEDED", "document")
        if type(value) in (dict, list):
            _require(len(value) <= 10000, "JSON_SHAPE_LIMIT_EXCEEDED", "document")
        if type(value) is dict:
            _require(all(type(key) is str and len(key) <= 256 for key in value), "INVALID_JSON_KEY", "document")
            stack.extend((item, depth + 1) for item in value.values())
        elif type(value) is list:
            stack.extend((item, depth + 1) for item in value)
        elif type(value) is str:
            _require(len(value) <= 65536, "JSON_SHAPE_LIMIT_EXCEEDED", "document")
        elif type(value) in (int, float):
            _require(abs(value) <= 10 ** 18 and (type(value) is int or math.isfinite(value)),
                     "INVALID_JSON_NUMBER", "document")
        else:
            _require(value is None or type(value) is bool, "INVALID_JSON_TYPE", "document")


def _clock(value, scope):
    _require(type(value) is str and len(value) <= 40, "AWARE_TIMESTAMP_REQUIRED", scope)
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        _require(result.tzinfo is not None, "AWARE_TIMESTAMP_REQUIRED", scope)
        return result.astimezone(timezone.utc)
    except (ValueError, OverflowError) as error:
        if isinstance(error, _Problem):
            raise
        raise _Problem("AWARE_TIMESTAMP_REQUIRED", scope) from error


def _session(value, scope):
    _require(type(value) is str and len(value) == 10, "INVALID_SESSION", scope)
    try:
        _require(date.fromisoformat(value).isoformat() == value, "INVALID_SESSION", scope)
    except ValueError as error:
        raise _Problem("INVALID_SESSION", scope) from error
    return value


def _quantity(value, scope, *, native=False):
    if native:
        _require(type(value) in (int, float), "INVALID_QUANTITY", scope)
    else:
        _require(type(value) is str and len(value) <= 32 and _DECIMAL.fullmatch(value) is not None,
                 "INVALID_QUANTITY", scope)
    result = Decimal(str(value))
    _require(result.is_finite() and 0 < result <= Decimal("1000000000000000000"), "INVALID_QUANTITY", scope)
    return result


def _order_id(value, scope):
    valid = (type(value) is int and value > 0) or (
        type(value) is str and re.fullmatch(r"[1-9][0-9]{0,63}", value) is not None)
    _require(valid, "INVALID_ORDER_ID", scope)
    return str(value)


def _schema(payload, expected, scope):
    _require(_need(payload, "schema_version", scope) == expected, "SOURCE_SCHEMA_UNSUPPORTED", scope, blocked=True)


def _exact_fields(payload, fields, scope):
    _require(fields <= set(payload), "MISSING_EVIDENCE", scope, blocked=True)
    _require(set(payload) == fields, "NATIVE_FIELD_SET_MISMATCH", scope)


def _binding(document):
    binding = _object(_need(document, "binding", "binding"), "binding")
    for key in _ASSOCIATION:
        value = _need(binding, key, "binding")
        if key == "dry_run":
            _require(type(value) is bool, "INVALID_DRY_RUN", "binding")
        else:
            _text(value, "binding")
    _require(binding["platform"] == "charles_schwab", "PLATFORM_UNSUPPORTED", "binding", blocked=True)
    _require(binding["strategy_profile"] in ("tqqq_growth_income", "soxl_soxx_trend_income"),
             "PROFILE_UNSUPPORTED", "binding", blocked=True)
    _require(binding["execution_environment"] in ("live", "paper", "dry_run")
             and binding["execution_mode"] in ("live", "paper")
             and binding["record_kind"] in ("execution", "reconciliation", "diagnostic"),
             "ACCOUNT_DOMAIN_UNSUPPORTED", "binding", blocked=True)
    _require(re.fullmatch(r"acct_[0-9a-f]{16,64}", binding["account_alias"]) is not None,
             "ACCOUNT_ALIAS_REQUIRED", "binding")
    release = _object(_need(binding, "release", "release"), "release")
    _require(_need(release, "release_id", "release") == binding["release_id"], "RELEASE_BINDING_MISMATCH", "release")
    for key in _RELEASE_HASHES:
        _hash(_need(release, key, "release"), "release")
    revision = _need(release, "strategy_revision", "release")
    _require(type(revision) is str and re.fullmatch(r"[0-9a-f]{40}", revision) is not None, "INVALID_REVISION", "release")
    _session(_need(release, "effective_session", "release"), "release")
    _clock(_need(binding, "as_of", "binding"), "binding")
    return binding


def _envelope(row, kind, binding, hashes):
    source_id = _text(_need(row, "source_id", kind), kind)
    association = _object(_need(row, "association", kind), kind)
    for key in _ASSOCIATION:
        value = _need(association, key, kind)
        _require(type(value) is type(binding[key]) and value == binding[key], "ASSOCIATION_MISMATCH", kind)
    producer = _hash(_need(row, "producer_sha256", kind), kind)
    _require(producer == _hash(_need(hashes, kind, kind), kind), "PRODUCER_DIGEST_MISMATCH", kind)
    payload = _need(row, "payload", kind)
    _require(_digest(payload) == _hash(_need(row, "payload_sha256", kind), kind), "PAYLOAD_DIGEST_MISMATCH", kind)
    event = _clock(_need(row, "event_at", kind), kind)
    available = _clock(_need(row, "available_at", kind), kind)
    observed = _clock(_need(row, "observed_at", kind), kind)
    _require(event <= available, "AVAILABILITY_PRECEDES_EVENT", kind)
    _require(available <= observed, "OBSERVATION_PRECEDES_AVAILABILITY", kind)
    _require(observed <= _clock(binding["as_of"], kind), "OBSERVATION_AFTER_CUTOFF", kind)
    return {"source_id": source_id, "runtime_source_id": _text(_need(row, "runtime_source_id", kind), kind),
            "payload": payload, "normalized": _object(_need(row, "normalized", kind), kind), "event": event}


def _runtime(row, binding):
    scope, payload = "runtime_report", _object(row["payload"], "runtime_report")
    _schema(payload, "runtime_report.v1", scope)
    for key in ("platform", "run_id", "strategy_profile", "account_scope", "dry_run"):
        value = _need(payload, key, scope)
        _require(type(value) is type(binding[key]) and value == binding[key], "NATIVE_BINDING_MISMATCH", scope)
    target = _object(_need(payload, "runtime_target", scope), scope)
    mode = _need(target, "execution_mode", scope)
    _require(mode == binding["execution_mode"], "NATIVE_BINDING_MISMATCH", scope)
    environment = _need(target, "execution_environment", scope)
    _require(environment == binding["execution_environment"], "NATIVE_BINDING_MISMATCH", scope)
    target_dry_run = _need(target, "dry_run_only", scope)
    _require(type(target_dry_run) is bool, "INVALID_DRY_RUN", scope)
    _require(mode == ("paper" if target_dry_run else "live"), "RUNTIME_TARGET_MODE_CONFLICT", scope)
    _require((environment == "dry_run") == target_dry_run, "RUNTIME_TARGET_ENVIRONMENT_CONFLICT", scope)
    loaded = _object(_need(payload, "runtime_release_receipt", scope), scope)
    _schema(loaded, "runtime_loaded_receipt.v1", scope)
    _require(_need(loaded, "attestation_state", scope) == "self_attested", "RELEASE_IDENTITY_UNAVAILABLE", scope, blocked=True)
    _require(_need(loaded, "release_id", scope) == binding["release_id"], "RELEASE_BINDING_MISMATCH", scope)
    release = _object(_need(loaded, "strategy_release", scope), scope)
    for key in ("release_id", "strategy_revision", "effective_session", *_RELEASE_HASHES):
        _require(_need(release, key, scope) == binding["release"][key], "RELEASE_BINDING_MISMATCH", scope)
    start = _clock(_need(payload, "started_at", scope), scope)
    decision = _clock(_need(payload, "decision_at", scope), scope)
    finish = _clock(_need(payload, "finished_at", scope), scope)
    _require(decision.microsecond == 0, "DECISION_CLOCK_PRECISION_CONFLICT", scope)
    _require(start <= finish and decision <= finish, "NATIVE_CLOCK_SEQUENCE_CONFLICT", scope)
    _require(_clock(_need(loaded, "loaded_at", scope), scope) <= finish, "RELEASE_LOADED_AFTER_FINISH", scope)
    _require(finish == row["event"], "NATIVE_CLOCK_MISMATCH", scope)
    return {"start": start, "decision": decision, "finish": finish, "target_dry_run": target_dry_run}


def _receipt(row, binding):
    scope, payload = "execution_receipt", _object(row["payload"], "execution_receipt")
    _schema(payload, "qsl_execution_receipt.v1", scope)
    _exact_fields(payload, {"schema_version", "receipt_id", "platform", "strategy_profile", "strategy_revision",
                            "execution_mode", "outcome", "broker_confirmation", "observed_at"}, scope)
    expected = {"platform": "schwab", "strategy_profile": binding["strategy_profile"],
                "strategy_revision": binding["release"]["strategy_revision"], "execution_mode": binding["execution_mode"]}
    _require(all(payload[key] == value for key, value in expected.items()), "NATIVE_BINDING_MISMATCH", scope)
    outcome = _text(payload["outcome"], scope)
    _require(outcome in _CONFIRMATIONS, "RECEIPT_OUTCOME_UNSUPPORTED", scope, blocked=True)
    _require(payload["broker_confirmation"] in _CONFIRMATIONS[outcome], "RECEIPT_CONFIRMATION_MISMATCH", scope)
    native_time = _clock(payload["observed_at"], scope)
    _require(native_time.strftime("%Y-%m-%dT%H:%M:%SZ") == payload["observed_at"]
             and native_time == row["event"], "NATIVE_CLOCK_MISMATCH", scope)
    digest_payload = {key: value for key, value in payload.items() if key != "receipt_id"}
    _require(payload["receipt_id"] == "execution-receipt." + _digest(digest_payload)[:32], "NATIVE_DIGEST_MISMATCH", scope)
    _require(outcome not in ("filled", "partially_filled"), "FILL_RECORDS_UNAVAILABLE", scope, blocked=True)


def _request(row, binding):
    scope, payload, normalized = "order_request", _object(row["payload"], "order_request"), row["normalized"]
    _require(all(_need(payload, key, scope) == expected for key, expected in
                 (("orderType", "MARKET"), ("session", "NORMAL"), ("duration", "DAY"), ("orderStrategyType", "SINGLE"))),
             "ORDER_REQUEST_ADAPTER_UNAVAILABLE", scope, blocked=True)
    legs = _need(payload, "orderLegCollection", scope)
    _require(type(legs) is list and len(legs) == 1, "ORDER_REQUEST_ADAPTER_UNAVAILABLE", scope, blocked=True)
    leg = _object(legs[0], scope)
    _require(_need(leg, "quantityType", scope) == "DOLLARS", "QUANTITY_UNIT_UNSUPPORTED", scope, blocked=True)
    _require(_need(leg, "instruction", scope) == "BUY", "ORDER_REQUEST_ADAPTER_UNAVAILABLE", scope, blocked=True)
    instrument = _object(_need(leg, "instrument", scope), scope)
    _require(_need(instrument, "assetType", scope) == "EQUITY", "ORDER_REQUEST_ADAPTER_UNAVAILABLE", scope, blocked=True)
    symbol = _text(_need(instrument, "symbol", scope), scope)
    quantity = _quantity(_need(leg, "quantity", scope), scope, native=True)
    _require(quantity >= 1 and quantity.as_tuple().exponent >= -2, "DOLLAR_REQUEST_FIELD_CONFLICT", scope)
    _require(_need(normalized, "quantity_unit", scope) == "DOLLARS", "QUANTITY_UNIT_MISMATCH", scope)
    _require(_quantity(_need(normalized, "quantity", scope), scope) == quantity, "QUANTITY_MISMATCH", scope)
    _require(_need(normalized, "symbol", scope) == symbol and _need(normalized, "side", scope) == "buy",
             "ORDER_FIELDS_MISMATCH", scope)
    return {"quantity": quantity, "unit": "DOLLARS", "symbol": symbol, "side": "buy"}


def _response(row, binding):
    scope, payload, normalized = "submission_response", _object(row["payload"], "submission_response"), row["normalized"]
    raw = _object(_need(payload, "raw_payload", scope), scope)
    code = _need(raw, "status_code", scope)
    _require(code is None or type(code) is int, "INVALID_HTTP_STATUS", scope)
    source_status = _need(payload, "status", scope)
    declared = _need(normalized, "outcome", scope)
    released = _need(normalized, "reservation_released", scope)
    _require(type(released) is bool, "INVALID_RESERVATION_CLAIM", scope)
    order_id = payload.get("broker_order_id")
    if order_id is not None:
        order_id = _order_id(order_id, scope)
    confirmed = "unknown"
    if code in (200, 201) and order_id is not None:
        confirmed = "accepted"
    elif code is not None and 400 <= code < 500 and code != 408:
        confirmed = "rejected"
    _require(not (confirmed == "accepted" and declared in ("filled", "partially_filled")), "ACK_IS_NOT_FILL", scope)
    _require(source_status == confirmed and declared == confirmed, "OUTCOME_CONTRADICTION", scope)
    _require(not (confirmed == "unknown" and released), "UNKNOWN_RESERVATION_RELEASE", scope)
    _require(not (confirmed == "accepted" and released), "ACK_RESERVATION_RELEASE_UNPROVEN", scope, blocked=True)
    _require(confirmed != "unknown", "SUBMISSION_OUTCOME_UNKNOWN", scope, blocked=True)
    _require(confirmed == "accepted", "REJECTION_RECONCILIATION_NOT_CHECKED", scope, blocked=True)
    _require(_order_id(_need(normalized, "broker_order_id", scope), scope) == order_id, "ORDER_ID_MISMATCH", scope)
    quantity = _quantity(_need(payload, "quantity", scope), scope, native=True)
    _require(_quantity(_need(normalized, "quantity", scope), scope) == quantity, "QUANTITY_MISMATCH", scope)
    unit = _need(normalized, "quantity_unit", scope)
    _require(unit in ("DOLLARS", "shares"), "QUANTITY_UNIT_UNSUPPORTED", scope, blocked=True)
    return {"order_id": order_id, "quantity": quantity, "unit": unit,
            "symbol": _text(_need(payload, "symbol", scope), scope), "side": _need(payload, "side", scope)}


def _submitted(row, binding):
    scope, payload, normalized = "submitted_orders", row["payload"], row["normalized"]
    _require(type(payload) is list, "INVALID_STRUCTURE", scope)
    _require(bool(payload), "SUBMITTED_ORDER_COVERAGE_UNKNOWN", scope, blocked=True)
    orders = {}
    for item in payload:
        item = _object(item, scope)
        _require(_need(item, "status", scope) == "accepted", "SUBMITTED_ORDER_COVERAGE_UNKNOWN", scope, blocked=True)
        order_id = _order_id(_need(item, "broker_order_id", scope), scope)
        _require(order_id not in orders, "DUPLICATE_ORDER_ID", scope)
        unit = "DOLLARS" if "notional_usd" in item else "shares"
        quantity = _quantity(_need(item, "quantity", scope), scope, native=True)
        sent = _quantity(item["notional_usd"], scope, native=True) if unit == "DOLLARS" else quantity
        orders[order_id] = {"order_id": order_id, "unit": unit, "quantity": quantity, "transmitted_quantity": sent,
                            "symbol": _text(_need(item, "symbol", scope), scope), "side": _need(item, "side", scope)}
    selected_id = _order_id(_need(normalized, "broker_order_id", scope), scope)
    _require(selected_id in orders, "ORDER_ID_MISMATCH", scope)
    selected = orders[selected_id]
    _require(_need(normalized, "quantity_unit", scope) == selected["unit"], "QUANTITY_UNIT_MISMATCH", scope)
    _require(_quantity(_need(normalized, "quantity", scope), scope) == selected["quantity"], "QUANTITY_MISMATCH", scope)
    return selected


def _state(row, binding):
    scope, payload = "daily_state_transition", _object(row["payload"], "daily_state_transition")
    _schema(payload, "strategy_risk_state_transition.v1", scope)
    _require(binding["strategy_profile"] == "tqqq_growth_income", "STATE_ADAPTER_UNAVAILABLE", scope, blocked=True)
    _exact_fields(payload, {"schema_version", "identity", "effective_session", "input_sha256",
                            "previous_transition_sha256", "state", "transition_sha256"}, scope)
    identity = _object(payload["identity"], scope)
    _exact_fields(identity, {"strategy_profile", "account_scope", "candidate_id", "config_sha256"}, scope)
    for key in ("strategy_profile", "account_scope", "candidate_id"):
        _require(identity[key] == _need(binding, key, scope), "STATE_IDENTITY_MISMATCH", scope)
        _require(type(identity[key]) is str and re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", identity[key]) is not None,
                 "INVALID_STATE_IDENTITY", scope)
    _require(identity["config_sha256"] == binding["release"]["config_sha256"], "STATE_IDENTITY_MISMATCH", scope)
    _require(_hash(payload["input_sha256"], scope) == _hash(_need(binding, "input_sha256", scope), scope),
             "STATE_INPUT_MISMATCH", scope)
    if payload["previous_transition_sha256"] is not None:
        _hash(payload["previous_transition_sha256"], scope)
    session = _session(payload["effective_session"], scope)
    state = _object(payload["state"], scope)
    _schema(state, "tqqq_daily_state.v1", scope)
    state_fields = {"schema_version", "session", "alpha_trend_active", "alpha_epoch",
                    "volatility_latch_active", "pending_intent", "intent_invalidation_reason"}
    _require(state_fields <= set(state), "MISSING_EVIDENCE", scope, blocked=True)
    _require(set(state) == state_fields, "INVALID_STATE_FIELD", scope)
    _require(_need(state, "session", scope) == session, "STATE_SESSION_MISMATCH", scope)
    _require(len(_canonical(state).encode()) <= 8192, "STATE_SIZE_LIMIT_EXCEEDED", scope)
    for key in ("alpha_trend_active", "volatility_latch_active"):
        _require(type(_need(state, key, scope)) is bool, "INVALID_STATE_FIELD", scope)
    epoch = _need(state, "alpha_epoch", scope)
    _require(type(epoch) is int and epoch >= (1 if state["alpha_trend_active"] else 0), "INVALID_STATE_FIELD", scope)
    intent = state["pending_intent"]
    if intent is not None:
        _require(type(intent) is dict and set(intent) == {"id", "alpha_epoch", "signal_session"},
                 "INVALID_STATE_FIELD", scope)
        _require(type(intent["id"]) is str and 0 < len(intent["id"]) <= 128
                 and type(intent["alpha_epoch"]) is int and intent["alpha_epoch"] == epoch
                 and intent["signal_session"] == session and state["alpha_trend_active"]
                 and not state["volatility_latch_active"], "INVALID_STATE_FIELD", scope)
    _require(state["intent_invalidation_reason"] in (None, "alpha_exit", "new_session", "risk_latched"),
             "INVALID_STATE_FIELD", scope)
    _require(_hash(payload["transition_sha256"], scope) == _digest({key: value for key, value in payload.items()
                                                                 if key != "transition_sha256"}),
             "NATIVE_DIGEST_MISMATCH", scope)


_CHECKERS = dict(zip(_KINDS, (_runtime, _receipt, _request, _response, _submitted, _state)))


def _cross_sources(records, parsed, binding):
    runtime = records.get("runtime_report")
    _require(runtime is not None, "MISSING_RUNTIME_ANCHOR", "association", blocked=True)
    for row in records.values():
        _require(row["runtime_source_id"] == runtime["source_id"], "RUNTIME_REFERENCE_MISMATCH", "association")
    embedded = _object(runtime["payload"], "runtime_report").get("execution_receipt")
    if embedded is not None and "execution_receipt" in records:
        _require(embedded == records["execution_receipt"]["payload"], "EMBEDDED_RECEIPT_MISMATCH", "association")
    if "runtime_report" in parsed:
        times = parsed["runtime_report"]
        for kind in ("order_request", "submission_response", "submitted_orders"):
            if kind in records:
                _require(times["decision"] <= records[kind]["event"] <= times["finish"],
                         "ORDER_EVENT_SEQUENCE_CONFLICT", "association")
    order_kinds = ("order_request", "submission_response", "submitted_orders")
    if any(kind in records for kind in order_kinds):
        _require(all(kind in records for kind in order_kinds), "ORDER_SOURCE_MISSING", "association", blocked=True)
        _require(records["order_request"]["event"] <= records["submission_response"]["event"]
                 <= records["submitted_orders"]["event"], "ORDER_EVENT_SEQUENCE_CONFLICT", "association")
        _require(not binding["dry_run"] and not parsed.get("runtime_report", {}).get("target_dry_run"),
                 "DRY_RUN_IS_NOT_BROKER_EVIDENCE", "association", blocked=True)
    if all(kind in parsed for kind in order_kinds):
        request, response, submitted = (parsed[kind] for kind in order_kinds)
        _require(response["order_id"] == submitted["order_id"], "CROSS_SOURCE_ORDER_ID_MISMATCH", "association")
        _require(request["unit"] == response["unit"] == submitted["unit"], "QUANTITY_UNIT_MISMATCH", "association")
        _require(request["quantity"] == submitted["transmitted_quantity"]
                 and response["quantity"] == submitted["quantity"], "CROSS_SOURCE_QUANTITY_MISMATCH", "association")
        _require(request["symbol"] == response["symbol"] == submitted["symbol"]
                 and request["side"] == response["side"] == submitted["side"], "CROSS_SOURCE_ORDER_FIELDS_MISMATCH", "association")
    if "runtime_report" in parsed and "execution_receipt" in records:
        receipt_time = records["execution_receipt"]["event"]
        _require(parsed["runtime_report"]["decision"] <= receipt_time, "RECEIPT_CLOCK_SEQUENCE_CONFLICT", "association")
        _require(receipt_time <= parsed["runtime_report"]["finish"],
                 "RECEIPT_TIMING_ASSOCIATION_UNVERIFIED", "association", blocked=True)


def _redaction(document):
    row = _object(_need(document, "redaction", "redaction"), "redaction")
    state = _need(row, "original_order_id_state", "redaction")
    _require(state != "invalid", "ORIGINAL_ORDER_ID_INVALID", "redaction")
    _require(state in ("unknown", "canonical"), "ORIGINAL_ID_OBSERVATION_UNSUPPORTED", "redaction", blocked=True)
    representation = _need(row, "order_id_representation", "redaction")
    _require(representation in ("synthetic", "preserved"), "ORIGINAL_ORDER_ID_UNVERIFIED", "redaction", blocked=True)


def validate_source_evidence(document) -> dict:
    """Return bounded observations; no result grants execution or replay acceptance.

    This API accepts already decoded JSON. It never fetches declared locators or
    source hashes, and never mutates the supplied document. CONSISTENT describes
    the supported supplied fields only, including one selected submitted order.
    """
    result = {"schema_version": SCHEMA, "status": "BLOCKED", "local_consistency": "BLOCKED",
              "execution_authorized": False, "production_ready": False, "historical_parity_established": False,
              "source_authentication": "UNVERIFIED", "findings": [], "checked_kinds": [], "unresolved": list(_UNRESOLVED)}

    def observe(operation):
        try:
            return True, operation()
        except _Problem as error:
            result["findings"].append({"code": error.code, "scope": error.scope,
                                       "status": "BLOCKED" if error.blocked else "FAIL"})
            return False, None

    def inspect():
        _shape(document)
        _require(_need(document, "schema_version", "document") == SCHEMA, "DOCUMENT_SCHEMA_UNSUPPORTED", "document", blocked=True)
        binding = _binding(document)
        hashes = _object(_need(document, "source_hashes", "document"), "document")
        supplied = _need(document, "records", "document")
        _require(type(supplied) is list and len(supplied) <= 32, "INVALID_STRUCTURE", "document")
        records, parsed, source_ids = {}, {}, set()
        observe(lambda: _redaction(document))
        for supplied_row in supplied:
            def inspect_row():
                row = _object(supplied_row, "record")
                kind = _need(row, "kind", "record")
                _require(type(kind) is str and kind in _KINDS, "SOURCE_KIND_UNSUPPORTED", "record", blocked=True)
                _require(kind not in records, "DUPLICATE_SOURCE_KIND", "record")
                value = _envelope(row, kind, binding, hashes)
                _require(value["source_id"] not in source_ids, "DUPLICATE_SOURCE_ID", "record")
                source_ids.add(value["source_id"])
                records[kind] = value
                parsed[kind] = _CHECKERS[kind](value, binding)
                result["checked_kinds"].append(kind)
            observe(inspect_row)
        observe(lambda: _cross_sources(records, parsed, binding))

    try:
        observe(inspect)
    except (ValueError, TypeError, OverflowError, RecursionError):
        result["findings"].append({"code": "INVALID_STRUCTURE", "scope": "document", "status": "FAIL"})
    if any(item["status"] == "FAIL" for item in result["findings"]):
        result["status"] = result["local_consistency"] = "FAIL"
    elif not result["findings"]:
        result["local_consistency"] = "CONSISTENT"
    return result
