"""Private recovery I/O and reported-position decoding for the Schwab adopter.

The optional caller session requires explicit qualified new-cohort enrollment,
restores exact original references, and fences native attempts before transport.
It cannot resolve prior attempts or establish cloud/broker qualification. A caller
must supply its existing qualified client; no provider/default-auth factory is used.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any


class _LocatorStateError(RuntimeError):
    def __init__(self, reason: str, *, status_code: int | None = None):
        super().__init__(reason)
        self.reason = reason
        self.status_code = status_code


class _LocatorConflict(_LocatorStateError):
    pass


class _LocatorUnavailable(_LocatorStateError):
    pass


class _LocatorUncertain(_LocatorStateError):
    pass


class _EvidenceRequired(ValueError):
    pass


@dataclass(frozen=True)
class _RecoveryScope:
    account_hash: str
    profile_id: str
    release_id: str
    cohort_id: str

    def __post_init__(self):
        if any(not isinstance(value, str) or not value.strip() for value in asdict(self).values()):
            raise ValueError("recovery scope requires explicit nonempty identities")


@dataclass(frozen=True)
class _LocatorVersion:
    generation: int
    payload_json: str
    payload_sha256: str
    locator_uri: str
    scope: _RecoveryScope


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate recovery JSON key")
        result[key] = value
    return result


def _reject_constant(_value):
    raise ValueError("nonfinite recovery JSON value")


def _object_json(text: str) -> dict:
    if not isinstance(text, str) or not text.strip():
        raise ValueError("recovery payload must be a nonempty JSON object")
    value = json.loads(text, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    if not isinstance(value, dict):
        raise ValueError("recovery payload must be a JSON object")
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _generation(value: object) -> int:
    if isinstance(value, str) and value.isascii() and value.isdigit():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("positive object generation unavailable")
    return value


class _RecoveryLocatorStore:
    """Conditional operations on one fixed, scope-bound recovery locator.

    The explicit initialization method is only for a positively enrolled new
    managed cohort. A failed registered read never invokes it. Payload identity,
    original-descriptor validation and attempt authority belong to the future
    caller; reading matching bytes does not clear a prior uncertain attempt.
    A consistent versioned read does not promise that another writer has not
    advanced the live name; compare_update still requires its exact version.
    """
    _SCHEMA = "schwab_recovery_locator.v1"

    def __init__(self, client: Any, *, locator_uri: str, scope: _RecoveryScope):
        if not isinstance(locator_uri, str) or not locator_uri.startswith("gs://"):
            raise ValueError("explicit existing gs locator required")
        bucket, separator, name = locator_uri[5:].partition("/")
        if not bucket or not separator or not name or not isinstance(scope, _RecoveryScope):
            raise ValueError("fixed locator and recovery scope required")
        self._client = client
        self._uri = locator_uri
        self._bucket = bucket
        self._name = name
        self._scope = scope

    def _blob(self, *, reading: bool):
        # A fresh Blob per operation avoids sharing mutable SDK generation state.
        try:
            blob = self._client.bucket(self._bucket).blob(self._name)
        except Exception as exc:
            self._io_failure(exc, reading=reading)
        if getattr(blob, "name", None) != self._name or getattr(getattr(blob, "bucket", None), "name", None) != self._bucket:
            raise _LocatorUncertain("locator_identity_mismatch")
        return blob

    @staticmethod
    def _io_failure(exc: Exception, *, reading: bool):
        code = getattr(exc, "code", None)
        if isinstance(code, bool) or not isinstance(code, int):
            code = None
        if code in {409, 412}:
            raise _LocatorConflict("conditional_version_conflict", status_code=code) from None
        if reading and code == 404:
            raise _LocatorUnavailable("registered_locator_missing", status_code=code) from None
        # A 403 does not establish whether permissions or retention caused it.
        # Timeouts/5xx may follow a committed write; do not retry or report absent.
        raise _LocatorUncertain("read_uncertain" if reading else "write_uncertain", status_code=code) from None

    def _encode(self, payload_json: str) -> tuple[str, str, str]:
        canonical = _canonical_json(_object_json(payload_json))
        digest = _digest(canonical)
        envelope = _canonical_json({"schema_version": self._SCHEMA, "scope": asdict(self._scope),
                                    "payload_json": canonical, "payload_sha256": digest})
        return envelope, canonical, digest

    def _decode(self, text: str, generation: int) -> _LocatorVersion:
        try:
            envelope = _object_json(text)
            if (set(envelope) != {"schema_version", "scope", "payload_json", "payload_sha256"}
                    or envelope["schema_version"] != self._SCHEMA or envelope["scope"] != asdict(self._scope)):
                raise ValueError("locator envelope scope/version mismatch")
            canonical = _canonical_json(_object_json(envelope["payload_json"]))
            digest = _digest(canonical)
            if envelope["payload_json"] != canonical or envelope["payload_sha256"] != digest:
                raise ValueError("locator payload digest mismatch")
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise _LocatorUncertain("registered_locator_invalid") from None
        return _LocatorVersion(generation, canonical, digest, self._uri, self._scope)

    def read_registered(self) -> _LocatorVersion:
        blob = self._blob(reading=True)
        try:
            blob.reload(retry=None)
        except Exception as exc:
            self._io_failure(exc, reading=True)
        try:
            generation = _generation(blob.generation)
        except (ValueError, OverflowError):
            raise _LocatorUncertain("read_generation_unavailable") from None
        try:
            text = blob.download_as_text(if_generation_match=generation, retry=None)
        except Exception as exc:
            self._io_failure(exc, reading=True)
        return self._decode(text, generation)

    def _write(self, payload_json: str, expected_generation: int) -> _LocatorVersion:
        envelope, canonical, digest = self._encode(payload_json)
        blob = self._blob(reading=False)
        try:
            blob.upload_from_string(envelope, content_type="application/json", if_generation_match=expected_generation, retry=None)
        except Exception as exc:
            self._io_failure(exc, reading=False)
        try:
            acknowledged_generation = _generation(blob.generation)
            if acknowledged_generation == expected_generation:
                raise ValueError("no new version bound to write acknowledgement")
        except (ValueError, OverflowError):
            raise _LocatorUncertain("write_generation_unavailable") from None
        return _LocatorVersion(acknowledged_generation, canonical, digest, self._uri, self._scope)

    def initialize_new_cohort(self, payload_json: str) -> _LocatorVersion:
        return self._write(payload_json, 0)

    def compare_update(self, payload_json: str, *, expected: _LocatorVersion) -> _LocatorVersion:
        if (not isinstance(expected, _LocatorVersion) or isinstance(expected.generation, bool)
                or not isinstance(expected.generation, int) or expected.generation <= 0):
            raise ValueError("CAS requires a positive version from a qualified read or write acknowledgement")
        if (expected.locator_uri != self._uri or expected.scope != self._scope
                or not isinstance(expected.payload_json, str) or _digest(expected.payload_json) != expected.payload_sha256):
            raise ValueError("CAS version must bind the same locator, scope and payload")
        return self._write(payload_json, expected.generation)


@dataclass(frozen=True)
class _PositionQuantities:
    long_quantities: Mapping[str, float]
    short_quantities: Mapping[str, float]
    unavailable_fields: Mapping[str, tuple[str, ...]]
    source_digest_sha256: str
    observation_started_at: datetime
    observation_completed_at: datetime


def _aware(value: object) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise _EvidenceRequired("aware_client_observation_window_required")
    return value


def _numeric_quantity(text: object) -> float | None:
    if not isinstance(text, str) or not text or len(text) > 128:
        return None
    try:
        value = Decimal(text)
        if not value.is_finite() or value < 0 or str(value) != text:
            return None
        number = float(value)
        if not math.isfinite(number) or (value != 0 and number == 0):
            return None
        # Preserve the decimal value exposed by the native float contract;
        # reject silent precision loss rather than round a resource fact.
        if Decimal(str(number)) != value:
            return None
        return number
    except (InvalidOperation, ValueError, OverflowError):
        return None


def _read_schwab_position_quantities(
    snapshot: Any, *, expected_account_hash: str, symbols: Sequence[str],
    window_start: datetime, window_end: datetime,
) -> _PositionQuantities:
    """Decode reported long/short facts, never sellable or broker atomicity.

    Bounds are supplied by the caller's existing clock/freshness contract. No
    new freshness duration or fallback to Position.quantity is introduced.
    Invalid fields remain unknown for that symbol; untrusted bundle provenance
    prevents consumption of the requested scope, not an account-wide policy.
    """
    if (not isinstance(expected_account_hash, str) or not expected_account_hash.strip()
            or isinstance(symbols, (str, bytes))):
        raise _EvidenceRequired("explicit_account_and_symbol_scope_required")
    try:
        requested = tuple(symbols)
    except TypeError:
        raise _EvidenceRequired("explicit_account_and_symbol_scope_required") from None
    if (not requested or any(not isinstance(s, str) or not s or s != s.strip().upper() for s in requested)
            or len(set(requested)) != len(requested)):
        raise _EvidenceRequired("explicit_account_and_symbol_scope_required")
    lower, upper = _aware(window_start), _aware(window_end)
    metadata = getattr(snapshot, "metadata", None)
    if not isinstance(metadata, Mapping) or metadata.get("account_hash") != expected_account_hash:
        raise _EvidenceRequired("position_account_binding_unavailable")
    facts = metadata.get("schwab_position_quantities")
    if not isinstance(facts, Mapping):
        raise _EvidenceRequired("position_fact_bundle_unavailable")
    digest = metadata.get("source_digest_sha256")
    if (not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            or facts.get("source_digest_sha256") != digest):
        raise _EvidenceRequired("position_source_binding_unavailable")
    if (facts.get("positions_field_present") is not True or facts.get("observation_window_valid") is not True
            or facts.get("time_semantics") != "client_observation_window" or facts.get("orders_observed") is not False):
        raise _EvidenceRequired("position_observation_qualification_unavailable")
    try:
        started = _aware(datetime.fromisoformat(facts["observation_started_at"]))
        completed = _aware(datetime.fromisoformat(facts["observation_completed_at"]))
        if not lower <= started <= completed <= upper or _aware(snapshot.as_of) != completed:
            raise _EvidenceRequired("position_observation_outside_bound_window")
    except (KeyError, ValueError, TypeError, AttributeError):
        raise _EvidenceRequired("position_observation_window_unavailable") from None
    rows = facts.get("records")
    long_map, short_map = facts.get("long_quantities"), facts.get("short_quantities")
    ambiguous = facts.get("ambiguous_symbols")
    if (not isinstance(rows, list) or not isinstance(long_map, Mapping) or not isinstance(short_map, Mapping)
            or not isinstance(ambiguous, list) or any(not isinstance(s, str) for s in ambiguous)):
        raise _EvidenceRequired("position_fact_structure_unavailable")
    grouped: dict[str, list[Mapping]] = {s: [] for s in requested}
    for row in rows:
        if not isinstance(row, Mapping) or not isinstance(row.get("symbol"), str):
            raise _EvidenceRequired("position_record_scope_unavailable")
        if row["symbol"] in grouped:
            grouped[row["symbol"]].append(row)
    longs, shorts, unavailable = {}, {}, {}
    for symbol in requested:
        records = grouped[symbol]
        if len(records) != 1 or symbol in ambiguous:
            unavailable[symbol] = ("position_ambiguous" if len(records) > 1 or symbol in ambiguous else "position_unobserved",)
            continue
        row = records[0]
        reported, omitted = row.get("reported_quantities"), row.get("unavailable_fields")
        if row.get("asset_type") != "EQUITY" or not isinstance(reported, Mapping) or not isinstance(omitted, Mapping):
            unavailable[symbol] = ("position_units_or_record_unavailable",)
            continue
        reasons = []
        for field, raw_map, target in (("longQuantity", long_map, longs), ("shortQuantity", short_map, shorts)):
            if field in omitted or field not in reported or symbol not in raw_map:
                reasons.append(field + ":unavailable")
                continue
            text = reported[field]
            if not isinstance(raw_map[symbol], str) or raw_map[symbol] != text:
                reasons.append(field + ":contract_mismatch")
                continue
            quantity = _numeric_quantity(text)
            if quantity is None:
                reasons.append(field + ":invalid_or_inexact")
                continue
            target[symbol] = quantity
        if reasons:
            unavailable[symbol] = tuple(reasons)
    return _PositionQuantities(MappingProxyType(longs), MappingProxyType(shorts), MappingProxyType(unavailable),
                               digest, started, completed)


# Adopter-private caller boundary. Selection is explicit; no environment flag,
# provider factory, legacy migration, or resolution authority lives here.
from collections.abc import Callable
from zoneinfo import ZoneInfo

from quant_platform_kit.common.execution_commands import ExecutionCommand, ExecutionCommandStore, ExecutionCommandState
from quant_platform_kit.common.models import OrderIntent
from quant_platform_kit.common.reduce_only_command_admission import (
    build_reduce_only_order_digest, evaluate_reduce_only_command_admission,
)
from quant_platform_kit.common.strategy_release import validate_runtime_loaded_receipt


class _ScopedCommandObjectStore:
    """Known native command/event paths on an explicitly supplied client only."""
    def __init__(self, client, *, prefix_uri: str):
        if (not isinstance(prefix_uri, str) or not re.fullmatch(r"gs://[^/\s?#*]+/[^\s?#*]+", prefix_uri)
                or any(part in {"", ".", ".."} for part in prefix_uri[5:].split("/"))):
            raise ValueError("fixed command prefix required")
        self.client = client
        self.prefix_uri = prefix_uri.rstrip("/")

    def _parts(self, uri):
        if not isinstance(uri, str) or not uri.startswith(self.prefix_uri + "/execution_commands/execution_command.v1/"):
            raise _EvidenceRequired("command_path_outside_scope")
        suffix = uri[len(self.prefix_uri):]
        if not re.fullmatch(r"/execution_commands/execution_command.v1/\d{4}-\d{2}-\d{2}/cmd-[a-f0-9]{32}/(?:command\.json|events(?:/\d{8}\.json)?)", suffix):
            raise _EvidenceRequired("command_path_invalid")
        return uri[5:].split("/", 1)

    def _blob(self, uri):
        bucket, key = self._parts(uri)
        blob = self.client.bucket(bucket).blob(key)
        if blob.name != key or blob.bucket.name != bucket:
            raise _EvidenceRequired("command_path_identity_mismatch")
        return blob

    def create_text(self, uri, data, content_type):
        blob = self._blob(uri)
        try:
            blob.upload_from_string(data, content_type=content_type, if_generation_match=0, retry=None)
        except Exception as exc:
            if getattr(exc, "code", None) in {409, 412}:
                return False
            raise _LocatorUncertain("command_create_uncertain") from None
        try:
            _generation(blob.generation)
        except ValueError:
            raise _LocatorUncertain("command_create_ack_unqualified") from None
        return True

    def read_text(self, uri):
        blob = self._blob(uri)
        try:
            blob.reload(retry=None)
            generation = _generation(blob.generation)
            return blob.download_as_text(if_generation_match=generation, retry=None)
        except Exception:
            raise _LocatorUncertain("command_read_uncertain") from None

    def list(self, uri):
        bucket, key = self._parts(uri)
        if not uri.endswith("/events"):
            raise _EvidenceRequired("only_known_command_event_listing_allowed")
        try:
            listing = self.client.list_blobs(bucket, prefix=key + "/", retry=None)
            # Materialize *all* pages before exposing even an empty history.
            blobs = tuple(listing)
            if getattr(listing, "next_page_token", None):
                raise ValueError("incomplete listing")
            paths = tuple("gs://" + bucket + "/" + blob.name for blob in blobs)
            if len(set(paths)) != len(paths):
                raise ValueError("duplicate listing")
            for path in paths:
                self._parts(path)
                if not path.startswith(uri + "/"):
                    raise ValueError("listing escaped prefix")
            return paths
        except Exception:
            raise _LocatorUncertain("command_events_incomplete") from None


@dataclass(frozen=True)
class _IntentContinuityContext:
    scope: _RecoveryScope
    enrollment: Mapping
    locator: _RecoveryLocatorStore
    command_store: ExecutionCommandStore
    consumption_clock: Callable[[], datetime]
    session_timezone: str
    runtime_target: Any = None
    runtime_release_receipt: Mapping | None = None
    resource_facts: Mapping | None = None
    # These are separately qualified immutable source references and exact
    # bytes supplied by the adopter. They are not an authentication mechanism,
    # a broker parser, a provider factory, or a production source qualifier.
    resolution_sources: Mapping[str, str] | None = None
    resolution_receipts: Mapping[str, str] | None = None


@dataclass(frozen=True)
class _IntentTicket:
    command: ExecutionCommand


def _resolution_time(value):
    if not isinstance(value, str):
        raise _EvidenceRequired("resolution_time_missing")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None or result.utcoffset() is None:
        raise _EvidenceRequired("resolution_time_naive")
    return result


def _validated_original_resolution(command, ref, body, *, scope, enrollment_sha256,
        strategy_release, snapshot_sha256, snapshot_as_of, now, source_uri, proof_sha256,
        known_order_ids, attempted, prior_binding):
    """Validate one qualified source's normalized existing-ledger facts only.

    No ledger/state transition is applied. The source-byte digest must already
    be qualified outside this module; matching data cannot qualify itself.
    Settlement/cash/sellable facts are never synthesized from this view.
    """
    from application.execution_event_accounting import (
        ExecutionEventLedger, _TERMINAL_STATUSES, _decimal, _decimal_text, _json_digest, _money,
    )
    expected_keys = {"schema_version", "scope", "enrollment_sha256", "strategy_release",
        "original_ref", "original_request_sha256", "identity_binding", "broker_requested_quantity",
        "broker_requested_notional", "terminal_observed_at", "coverage", "observed_at",
        "current_snapshot_sha256", "ledger"}
    if (set(body) != expected_keys or body["schema_version"] != "schwab_original_resolution.v1"
            or not attempted or body["scope"] != asdict(scope)
            or body["enrollment_sha256"] != enrollment_sha256
            or body["strategy_release"] != strategy_release.to_dict()
            or body["original_ref"] != ref or body["current_snapshot_sha256"] != snapshot_sha256):
        raise _EvidenceRequired("resolution_original_or_source_mismatch")
    order = command.intent["order"]
    if body["original_request_sha256"] != _digest(_canonical_json(order)):
        raise _EvidenceRequired("resolution_request_mismatch")
    binding = body["identity_binding"]
    if (set(binding) != {"command_id", "order_id", "method"}
            or binding["command_id"] != command.command_id
            or binding["method"] != "exact_persisted_source_binding"
            or not isinstance(binding["order_id"], str) or not binding["order_id"].strip()
            or any(order_id != binding["order_id"] for order_id in known_order_ids)):
        raise _EvidenceRequired("resolution_exact_broker_identity_required")
    broker_id = binding["order_id"]
    observed = _resolution_time(body["observed_at"])
    terminal_at = _resolution_time(body["terminal_observed_at"])
    coverage = body["coverage"]
    if (set(coverage) != {"from", "through", "orders_complete", "executions_complete", "fees_complete", "corrections_complete"}
            or any(coverage[key] is not True for key in ("orders_complete", "executions_complete", "fees_complete", "corrections_complete"))
            or observed != snapshot_as_of or observed > now
            or _resolution_time(coverage["through"]) != observed
            or _resolution_time(coverage["from"]) > _resolution_time(command.created_at)
            or not _resolution_time(command.created_at) <= terminal_at <= observed):
        raise _EvidenceRequired("resolution_current_complete_coverage_required")
    requested = _decimal(body["broker_requested_quantity"], "broker_requested_quantity", positive=True)
    if _decimal(order["quantity"], "original_quantity", nonnegative=True) > 0:
        if requested != _decimal(order["quantity"], "original_quantity") or body["broker_requested_notional"] is not None:
            raise _EvidenceRequired("resolution_requested_quantity_mismatch")
    else:
        # The existing ledger is share-quantity based. No qualified notional
        # request-to-terminal-quantity source contract is adopted here; never
        # manufacture a share quantity or budget rule for quantity=0 intents.
        raise _EvidenceRequired("resolution_notional_source_contract_unavailable")
    ledger = body["ledger"]
    conflict_lists = ("unsupported_event_ids", "quarantined_order_ids", "quarantined_event_ids")
    # Even a zero-fill terminal needs explicit known-empty coverage. Missing
    # collections, integer zero and empty objects are unknown, not empty lists.
    if (not isinstance(ledger, dict)
            or any(not isinstance(ledger.get(key), list) for key in (*conflict_lists, "events"))
            or any(not isinstance(item, str) or not item.strip()
                   for key in conflict_lists for item in ledger[key])
            or ledger["schema_version"] != ExecutionEventLedger.schema_version
            or ledger["unsupported_event_ids"]):
        raise _EvidenceRequired("resolution_ledger_unsupported")
    intents, orders = ledger["intents"], ledger["orders"]
    if (not isinstance(intents, dict) or not isinstance(orders, dict)
            or any(not isinstance(row, dict) for row in (*intents.values(), *orders.values()))):
        raise _EvidenceRequired("resolution_identity_component_unknown")
    # Check both directions inside this supplied original-related component.
    # An active alias or duplicate fact under another key cannot disappear by
    # selecting the one terminal row. Unrelated correctly bound rows may remain.
    intent_keys = {key for key, row in intents.items() if key == command.command_id
                   or row.get("intent_id") == command.command_id or row.get("order_id") == broker_id}
    order_keys = {key for key, row in orders.items() if key == broker_id
                  or row.get("intent_id") == command.command_id}
    if intent_keys != {command.command_id} or order_keys != {broker_id}:
        raise _EvidenceRequired("resolution_identity_component_conflict")
    intent = ledger["intents"][command.command_id]
    state = ledger["orders"][broker_id]
    if (intent["intent_id"] != command.command_id or intent["order_id"] != broker_id
            or intent["owner_id"] != scope.profile_id or intent["symbol"] != order["symbol"]
            or intent["side"] != order["side"].upper() or _decimal(intent["quantity"], "ledger_request") != requested
            or state["intent_id"] != command.command_id or state["owner_id"] != scope.profile_id
            or state["symbol"] != order["symbol"] or state["side"] != order["side"].upper()
            or _decimal(state["requested_quantity"], "state_request") != requested
            or state["status"] not in _TERMINAL_STATUSES or state["terminal"] is not True
            or state["reconciliation_status"] != "complete" or state["reservation_status"] != "released"
            or broker_id in ledger["quarantined_order_ids"]):
        raise _EvidenceRequired("resolution_terminal_binding_incomplete")
    cumulative = _decimal(state["cumulative_filled_quantity"], "terminal_cumulative", nonnegative=True)
    if cumulative > requested or (state["status"] == "FILLED" and cumulative != requested):
        raise _EvidenceRequired("resolution_terminal_quantity_mismatch")
    total = _decimal("0", "zero")
    total_fees = _decimal("0", "zero")
    owner_fees = {}
    ids = set()
    fills = []
    event_keys = {"event_id", "event_type", "order_id", "owner_id", "symbol", "side",
                  "quantity", "price", "fee", "fee_source", "event_time", "event_digest"}
    for event in ledger["events"]:
        event_id = event["event_id"]
        if not isinstance(event_id, str) or not event_id.strip() or event_id in ids:
            raise _EvidenceRequired("resolution_duplicate_execution")
        ids.add(event_id)
        # Reconcile normalized per-fill fees with the existing ledger's fee
        # summaries. This validates the source; it never credits buying power.
        event_fee = _decimal(event["fee"], "ledger_event_fee", nonnegative=True)
        if event_fee != _decimal(_money(event_fee), "normalized_fee"):
            raise _EvidenceRequired("resolution_fee_precision_mismatch")
        total_fees += event_fee
        owner_fees[event["owner_id"]] = owner_fees.get(event["owner_id"], _decimal("0", "zero")) + event_fee
        if event["order_id"] != broker_id:
            continue
        fee_source = event["fee_source"].lower().replace("-", "_").replace(" ", "_")
        if (set(event) != event_keys or event["event_type"] != "FILL"
                or event["owner_id"] != scope.profile_id or event["symbol"] != order["symbol"]
                or event["side"] != order["side"].upper()
                or event_id in ledger["quarantined_event_ids"]
                or any(token in fee_source for token in ("cumulative", "aggregate", "order_commission", "order_level"))
                or not any(token in fee_source for token in ("per_fill", "per_execution", "fill_level", "execution_level"))):
            raise _EvidenceRequired("resolution_fill_binding_or_fee_source_invalid")
        when = _resolution_time(event["event_time"])
        if not _resolution_time(command.created_at) <= when <= terminal_at:
            raise _EvidenceRequired("resolution_late_execution")
        quantity = _decimal(event["quantity"], "fill_quantity", positive=True)
        _decimal(event["price"], "fill_price", positive=True)
        fee = _decimal(event["fee"], "fill_fee", nonnegative=True)
        material = {key: value for key, value in event.items() if key != "event_digest"}
        if fee != _decimal(_money(fee), "normalized_fee") or event["event_digest"] != _json_digest(material):
            raise _EvidenceRequired("resolution_fee_or_execution_digest_mismatch")
        total += quantity
        fills.append(event)
    if (_decimal(ledger["account"]["fees"], "account_fees", nonnegative=True) != total_fees
            or not set(owner_fees).issubset(ledger["owners"])
            or any(_decimal(owner["fees"], "owner_fees", nonnegative=True) != owner_fees.get(owner_id, _decimal("0", "zero"))
                   for owner_id, owner in ledger["owners"].items())):
        raise _EvidenceRequired("resolution_fee_totals_inconsistent")
    if total != cumulative or _decimal(state["recorded_filled_quantity"], "recorded_quantity") != cumulative:
        raise _EvidenceRequired("resolution_execution_coverage_incomplete")
    economics = _digest(_canonical_json({"original_ref": ref, "identity_binding": binding,
        "original_request_sha256": body["original_request_sha256"],
        "requested_quantity": _decimal_text(requested), "requested_notional": (None if body["broker_requested_notional"] is None else _decimal_text(_decimal(body["broker_requested_notional"], "notional"))),
        "terminal_status": state["status"], "terminal_quantity": _decimal_text(cumulative),
        "fills": sorted(fills, key=lambda item: (item["event_time"], item["event_id"]))}))
    if prior_binding is not None and (prior_binding["economics_sha256"] != economics
            or observed < _resolution_time(prior_binding["observed_at"])
            or (observed == _resolution_time(prior_binding["observed_at"]) and prior_binding["proof_sha256"] != proof_sha256)):
        raise _EvidenceRequired("resolution_conflicts_with_persisted_economics")
    return {"descriptor_sha256": ref["descriptor_sha256"], "economics_sha256": economics,
            "source_uri": source_uri, "proof_sha256": proof_sha256, "observed_at": observed.isoformat()}


class _IntentSession:
    """One invocation, one bounded registration CAS; durable claims never clear."""
    def __init__(self, context, *, snapshot, profile_id):
        self.context = context
        self.fatal_reason = None
        self.reasons = []
        self.originals = []
        self.current = []
        self.version = None
        self.plan_sources = {}
        self.resolution_diagnostics = {}
        try:
            self._restore(snapshot, profile_id)
        except Exception:
            self.fatal_reason = "continuity_restore_evidence_required"
            self.reasons.append(self.fatal_reason)

    def _restore(self, snapshot, profile_id):
        c = self.context
        if not isinstance(c, _IntentContinuityContext):
            raise _EvidenceRequired("selected_context_invalid")
        scope = c.scope
        if not isinstance(scope, _RecoveryScope) or profile_id != scope.profile_id:
            raise _EvidenceRequired("profile_scope_mismatch")
        if snapshot.metadata.get("account_hash") != scope.account_hash:
            raise _EvidenceRequired("snapshot_route_mismatch")
        target = c.runtime_target
        enrollment = dict(c.enrollment)
        if (enrollment.get("schema_version") != "schwab_intent_enrollment.v1"
                or enrollment.get("scope") != asdict(scope)
                or not re.fullmatch("[a-f0-9]{64}", str(enrollment.get("authority_sha256") or ""))
                or enrollment.get("managed_writers") != "native_create_only.v1"
                or enrollment.get("complete_event_reads") is not True):
            raise _EvidenceRequired("positive_new_cohort_enrollment_required")
        # Original release remains enrolled even when a runtime release changes.
        # New candidate release must independently match its current receipt.
        from quant_platform_kit.common.strategy_release import StrategyReleaseIdentity
        self.original_release = StrategyReleaseIdentity(**enrollment["strategy_release"])
        if self.original_release.release_id != scope.release_id:
            raise _EvidenceRequired("enrolled_release_mismatch")
        if (target is None or target.strategy_profile != scope.profile_id or target.platform_id not in {"schwab", "charles_schwab"}
                or target.strategy_release is None or target.dry_run_only):
            raise _EvidenceRequired("current_runtime_binding_required")
        if not validate_runtime_loaded_receipt(c.runtime_release_receipt,
                expected_strategy_release=target.strategy_release, required=True).is_valid:
            raise _EvidenceRequired("current_release_receipt_required")
        if not target.market_timezone or c.session_timezone != target.market_timezone:
            raise _EvidenceRequired("runtime_consumption_timezone_mismatch")
        now = c.consumption_clock()
        if not isinstance(now, datetime) or now.tzinfo is None or not c.session_timezone:
            raise _EvidenceRequired("independent_current_clock_required")
        self.now = now
        self.session = now.astimezone(ZoneInfo(c.session_timezone)).date().isoformat()
        store = c.command_store
        driver = getattr(store, "object_store", None)
        if (not isinstance(store, ExecutionCommandStore) or not isinstance(driver, _ScopedCommandObjectStore)
                or store.cloud_prefix_uri != driver.prefix_uri or store.namespace != "execution_commands"
                or not isinstance(c.locator, _RecoveryLocatorStore) or c.locator._scope != scope
                or c.locator._client is not driver.client):
            raise _EvidenceRequired("explicit_scoped_native_store_required")
        snapshot_source = asdict(snapshot)
        snapshot_source["as_of"] = snapshot.as_of.isoformat()
        self.snapshot_sha256 = _digest(_canonical_json(snapshot_source))
        self.snapshot_as_of = snapshot.as_of
        self.resolution_sources = dict(c.resolution_sources) if isinstance(c.resolution_sources, Mapping) else None
        self.resolution_receipts = dict(c.resolution_receipts) if isinstance(c.resolution_receipts, Mapping) else None
        self.enrollment_sha256 = _digest(_canonical_json(enrollment))
        self.version = c.locator.read_registered()
        payload = self._payload(self.version)
        for ref in payload["references"]:
            command = self._read_reference(ref)
            events = store.events(command)
            self.originals.append((command, events))

    def register_plan_source(self, plan, snapshot):
        """Bind the exact planner input without changing original lineage."""
        if self.fatal_reason:
            return
        try:
            if snapshot.metadata.get("account_hash") != self.context.scope.account_hash:
                raise _EvidenceRequired("refreshed_snapshot_scope_mismatch")
            source = asdict(snapshot)
            source["as_of"] = snapshot.as_of.isoformat()
            digest = _digest(_canonical_json(source))
            prior = self.plan_sources.get(id(plan))
            if prior is not None and prior[1] != digest:
                raise _EvidenceRequired("reused_plan_has_ambiguous_source")
            # Retain the object itself, so an id cannot be reused in this call.
            self.plan_sources[id(plan)] = (plan, digest, snapshot.as_of)
        except Exception:
            self.fatal_reason = "continuity_plan_source_evidence_required"
            self.reasons.append(self.fatal_reason)

    def select_plan_source(self, plan):
        """Activate the chosen plan, including an earlier best refresh."""
        if self.fatal_reason:
            return
        bound = self.plan_sources.get(id(plan))
        if bound is None or bound[0] is not plan:
            self.fatal_reason = "continuity_plan_source_evidence_required"
            self.reasons.append(self.fatal_reason)
            return
        self.snapshot_sha256 = bound[1]
        self.snapshot_as_of = bound[2]

    def _payload(self, version):
        value = _object_json(version.payload_json)
        if (set(value) not in ({"enrollment_sha256", "references"}, {"enrollment_sha256", "references", "resolution_bindings"}) or value["enrollment_sha256"] != self.enrollment_sha256
                or not isinstance(value["references"], list)):
            raise _EvidenceRequired("registered_references_invalid")
        ids = [ref["command_id"] for ref in value["references"]]
        if len(set(ids)) != len(ids):
            raise _EvidenceRequired("duplicate_original_reference")
        anchors = value.get("resolution_bindings", {})
        if not isinstance(anchors, dict) or not set(anchors).issubset(ids):
            raise _EvidenceRequired("resolution_binding_scope_invalid")
        refs = {ref["command_id"]: ref for ref in value["references"]}
        for command_id, anchor in anchors.items():
            if (set(anchor) != {"descriptor_sha256", "economics_sha256", "source_uri", "proof_sha256", "observed_at"}
                    or anchor["descriptor_sha256"] != refs[command_id]["descriptor_sha256"]
                    or any(not re.fullmatch("[a-f0-9]{64}", str(anchor[key])) for key in ("economics_sha256", "proof_sha256"))
                    or not isinstance(anchor["source_uri"], str) or not anchor["source_uri"].strip()):
                raise _EvidenceRequired("resolution_binding_invalid")
            _resolution_time(anchor["observed_at"])
        return value

    def _resolved_originals(self):
        """Read-only proof view; old claims/events are never cleared or retried."""
        resolved = {}
        self.resolution_diagnostics = {}
        if self.resolution_sources is None and self.resolution_receipts is None:
            return resolved
        try:
            if (not isinstance(self.resolution_sources, dict) or not isinstance(self.resolution_receipts, dict)
                    or set(self.resolution_sources) != set(self.resolution_receipts)
                    or len(self.resolution_sources) > len(self.originals)):
                raise _EvidenceRequired("resolution_qualified_source_missing")
            payload = self._payload(self.version)
            refs = {ref["command_id"]: ref for ref in payload["references"]}
            originals = {command.command_id: (command, events) for command, events in self.originals}
            duplicates = set()
            # Bounded cohort facts already restored in this invocation, plus
            # validated supplied proofs. No external account lookup or index.
            broker_claims = {}
            for command, events in self.originals:
                for event in events:
                    broker_id = event.details.get("broker_order_id")
                    if broker_id:
                        broker_claims.setdefault(broker_id, set()).add(command.command_id)
            for uri, raw in self.resolution_receipts.items():
                if (not isinstance(uri, str) or not uri or len(uri) > 2048
                        or any(character.isspace() or character in "?#@" for character in uri)
                        or not isinstance(raw, str)
                        or self.resolution_sources[uri] != _digest(raw)):
                    raise _EvidenceRequired("resolution_source_bytes_unqualified")
                body = _object_json(raw)
                if raw != _canonical_json(body):
                    raise _EvidenceRequired("resolution_source_not_canonical")
                command_id = body["original_ref"]["command_id"]
                command, events = originals[command_id]
                try:
                    view = _validated_original_resolution(command, refs[command_id], body,
                        scope=self.context.scope, enrollment_sha256=self.enrollment_sha256,
                        strategy_release=self.original_release, snapshot_sha256=self.snapshot_sha256,
                        snapshot_as_of=self.snapshot_as_of, now=self.now, source_uri=uri,
                        proof_sha256=self.resolution_sources[uri],
                        known_order_ids={event.details.get("broker_order_id") for event in events if event.details.get("broker_order_id")},
                        attempted=any(event.state is ExecutionCommandState.CLAIMED for event in events),
                        prior_binding=payload.get("resolution_bindings", {}).get(command_id))
                    claims = broker_claims.setdefault(body["identity_binding"]["order_id"], set())
                    if claims - {command_id}:
                        duplicates.update(claims | {command_id})
                    claims.add(command_id)
                    if command_id in resolved:
                        duplicates.add(command_id)
                    resolved[command_id] = view
                except Exception:
                    duplicates.add(command_id)
                    self.resolution_diagnostics[command_id] = "resolution_evidence_required"
            for command_id in duplicates:
                resolved.pop(command_id, None)
                self.resolution_diagnostics[command_id] = "resolution_evidence_required"
        except Exception:
            self.resolution_diagnostics["source"] = "resolution_source_evidence_required"
            return {}
        return resolved

    def _read_command(self, uri):
        store = self.context.command_store
        raw = _object_json(store.object_store.read_text(uri))
        if not raw.get("created_at") or datetime.fromisoformat(raw["created_at"].replace("Z", "+00:00")).tzinfo is None:
            raise _EvidenceRequired("original_creation_time_missing")
        command = ExecutionCommand.from_dict(raw)
        if raw != command.to_dict() or str(store._command_location(command)) != uri:
            raise _EvidenceRequired("descriptor_identity_mismatch")
        intent = command.intent
        scope = self.context.scope
        if (command.platform != "schwab" or command.strategy_profile != scope.profile_id.lower()
                or command.account_scope != scope.account_hash.lower() or command.execution_mode != "live"
                or intent.get("routing_account_hash") != scope.account_hash or intent.get("cohort_id") != scope.cohort_id
                or not isinstance(intent.get("order"), dict) or not re.fullmatch("[a-f0-9]{64}", command.decision_digest)):
            raise _EvidenceRequired("descriptor_scope_mismatch")
        if _digest(_canonical_json(intent.get("source_plan"))) != command.decision_digest:
            raise _EvidenceRequired("source_plan_digest_mismatch")
        order = OrderIntent(**intent["order"])
        if not order.symbol or order.side not in {"buy", "sell"}:
            raise _EvidenceRequired("descriptor_order_invalid")
        return command

    def _read_reference(self, ref):
        if set(ref) != {"command_id", "effective_date", "descriptor_uri", "descriptor_sha256"}:
            raise _EvidenceRequired("exact_original_reference_required")
        command = self._read_command(ref["descriptor_uri"])
        if (command.command_id != ref["command_id"] or command.effective_date != ref["effective_date"]
                or _digest(_canonical_json(command.to_dict())) != ref["descriptor_sha256"]):
            raise _EvidenceRequired("original_reference_content_mismatch")
        # Original release identity is in the immutable full descriptor, not the
        # current plan's release. A later runtime cannot rename that obligation.
        if command.intent.get("strategy_release") != self.original_release.to_dict():
            raise _EvidenceRequired("original_release_mismatch")
        return command

    def _independent_reduction(self, order, candidate, *, originals=None):
        facts = self.context.resource_facts
        if not isinstance(facts, Mapping):
            return False
        obligations = [{"command_id": cmd.command_id, "descriptor_sha256": _digest(_canonical_json(cmd.to_dict())),
                        "order": cmd.intent["order"], "routing_account_hash": cmd.intent["routing_account_hash"]}
                       for cmd, _ in (self.originals if originals is None else originals) if cmd.command_id != candidate.command_id]
        if (facts.get("scope") != asdict(self.context.scope) or facts.get("session") != self.session
                or facts.get("complete_original_obligations") != obligations
                or facts.get("resource_reconciliation") != "verified_complete"
                or facts.get("snapshot_sha256") != self.snapshot_sha256
                or facts.get("proposal_order_sha256") != _digest(_canonical_json(asdict(order)))
                or facts.get("strategy_release") != self.context.runtime_target.strategy_release.to_dict()
                or not re.fullmatch("[a-f0-9]{64}", str(facts.get("source_sha256") or ""))):
            return False
        # No reserve release or new quantity formula. Explicitly qualified
        # disjoint reduction only; another pending sell in this symbol refuses.
        if any(item["order"]["symbol"] == order.symbol for item in obligations):
            return False
        admission = evaluate_reduce_only_command_admission(
            order=order, command=candidate, as_of_session=self.session,
            long_quantities=facts.get("long_quantities", {}), short_quantities=facts.get("short_quantities", {}),
            sellable_quantities=facts.get("sellable_quantities", {}), allowed_symbols=facts.get("allowed_symbols", ()),
            runtime_release_receipt=self.context.runtime_release_receipt,
            expected_strategy_release=self.context.runtime_target.strategy_release,
            command_state=ExecutionCommandState.QUEUED)
        return admission.approved

    def prepare(self, *, order, plan, action_type):
        if self.fatal_reason:
            return None
        c = self.context
        try:
            if plan.get("account_hash") != c.scope.account_hash:
                raise _EvidenceRequired("routed_account_changed")
            if c.runtime_target.strategy_release != self.original_release:
                raise _EvidenceRequired("new_release_enrollment_required")
            execution = plan["execution"]
            # Final business inputs, never a notification/display string.
            source = {"portfolio": plan["portfolio"], "allocation": plan["allocation"],
                      "signal_date": execution.get("signal_date"), "effective_date": execution.get("effective_date"),
                      "execution_timing_contract": execution.get("execution_timing_contract"),
                      "reserved_cash": execution.get("reserved_cash"), "trade_threshold_value": execution.get("trade_threshold_value"),
                      "snapshot_sha256": self.snapshot_sha256}
            intent = {"order": asdict(order), "action_type": action_type, "routing_account_hash": c.scope.account_hash,
                      "cohort_id": c.scope.cohort_id, "strategy_release": c.runtime_target.strategy_release.to_dict(),
                      "source_plan": source, "reduce_only_order_sha256": build_reduce_only_order_digest(order)}
            if action_type == "BUY_NOTIONAL" and not (order.quantity == 0 and float(order.metadata["notional_usd"]) > 0):
                raise _EvidenceRequired("notional_obligation_missing")
            candidate = ExecutionCommand.from_decision(platform="schwab", account_scope=c.scope.account_hash,
                strategy_profile=c.scope.profile_id, execution_mode="live", signal_date=execution["signal_date"],
                effective_date=execution["effective_date"], execution_timing_contract=execution["execution_timing_contract"],
                decision_digest=_digest(_canonical_json(source)), intent=intent, created_at=self.now.isoformat())
            resolved = self._resolved_originals()
            unresolved = [(cmd, history) for cmd, history in self.originals if cmd.command_id not in resolved]
            # Explicit resolution-input selection fails before creating a new
            # orphan for missing/conflicting/late closure evidence. Default-None
            # retains the existing #548 registration/independent-reduction path.
            if self.resolution_sources is not None or self.resolution_receipts is not None:
                if "source" in self.resolution_diagnostics:
                    self.reasons.append("original_resolution_source_evidence_required")
                    return None
                if any(cmd.command_id != candidate.command_id for cmd, _ in unresolved) and not self._independent_reduction(order, candidate, originals=unresolved):
                    self.reasons.append("original_obligations_evidence_required")
                    return None
            # Enqueue false is not proof: read and compare full immutable content,
            # preserving its first timestamp (not part of the native ID hash).
            c.command_store.enqueue(candidate)
            stored = self._read_command(str(c.command_store._command_location(candidate)))
            expected, actual = candidate.to_dict(), stored.to_dict()
            expected.pop("created_at"); actual.pop("created_at")
            if expected != actual:
                raise _EvidenceRequired("existing_descriptor_mismatch")
            candidate = stored
            ref = {"command_id": candidate.command_id, "effective_date": candidate.effective_date,
                   "descriptor_uri": str(c.command_store._command_location(candidate)),
                   "descriptor_sha256": _digest(_canonical_json(candidate.to_dict()))}
            payload = self._payload(self.version)
            resolved = self._resolved_originals()
            if resolved:
                payload.setdefault("resolution_bindings", {}).update(resolved)
            known = next((r for r in payload["references"] if r["command_id"] == candidate.command_id), None)
            if known is not None and known != ref:
                raise _EvidenceRequired("registered_descriptor_changed")
            if known is None:
                # Exactly one CAS, including losing/uncertain replies. No loop.
                payload["references"].append(ref)
            self.version = c.locator.compare_update(_canonical_json(payload), expected=self.version)
            verified = c.locator.read_registered()
            if verified != self.version:
                raise _EvidenceRequired("registration_readback_changed")
            self._read_command(ref["descriptor_uri"])
            events = c.command_store.events(candidate)
            if events:
                self.reasons.append("original_attempt_pending_reconciliation")
                return None
            unresolved = [(cmd, history) for cmd, history in self.originals if cmd.command_id not in resolved]
            other_originals = [cmd for cmd, _ in unresolved if cmd.command_id != candidate.command_id]
            if other_originals and not self._independent_reduction(order, candidate, originals=unresolved):
                self.reasons.append("original_obligations_evidence_required")
                return None
            # A future release needs a separate cohort enrollment; it may read
            # old obligations but cannot create attempts in this old cohort.
            if c.runtime_target.strategy_release != self.original_release:
                raise _EvidenceRequired("new_release_enrollment_required")
            ticket = _IntentTicket(candidate)
            claim = c.command_store.claim_due(candidate, as_of_date=self.session, claimant="schwab-managed-caller")
            if claim is None:
                self.reasons.append("original_claim_unavailable_or_not_due")
                return None
            self.current.append(candidate)
            return ticket
        except Exception:
            self.fatal_reason = "continuity_registration_or_claim_evidence_required"
            self.reasons.append(self.fatal_reason)
            return None

    def record_outcome(self, ticket, *, accepted=None, broker_order_id=None):
        try:
            store = self.context.command_store
            if accepted is None:
                self.reasons.append("original_attempt_pending_reconciliation")
                states = (ExecutionCommandState.RECONCILIATION_REQUIRED,)
            else:
                states = (ExecutionCommandState.SUBMITTED,
                          ExecutionCommandState.ACCEPTED if accepted else ExecutionCommandState.REJECTED)
            for state in states:
                event = store.append_event(ticket.command, next_state=state,
                    details={"broker_order_id": broker_order_id} if broker_order_id is not None else {})
                if event is None:
                    raise _EvidenceRequired("attempt_outcome_conflict")
            return True
        except Exception:
            self.fatal_reason = "continuity_attempt_outcome_evidence_required"
            self.reasons.append(self.fatal_reason)
            return False

    def present(self, execution, *, current_submitted=()):
        value = dict(execution)
        resolved = self._resolved_originals() if not self.fatal_reason else {}
        unresolved = [(cmd, history) for cmd, history in self.originals if cmd.command_id not in resolved]
        if resolved or self.resolution_diagnostics:
            value["continuity_resolved_originals"] = resolved
            value["continuity_resolution_findings"] = dict(self.resolution_diagnostics)
            value["original_resubmission_permitted"] = False
        if self.originals:
            value["continuity_prior_orders"] = [{"command_id": cmd.command_id, "effective_date": cmd.effective_date,
                "order": cmd.intent["order"], "state": events[-1].state.value if events else "queued",
                "broker_order_id": next((e.details.get("broker_order_id") for e in reversed(events)
                                         if e.details.get("broker_order_id") is not None), None)} for cmd, events in self.originals]
        value["continuity_coverage"] = "evidence_required" if self.reasons else "managed_new_cohort"
        value["continuity_reason_codes"] = tuple(dict.fromkeys(self.reasons))
        if self.reasons or unresolved:
            # Keep prior DP02/T1 diagnostics while presenting an honest pending
            # obligation. Never copy an old order into current submitted_orders.
            value.setdefault("no_op_reason", "continuity_evidence_required")
            if value.get("execution_status") not in {"blocked", "skipped_already_executed"}:
                value["execution_status"] = "pending_reconciliation"
            value["broker_submission_done"] = False
            value["orders_pending_count"] = max(int(value.get("orders_pending_count") or 0),
                len(current_submitted) + sum(bool(events) for _, events in unresolved))
        return value
