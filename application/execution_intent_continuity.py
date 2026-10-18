"""Private recovery I/O and reported-position decoding for the Schwab adopter.

These primitives do not enroll a cohort, authorize an attempt, resolve UNKNOWN,
or call transport. Runtime caller wiring and backend/broker qualification remain
separate. A caller must supply an already-qualified existing GCS client; this
module never constructs a client or invokes a provider/default-auth factory.
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
        return _LocatorVersion(generation, canonical, digest)

    def read_registered(self) -> _LocatorVersion:
        blob = self._blob(reading=True)
        try:
            blob.reload()
        except Exception as exc:
            self._io_failure(exc, reading=True)
        try:
            generation = _generation(blob.generation)
        except (ValueError, OverflowError):
            raise _LocatorUncertain("read_generation_unavailable") from None
        try:
            text = blob.download_as_text(if_generation_match=generation)
        except Exception as exc:
            self._io_failure(exc, reading=True)
        return self._decode(text, generation)

    def _write(self, payload_json: str, expected_generation: int) -> _LocatorVersion:
        envelope, canonical, digest = self._encode(payload_json)
        blob = self._blob(reading=False)
        try:
            blob.upload_from_string(envelope, content_type="application/json", if_generation_match=expected_generation)
        except Exception as exc:
            self._io_failure(exc, reading=False)
        try:
            acknowledged_generation = _generation(blob.generation)
            if acknowledged_generation == expected_generation:
                raise ValueError("no new version bound to write acknowledgement")
        except (ValueError, OverflowError):
            raise _LocatorUncertain("write_generation_unavailable") from None
        return _LocatorVersion(acknowledged_generation, canonical, digest)

    def initialize_new_cohort(self, payload_json: str) -> _LocatorVersion:
        return self._write(payload_json, 0)

    def compare_update(self, payload_json: str, *, expected_generation: int) -> _LocatorVersion:
        if isinstance(expected_generation, bool) or not isinstance(expected_generation, int) or expected_generation <= 0:
            raise ValueError("CAS requires a positive integer version from a qualified read")
        return self._write(payload_json, expected_generation)


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
