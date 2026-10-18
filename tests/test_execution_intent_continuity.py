"""Offline private-adapter contracts: native files, synthetic GCS conditions.

The file-backed Blob double models conditional generations for a current object.
GCS may also serve a retained old version consistently; stale CAS must still
conflict. This double is not a cloud permission/retention receipt or live caller.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from application.execution_intent_continuity import (
    _EvidenceRequired, _LocatorConflict, _LocatorStateError, _LocatorUncertain,
    _LocatorUnavailable, _LocatorVersion, _RecoveryLocatorStore, _RecoveryScope,
    _read_schwab_position_quantities,
)
from quant_platform_kit.common.long_only_reduce import validate_long_only_reduce_only_order
from quant_platform_kit.common.models import OrderIntent, PortfolioSnapshot, Position
from quant_platform_kit.cloud.local_provider import LocalObjectStore


SCOPE = _RecoveryScope("SYNTH_HASH", "SYNTH_PROFILE", "SYNTH_RELEASE", "SYNTH_COHORT")
URI = "gs://synthetic-offline-bucket/state/fixed-cohort/locator.json"
ORIGINAL = json.dumps({"original_id": "synthetic-original", "effective_date": "2026-09-30",
                       "state": "UNKNOWN", "native_order_id": None}, sort_keys=True)


class ContractHTTPError(Exception):
    def __init__(self, code):
        self.code = code


class FileClient:
    """Real local files + flock, with explicit synthetic generation semantics."""
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "object.json"
        self.lock_path = self.root / "contract.lock"
        self.calls = []
        self.native_create_store = LocalObjectStore()
        self.lose_ack = False
        self.hide_ack_generation = False
        self.after_reload = None
        self.reload_error = None

    @contextmanager
    def locked(self):
        with self.lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def load(self):
        if not self.path.exists():
            raise ContractHTTPError(404)
        return json.loads(self.path.read_text())

    def publish(self, value):
        tmp = self.path.with_suffix(".pending")
        with tmp.open("w") as stream:
            json.dump(value, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, self.path)

    def bucket(self, name):
        return SimpleNamespace(name=name, blob=lambda key: FileBlob(self, name, key))


class FileBlob:
    def __init__(self, owner, bucket, name):
        self.owner = owner
        self.bucket = SimpleNamespace(name=bucket)
        self.name = name
        self.generation = None

    def reload(self, *, retry):
        assert retry is None
        self.owner.calls.append(("reload",))
        if self.owner.reload_error:
            raise self.owner.reload_error
        with self.owner.locked():
            self.generation = str(self.owner.load()["generation"])
        callback = self.owner.after_reload
        self.owner.after_reload = None
        if callback:
            callback()

    def download_as_text(self, *, if_generation_match, retry):
        assert retry is None
        self.owner.calls.append(("download", if_generation_match))
        with self.owner.locked():
            state = self.owner.load()
            if state["generation"] != if_generation_match:
                raise ContractHTTPError(412)
            return state["data"]

    def upload_from_string(self, data, *, content_type, if_generation_match, retry):
        assert retry is None
        assert content_type == "application/json"
        self.owner.calls.append(("upload", if_generation_match))
        if if_generation_match == 0:
            # The create competition uses the actual QPK O_EXCL primitive,
            # with an explicit absolute path in pytest's task-local directory.
            created = self.owner.native_create_store.create_text(
                str(self.owner.path), json.dumps({"generation": 1, "data": data}), "application/json",
            )
            if not created:
                raise ContractHTTPError(412)
            generation = 1
        else:
            with self.owner.locked():
                state = self.owner.load()
                if state["generation"] != if_generation_match:
                    raise ContractHTTPError(412)
                generation = state["generation"] + 1
                self.owner.publish({"generation": generation, "data": data})
        self.generation = None if self.owner.hide_ack_generation else str(generation)
        lose_ack = self.owner.lose_ack
        self.owner.lose_ack = False
        if lose_ack:
            raise TimeoutError("synthetic response lost after durable local publication")


def store(client, *, scope=SCOPE):
    return _RecoveryLocatorStore(client, locator_uri=URI, scope=scope)


_SDK_DEFAULT_RETRY = object()


class _SdkRetryClient:
    """C's explicit lost-503/retried-412 model; no SDK or HTTP execution."""
    def __init__(self, generation=0, blob_type=None):
        self.generation = generation
        self.data = None
        self.attempts = 0
        self.policies = []
        self.blob_type = blob_type

    def bucket(self, name):
        cls = self.blob_type or _SdkRetryBlob
        return SimpleNamespace(name=name, blob=lambda key: cls(self, name, key))


class _SdkRetryBlob:
    def __init__(self, owner, bucket, name):
        self.owner = owner
        self.bucket = SimpleNamespace(name=bucket)
        self.name = name
        self.generation = None

    def reload(self, *, retry=_SDK_DEFAULT_RETRY):
        self.owner.policies.append(("reload", None if retry is None else "SDK_DEFAULT"))
        self.generation = str(self.owner.generation)

    def download_as_text(self, *, if_generation_match, retry=_SDK_DEFAULT_RETRY):
        self.owner.policies.append(("download", None if retry is None else "SDK_DEFAULT"))
        assert if_generation_match == self.owner.generation
        return self.owner.data

    def upload_from_string(self, data, *, content_type, if_generation_match, retry=_SDK_DEFAULT_RETRY):
        self.owner.policies.append(("upload", None if retry is None else "SDK_DEFAULT"))
        self.owner.attempts += 1
        assert content_type == "application/json" and if_generation_match == self.owner.generation
        self.owner.generation += 1
        self.owner.data = data
        self.generation = str(self.owner.generation)
        if retry is None:
            raise ContractHTTPError(503)  # committed, acknowledgement lost
        self.owner.attempts += 1  # modeled SDK retry reuses the now-stale precondition
        raise ContractHTTPError(412)


@pytest.mark.parametrize("generation", [0, 7], ids=["create", "cas"])
def test_sdk_lost_reply_has_one_attempt_and_remains_uncertain(generation):
    client = _SdkRetryClient(generation)
    target = store(client)
    token = _LocatorVersion(generation, "{}", hashlib.sha256(b"{}").hexdigest(), URI, SCOPE)
    with pytest.raises(_LocatorUncertain) as raised:
        if generation == 0:
            target.initialize_new_cohort(ORIGINAL)
        else:
            target.compare_update(ORIGINAL, expected=token)
    assert raised.value.status_code == 503
    assert client.attempts == 1
    assert client.policies == [("upload", None)]
    payload = json.loads(json.loads(client.data)["payload_json"])
    assert payload["state"] == "UNKNOWN" and payload["native_order_id"] is None


def test_registered_read_explicitly_disables_both_sdk_retry_defaults():
    client = _SdkRetryClient(7)
    target = store(client)
    client.data = target._encode(ORIGINAL)[0]
    assert target.read_registered().generation == 7
    assert client.policies == [("reload", None), ("download", None)]


@pytest.mark.parametrize("generation", [0, 7], ids=["create", "cas"])
def test_sdk_default_policy_negative_control_exposes_retry_and_conflict(generation):
    class DropWriteRetry(_SdkRetryBlob):
        def upload_from_string(self, data, *, content_type, if_generation_match, retry=_SDK_DEFAULT_RETRY):
            # Deliberately remove the fixed wrapper's policy at this model seam.
            return super().upload_from_string(data, content_type=content_type, if_generation_match=if_generation_match)

    client = _SdkRetryClient(generation, blob_type=DropWriteRetry)
    target = store(client)
    token = _LocatorVersion(generation, "{}", hashlib.sha256(b"{}").hexdigest(), URI, SCOPE)
    with pytest.raises(_LocatorConflict) as raised:
        if generation == 0:
            target.initialize_new_cohort(ORIGINAL)
        else:
            target.compare_update(ORIGINAL, expected=token)
    assert raised.value.status_code == 412
    assert client.attempts == 2
    assert client.policies == [("upload", "SDK_DEFAULT")]


def test_create_and_fresh_instance_recovery_preserve_original(tmp_path):
    client = FileClient(tmp_path)
    first = store(client).initialize_new_cohort(ORIGINAL)
    assert first.generation == 1
    before = client.path.read_bytes()
    recovered = store(FileClient(tmp_path)).read_registered()
    assert recovered == first
    assert json.loads(recovered.payload_json)["effective_date"] == "2026-09-30"
    assert json.loads(recovered.payload_json)["state"] == "UNKNOWN"
    assert json.loads(recovered.payload_json)["native_order_id"] is None
    with pytest.raises(_LocatorConflict):
        store(client).initialize_new_cohort(ORIGINAL)
    assert client.path.read_bytes() == before
    assert [x for x in client.calls if x[0] == "upload"] == [("upload", 0), ("upload", 0)]


def test_missing_registered_locator_never_initializes_empty(tmp_path):
    client = FileClient(tmp_path)
    with pytest.raises(_LocatorUnavailable):
        store(client).read_registered()
    assert not client.path.exists()
    assert client.calls == [("reload",)]


def test_metadata_data_read_conflict_never_returns_mixed_or_empty_state(tmp_path):
    client = FileClient(tmp_path)
    original = store(client).initialize_new_cohort(ORIGINAL)
    client.after_reload = lambda: store(client).compare_update('{"original_id":"new-reference"}', expected=original)
    with pytest.raises(_LocatorConflict):
        store(client).read_registered()
    assert ("download", 1) in client.calls
    recovered = store(client).read_registered()
    assert recovered.generation == 2
    assert json.loads(recovered.payload_json)["original_id"] == "new-reference"


def test_consistent_retained_old_version_cannot_authorize_stale_cas(tmp_path):
    client = FileClient(tmp_path)
    original = store(client).initialize_new_cohort(ORIGINAL)
    old_data = client.load()["data"]
    client.after_reload = lambda: store(client).compare_update('{"references":["newer"]}', expected=original)

    def retained_download(blob, *, if_generation_match, retry):
        assert retry is None
        assert if_generation_match == 1
        return old_data  # synthetic retained-version response, not a cloud call

    with patch.object(FileBlob, "download_as_text", retained_download):
        read = store(client).read_registered()
    assert read == original  # same-generation bytes may remain consistently readable
    with pytest.raises(_LocatorConflict):
        store(client).compare_update(ORIGINAL, expected=read)
    assert json.loads(store(client).read_registered().payload_json) == {"references": ["newer"]}


@pytest.mark.parametrize("generation", [0, -1, True, False, "1", 1.0, None])
def test_cas_requires_positive_integer_version_before_io(tmp_path, generation):
    client = FileClient(tmp_path)
    token = _LocatorVersion(generation, "{}", hashlib.sha256(b"{}").hexdigest(), URI, SCOPE)
    with pytest.raises(ValueError):
        store(client).compare_update(ORIGINAL, expected=token)
    assert client.calls == []


def test_cas_rejects_version_from_other_locator_scope_or_payload(tmp_path):
    client = FileClient(tmp_path)
    original = store(client).initialize_new_cohort(ORIGINAL)
    tokens = [replace(original, locator_uri="gs://other/locator"),
              replace(original, scope=_RecoveryScope("OTHER", "SYNTH_PROFILE", "SYNTH_RELEASE", "SYNTH_COHORT")),
              replace(original, payload_sha256="0" * 64)]
    client.calls.clear()
    for token in tokens:
        with pytest.raises(ValueError):
            store(client).compare_update(ORIGINAL, expected=token)
    assert client.calls == []


def test_stale_cas_cannot_overwrite_winner(tmp_path):
    client = FileClient(tmp_path)
    original = store(client).initialize_new_cohort(ORIGINAL)
    winner = store(client).compare_update('{"references":["original","additional"]}', expected=original)
    before = client.path.read_bytes()
    with pytest.raises(_LocatorConflict):
        store(client).compare_update('{"references":["stale"]}', expected=original)
    assert client.path.read_bytes() == before
    assert store(client).read_registered() == winner


@pytest.mark.parametrize("operation", ["create", "cas"])
def test_concurrent_conditional_writers_have_one_winner(tmp_path, operation):
    if operation == "cas":
        initial = store(FileClient(tmp_path)).initialize_new_cohort(ORIGINAL)
    barrier = Barrier(2)

    def writer(index):
        client = FileClient(tmp_path)
        barrier.wait(timeout=10)
        try:
            target = store(client)
            result = (target.initialize_new_cohort if operation == "create" else
                      lambda data: target.compare_update(data, expected=initial))(json.dumps({"writer": index}))
            return "winner", json.loads(result.payload_json)["writer"]
        except _LocatorConflict:
            return "conflict", index

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(writer, (1, 2)))
    assert sorted(x[0] for x in results) == ["conflict", "winner"]
    recovered = store(FileClient(tmp_path)).read_registered()
    assert json.loads(recovered.payload_json)["writer"] == next(x[1] for x in results if x[0] == "winner")


@pytest.mark.parametrize("operation", ["create", "cas"])
def test_lost_write_response_stays_uncertain_and_is_recoverable(tmp_path, operation):
    client = FileClient(tmp_path)
    target = store(client)
    if operation == "cas":
        initial = target.initialize_new_cohort('{"original_id":"synthetic-original"}')
    client.calls.clear()
    client.lose_ack = True
    with pytest.raises(_LocatorUncertain):
        if operation == "create":
            target.initialize_new_cohort(ORIGINAL)
        else:
            target.compare_update(ORIGINAL, expected=initial)
    assert len([x for x in client.calls if x[0] == "upload"]) == 1  # no automatic retry
    recovered = store(FileClient(tmp_path)).read_registered()
    assert json.loads(recovered.payload_json)["state"] == "UNKNOWN"
    assert json.loads(recovered.payload_json)["native_order_id"] is None
    with pytest.raises(_LocatorConflict):
        if operation == "create":
            target.initialize_new_cohort(ORIGINAL)
        else:
            target.compare_update(ORIGINAL, expected=initial)


def test_unbound_write_ack_generation_is_not_success(tmp_path):
    client = FileClient(tmp_path)
    client.hide_ack_generation = True
    with pytest.raises(_LocatorUncertain):
        store(client).initialize_new_cohort(ORIGINAL)
    assert store(FileClient(tmp_path)).read_registered().generation == 1


@pytest.mark.parametrize("bad", ["", "{", "[]", '{"schema_version":1,"schema_version":2}'])
def test_corrupt_registered_payload_is_not_recreated(tmp_path, bad):
    client = FileClient(tmp_path)
    store(client).initialize_new_cohort(ORIGINAL)
    client.publish({"generation": 1, "data": bad})
    before = client.path.read_bytes()
    client.calls.clear()
    with pytest.raises(_LocatorStateError):
        store(client).read_registered()
    assert client.path.read_bytes() == before
    assert not any(x[0] == "upload" for x in client.calls)


def test_scope_and_payload_digest_mismatch_are_rejected(tmp_path):
    client = FileClient(tmp_path)
    store(client).initialize_new_cohort(ORIGINAL)
    with pytest.raises(_LocatorStateError):
        store(client, scope=_RecoveryScope("OTHER", "SYNTH_PROFILE", "SYNTH_RELEASE", "SYNTH_COHORT")).read_registered()
    state = client.load()
    envelope = json.loads(state["data"])
    envelope["payload_sha256"] = "0" * 64
    client.publish({"generation": 1, "data": json.dumps(envelope)})
    with pytest.raises(_LocatorStateError):
        store(client).read_registered()


@pytest.mark.parametrize("failure", [ContractHTTPError(403), ContractHTTPError(500), TimeoutError()])
def test_read_failures_are_not_missing_or_auto_retried(tmp_path, failure):
    client = FileClient(tmp_path)
    client.reload_error = failure
    with pytest.raises(_LocatorUncertain):
        store(client).read_registered()
    assert client.calls == [("reload",)]


def test_client_binding_failure_stays_uncertain_before_data_io():
    def unavailable_bucket(_name):
        raise TimeoutError()

    with pytest.raises(_LocatorUncertain):
        store(SimpleNamespace(bucket=unavailable_bucket)).read_registered()


def synthetic_snapshot(*, long="10", short="0", rows=None):
    """Explicit synthetic contract; does not pretend the platform pin emits it.

    Actual #698 producer interoperability and old-pin absence are exercised by
    the task's separate native-producer witness without changing platform pins.
    """
    position = {"instrument": {"symbol": "TQQQ", "assetType": "EQUITY"},
                "longQuantity": long, "shortQuantity": short, "marketValue": 1000}
    source_rows = [position] if rows is None else rows
    records = [{"symbol": row["instrument"]["symbol"], "asset_type": row["instrument"]["assetType"],
                "reported_quantities": {field: str(row[field]) for field in ("longQuantity", "shortQuantity") if field in row},
                "unavailable_fields": {}} for row in source_rows]
    as_of = datetime(2026, 10, 18, 10, 0, tzinfo=timezone.utc)
    digest = hashlib.sha256(json.dumps(source_rows, sort_keys=True).encode()).hexdigest()
    facts = {"records": records, "long_quantities": {"TQQQ": str(long)}, "short_quantities": {"TQQQ": str(short)},
             "ambiguous_symbols": [], "source_digest_sha256": digest,
             "observation_started_at": (as_of - timedelta(milliseconds=1)).isoformat(),
             "observation_completed_at": as_of.isoformat(), "observation_window_valid": True,
             "positions_field_present": True, "time_semantics": "client_observation_window", "orders_observed": False}
    return PortfolioSnapshot(as_of=as_of, total_equity=101000, buying_power=100000, cash_balance=100000,
                             positions=[Position(symbol="TQQQ", quantity=float(long), market_value=1000)],
                             metadata={"account_hash": "SYNTH_HASH", "source_digest_sha256": digest,
                                       "schwab_position_quantities": facts})


def converted(snapshot):
    before = deepcopy(snapshot.metadata)
    result = _read_schwab_position_quantities(
        snapshot, expected_account_hash="SYNTH_HASH", symbols=("TQQQ",),
        window_start=snapshot.as_of - timedelta(minutes=1), window_end=snapshot.as_of + timedelta(seconds=1),
    )
    assert snapshot.metadata == before
    return result


@pytest.mark.parametrize("short,findings", [("0", ("sellable_quantity_unavailable",)),
                                           ("3", ("short_quantity_present", "sellable_quantity_unavailable"))])
def test_synthetic_contract_conversion_reaches_native_numeric_consumer(short, findings):
    facts = converted(synthetic_snapshot(long="10.00", short=short))
    assert facts.long_quantities == {"TQQQ": 10.0}
    assert facts.short_quantities == {"TQQQ": float(short)}
    assert not hasattr(facts, "sellable_quantities")
    result = validate_long_only_reduce_only_order(
        OrderIntent("TQQQ", "sell", 1), long_quantities=facts.long_quantities,
        short_quantities=facts.short_quantities, sellable_quantities={}, allowed_symbols=("TQQQ",),
    )
    assert result.approved is False
    assert result.findings == findings


@pytest.mark.parametrize("bad", [None, True, False, 0, 1.0, {}, [], "-1", "NaN", "Infinity", "1E+9999", "1E-9999",
                                 "9" * 129, "9007199254740993", "0.1000000000000000000001", " 1", "+1", "01", "1_0"])
def test_invalid_or_inexact_numeric_fact_is_unknown_not_zero(bad):
    snapshot = synthetic_snapshot()
    metadata = deepcopy(snapshot.metadata)
    raw = metadata["schwab_position_quantities"]
    raw["records"][0]["reported_quantities"]["longQuantity"] = bad
    raw["long_quantities"]["TQQQ"] = bad
    facts = converted(replace(snapshot, metadata=metadata))
    assert "TQQQ" not in facts.long_quantities
    assert facts.short_quantities == {"TQQQ": 0.0}
    assert "TQQQ" in facts.unavailable_fields


@pytest.mark.parametrize("bad", [None, False, "1E-9999", "9007199254740993"])
def test_invalid_short_is_not_a_known_zero(bad):
    snapshot = synthetic_snapshot()
    metadata = deepcopy(snapshot.metadata)
    raw = metadata["schwab_position_quantities"]
    raw["records"][0]["reported_quantities"]["shortQuantity"] = bad
    raw["short_quantities"]["TQQQ"] = bad
    facts = converted(replace(snapshot, metadata=metadata))
    assert facts.long_quantities == {"TQQQ": 10.0}
    assert "TQQQ" not in facts.short_quantities


@pytest.mark.parametrize("value", ["0.00", "0.125", "1E+2"])
def test_explicit_zero_fraction_and_exponent_keep_numeric_value(value):
    facts = converted(synthetic_snapshot(long=value))
    assert facts.long_quantities == {"TQQQ": float(value)}


def test_missing_field_absent_symbol_duplicate_and_units_do_not_invent_holdings():
    snapshot = synthetic_snapshot()
    metadata = deepcopy(snapshot.metadata)
    del metadata["schwab_position_quantities"]["long_quantities"]["TQQQ"]
    facts = converted(replace(snapshot, metadata=metadata))
    assert "TQQQ" not in facts.long_quantities  # no fallback to Position.quantity
    assert converted(synthetic_snapshot(rows=[])).long_quantities == {}
    row = {"instrument": {"symbol": "TQQQ", "assetType": "EQUITY"},
           "longQuantity": 10, "shortQuantity": 0, "marketValue": 1000}
    for rows in ([row, deepcopy(row)], [{**row, "instrument": {"symbol": "TQQQ", "assetType": "OPTION"}}]):
        facts = converted(synthetic_snapshot(rows=rows))
        assert facts.long_quantities == facts.short_quantities == {}


def test_record_map_disagreement_is_unavailable_not_coerced():
    snapshot = synthetic_snapshot()
    metadata = deepcopy(snapshot.metadata)
    metadata["schwab_position_quantities"]["short_quantities"]["TQQQ"] = "3"
    facts = converted(replace(snapshot, metadata=metadata))
    assert facts.long_quantities == {"TQQQ": 10.0}
    assert "TQQQ" not in facts.short_quantities


@pytest.mark.parametrize("change", ["account", "digest", "window_flag", "positions_absent", "time_kind", "orders_claim", "naive_time", "outside_window", "snapshot_asof"])
def test_scope_provenance_and_time_mismatch_reject_bundle(change):
    snapshot = synthetic_snapshot()
    metadata = deepcopy(snapshot.metadata)
    raw = metadata["schwab_position_quantities"]
    if change == "account": metadata["account_hash"] = "OTHER"
    elif change == "digest": raw["source_digest_sha256"] = "0" * 64
    elif change == "window_flag": raw["observation_window_valid"] = False
    elif change == "positions_absent": raw["positions_field_present"] = False
    elif change == "time_kind": raw["time_semantics"] = "broker_atomic"
    elif change == "orders_claim": raw["orders_observed"] = True
    elif change == "naive_time": raw["observation_started_at"] = snapshot.as_of.replace(tzinfo=None).isoformat()
    elif change == "outside_window": raw["observation_started_at"] = (snapshot.as_of - timedelta(days=2)).isoformat()
    elif change == "snapshot_asof": snapshot = replace(snapshot, as_of=snapshot.as_of + timedelta(seconds=1))
    with pytest.raises(_EvidenceRequired):
        converted(replace(snapshot, metadata=metadata))


def process_worker(action, root):
    client = FileClient(root)
    target = store(client)
    if action == "lost-create":
        client.lose_ack = True
        try:
            target.initialize_new_cohort(ORIGINAL)
        except _LocatorUncertain:
            print(json.dumps({"outcome": "UNCERTAIN", "uploads": len([x for x in client.calls if x[0] == "upload"]),
                              "adapter_source": sys.modules[_RecoveryLocatorStore.__module__].__file__,
                              "native_create_source": sys.modules[LocalObjectStore.__module__].__file__}))
        else:
            raise AssertionError("lost response must remain uncertain")
    elif action == "recover":
        state = target.read_registered()
        before = client.path.read_bytes()
        try:
            target.initialize_new_cohort(ORIGINAL)
        except _LocatorConflict:
            pass
        else:
            raise AssertionError("original locator must not become a new cohort")
        assert before == client.path.read_bytes()
        print(json.dumps({"outcome": "RECOVERED_ORIGINAL", "generation": state.generation,
                          "payload": json.loads(state.payload_json), "file_sha256": hashlib.sha256(before).hexdigest()}))
    else:
        raise ValueError(action)


if __name__ == "__main__":
    process_worker(sys.argv[1], sys.argv[2])


class MultiFileClient:
    """Test-only per-object durable synthetic GCS; every SDK call records retry."""
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.owners = {}
        self.list_error = None
        self.partial = False
        self.list_calls = []

    def owner(self, name):
        if name not in self.owners:
            owner = FileClient(self.root / hashlib.sha256(name.encode()).hexdigest())
            (owner.root / "key.json").write_text(json.dumps(name))
            self.owners[name] = owner
        return self.owners[name]

    def bucket(self, name):
        return SimpleNamespace(name=name, blob=lambda key: FileBlob(self.owner(key), name, key))

    def list_blobs(self, bucket, *, prefix, retry):
        assert retry is None
        self.list_calls.append((bucket, prefix, retry))
        if self.list_error:
            raise self.list_error
        names = []
        for key_file in self.root.glob("*/key.json"):
            key = json.loads(key_file.read_text())
            if key.startswith(prefix) and (key_file.parent / "object.json").exists():
                names.append(key)
        class Listing:
            next_page_token = "synthetic-incomplete" if self.partial else None
            def __iter__(self):
                return iter(SimpleNamespace(name=name) for name in names)
        return Listing()


def test_locator_competition_preserves_winner_and_later_reference_union(tmp_path):
    from test_rebalance_service import continuity_case
    from application.execution_intent_continuity import _IntentSession
    c = continuity_case(tmp_path)
    a = _IntentSession(c.context, snapshot=c.snapshot, profile_id=c.scope.profile_id)
    b = _IntentSession(c.context, snapshot=c.snapshot, profile_id=c.scope.profile_id)
    sell = OrderIntent("SOXL", "sell", 4)
    other = OrderIntent("SOXX", "sell", 2)
    winner = a.prepare(order=sell, plan=c.plan, action_type="SELL")
    assert winner is not None
    assert b.prepare(order=other, plan=c.plan, action_type="SELL") is None
    assert b.fatal_reason
    first = json.loads(c.locator.read_registered().payload_json)["references"]
    assert len(first) == 1 and first[0]["command_id"] == winner.command.command_id
    later = _IntentSession(c.context, snapshot=c.snapshot, profile_id=c.scope.profile_id)
    assert later.prepare(order=other, plan=c.plan, action_type="SELL") is None
    refs = json.loads(c.locator.read_registered().payload_json)["references"]
    assert len(refs) == 2 and first[0] in refs
    assert c.store.events(later._read_reference(refs[1])) == ()


def test_competing_native_attempt_writers_have_one_durable_claim(tmp_path):
    from test_rebalance_service import continuity_case
    from application.execution_intent_continuity import _IntentSession
    c = continuity_case(tmp_path)
    session = _IntentSession(c.context, snapshot=c.snapshot, profile_id=c.scope.profile_id)
    with patch.object(type(c.store), "claim_due", return_value=None):
        session.prepare(order=OrderIntent("SOXL", "sell", 4), plan=c.plan, action_type="SELL")
    ref = json.loads(c.locator.read_registered().payload_json)["references"][0]
    command = session._read_reference(ref)
    barrier = Barrier(2)
    original = c.store.object_store.create_text
    def create(uri, *args):
        if uri.endswith("00000001.json"):
            barrier.wait(timeout=10)
        return original(uri, *args)
    with patch.object(c.store.object_store, "create_text", create), ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: c.store.claim_due(command, as_of_date="2026-09-30", claimant="synthetic"), range(2)))
    assert sum(result is not None for result in results) == 1
    assert len(c.store.events(command)) == 1


@pytest.mark.parametrize("qualification", ["complete", "absent", "same_symbol", "missing_sellable", "incomplete_obligations", "missing_release", "wrong_snapshot"])
def test_native_independent_reduction_requires_complete_original_resource_facts(tmp_path, qualification):
    from test_rebalance_service import continuity_case, run_strategy_core
    from application.execution_intent_continuity import _canonical_json, _digest
    from quant_platform_kit.common.port_adapters import CallablePortfolioPort
    c = continuity_case(tmp_path)
    run_strategy_core(runtime=c.runtime, config=c.config)
    later = continuity_case(tmp_path, "2026-10-01")
    symbol = "SOXL" if qualification == "same_symbol" else "SOXX"
    later.plan["portfolio"]["market_values"] = {symbol: 400}
    later.plan["portfolio"]["quantities"] = {symbol: 4}
    later.plan["allocation"].update(strategy_symbols=(symbol,), risk_symbols=(symbol,), targets={symbol: 0})
    later.snapshot = replace(later.snapshot, positions=(Position(symbol, 4, 400),))
    later.runtime = replace(later.runtime, portfolio_port=CallablePortfolioPort(lambda: later.snapshot))
    session = __import__("application.execution_intent_continuity", fromlist=["_IntentSession"])._IntentSession(
        later.context, snapshot=later.snapshot, profile_id=later.scope.profile_id)
    obligations = [{"command_id": cmd.command_id, "descriptor_sha256": _digest(_canonical_json(cmd.to_dict())),
                    "order": cmd.intent["order"], "routing_account_hash": cmd.intent["routing_account_hash"]} for cmd, _ in session.originals]
    order = OrderIntent(symbol, "sell", 4)
    facts = {"scope": __import__("dataclasses").asdict(later.scope), "session": "2026-10-01",
             "complete_original_obligations": obligations, "resource_reconciliation": "verified_complete",
             "snapshot_sha256": session.snapshot_sha256,
             "source_sha256": "1"*64, "proposal_order_sha256": _digest(_canonical_json(__import__("dataclasses").asdict(order))),
             "strategy_release": later.context.runtime_target.strategy_release.to_dict(),
             "long_quantities": {symbol: 4}, "short_quantities": {symbol: 0},
             "sellable_quantities": {symbol: 4}, "allowed_symbols": (symbol,)}
    if qualification == "absent": facts = None
    elif qualification == "missing_sellable": facts.pop("sellable_quantities")
    elif qualification == "incomplete_obligations": facts["complete_original_obligations"] = []
    elif qualification == "missing_release": facts.pop("strategy_release")
    elif qualification == "wrong_snapshot": facts["snapshot_sha256"] = "0"*64
    result = run_strategy_core(runtime=later.runtime, config=replace(later.config,
        intent_continuity=replace(later.context, resource_facts=facts)))
    assert len(later.calls) == int(qualification == "complete")
    assert result.execution["broker_submission_done"] is False # original still pending, including positive independent control


# Reuse explicit test-only external ports under ordinary pytest as well.
from test_rebalance_service import _offline_caller_external_ports
