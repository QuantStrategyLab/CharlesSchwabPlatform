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
    _LocatorUnavailable, _RecoveryLocatorStore, _RecoveryScope,
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

    def reload(self):
        self.owner.calls.append(("reload",))
        if self.owner.reload_error:
            raise self.owner.reload_error
        with self.owner.locked():
            self.generation = str(self.owner.load()["generation"])
        callback = self.owner.after_reload
        self.owner.after_reload = None
        if callback:
            callback()

    def download_as_text(self, *, if_generation_match):
        self.owner.calls.append(("download", if_generation_match))
        with self.owner.locked():
            state = self.owner.load()
            if state["generation"] != if_generation_match:
                raise ContractHTTPError(412)
            return state["data"]

    def upload_from_string(self, data, *, content_type, if_generation_match):
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
    store(client).initialize_new_cohort(ORIGINAL)
    client.after_reload = lambda: store(client).compare_update('{"original_id":"new-reference"}', expected_generation=1)
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
    client.after_reload = lambda: store(client).compare_update('{"references":["newer"]}', expected_generation=1)

    def retained_download(blob, *, if_generation_match):
        assert if_generation_match == 1
        return old_data  # synthetic retained-version response, not a cloud call

    with patch.object(FileBlob, "download_as_text", retained_download):
        read = store(client).read_registered()
    assert read == original  # same-generation bytes may remain consistently readable
    with pytest.raises(_LocatorConflict):
        store(client).compare_update(ORIGINAL, expected_generation=read.generation)
    assert json.loads(store(client).read_registered().payload_json) == {"references": ["newer"]}


@pytest.mark.parametrize("generation", [0, -1, True, False, "1", 1.0, None])
def test_cas_requires_positive_integer_version_before_io(tmp_path, generation):
    client = FileClient(tmp_path)
    with pytest.raises(ValueError):
        store(client).compare_update(ORIGINAL, expected_generation=generation)
    assert client.calls == []


def test_stale_cas_cannot_overwrite_winner(tmp_path):
    client = FileClient(tmp_path)
    original = store(client).initialize_new_cohort(ORIGINAL)
    winner = store(client).compare_update('{"references":["original","additional"]}', expected_generation=original.generation)
    before = client.path.read_bytes()
    with pytest.raises(_LocatorConflict):
        store(client).compare_update('{"references":["stale"]}', expected_generation=original.generation)
    assert client.path.read_bytes() == before
    assert store(client).read_registered() == winner


@pytest.mark.parametrize("operation", ["create", "cas"])
def test_concurrent_conditional_writers_have_one_winner(tmp_path, operation):
    if operation == "cas":
        store(FileClient(tmp_path)).initialize_new_cohort(ORIGINAL)
    barrier = Barrier(2)

    def writer(index):
        client = FileClient(tmp_path)
        barrier.wait(timeout=10)
        try:
            target = store(client)
            result = (target.initialize_new_cohort if operation == "create" else
                      lambda data: target.compare_update(data, expected_generation=1))(json.dumps({"writer": index}))
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
        target.initialize_new_cohort('{"original_id":"synthetic-original"}')
    client.calls.clear()
    client.lose_ack = True
    with pytest.raises(_LocatorUncertain):
        if operation == "create":
            target.initialize_new_cohort(ORIGINAL)
        else:
            target.compare_update(ORIGINAL, expected_generation=1)
    assert len([x for x in client.calls if x[0] == "upload"]) == 1  # no automatic retry
    recovered = store(FileClient(tmp_path)).read_registered()
    assert json.loads(recovered.payload_json)["state"] == "UNKNOWN"
    assert json.loads(recovered.payload_json)["native_order_id"] is None
    with pytest.raises(_LocatorConflict):
        if operation == "create":
            target.initialize_new_cohort(ORIGINAL)
        else:
            target.compare_update(ORIGINAL, expected_generation=1)


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
