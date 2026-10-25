from __future__ import annotations

import copy
import hashlib
import json
import logging
import stat
import sys
from pathlib import Path

import pytest

from scripts import diagnose_pilot_state as reader


ROOT = Path(__file__).resolve().parents[1]
SERVICE = "synthetic-serving-service"
REVISION = SERVICE + "-00001-state"
ACCOUNT = "private-native-account-canary"
URI = "gs://synthetic-private-bucket/private-state-canary"
PROFILE = "soxl_soxx_trend_income"


@pytest.fixture(autouse=True)
def runner_mode(monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")


@pytest.fixture
def target():
    return {
        "service_name": SERVICE, "platform_id": "schwab", "account_scope": "private-scope-canary",
        "account_selector": ["private-selector-canary"], "deployment_selector": "live",
        "strategy_profile": PROFILE, "dry_run_only": False,
        "runtime_risk_limits": {"binding": {"account_hash": ACCOUNT}, "private_amount": 123456.78},
        "live_continuity": {"state": "PAUSED", "baseline_id": "private-baseline-canary"},
    }


@pytest.fixture
def source(target):
    return {"GCP_PROJECT_ID": "synthetic-project", "CLOUD_RUN_REGION": "synthetic-region1",
            "CLOUD_RUN_SERVICE": SERVICE, "RUNTIME_TARGET_JSON": json.dumps(target)}


def _service(revision=REVISION):
    return {"metadata": {"name": SERVICE}, "status": {"traffic": [{"revisionName": revision, "percent": 100}]}}


def _revision(target, env=None):
    return {"metadata": {"name": REVISION, "labels": {"serving.knative.dev/service": SERVICE}},
            "spec": {"containers": [{"env": [{"name": "RUNTIME_TARGET_JSON", "value": json.dumps(target)},
                *({"name": name, **(value if isinstance(value, dict) else {"value": value})}
                  for name, value in (env if env is not None else {"SCHWAB_EXECUTION_STATE_CLOUD_URI": URI}).items())]}]}}


def _owner(owner=PROFILE):
    from quant_platform_kit.common.execution_state import build_account_owner_marker_key

    return {"schema_version": "execution_claim.v1", "state": "claimed",
            "marker_key": build_account_owner_marker_key(broker="schwab", account_id=ACCOUNT),
            "metadata": {"owner_id": owner, "private_amount": 123456.78}}


class Store:
    def __init__(self, *, present=True, owner=None, digest=None, error=None):
        self.present = present
        self.owner = _owner() if owner is None else owner
        self.digest = digest
        self.error = error
        self.events = []

    def has_marker(self, key):
        self.events.append(("exists", key))
        if self.error == "owner":
            raise PermissionError("private-storage-error-canary")
        return self.present

    def read_marker(self, key):
        self.events.append(("owner", key))
        return self.owner

    def calculate_recent_ledger_digest(self, **kwargs):
        self.events.append(("ledger", kwargs))
        if self.error == "ledger":
            raise PermissionError("private-storage-error-canary")
        entries = self._entries()
        return self.digest if self.digest is not None else (reader._entries_digest(entries), sum(map(len, entries.values())))

    def _cloud_uri_for_namespace(self, key, namespace):
        return URI + "/" + namespace + "/" + key + ".json"

    namespace = "execution_markers"
    outcome_namespace = "execution_outcomes"

    def _entries(self):
        key, claim, outcome = _records()
        return {self.namespace: [(self._cloud_uri_for_namespace(key, self.namespace), json.dumps(claim))],
                self.outcome_namespace: [(self._cloud_uri_for_namespace(key, self.outcome_namespace), json.dumps(outcome))]}

    def _recent_ledger_entries(self, *, namespace, relative_prefix, record_limit):
        self.events.append(("entries", {"namespace": namespace, "relative_prefix": relative_prefix, "record_limit": record_limit}))
        return self._entries()[namespace]


def _records(*, status="pending_reconciliation", order_status="unknown", signal="2026-10-24", effective="2026-10-25"):
    from quant_platform_kit.common.execution_state import build_execution_marker_key

    key = build_execution_marker_key(platform="schwab", strategy_profile=PROFILE, account_scope=ACCOUNT,
        execution_mode="live", signal_date=signal, effective_date=effective)
    common = {"strategy_profile": PROFILE, "account_scope": ACCOUNT, "dry_run_only": False,
              "signal_date": signal, "effective_date": effective}
    claim = {"schema_version": "execution_claim.v1", "marker_key": key, "state": "claimed", "metadata": dict(common)}
    outcome = {"schema_version": "execution_outcome.v1", "marker_key": key, "metadata": {
        **common, "execution_status": status, "broker_submission_done": False,
        "orders_pending_count": 1, "submitted_orders": [{"status": order_status, "private_amount": 123456.78}],
    }}
    return key, claim, outcome


def _probe(source, target, *, store=None, replies=None, env=None, factory=None):
    store = Store() if store is None else store
    events = []
    replies = [_service(), _revision(target, env), _service(), _service()] if replies is None else replies

    def describe(resource, name, project, region):
        events.append((resource, name, project, region))
        response = replies.pop(0)
        if isinstance(response, Exception):
            raise response
        return copy.deepcopy(response)

    def create(**kwargs):
        events.append(("store", kwargs))
        return store if factory is None else factory(**kwargs)

    return reader.diagnose_state(source, describe=describe, store_factory=create), events, store


def _no_authority(result):
    for key in ("broker_requested", "token_read", "reports_read", "cloud_write_attempted", "claims_changed",
                "breaker_reset", "trading_state_changed", "grant_exclusivity_verified", "recovery_qualified", "execution_enabled_by_check"):
        assert result[key] is False
    assert result["ledger"]["full_history_verified"] is False
    assert result["ledger"]["unresolved_claims_verified"] is False
    assert result["ledger"]["broker_complete"] is False


def test_actual_serving_identity_and_native_namespace_gate_storage(source, target, capsys):
    result, events, store = _probe(source, target)
    assert result["status"] == "observed_state" and result["reason"] == "observed"
    assert result["serving_revision_sha256"] == hashlib.sha256(REVISION.encode()).hexdigest()
    assert all(result["identity_matches"].values())
    assert result["binding_account_matches_expected"] is True
    assert result["serving_stable_before_storage"] is True and result["serving_stable_after_storage"] is True
    assert [event[0] for event in events] == ["services", "revisions", "services", "store", "services"]
    assert events[3][1] == {"source": "SCHWAB_EXECUTION_STATE_CLOUD_URI", "uri": URI, "project": "synthetic-project"}
    assert store.events[-1] == ("ledger", {"platform": "schwab", "strategy_profile": PROFILE,
        "account_scope": ACCOUNT, "execution_mode": "live", "record_limit": 256})
    assert result["owner"] == {"read_status": "observed", "present": True, "record_valid": True, "owner_matches_strategy_profile": True}
    assert result["continuity"] == {"state": "PAUSED", "paused": True, "structured_reason_field_present": False, "reason_qualified": False}
    _no_authority(result)
    output = capsys.readouterr()
    assert output.err == "" and all(line.startswith("::add-mask::") for line in output.out.splitlines())
    encoded = json.dumps(result)
    for value in (SERVICE, REVISION, ACCOUNT, URI, "private-scope-canary", "private-selector-canary", "private-baseline-canary", "123456.78"):
        assert value not in encoded


@pytest.mark.parametrize("field,value", [("service_name", "other-service"), ("account_scope", "other"),
    ("account_selector", ["other"]), ("deployment_selector", "other"), ("strategy_profile", "other"), ("platform_id", "other")])
def test_each_identity_mismatch_stops_before_storage(source, target, field, value):
    result, events, store = _probe(source, {**target, field: value})
    assert result["status"] == "unavailable" and result["storage_read_attempted"] is False
    assert len(events) == 2 and store.events == []


@pytest.mark.parametrize("side", ["expected", "actual"])
@pytest.mark.parametrize("account", [None, "", " invalid ", False, "x" * 513])
def test_invalid_native_binding_never_falls_back_to_scope(source, target, side, account):
    actual = copy.deepcopy(target)
    changed = target if side == "expected" else actual
    changed["runtime_risk_limits"]["binding"]["account_hash"] = account
    source["RUNTIME_TARGET_JSON"] = json.dumps(target)
    result, events, store = _probe(source, actual)
    assert result["reason"] == "serving_account_binding_unavailable"
    assert len(events) == 2 and store.events == []


def test_different_actual_binding_and_daily_override_cannot_certify_storage(source, target):
    actual = copy.deepcopy(target)
    actual["runtime_risk_limits"]["binding"]["account_hash"] = "other-account-canary"
    source["SCHWAB_RUNTIME_DAILY_TARGET_JSON"] = json.dumps(actual)
    source["SCHWAB_EXECUTION_STATE_CLOUD_URI"] = "gs://runner-must-not-win/"
    result, events, store = _probe(source, actual)
    assert result["reason"] == "serving_account_binding_mismatch" and len(events) == 2
    assert store.events == []


@pytest.mark.parametrize("when", ["before", "after"])
def test_serving_change_preserves_partial_facts_without_qualification(source, target, when):
    replies = [_service(), _revision(target), _service(), _service()]
    replies[2 if when == "before" else 3] = _service(SERVICE + "-changed")
    result, _, store = _probe(source, target, replies=replies)
    assert result["reason"] == "serving_changed_" + when + "_storage"
    assert result["status"] == ("unavailable" if when == "before" else "partial_state")
    assert bool(store.events) is (when == "after")
    _no_authority(result)


@pytest.mark.parametrize("env,selected", [
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": URI, "SCHWAB_EXECUTION_STATE_GCS_URI": "gs://old-ignored/path"}, "SCHWAB_EXECUTION_STATE_CLOUD_URI"),
    ({"SCHWAB_EXECUTION_STATE_GCS_URI": URI}, "SCHWAB_EXECUTION_STATE_GCS_URI"),
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": "", "SCHWAB_EXECUTION_STATE_GCS_URI": "gs://old-ignored/path", "QSL_EXECUTION_REPORT_CLOUD_URI": URI}, "QSL_EXECUTION_REPORT_CLOUD_URI"),
    ({"QSL_EXECUTION_REPORT_CLOUD_URI": URI}, "QSL_EXECUTION_REPORT_CLOUD_URI"),
    ({"QSL_EXECUTION_REPORT_CLOUD_URI": "", "QSL_EXECUTION_REPORT_GCS_URI": URI}, "QSL_EXECUTION_REPORT_GCS_URI"),
    ({"EXECUTION_REPORT_CLOUD_URI": URI}, "EXECUTION_REPORT_CLOUD_URI"),
    ({"EXECUTION_REPORT_GCS_URI": URI}, "EXECUTION_REPORT_GCS_URI"),
])
def test_original_actual_environment_storage_priority(source, target, env, selected):
    source["SCHWAB_EXECUTION_STATE_CLOUD_URI"] = "gs://runner-must-not-win/path"
    result, events, _ = _probe(source, target, env=env)
    assert result["status"] == "observed_state" and result["storage_source"] == selected
    assert events[3][1]["uri"] == URI


@pytest.mark.parametrize("env,reason", [
    ({}, "storage_source_unavailable"),
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": "", "SCHWAB_EXECUTION_STATE_GCS_URI": URI}, "storage_source_unavailable"),
    ({"EXECUTION_REPORT_CLOUD_URI": "", "EXECUTION_REPORT_GCS_URI": URI}, "storage_source_unavailable"),
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": {"valueFrom": {"secretKeyRef": {"name": "private-secret-canary"}}}}, "storage_source_secret_reference"),
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": "s3://other/path"}, "storage_source_invalid"),
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": URI + "?query"}, "storage_source_invalid"),
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": " " + URI}, "storage_source_invalid"),
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": URI, "QSL_CLOUD_PROVIDER": "local"}, "storage_provider_unavailable"),
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": URI, "QSL_CLOUD_PROVIDER": ""}, "storage_provider_unavailable"),
    ({"SCHWAB_EXECUTION_STATE_CLOUD_URI": URI, "QSL_CLOUD_PROVIDER": {"valueFrom": {}}}, "storage_provider_unavailable"),
])
def test_absent_ambiguous_or_unreadable_source_never_guesses_storage(source, target, env, reason):
    result, events, store = _probe(source, target, env=env)
    assert result["reason"] == reason and result["storage_read_attempted"] is False
    assert len(events) == 2 and store.events == []
    assert result["ledger"]["records_count"] is None


@pytest.mark.parametrize("present,owner,expected", [(False, None, None), (True, PROFILE, True), (True, "other-owner-canary", False)])
def test_owner_presence_and_same_owner_are_not_a_grant_lease(source, target, present, owner, expected):
    result, _, store = _probe(source, target, store=Store(present=present, owner=_owner(owner)))
    assert result["status"] == "observed_state" and result["owner"]["present"] is present
    assert result["owner"]["owner_matches_strategy_profile"] is expected
    assert sum(event[0] == "owner" for event in store.events) == int(present)
    _no_authority(result)


@pytest.mark.parametrize("payload", [{}, {"schema_version": "other"}, [], {"schema_version": "execution_claim.v1", "state": "claimed", "metadata": {"owner_id": PROFILE}}])
def test_present_but_invalid_owner_does_not_become_absent_or_retry(source, target, payload):
    result, events, store = _probe(source, target, store=Store(owner=payload))
    assert result["reason"] == "owner_record_invalid" and result["owner"]["present"] is None
    assert result["ledger"]["records_count"] is None
    assert [event[0] for event in store.events] == ["exists", "owner"] and len(events) == 4


def test_owner_disappearing_after_exists_is_unknown_not_absent(source, target):
    store = Store()
    store.owner = None
    result, _, _ = _probe(source, target, store=store)
    assert result["reason"] == "owner_record_invalid"
    assert result["owner"]["present"] is None and result["ledger"]["records_count"] is None


def test_final_metadata_failure_keeps_read_state_partial_without_retry(source, target):
    result, events, _ = _probe(source, target, replies=[_service(), _revision(target), _service(), RuntimeError("private-cloud-error-canary")])
    assert result["reason"] == "metadata_unavailable" and result["status"] == "partial_state"
    assert result["ledger"]["records_count"] == 2 and result["owner"]["read_status"] == "observed"
    assert result["serving_stable_after_storage"] is None and len(events) == 5
    assert "private-cloud-error" not in json.dumps(result)
    _no_authority(result)


@pytest.mark.parametrize("stage", ["owner", "ledger"])
def test_read_failure_retains_unknown_and_observed_partial_without_retry(source, target, stage, capsys):
    result, events, store = _probe(source, target, store=Store(error=stage))
    assert result["reason"] == stage + "_read_failed"
    assert result["status"] == ("unavailable" if stage == "owner" else "partial_state")
    assert result["ledger"]["records_count"] is None and result["ledger"]["sha256"] is None
    assert len(events) == 4
    assert sum(event[0] == stage for event in store.events) <= 1
    assert "private-storage-error-canary" not in json.dumps(result)
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("digest", [("private-digest-canary", 1), ("a" * 64, -1), ("a" * 64, 513), ("a" * 64, True)])
def test_invalid_ledger_result_does_not_escape_or_qualify(source, target, digest):
    result, _, _ = _probe(source, target, store=Store(digest=digest))
    assert result["reason"] == "ledger_result_invalid" and result["status"] == "partial_state"
    assert result["ledger"]["records_count"] is None


def test_dependency_output_and_logging_are_suppressed_and_restored(source, target, capsys):
    previous = logging.root.manager.disable

    def factory(**_kwargs):
        print("private-stdout-canary")
        print("private-stderr-canary", file=sys.stderr)
        logging.getLogger("private").critical("private-logging-canary")
        return Store()

    result, _, _ = _probe(source, target, factory=factory)
    assert result["status"] == "observed_state" and logging.root.manager.disable == previous
    output = capsys.readouterr()
    assert output.err == "" and "private-stdout" not in output.out and "private-logging" not in output.out


@pytest.mark.parametrize("records", [0, 1, 300])
def test_original_cloud_only_store_reads_owner_and_native_scoped_bounded_ledger(source, target, monkeypatch, records):
    import socket

    import quant_platform_kit.cloud as cloud
    from quant_platform_kit.common.execution_state import ExecutionMarkerStore

    original = ExecutionMarkerStore(local_dir=None, cloud_prefix_uri=URI)
    objects = {original._cloud_uri(_owner()["marker_key"]): json.dumps(_owner())}
    for index in range(records):
        key, claim, outcome = _records(signal=f"{index:04d}")
        objects[original._cloud_uri(key)] = json.dumps(claim)
        objects[original._outcome_cloud_uri(key)] = json.dumps(outcome)
    # A wrong control-plane namespace must not leak into this account's digest.
    objects[URI + "/execution_markers/v1/schwab/private-scope-canary/other.json"] = "unrelated"
    calls = []

    class Backend:
        def exists(self, uri):
            calls.append(("exists", uri))
            return uri in objects

        def read_text(self, uri):
            calls.append(("read", uri))
            assert uri in objects
            return objects[uri]

        def list(self, prefix):
            calls.append(("list", prefix))
            assert "/" + ACCOUNT + "/" in prefix
            return [uri for uri in objects if uri.startswith(prefix)]

        def __getattr__(self, name):
            pytest.fail("unexpected storage operation: " + name)

    monkeypatch.setattr(cloud, "_PROVIDER", "gcp")
    monkeypatch.setattr(cloud, "get_object_store", lambda **_kwargs: Backend())
    monkeypatch.setattr(socket, "socket", lambda *_a, **_kw: pytest.fail("unexpected external connection"))
    result, _, _ = _probe(source, target, factory=reader._existing_store)
    assert result["status"] == "observed_state"
    assert result["ledger"]["records_count"] == 2 * min(256, records)
    assert sum(call[0] == "list" for call in calls) == 6
    assert sum(call[0] == "read" for call in calls) == 1 + 6 * min(256, records)
    classification = result["ledger"]["classification"]
    assert classification["claim_records_count"] == classification["outcome_records_count"] == min(256, records)
    assert classification["paired_count"] == classification["explicit_unknown_outcomes_count"] == min(256, records)
    assert classification["complete"] is True
    _no_authority(result)
    assert "private_amount" not in json.dumps(result) and "UNKNOWN" not in json.dumps(result)


def test_original_factory_disables_local_fallback_and_rejects_runner_provider(monkeypatch):
    import quant_platform_kit.cloud as cloud

    monkeypatch.setattr(cloud, "_PROVIDER", "gcp")
    store = reader._existing_store(source="SCHWAB_EXECUTION_STATE_CLOUD_URI", uri=URI, project="synthetic-project")
    assert store.local_dir is None and store.cloud_prefix_uri == URI and store.project_id == "synthetic-project"
    monkeypatch.setattr(cloud, "_PROVIDER", "local")
    with pytest.raises(reader.metadata._Unavailable, match="storage_provider_unavailable"):
        reader._existing_store(source="SCHWAB_EXECUTION_STATE_CLOUD_URI", uri=URI, project="synthetic-project")


def _entry_store(records):
    store = Store()
    entries = {store.namespace: [], store.outcome_namespace: []}
    for namespace, key, payload in records:
        entries[namespace].append((store._cloud_uri_for_namespace(key, namespace),
                                   payload if isinstance(payload, str) else json.dumps(payload)))
    store._entries = lambda: entries
    return store


def test_pairing_counts_are_only_within_recent_window_and_unknown_is_not_resolved(source, target):
    first, claim, unknown = _records()
    second, claim2, _ = _records(signal="2026-10-23")
    third, _, orphan = _records(signal="2026-10-22", order_status="accepted")
    store = _entry_store([(Store.namespace, first, claim), (Store.namespace, second, claim2),
                          (Store.outcome_namespace, first, unknown), (Store.outcome_namespace, third, orphan)])
    result, _, _ = _probe(source, target, store=store)
    counts = result["ledger"]["classification"]
    assert result["status"] == "observed_state" and counts["complete"] is True
    assert counts["claim_records_count"] == counts["outcome_records_count"] == 2
    assert counts["paired_count"] == 1
    assert counts["claims_without_outcome_in_window_count"] == counts["outcomes_without_claim_in_window_count"] == 1
    assert counts["explicit_unknown_outcomes_count"] == counts["pending_reconciliation_outcomes_count"] == 1
    assert counts["recorded_no_pending_outcomes_count"] == counts["invalid_records_count"] == 0
    _no_authority(result)
    assert first not in json.dumps(result) and second not in json.dumps(result)


@pytest.mark.parametrize("status", ["completed", "no_action", "no_op"])
@pytest.mark.parametrize("signal,effective", [("2026-10-24", "2026-10-25"), ("", "2026-10-25"), ("2026-10-24", "")])
def test_only_original_replay_empty_no_pending_contract_is_classified(source, target, status, signal, effective):
    key, claim, outcome = _records(status=status, signal=signal, effective=effective)
    outcome["metadata"].update(orders_pending_count=0, submitted_orders=[])
    result, _, _ = _probe(source, target, store=_entry_store([
        (Store.namespace, key, claim), (Store.outcome_namespace, key, outcome)]))
    counts = result["ledger"]["classification"]
    assert counts["recorded_no_pending_outcomes_count"] == 1 and counts["complete"] is True
    _no_authority(result)


@pytest.mark.parametrize("status,orders", [("executed", []), ("submitted", []), ("UNKNOWN", []),
    ("private-unrecognized-status-canary", []), ("completed", [{"status": "accepted"}])])
def test_unrecognized_or_nonempty_terminal_states_remain_unclassified_partial(source, target, status, orders):
    key, claim, outcome = _records(status=status)
    outcome["metadata"].update(orders_pending_count=0, submitted_orders=orders)
    result, events, _ = _probe(source, target, store=_entry_store([
        (Store.namespace, key, claim), (Store.outcome_namespace, key, outcome)]))
    counts = result["ledger"]["classification"]
    assert result["status"] == "partial_state" and result["reason"] == "ledger_classification_incomplete"
    assert counts["unclassified_outcomes_count"] == 1 and counts["complete"] is False
    assert counts["recorded_no_pending_outcomes_count"] == 0 and len(events) == 5
    assert status not in json.dumps(result) if "private" in status else True
    _no_authority(result)


@pytest.mark.parametrize("fault", ["json", "duplicate_json", "wrong_key", "wrong_account", "wrong_profile", "dry_run", "invalid_count", "invalid_pending", "wrong_dates", "legacy_schema", "duplicate_entry"])
def test_invalid_records_do_not_turn_subset_pairing_or_unknown_into_exact_counts(source, target, fault):
    key, claim, outcome = _records()
    if fault == "wrong_key":
        outcome["marker_key"] += "/other"
    elif fault == "wrong_account":
        outcome["metadata"]["account_scope"] = "other-account-canary"
    elif fault == "wrong_profile":
        outcome["metadata"]["strategy_profile"] = "other-profile"
    elif fault == "dry_run":
        outcome["metadata"]["dry_run_only"] = True
    elif fault == "invalid_count":
        outcome["metadata"]["orders_pending_count"] = True
    elif fault == "invalid_pending":
        outcome["metadata"]["orders_pending_count"] = 0
    elif fault == "wrong_dates":
        outcome["metadata"]["signal_date"] = "wrong-date-canary"
    elif fault == "legacy_schema":
        outcome["schema_version"] = "execution_marker.v1"
    elif fault in {"json", "duplicate_json"}:
        outcome = "private-malformed-canary" if fault == "json" else '{"marker_key":"private-a","marker_key":"private-b"}'
    values = [(Store.namespace, key, claim), (Store.outcome_namespace, key, outcome)]
    if fault == "duplicate_entry":
        values.append((Store.outcome_namespace, key, outcome))
    result, _, _ = _probe(source, target, store=_entry_store(values))
    counts = result["ledger"]["classification"]
    assert result["status"] == "partial_state" and counts["invalid_records_count"] == 1
    assert counts["paired_count"] is None and counts["explicit_unknown_outcomes_count"] is None
    assert counts["complete"] is False and "canary" not in json.dumps(result)


@pytest.mark.parametrize("change", ["before_after", "middle_only"])
def test_changed_window_is_partial_even_when_endpoint_digests_match(source, target, change):
    store = Store()
    original = store.calculate_recent_ledger_digest(platform="schwab")
    if change == "middle_only":
        changed = copy.deepcopy(store._entries())
        namespace = Store.outcome_namespace
        uri, content = changed[namespace][0]
        changed[namespace][0] = (uri, content + " ")
        store._entries = lambda: changed
        store.digest = original
    else:
        original_method = store.calculate_recent_ledger_digest
        calls = []

        def digest(**kwargs):
            calls.append(1)
            return original_method(**kwargs) if len(calls) == 1 else ("f" * 64, 2)

        store.calculate_recent_ledger_digest = digest
    result, _, _ = _probe(source, target, store=store)
    counts = result["ledger"]["classification"]
    assert result["reason"] == "ledger_window_changed" and result["status"] == "partial_state"
    assert counts["window_stable"] is False and counts["complete"] is False
    assert counts["paired_count"] is None and counts["invalid_records_count"] is None
    _no_authority(result)


def test_second_namespace_failure_keeps_first_count_and_other_fields_unknown(source, target):
    store = Store()
    original = store._recent_ledger_entries

    def entries(**kwargs):
        if kwargs["namespace"] == store.outcome_namespace:
            raise PermissionError("private-ledger-read-canary")
        return original(**kwargs)

    store._recent_ledger_entries = entries
    result, _, _ = _probe(source, target, store=store)
    counts = result["ledger"]["classification"]
    assert result["reason"] == "ledger_read_failed" and result["status"] == "partial_state"
    assert counts["claim_records_count"] == 1 and counts["outcome_records_count"] is None
    assert counts["paired_count"] is None and counts["explicit_unknown_outcomes_count"] is None
    assert "canary" not in json.dumps(result)


def test_safe_artifact_is_exclusive_private_and_arguments_cannot_select_sources(source, target, monkeypatch, tmp_path, capsys):
    result, _, _ = _probe(source, target)
    output = tmp_path / "safe.json"
    monkeypatch.setattr(reader, "diagnose_state", lambda _env: result)
    monkeypatch.setattr(sys, "argv", ["diagnose_pilot_state"])
    monkeypatch.setenv("PILOT_STATE_RESULT_PATH", str(output))
    assert reader.main() == 0
    assert json.loads(output.read_text()) == result
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    assert reader.main() == 1
    assert json.loads(output.read_text()) == result
    output.unlink()
    monkeypatch.setattr(sys, "argv", ["diagnose_pilot_state", "--service=untrusted"])
    assert reader.main() == 1
    assert json.loads(output.read_text())["reason"] == "arguments_not_supported"
    assert capsys.readouterr().err == ""


def test_workflows_keep_original_protected_sources_without_dispatch_overrides():
    wrapper = (ROOT / ".github/workflows/pilot-state-readback.yml").read_text()
    protected = (ROOT / ".github/workflows/pilot-state-readback-protected.yml").read_text()
    for workflow in (wrapper, protected):
        assert "github.repository == 'QuantStrategyLab/CharlesSchwabPlatform'" in workflow
        assert "github.ref == 'refs/heads/main'" in workflow and "github.run_attempt == 1" in workflow
        assert "inputs:" not in workflow and "jobs:\n  read_state:" in workflow
    assert "vars.CLOUD_RUN_SERVICE_TARGETS_JSON || secrets.CLOUD_RUN_SERVICE_TARGETS_JSON" in wrapper
    assert "vars.RUNTIME_TARGET_JSON || secrets.RUNTIME_TARGET_JSON" in wrapper
    assert "uses: ./.github/workflows/pilot-state-readback-protected.yml" in wrapper
    assert "--bootstrap-cloud-source" in protected and "--frozen --no-dev" in protected
    assert protected.count("secrets.runtime_target") == 1 and "vars." not in protected
    assert "-m scripts.diagnose_pilot_state" in protected
    for forbidden in ("gcloud run deploy", "renew-access-token", "schwab_native_identity --", "scheduler", "gsutil"):
        assert forbidden not in protected
