"""Synthetic only: no credentials, subprocesses, buckets or HTTP destinations used."""

import copy
import datetime as dt
import io
import json
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

from quant_platform_kit.common.execution_receipts import build_execution_receipt
from scripts import publish_runtime_daily_from_reports as caller
from scripts.publish_account_facts_from_reports import _binding_id
from test_runtime_daily_report_projection import NOW, TARGET, report as make_report

PREFIX = (
    "gs://synthetic-private/execution-reports/charles_schwab/soxl_soxx_trend_income/"
)
REVISION = "charles-schwab-quant-service-00001-test"
HASH = "PRIVATE-ACCOUNT-hash-CaseSensitive"
ACCOUNT = "synthetic-ui-account"


def environment():
    return {
        "CLOUD_RUN_SERVICE": TARGET["service"],
        "RUNTIME_TARGET_JSON": json.dumps(
            {
                **TARGET,
                "account_selector": ["live"],
                "runtime_risk_limits": {"binding": {"account_hash": HASH}},
                "scheduler": {
                    "main_time": "0 16 * * 1-5",
                    "timezone": "America/New_York",
                },
                "market_calendar": "NYSE",
                "market_timezone": "America/New_York",
            }
        ),
        "EXECUTION_EVIDENCE_SYNC_URL": caller.SYNC_URL,
        "EXECUTION_EVIDENCE_SYNC_TOKEN": "PRIVATE-TOKEN",
    }


def envelope(outcome="no_signal", stamp="20261006T200100Z"):
    value = make_report(outcome)
    instant = dt.datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(
        tzinfo=dt.timezone.utc
    )
    value.update(
        run_id=stamp,
        started_at=instant.isoformat(),
        finished_at=(instant + dt.timedelta(minutes=1)).isoformat(),
        diagnostics={"runtime_revision": REVISION},
    )
    value["summary"]["account_observation"] = {
        "account_hash": HASH,
        "net_assets": "PRIVATE-AMOUNT",
    }
    # Receipt time is part of the same synthetic run.
    value["execution_receipt"] = build_execution_receipt(
        platform="schwab",
        strategy_profile=TARGET["strategy_profile"],
        strategy_revision="a" * 40,
        execution_mode="live",
        outcome=outcome,
        observed_at=value["finished_at"],
        broker_confirmation="not_observed" if outcome == "failed" else None,
    )
    return {
        "object_uri": PREFIX + instant.strftime("%Y-%m/") + stamp + ".json",
        "payload": value,
    }


def prepare(entries=None, **kwargs):
    arguments = dict(
        environ=environment(),
        batch=caller.ReadBatch(entries or ()),
        report_prefix=PREFIX,
        expected_runtime_revision=REVISION,
        observed_at=NOW,
        session_dates_loader=lambda *_a, **_k: {NOW.date()},
    )
    arguments.update(kwargs)
    return caller.prepare_daily(**arguments)


def ack(prepared):
    return {
        "ok": True,
        "stored": True,
        "platform": "schwab",
        "target_key": "|".join(TARGET.values()),
        "business_date": "2026-10-06",
        "account_key": ACCOUNT,
    }


class Response(io.BytesIO):
    status = 200


def publish(prepared, payload=None, **kwargs):
    response = Response(
        json.dumps(ack(prepared) if payload is None else payload).encode()
    )
    opener = Mock()
    opener.open.return_value = response
    arguments = dict(
        environ=environment(),
        publish=True,
        expected_account_key=ACCOUNT,
        opener_factory=lambda *_a: opener,
    )
    arguments.update(kwargs)
    return caller.publish_prepared(prepared, **arguments), opener


def test_default_preparation_preserves_contract_and_is_incomplete():
    result = prepare([envelope()])
    assert result.reason == "prepared"
    assert set(result.projection) == {
        "platform",
        "observed_at",
        "completeness",
        "read_errors",
        "records",
        "unmatched_reports",
    }
    assert result.projection["completeness"] == "incomplete"
    assert "coverage_unconfirmed" in result.projection["read_errors"]
    item = result.projection["records"][0]
    assert item["schedule"]["state"] == "due"
    assert item["fills"] == {"source": "not_connected", "records": [], "count": None}
    assert item["runs"][0]["activity"] == "no_signal"
    assert item["runs"][0]["source_object"] is None
    assert len(item["runs"][0]["evidence"]) == 9


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "blank",
        "report_only",
        "case",
        "ambiguous",
        "other_profile",
        "disabled",
        "bad_json",
    ],
)
def test_independent_identity_fails_closed(mutation):
    env = environment()
    config = json.loads(env["RUNTIME_TARGET_JSON"])
    if mutation in {"missing", "report_only"}:
        del config["runtime_risk_limits"]
    elif mutation == "blank":
        config["runtime_risk_limits"]["binding"]["account_hash"] = " "
    elif mutation == "case":
        config["runtime_risk_limits"]["binding"]["account_hash"] = HASH.lower()
    elif mutation == "ambiguous":
        env["CLOUD_RUN_SERVICE_TARGETS_JSON"] = json.dumps([config, config])
    elif mutation == "other_profile":
        config["strategy_profile"] = "other"
    elif mutation == "disabled":
        config["runtime_target_enabled"] = False
    env["RUNTIME_TARGET_JSON"] = (
        "{PRIVATE-ERROR" if mutation == "bad_json" else json.dumps(config)
    )
    result = prepare([envelope()], environ=env)
    assert result.reason in {"source_identity_unavailable", "source_identity_mismatch"}
    assert result.projection is None
    _, opener = publish(result, environ=env)
    opener.open.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        (
            "uri",
            PREFIX.replace("synthetic-private", "other")
            + "2026-10/20261006T200100Z.json",
        ),
        ("uri", PREFIX + "2026-09/20261006T200100Z.json"),
        ("uri", PREFIX + "2026-10/20261006T200100Z.json#12"),
        ("revision", "wrong-revision"),
        ("start", "2026-10-06T21:01:00Z"),
        ("start", "2026-10-06T20:01:00"),
        ("finish", "2026-10-06T19:00:00Z"),
        ("run_id", "20261006T200200Z"),
        ("service_name", "other"),
    ],
)
def test_bad_provenance_is_rejected_without_raw_detail(field, value):
    item = envelope()
    if field == "uri":
        item["object_uri"] = value
    elif field == "revision":
        item["payload"]["diagnostics"]["runtime_revision"] = value
    elif field in {"start", "finish"}:
        item["payload"][{"start": "started_at", "finish": "finished_at"}[field]] = value
    else:
        item["payload"][field] = value
    result = prepare([item])
    assert result.projection["records"][0]["runs"] == []
    assert "report_read_error" in result.projection["read_errors"]
    assert result.projection["completeness"] == "incomplete"


@pytest.mark.parametrize(
    "outcome,status",
    [("reconciliation_required", "reconciliation_required"), ("failed", "failed")],
)
def test_anomalies_survive_incomplete_coverage_and_schedule(outcome, status):
    env = environment()
    config = json.loads(env["RUNTIME_TARGET_JSON"])
    config.pop("scheduler")
    env["RUNTIME_TARGET_JSON"] = json.dumps(config)
    result = prepare([envelope(outcome)], environ=env)
    assert result.projection["records"][0]["status"] == status
    assert result.projection["records"][0]["schedule"]["state"] == "unevaluable"


def test_prior_unresolved_is_not_hidden_by_current_good_report():
    prior = envelope("reconciliation_required", "20261005T200100Z")
    prior["payload"]["summary"]["orders_pending_count"] = 1
    result = prepare([envelope(), prior])
    assert result.projection["records"][0]["status"] == "reconciliation_required"
    assert result.projection["completeness"] == "incomplete"


@pytest.mark.parametrize(
    "change", ["calendar_failure", "no_calendar", "no_due", "grace_open", "bad_cron"]
)
def test_schedule_uncertainty_never_becomes_not_due(change):
    env = environment()
    config = json.loads(env["RUNTIME_TARGET_JSON"])

    def loader(*_a, **_k):
        return {NOW.date()}

    if change == "calendar_failure":
        loader = Mock(side_effect=RuntimeError("PRIVATE-CALENDAR"))
    if change == "no_calendar":
        config["market_calendar"] = ""
    if change == "no_due":
        config["scheduler"]["main_time"] = "0 22 * * *"
    if change == "grace_open":
        config["scheduler"]["main_time"] = "45 16 * * *"
    if change == "bad_cron":
        config["scheduler"]["main_time"] = "invalid"
    env["RUNTIME_TARGET_JSON"] = json.dumps(config)
    item = prepare([envelope()], environ=env, session_dates_loader=loader).projection[
        "records"
    ][0]
    assert item["schedule"]["state"] == "unevaluable"


def test_twenty_item_budget_never_promotes_a_truncated_day():
    batch = caller.ReadBatch(
        [envelope(stamp=f"20261006T20{i:02d}00Z") for i in range(21)]
    )
    result = prepare(batch=batch)
    assert len(result.projection["records"][0]["runs"]) <= 20
    assert result.projection["completeness"] == "incomplete"
    assert "report_read_error" in result.projection["read_errors"]


def test_failed_read_is_an_explicit_incomplete_projection():
    result = prepare(batch=caller.ReadBatch((), read_failed=True))
    assert result.projection["read_errors"] == [
        "report_read_error",
        "coverage_unconfirmed",
    ]


def test_default_publish_is_zero_posts_and_canonical_header_only_on_explicit_publish():
    prepared = prepare([envelope()])
    opener = Mock()
    result = caller.publish_prepared(
        prepared, environ=environment(), opener_factory=opener
    )
    assert result == {"status": "prepared", "reason": "publication_not_requested"}
    opener.assert_not_called()
    result, opener = publish(prepared)
    assert result["status"] == "stored_acknowledged"
    request = opener.open.call_args.args[0]
    assert request.get_header("X-qsl-source-binding-id") == _binding_id(
        HASH, TARGET["service"]
    )
    assert request.full_url == caller.SYNC_URL
    assert request.get_header("Authorization") == "Bearer PRIVATE-TOKEN"
    assert json.loads(request.data) == prepared.projection
    opener.open.assert_called_once()


@pytest.mark.parametrize(
    "key,value",
    [
        ("platform", "longbridge"),
        ("target_key", "other"),
        ("business_date", "2026-10-05"),
        ("account_key", "wrong"),
        ("stored", False),
        ("ok", False),
        ("account_key", ""),
    ],
)
def test_wrong_ack_never_confirms_storage(key, value):
    prepared = prepare([envelope()])
    payload = ack(prepared)
    payload[key] = value
    result, opener = publish(prepared, payload=payload)
    assert result["status"] == "unconfirmed"
    assert result["reason"] == "ack_invalid"
    opener.open.assert_called_once()


@pytest.mark.parametrize(
    "error",
    [
        TimeoutError("PRIVATE-TIMEOUT"),
        OSError("PRIVATE-OS"),
        HTTPError(caller.SYNC_URL, 302, "PRIVATE-REDIRECT", {}, None),
        HTTPError(caller.SYNC_URL, 409, "PRIVATE-DRIFT", {}, None),
    ],
)
def test_no_retry_on_any_ambiguous_transport(error):
    prepared = prepare([envelope()])
    opener = Mock()
    opener.open.side_effect = error
    result = caller.publish_prepared(
        prepared,
        environ=environment(),
        publish=True,
        expected_account_key=ACCOUNT,
        opener_factory=lambda *_a: opener,
    )
    assert result["status"] == "unconfirmed"
    assert "PRIVATE" not in json.dumps(result)
    opener.open.assert_called_once()


def test_header_drift_and_missing_token_are_preflight_failures():
    prepared = prepare([envelope()])
    for change in ["hash", "token", "url"]:
        env = environment()
        if change == "hash":
            env["RUNTIME_TARGET_JSON"] = env["RUNTIME_TARGET_JSON"].replace(
                HASH, "new-hash"
            )
        if change == "token":
            env.pop("EXECUTION_EVIDENCE_SYNC_TOKEN")
            env["SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN"] = "PRIVATE-FALLBACK"
        if change == "url":
            env["EXECUTION_EVIDENCE_SYNC_URL"] = caller.SYNC_URL.replace(
                "https:", "http:"
            )
        result, opener = publish(prepared, environ=env)
        assert result["status"] == "skipped"
        opener.open.assert_not_called()


def test_bounded_body_and_ack():
    prepared = prepare([envelope()])
    prepared.projection["read_errors"] = ["coverage_unconfirmed"] * 21
    result, opener = publish(prepared)
    assert result["reason"] == "projection_budget_exceeded"
    opener.open.assert_not_called()
    prepared = prepare([envelope()])
    prepared.projection["records"][0]["runs"][0]["run_id"] = "x" * 65536
    result, opener = publish(prepared)
    assert result["reason"] == "projection_budget_exceeded"
    opener.open.assert_not_called()
    result, opener = publish(prepare([envelope()]), payload={"private": "x" * 65536})
    assert result["reason"] == "ack_invalid"
    assert caller._NoRedirect().redirect_request(None, None) is None


def test_default_main_never_invokes_cloud_or_post(monkeypatch, capsys):
    monkeypatch.setattr(
        caller, "read_archive", Mock(side_effect=AssertionError("cloud invoked"))
    )
    monkeypatch.setattr(
        caller, "publish_prepared", Mock(side_effect=AssertionError("post invoked"))
    )
    monkeypatch.setattr(caller.os, "environ", environment())
    assert caller.main([]) == 0
    assert capsys.readouterr().out == "prepared:coverage_unconfirmed\n"


def test_private_values_never_escape_projection_repr_or_status(capsys):
    entry = envelope("failed")
    entry["payload"]["errors"] = ["PRIVATE-ERROR"]
    entry["payload"]["summary"]["orders"] = [
        {"symbol": "PRIVATE-SYMBOL", "id": "PRIVATE-ORDER"}
    ]
    prepared = prepare([entry])
    result, _ = publish(prepared)
    safe = (
        json.dumps(prepared.projection)
        + repr(prepared)
        + json.dumps(result)
        + capsys.readouterr().out
    )
    for value in [
        HASH,
        PREFIX,
        _binding_id(HASH, TARGET["service"]),
        ACCOUNT,
        "PRIVATE-ERROR",
        "PRIVATE-AMOUNT",
        "PRIVATE-TOKEN",
        "PRIVATE-SYMBOL",
        "PRIVATE-ORDER",
    ]:
        assert value not in safe


class Blob:
    def __init__(self, entry):
        self.name = entry["object_uri"].split("/", 3)[3]
        self.data = json.dumps(entry["payload"]).encode()
        self.size = len(self.data)
        self.generation = 1
        self.content_encoding = None
        self.download_as_bytes = Mock(return_value=self.data)


def test_bounded_reader_has_no_completeness_override_and_caps_network_reads():
    blobs = [Blob(envelope(stamp=f"20261006T20{i:02d}00Z")) for i in range(21)]
    client = Mock()
    client.list_blobs.side_effect = [iter(blobs), iter(())]
    batch = caller.read_archive(report_prefix=PREFIX, observed_at=NOW, client=client)
    assert len(batch.entries) == 20 and batch.truncated
    assert sum(b.download_as_bytes.call_count for b in blobs) == 20
    for call in client.list_blobs.call_args_list:
        assert call.kwargs["max_results"] <= 21 and call.kwargs["retry"] is None
    for blob in blobs[:20]:
        assert blob.download_as_bytes.call_args.kwargs["end"] == caller.MAX_REPORT_BYTES
        assert blob.download_as_bytes.call_args.kwargs["retry"] is None
    assert prepare(batch=batch).projection["completeness"] == "incomplete"


def test_reader_drops_oversized_failed_and_foreign_objects_without_logging(capsys):
    oversized = Blob(envelope())
    oversized.size = caller.MAX_REPORT_BYTES + 1
    failed = Blob(envelope(stamp="20261006T200200Z"))
    failed.download_as_bytes.side_effect = OSError("PRIVATE-READ")
    invalid = Blob(envelope(stamp="20261006T200300Z"))
    invalid.name = "other/private.json"
    client = Mock()
    client.list_blobs.side_effect = [iter([oversized, failed, invalid]), iter(())]
    batch = caller.read_archive(report_prefix=PREFIX, observed_at=NOW, client=client)
    assert not batch.entries and batch.read_failed
    oversized.download_as_bytes.assert_not_called()
    invalid.download_as_bytes.assert_not_called()
    assert capsys.readouterr().out == capsys.readouterr().err == ""


def test_receiver_alias_is_reported_without_inventing_an_independent_identity():
    prepared = prepare([envelope()])
    payload = ack(prepared)
    payload["account_key"] = "receiver-canonical-alias"
    result, opener = publish(prepared, payload=payload, expected_account_key=None)
    assert result == {
        "status": "stored_acknowledged",
        "account_attribution": "receiver_reported_account",
    }
    assert "receiver-canonical-alias" not in json.dumps(result)
    opener.open.assert_called_once()


@pytest.mark.parametrize("value", ["120", "-1", "NaN", "Infinity", "PRIVATE-GRACE"])
def test_existing_grace_configuration_is_respected_or_fails_closed(value):
    env = environment()
    env["RUNTIME_HEARTBEAT_PUBLICATION_GRACE_MINUTES"] = value
    result = prepare([envelope()], environ=env)
    assert result.projection["records"][0]["schedule"]["state"] == "unevaluable"


def test_due_mapping_respects_existing_shorter_publication_grace():
    env = environment()
    env["RUNTIME_HEARTBEAT_PUBLICATION_GRACE_MINUTES"] = "15"
    item = prepare([envelope()], environ=env).projection["records"][0]
    assert item["schedule"]["grace_ends_at"] == "2026-10-06T20:15:00Z"


def test_list_failure_or_partial_page_never_becomes_complete(capsys):
    blob = Blob(envelope())

    def pages():
        yield blob
        raise OSError("PRIVATE-PAGINATION")

    client = Mock()
    client.list_blobs.return_value = pages()
    batch = caller.read_archive(report_prefix=PREFIX, observed_at=NOW, client=client)
    assert batch.read_failed and len(batch.entries) == 1
    assert prepare(batch=batch).projection["completeness"] == "incomplete"
    assert capsys.readouterr().err == ""


def test_successful_empty_enumeration_does_not_prove_historical_retention():
    client = Mock()
    client.list_blobs.side_effect = [iter(()), iter(())]
    batch = caller.read_archive(report_prefix=PREFIX, observed_at=NOW, client=client)
    assert not batch.read_failed and not batch.truncated
    assert prepare(batch=batch).projection["completeness"] == "incomplete"
    assert client.list_blobs.call_count == 2


def test_bad_reader_body_remains_incomplete():
    for content in [b"not JSON", b"[]", b"x" * (caller.MAX_REPORT_BYTES + 1)]:
        blob = Blob(envelope())
        blob.download_as_bytes.return_value = content
        client = Mock()
        client.list_blobs.side_effect = [iter([blob]), iter(())]
        batch = caller.read_archive(
            report_prefix=PREFIX, observed_at=NOW, client=client
        )
        assert not batch.entries and batch.read_failed


def test_preparation_matches_merged_pure_adapter_body_exactly():
    from scripts.runtime_daily_report_projection import project_daily_runtime

    env = environment()
    entry = envelope()
    prepared = prepare([entry])
    _, policy = caller._select_identity(env)
    expected = project_daily_runtime(
        target=TARGET,
        reports=[entry],
        observed_at=NOW,
        coverage_complete=False,
        schedule_facts=caller.matured_schedule(
            policy, observed_at=NOW, session_dates_loader=lambda *_a, **_k: {NOW.date()}
        ),
    )
    assert prepared.projection == expected


def test_unsupported_cli_arguments_do_not_echo_private_values(capsys):
    assert caller.main(["--coverage-complete", "PRIVATE-CLI"]) == 2
    assert capsys.readouterr().out == "skipped:unsupported_arguments\n"


@pytest.mark.parametrize(
    "mutation",
    ["top_level_private", "nested_private", "changed_date", "changed_status"],
)
def test_mutated_preparation_is_rejected_before_transport(mutation):
    prepared = prepare([envelope()])
    if mutation == "top_level_private":
        prepared.projection["private_report"] = "PRIVATE-MUTATION-SENTINEL"
    elif mutation == "nested_private":
        prepared.projection["records"][0]["runs"][0]["private_account"] = (
            "PRIVATE-MUTATION-SENTINEL"
        )
    elif mutation == "changed_date":
        prepared.projection["records"][0]["business_date"] = "2026-10-05"
    else:
        prepared.projection["records"][0]["status"] = "filled"
    result, opener = publish(prepared)
    opener.open.assert_not_called()
    assert result == {"status": "skipped", "reason": "projection_changed"}
    assert "PRIVATE-MUTATION-SENTINEL" not in json.dumps(result)


def test_handmade_prepared_object_without_freeze_evidence_never_posts():
    original = prepare([envelope()])
    handmade = caller.PreparedDaily(
        "prepared", copy.deepcopy(original.projection), original._source_binding_id
    )
    result, opener = publish(handmade)
    opener.open.assert_not_called()
    assert result == {"status": "skipped", "reason": "projection_changed"}


def test_request_and_ack_use_the_verified_snapshot_even_if_exposed_dict_changes():
    prepared = prepare([envelope()])
    expected = copy.deepcopy(prepared.projection)
    opener = Mock()
    opener.open.return_value = Response(json.dumps(ack(prepared)).encode())

    def factory(*_args):
        prepared.projection["private_report"] = "PRIVATE-LATE-MUTATION"
        prepared.projection["records"][0]["business_date"] = "2026-10-05"
        return opener

    result = caller.publish_prepared(
        prepared,
        environ=environment(),
        publish=True,
        expected_account_key=ACCOUNT,
        opener_factory=factory,
    )
    assert result["status"] == "stored_acknowledged"
    assert json.loads(opener.open.call_args.args[0].data) == expected
    assert b"PRIVATE-LATE-MUTATION" not in opener.open.call_args.args[0].data
    opener.open.assert_called_once()


@pytest.mark.parametrize(
    "mutation,expected",
    [
        ("missing_summary", "source_observation_missing"),
        ("missing_observation", "source_observation_missing"),
        ("empty_observation", "source_hash_missing"),
        ("alias_only", "source_hash_missing"),
        ("null_hash", "source_identity_invalid_shape"),
        ("boolean_hash", "source_identity_invalid_shape"),
        ("number_hash", "source_identity_invalid_shape"),
        ("list_hash", "source_identity_invalid_shape"),
        ("object_hash", "source_identity_invalid_shape"),
        ("empty_hash", "source_identity_invalid_shape"),
        ("blank_hash", "source_identity_invalid_shape"),
        ("padded_hash", "source_identity_invalid_shape"),
        ("case_mismatch", "source_identity_mismatch"),
        ("other_hash", "source_identity_mismatch"),
    ],
)
def test_report_identity_failure_categories_are_private_and_keep_the_exact_gate(
    mutation,
    expected,
    capsys,
):
    entry = envelope()
    payload = entry["payload"]
    observation = payload["summary"]["account_observation"]
    values = {
        "null_hash": None,
        "boolean_hash": True,
        "number_hash": 42,
        "list_hash": [HASH],
        "object_hash": {"private": HASH},
        "empty_hash": "",
        "blank_hash": " \t",
        "padded_hash": " " + HASH + " ",
        "case_mismatch": HASH.swapcase(),
        "other_hash": "PRIVATE-OTHER-ACCOUNT",
    }
    if mutation == "missing_summary":
        payload.pop("summary")
    elif mutation == "missing_observation":
        payload["summary"].pop("account_observation")
    elif mutation == "empty_observation":
        payload["summary"]["account_observation"] = {}
    elif mutation == "alias_only":
        observation.pop("account_hash")
        observation["account_id"] = HASH
        payload["account_hash"] = HASH
    else:
        observation["account_hash"] = values[mutation]
    prepared = prepare([entry])
    assert prepared.reason == expected and prepared.projection is None
    assert prepared._source_binding_id is None and prepared._preparation_digest is None
    result, opener = publish(prepared)
    assert result == {"status": "skipped", "reason": "projection_unavailable"}
    opener.open.assert_not_called()
    assert "PRIVATE" not in repr(prepared) + json.dumps(result)
    assert capsys.readouterr().out == capsys.readouterr().err == ""


@pytest.mark.parametrize("container", ["payload", "summary", "account_observation"])
@pytest.mark.parametrize("value", [None, [], "PRIVATE-MALFORMED", False])
@pytest.mark.parametrize("bad_first", [False, True])
def test_malformed_containers_preserve_original_partial_batch_behavior(
    container,
    value,
    bad_first,
):
    valid = envelope()
    bad = envelope(stamp="20261006T200200Z")
    if container == "payload":
        bad["payload"] = value
    elif container == "summary":
        bad["payload"]["summary"] = value
    else:
        bad["payload"]["summary"]["account_observation"] = value
    entries = [bad, valid] if bad_first else [valid, bad]
    prepared = prepare(entries)
    assert prepared.reason == "prepared"
    assert (
        prepared.projection["records"][0]["runs"]
        == prepare([valid]).projection["records"][0]["runs"]
    )
    assert "report_read_error" in prepared.projection["read_errors"]
    assert prepared.projection["completeness"] == "incomplete"
    assert prepared.projection["records"][0]["status"] == "read_incomplete"
    result, opener = publish(prepared)
    assert result["status"] == "stored_acknowledged"
    opener.open.assert_called_once()


def test_unqualified_report_is_excluded_before_its_missing_identity_is_considered():
    entry = envelope()
    entry["payload"]["summary"].pop("account_observation")
    entry["payload"]["diagnostics"]["runtime_revision"] = "other-revision"
    entry["object_uri"] = "gs://PRIVATE-FOREIGN/other.json"
    prepared = prepare([entry])
    assert prepared.reason == "prepared"
    assert prepared.projection["records"][0]["runs"] == []
    assert "report_read_error" in prepared.projection["read_errors"]
    assert prepared.projection["completeness"] == "incomplete"


@pytest.mark.parametrize(
    "observation,expected",
    [
        ({}, "source_hash_missing"),
        ({"account_hash": None}, "source_identity_invalid_shape"),
    ],
)
def test_manual_runner_relays_fixed_identity_reason_without_publishing(
    observation, expected
):
    from scripts.run_runtime_daily_from_reports import run_daily
    from scripts.runtime_daily_source_facts import SourceFacts

    entry = envelope()
    entry["payload"]["summary"]["account_observation"] = observation
    env = {
        **environment(),
        "GCP_PROJECT_ID": caller.PROJECT_ID,
        "GCP_REGION": "us-central1",
        "SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX": PREFIX,
    }
    publisher = Mock()
    result = run_daily(
        env,
        publish=True,
        observed_at=NOW,
        fact_reader=Mock(
            return_value=SourceFacts(
                "verified", REVISION, "0 16 * * 1-5", "America/New_York"
            )
        ),
        archive_reader=Mock(return_value=caller.ReadBatch([entry])),
        publisher=publisher,
    )
    assert result == {"status": "skipped", "reason": expected}
    publisher.assert_not_called()


def diagnose(entries, *, batch=None, **overrides):
    args = {
        "environ": environment(),
        "batch": batch if batch is not None else caller.ReadBatch(entries),
        "report_prefix": PREFIX,
        "expected_runtime_revision": REVISION,
        "observed_at": NOW,
    }
    args.update(overrides)
    return caller.diagnose_identity_mismatch(**args)


def mismatch_entry(*, account_hash="PRIVATE-OTHER-IDENTITY"):
    entry = envelope()
    entry["payload"]["summary"]["account_observation"]["account_hash"] = account_hash
    return entry


def diagnostic_counts(passed=0, failed=0, unknown=0, case_only=0):
    return {
        "mismatch_provenance_passed": passed,
        "mismatch_provenance_failed": failed,
        "mismatch_provenance_unknown": unknown,
        "mismatch_passed_ascii_case_only": case_only,
    }


def test_diagnostic_mixed_batch_emits_only_bounded_whitelisted_counts(capsys):
    case_only = mismatch_entry(account_hash=HASH.swapcase())
    other = mismatch_entry()
    stale = mismatch_entry()
    stale["payload"]["diagnostics"]["runtime_revision"] = "PRIVATE-OLD-REVISION"
    malformed = mismatch_entry()
    malformed["payload"]["diagnostics"] = []
    entries = [case_only, other, stale, malformed, envelope()]
    before = copy.deepcopy(entries)
    assert diagnose(entries) == diagnostic_counts(2, 1, 1, 1)
    assert entries == before
    result = diagnose(entries)
    assert all(type(value) is int and 0 <= value <= 20 for value in result.values())
    assert "PRIVATE" not in json.dumps(result)
    assert capsys.readouterr().out == capsys.readouterr().err == ""


@pytest.mark.parametrize(
    "mutation,category",
    [
        ("service", "failed"),
        ("profile", "failed"),
        ("scope", "failed"),
        ("project", "failed"),
        ("alias", "failed"),
        ("revision", "failed"),
        ("month", "failed"),
        ("run_id", "failed"),
        ("future", "failed"),
        ("time_order", "failed"),
        ("oversized", "failed"),
        ("foreign_uri", "unknown"),
        ("naive_time", "unknown"),
        ("overflow_time", "unknown"),
        ("bad_diagnostics", "unknown"),
        ("unserializable", "unknown"),
    ],
)
def test_diagnostic_non_account_gates_never_turn_failure_or_uncertainty_into_pass(
    mutation, category
):
    entry = mismatch_entry()
    payload = entry["payload"]
    if mutation == "service":
        payload["service_name"] = "other-service"
    elif mutation == "profile":
        payload["strategy_profile"] = "other-profile"
    elif mutation == "scope":
        payload["account_scope"] = "paper"
    elif mutation == "project":
        payload["project_id"] = "other-project"
    elif mutation == "alias":
        payload["runtime_target"]["service_name"] = "other-service"
    elif mutation == "revision":
        payload["diagnostics"]["runtime_revision"] = "PRIVATE-OLD-REVISION"
    elif mutation == "month":
        entry["object_uri"] = PREFIX + "2026-09/20261006T200100Z.json"
    elif mutation == "run_id":
        payload["run_id"] = "20261006T200200Z"
    elif mutation == "future":
        payload["finished_at"] = "2026-10-07T20:02:00Z"
    elif mutation == "time_order":
        payload["finished_at"] = "2026-10-06T19:00:00Z"
    elif mutation == "oversized":
        payload["PRIVATE-LARGE"] = "x" * caller.MAX_REPORT_BYTES
    elif mutation == "foreign_uri":
        entry["object_uri"] = "gs://PRIVATE-OTHER/other.json"
    elif mutation == "naive_time":
        payload["started_at"] = "2026-10-06T20:01:00"
    elif mutation == "overflow_time":
        payload["started_at"] = "999999-10-06T20:01:00Z"
    elif mutation == "bad_diagnostics":
        payload["diagnostics"] = []
    else:
        payload["PRIVATE-OBJECT"] = object()
    assert diagnose([entry]) == diagnostic_counts(**{category: 1})
    prepared = prepare([entry])
    assert prepared.reason == "prepared"
    assert prepared.projection["records"][0]["runs"] == []
    assert prepared.projection["records"][0]["status"] == "read_incomplete"
    assert "report_read_error" in prepared.projection["read_errors"]


@pytest.mark.parametrize("value", [None, False, 7, [], {}, "", " ", " " + HASH])
def test_diagnostic_invalid_hashes_are_not_counted_as_legal_mismatches(value):
    assert diagnose([mismatch_entry(account_hash=value)]) == diagnostic_counts()


def test_diagnostic_never_substitutes_response_digest_or_binding_digest_for_hash():
    entry = envelope()
    observation = entry["payload"]["summary"]["account_observation"]
    observation["source_digest_sha256"] = "PRIVATE-RESPONSE-DIGEST"
    observation["source_binding"] = {"id": "PRIVATE-COMPOUND-BINDING-DIGEST"}
    assert diagnose([entry]) == diagnostic_counts()
    observation.pop("account_hash")
    assert diagnose([entry]) == diagnostic_counts()
    observation["account_hash"] = observation["source_digest_sha256"]
    assert diagnose([entry]) == diagnostic_counts(passed=1)


def test_diagnostic_prior_date_can_pass_existing_time_gate_without_being_today():
    entry = envelope(stamp="20261005T200100Z")
    entry["payload"]["summary"]["account_observation"]["account_hash"] = "PRIVATE-OTHER"
    assert diagnose([entry]) == diagnostic_counts(passed=1)


@pytest.mark.parametrize(
    "expected,reported", [("straße", "STRASSE"), ("é", "É"), (HASH, "PRIVATE-其他")]
)
def test_diagnostic_non_ascii_is_not_declared_ascii_case_only(expected, reported):
    env = environment()
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    target["runtime_risk_limits"]["binding"]["account_hash"] = expected
    env["RUNTIME_TARGET_JSON"] = json.dumps(target)
    assert diagnose(
        [mismatch_entry(account_hash=reported)], environ=env
    ) == diagnostic_counts(passed=1)


def test_diagnostic_case_count_is_limited_to_gate_passing_mismatches():
    entry = mismatch_entry(account_hash=HASH.swapcase())
    entry["payload"]["diagnostics"]["runtime_revision"] = "older-revision"
    assert diagnose([entry]) == diagnostic_counts(failed=1)


def test_diagnostic_never_reads_beyond_twenty_or_infers_unseen_mismatches():
    entries = [mismatch_entry()] * 20 + [mismatch_entry(account_hash=HASH.swapcase())]
    batch = caller.ReadBatch(entries, read_failed=True, truncated=True)
    assert diagnose([], batch=batch) == diagnostic_counts(passed=20)
    assert batch.read_failed and batch.truncated and len(batch.entries) == 21
    iterator = Mock()
    iterator.__iter__ = Mock(
        side_effect=AssertionError("must not iterate a new source")
    )
    assert diagnose([], batch=caller.ReadBatch(iterator)) is None
    iterator.__iter__.assert_not_called()


def test_diagnostic_no_network_no_hash_derivation_and_no_container_callbacks(
    monkeypatch,
):
    def denied(*_args, **_kwargs):
        raise AssertionError("diagnostic attempted an external or identity operation")

    for name in ("read_archive", "publish_prepared", "_binding_id"):
        monkeypatch.setattr(caller, name, denied)
    monkeypatch.setattr("requests.Session.request", denied)
    monkeypatch.setattr("urllib.request.OpenerDirector.open", denied)
    monkeypatch.setattr("google.auth.default", denied)

    class HostileList(list):
        def __getitem__(self, _key):
            raise AssertionError("untrusted container callback")

    assert diagnose([], batch=caller.ReadBatch(HostileList([mismatch_entry()]))) is None
    assert diagnose([mismatch_entry()]) == diagnostic_counts(passed=1)


@pytest.mark.parametrize("change", ["identity", "prefix", "revision", "time"])
def test_diagnostic_unavailable_context_is_not_fabricated_zero_counts(change):
    args = {}
    if change == "identity":
        args["environ"] = {}
    elif change == "prefix":
        args["report_prefix"] = "gs://PRIVATE-OTHER/"
    elif change == "revision":
        args["expected_runtime_revision"] = "PRIVATE INVALID"
    else:
        args["observed_at"] = dt.datetime(2026, 10, 6)
    assert diagnose([mismatch_entry()], **args) is None


def excluded_mismatch(kind="revision"):
    entry = mismatch_entry()
    if kind == "revision":
        entry["payload"]["diagnostics"]["runtime_revision"] = "older-revision"
    elif kind == "scope":
        entry["payload"]["account_scope"] = "paper"
    elif kind == "uri":
        entry["object_uri"] = "gs://synthetic-other/outside.json"
    elif kind == "time":
        entry["payload"]["finished_at"] = "2026-10-07T20:02:00Z"
    elif kind == "oversized":
        entry["payload"]["private_large"] = "x" * caller.MAX_REPORT_BYTES
    else:
        entry["payload"] = None
    return entry


@pytest.mark.parametrize(
    "kind", ["revision", "scope", "uri", "time", "oversized", "shape"]
)
@pytest.mark.parametrize("bad_first", [False, True])
def test_unqualified_mismatch_no_longer_blocks_qualified_matching_report(
    kind, bad_first
):
    bad, good = excluded_mismatch(kind), envelope()
    entries = [bad, good] if bad_first else [good, bad]
    prepared = prepare(entries)
    assert prepared.reason == "prepared"
    record = prepared.projection["records"][0]
    assert record["runs"] == prepare([good]).projection["records"][0]["runs"]
    assert record["status"] == "read_incomplete"
    assert record["completeness"] == prepared.projection["completeness"] == "incomplete"
    assert "report_read_error" in prepared.projection["read_errors"]
    assert "coverage_unconfirmed" in prepared.projection["read_errors"]
    assert prepared._preparation_digest


@pytest.mark.parametrize(
    "failure",
    [
        "observation",
        "hash_missing",
        "hash_invalid",
        "mismatch",
        "case_only",
        "prior_day",
    ],
)
@pytest.mark.parametrize(
    "order", [(0, 1, 2), (0, 2, 1), (1, 0, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0)]
)
def test_qualified_identity_failure_stops_every_mixed_batch_order_without_post(
    failure, order
):
    qualified = envelope(
        stamp="20261005T200100Z" if failure == "prior_day" else "20261006T200200Z"
    )
    observation = qualified["payload"]["summary"]["account_observation"]
    if failure == "observation":
        qualified["payload"]["summary"].pop("account_observation")
        expected = "source_observation_missing"
    elif failure == "hash_missing":
        observation.pop("account_hash")
        expected = "source_hash_missing"
    elif failure == "hash_invalid":
        observation["account_hash"] = None
        expected = "source_identity_invalid_shape"
    else:
        observation["account_hash"] = (
            HASH.swapcase() if failure == "case_only" else "PRIVATE-OTHER"
        )
        expected = "source_identity_mismatch"
    values = [qualified, envelope(), excluded_mismatch()]
    prepared = prepare([values[i] for i in order])
    assert prepared.reason == expected and prepared.projection is None
    assert prepared._source_binding_id is None and prepared._preparation_digest is None
    result, opener = publish(prepared)
    assert result == {"status": "skipped", "reason": "projection_unavailable"}
    opener.open.assert_not_called()


def test_all_excluded_reports_remain_unavailable_incomplete_and_never_infer_zero_fills():
    prepared = prepare([excluded_mismatch("revision"), excluded_mismatch("scope")])
    assert prepared.reason == "prepared"
    record = prepared.projection["records"][0]
    assert record["runs"] == []
    assert record["status"] == "read_incomplete"
    assert record["completeness"] == prepared.projection["completeness"] == "incomplete"
    assert set(prepared.projection["read_errors"]) == {
        "report_read_error",
        "coverage_unconfirmed",
    }
    assert record["fills"]["count"] is None and record["fills"]["records"] == []


@pytest.mark.parametrize(
    "read_failed,truncated", [(True, False), (False, True), (True, True)]
)
def test_qualification_does_not_clear_source_read_errors_or_truncation(
    read_failed, truncated
):
    batch = caller.ReadBatch(
        [excluded_mismatch(), envelope()], read_failed=read_failed, truncated=truncated
    )
    prepared = prepare(batch=batch)
    assert prepared.reason == "prepared"
    assert len(prepared.projection["records"][0]["runs"]) == 1
    assert prepared.projection["completeness"] == "incomplete"
    assert "report_read_error" in prepared.projection["read_errors"]
    assert (batch.read_failed, batch.truncated) == (read_failed, truncated)


def test_qualification_never_inspects_or_admits_the_twenty_first_report():
    prepared = prepare([excluded_mismatch()] * 20 + [envelope()])
    assert prepared.reason == "prepared"
    assert prepared.projection["records"][0]["runs"] == []
    assert prepared.projection["records"][0]["status"] == "read_incomplete"
    assert "report_read_error" in prepared.projection["read_errors"]


def test_admitted_projection_inputs_all_satisfy_the_existing_gates_and_exact_identity(
    monkeypatch,
):
    projector = caller.project_daily_runtime
    seen = []

    def checked_projector(**kwargs):
        for entry in kwargs["reports"]:
            assert caller._report_provenance_passes(
                entry["payload"],
                object_uri=entry["object_uri"],
                report_prefix=PREFIX,
                expected_runtime_revision=REVISION,
                observed_at=NOW,
            )
            assert (
                entry["payload"]["summary"]["account_observation"]["account_hash"]
                == HASH
            )
            seen.append(entry)
        return projector(**kwargs)

    monkeypatch.setattr(caller, "project_daily_runtime", checked_projector)
    good = envelope()
    bad = [
        excluded_mismatch(kind)
        for kind in ["revision", "scope", "uri", "time", "oversized", "shape"]
    ]
    assert prepare([*bad, good]).reason == "prepared"
    assert seen == [good]
