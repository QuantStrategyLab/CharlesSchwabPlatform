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
