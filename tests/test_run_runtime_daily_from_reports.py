"""Synthetic orchestration only; no ADC, cloud source or publish transport is used."""

import copy
import datetime as dt
import json
import re
from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import publish_runtime_daily_from_reports as caller
from scripts import run_runtime_daily_from_reports as runner
from scripts import runtime_daily_source_facts as facts
from test_publish_runtime_daily_from_reports import NOW, PREFIX, REVISION, envelope
from test_publish_runtime_daily_from_reports import Response as AckResponse
from test_publish_runtime_daily_from_reports import ack
from test_publish_runtime_daily_from_reports import environment as source_environment

ROOT = Path(__file__).resolve().parents[1]
SERVICE = caller.TARGET["service"]
RESOURCE = f"projects/charlesschwabquant/locations/us-central1/services/{SERVICE}"
SERVICE_URI = "https://synthetic-service.us-central1.run.app"


def test_transactions_once_dispatch_is_exclusive(monkeypatch):
    from scripts import capture_schwab_transactions_once

    capture_main = Mock(return_value=2)
    monkeypatch.setattr(capture_schwab_transactions_once, "main", capture_main)
    operation = Mock(side_effect=AssertionError("daily operation must not run"))
    assert runner.main(["--capture-transactions-once", "--binding-metadata", "private-input",
                        "--output-dir", "private-output"], environ={}, operation=operation) == 2
    capture_main.assert_called_once_with(["--binding-metadata", "private-input", "--output-dir", "private-output"], environ={})
    operation.assert_not_called()


@pytest.fixture(autouse=True)
def forbid_uninjected_cloud_or_network(monkeypatch):
    def denied(*_args, **_kwargs):
        raise AssertionError("real network and ADC are forbidden in synthetic tests")

    monkeypatch.setattr(facts, "_authorized_session", denied)
    monkeypatch.setattr("requests.Session.request", denied)
    monkeypatch.setattr("urllib.request.OpenerDirector.open", denied)
    monkeypatch.setattr("google.cloud.storage.Client", denied)


def environment():
    return {
        **source_environment(),
        "GCP_PROJECT_ID": "charlesschwabquant",
        "GCP_REGION": "us-central1",
        "SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX": PREFIX,
        "RUNTIME_HEARTBEAT_SCHEDULER_LOCATION": "us-central1",
    }


def environment_with_protected_daily_target():
    env = environment()
    env["SCHWAB_RUNTIME_DAILY_TARGET_JSON"] = env["RUNTIME_TARGET_JSON"]
    return env


def service():
    return {
        "name": RESOURCE,
        "generation": "7",
        "observedGeneration": "7",
        "reconciling": False,
        "terminalCondition": {"state": "CONDITION_SUCCEEDED"},
        "trafficStatuses": [{"revision": REVISION, "percent": 100}],
        "uri": SERVICE_URI,
    }


def job():
    return {
        "name": f"projects/charlesschwabquant/locations/us-central1/jobs/{SERVICE}-scheduler",
        "state": "ENABLED",
        "schedule": "0 16 * * 1-5",
        "timeZone": "America/New_York",
        "httpTarget": {"uri": SERVICE_URI + "/run", "httpMethod": "POST"},
    }


class Response:
    def __init__(self, payload=None, *, status=200, raw=None):
        self.status_code = status
        self.headers = {}
        self.raw_body = json.dumps(payload).encode() if raw is None else raw
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.raw_body), chunk_size):
            yield self.raw_body[offset : offset + chunk_size]


def session_for(service_value=None, job_value=None, *, second=None):
    session = Mock()
    session.get.side_effect = [
        Response(service() if service_value is None else service_value),
        Response(job() if job_value is None else job_value),
        Response(status=404) if second is None else Response(second),
    ]
    return session


def verified_facts():
    return facts.SourceFacts("verified", REVISION, "0 16 * * 1-5", "America/New_York")


def run(
    *,
    environ=None,
    publish=False,
    verify_native_identity_only=False,
    check_token_load_only=False,
    observation=None,
    **overrides,
):
    readers = {
        "identity_reader": Mock(return_value="verified"),
        "fact_reader": Mock(return_value=observation or verified_facts()),
        "archive_reader": Mock(return_value=caller.ReadBatch([envelope()])),
        "publisher": Mock(
            return_value={
                "status": "stored_acknowledged",
                "account_attribution": "receiver_reported_account",
            }
        ),
    }
    readers.update(overrides)
    result = runner.run_daily(
        environment() if environ is None else environ,
        publish=publish,
        verify_native_identity_only=verify_native_identity_only,
        check_token_load_only=check_token_load_only,
        observed_at=NOW,
        session_dates_loader=lambda *_a, **_k: {NOW.date()},
        **readers,
    )
    return result, readers


def test_metadata_uses_only_fixed_bounded_gets_and_redacted_objects(capsys):
    session = session_for()
    result = facts.read_source_facts(environment(), session=session)
    assert result == verified_facts()
    assert session.get.call_count == 3
    for call in session.get.call_args_list:
        assert call.args[0].startswith(
            (
                "https://run.googleapis.com/v2/",
                "https://cloudscheduler.googleapis.com/v1/",
            )
        )
        assert call.kwargs["allow_redirects"] is False
        assert call.kwargs["stream"] is True
        assert call.kwargs["timeout"] == (5, 15)
        assert "fields" in call.kwargs["params"]
        assert "secret" not in call.args[0]
    assert "trafficStatuses" in session.get.call_args_list[0].kwargs["params"]["fields"]
    assert "template" not in session.get.call_args_list[0].kwargs["params"]["fields"]
    assert SERVICE_URI not in repr(result) and REVISION not in repr(result)
    assert capsys.readouterr().out == capsys.readouterr().err == ""


@pytest.mark.parametrize(
    "mutation",
    [
        "split",
        "total",
        "boolean",
        "string",
        "reconciling",
        "generation",
        "condition",
        "foreign",
        "uri",
        "missing",
    ],
)
def test_unconfirmed_service_never_reads_scheduler(mutation):
    value = service()
    if mutation == "split":
        value["trafficStatuses"] = [
            {"revision": REVISION, "percent": 50},
            {"revision": "other-revision", "percent": 50},
        ]
    elif mutation == "total":
        value["trafficStatuses"][0]["percent"] = 99
    elif mutation == "boolean":
        value["trafficStatuses"][0]["percent"] = True
    elif mutation == "string":
        value["trafficStatuses"][0]["percent"] = "100"
    elif mutation == "reconciling":
        value["reconciling"] = True
    elif mutation == "generation":
        value["observedGeneration"] = "6"
    elif mutation == "condition":
        value["terminalCondition"]["state"] = "CONDITION_FAILED"
    elif mutation == "foreign":
        value["name"] += "-other"
    elif mutation == "uri":
        value["uri"] = "https://foreign.example/private"
    else:
        del value["trafficStatuses"]
    session = session_for(service_value=value)
    result = facts.read_source_facts(environment(), session=session)
    assert result.runtime_revision is None
    assert session.get.call_count == 1


def test_full_revision_resource_and_zero_traffic_tag_are_supported():
    value = service()
    value["trafficStatuses"][0]["revision"] = RESOURCE + "/revisions/" + REVISION
    value["trafficStatuses"].append({"revision": "old-revision", "tag": "candidate"})
    assert (
        facts.read_source_facts(environment(), session=session_for(service_value=value))
        == verified_facts()
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "paused",
        "disabled",
        "unknown",
        "uri",
        "method",
        "cron",
        "timezone",
        "name",
        "duplicate",
    ],
)
def test_unverified_effective_scheduler_keeps_serving_revision_but_no_due(mutation):
    value = job()
    if mutation in {"paused", "disabled", "unknown"}:
        value["state"] = {
            "paused": "PAUSED",
            "disabled": "DISABLED",
            "unknown": "STATE_UNSPECIFIED",
        }[mutation]
    elif mutation == "uri":
        value["httpTarget"]["uri"] = "https://private-other.example/run"
    elif mutation == "method":
        value["httpTarget"]["httpMethod"] = "GET"
    elif mutation == "cron":
        value["schedule"] = "0 16"
    elif mutation == "timezone":
        value["timeZone"] = "PRIVATE-TIMEZONE"
    elif mutation == "name":
        value["name"] = "projects/other/locations/us-central1/jobs/private"
    second = copy.deepcopy(value) if mutation == "duplicate" else None
    if second:
        second["name"] = second["name"].replace(
            SERVICE + "-scheduler", SERVICE.removesuffix("-service") + "-scheduler"
        )
    result = facts.read_source_facts(
        environment(), session=session_for(job_value=value, second=second)
    )
    assert result.runtime_revision == REVISION
    assert result.scheduler_cron is None
    assert facts.effective_schedule({}, facts=result, observed_at=NOW) is None


def test_second_alias_can_be_the_only_real_job():
    value = job()
    value["name"] = value["name"].replace(
        SERVICE + "-scheduler", SERVICE.removesuffix("-service") + "-scheduler"
    )
    session = Mock()
    session.get.side_effect = [
        Response(service()),
        Response(status=404),
        Response(value),
    ]
    assert facts.read_source_facts(environment(), session=session) == verified_facts()


@pytest.mark.parametrize(
    "response",
    [
        Response(status=403),
        Response(status=302),
        Response(raw=b"PRIVATE-NONJSON"),
        Response(raw=b"x" * 65537),
        Response([]),
    ],
)
def test_failed_or_oversized_metadata_is_private_and_never_retried(response, capsys):
    session = Mock()
    session.get.return_value = response
    result = facts.read_source_facts(environment(), session=session)
    assert result.runtime_revision is None
    assert session.get.call_count == 1 and response.closed
    assert "PRIVATE" not in repr(result)
    assert capsys.readouterr().out == capsys.readouterr().err == ""


def test_metadata_transport_failure_is_classified_without_details():
    session = Mock()
    session.get.side_effect = OSError("PRIVATE-METADATA-ERROR")
    result = facts.read_source_facts(environment(), session=session)
    assert result.runtime_revision is None
    assert session.get.call_count == 1


def test_owned_session_is_closed_and_metadata_deadline_is_bounded(monkeypatch):
    session = session_for()
    monkeypatch.setattr(facts, "_authorized_session", lambda: session)
    assert facts.read_source_facts(environment()) == verified_facts()
    session.close.assert_called_once()
    session = session_for()
    clock = iter([0, 16])
    monkeypatch.setattr(facts.time, "monotonic", lambda: next(clock))
    assert (
        facts.read_source_facts(environment(), session=session).runtime_revision is None
    )
    assert session.get.call_count == 1


def test_scheduler_read_failure_cannot_leak_details_or_repair_declared_cron(capsys):
    session = Mock()
    session.get.side_effect = [Response(service()), OSError("PRIVATE-SCHEDULER-URI")]
    observation = facts.read_source_facts(environment(), session=session)
    assert (
        observation.reason == "scheduler_unavailable"
        and observation.runtime_revision == REVISION
    )
    result, _ = run(observation=observation)
    assert result["schedule"] == "unevaluable"
    assert "PRIVATE" not in json.dumps(result)
    assert capsys.readouterr().out == capsys.readouterr().err == ""


@pytest.mark.parametrize(
    "key,value",
    [
        ("GCP_PROJECT_ID", "other"),
        ("GCP_REGION", "other"),
        ("RUNTIME_HEARTBEAT_SCHEDULER_LOCATION", "../private"),
    ],
)
def test_metadata_configuration_is_checked_before_opening_transport(key, value):
    env = environment()
    env[key] = value
    session = Mock()
    assert facts.read_source_facts(env, session=session).runtime_revision is None
    session.get.assert_not_called()


def test_actual_scheduler_overrides_declared_cron_without_mutating_policy():
    _, policy = caller._select_identity(environment())
    before = copy.deepcopy(policy)
    policy["scheduler"]["main_time"] = "0 23 * * 1-5"
    result = facts.effective_schedule(
        policy,
        facts=verified_facts(),
        observed_at=NOW,
        session_dates_loader=lambda *_a, **_k: {NOW.date()},
    )
    assert result["state"] == "due"
    assert policy["scheduler"]["main_time"] == "0 23 * * 1-5"
    assert before["scheduler"]["main_time"] != policy["scheduler"]["main_time"]


def test_prepare_default_never_posts_and_keeps_incomplete():
    result, readers = run()
    assert result["status"] == "prepared"
    assert result["completeness"] == "incomplete"
    assert result["reason"] == "coverage_unconfirmed"
    assert result["schedule"] == "due"
    readers["publisher"].assert_not_called()
    readers["archive_reader"].assert_called_once_with(
        report_prefix=PREFIX, observed_at=NOW
    )


@pytest.mark.parametrize(
    "reason",
    [
        "scheduler_paused",
        "scheduler_unavailable",
        "scheduler_target_mismatch",
        "scheduler_ambiguous",
    ],
)
def test_declared_five_field_cron_never_repairs_unknown_actual_scheduler(reason):
    result, readers = run(observation=facts.SourceFacts(reason, REVISION))
    assert result["schedule"] == "unevaluable"
    assert result["completeness"] == "incomplete"
    assert result["source_facts"] == reason
    readers["publisher"].assert_not_called()


def test_prepare_does_not_require_publication_credentials():
    env = environment()
    env.pop("EXECUTION_EVIDENCE_SYNC_TOKEN", None)
    env.pop("EXECUTION_EVIDENCE_SYNC_URL", None)
    assert run(environ=env)[0]["status"] == "prepared"


def test_explicit_publish_uses_same_sealed_caller_and_fixed_local_endpoint():
    env = environment()
    env["EXECUTION_EVIDENCE_SYNC_URL"] = "https://private-legacy.example/evidence"
    original = copy.deepcopy(env)
    result, readers = run(environ=env, publish=True)
    assert (
        result["status"] == "stored_acknowledged"
        and result["completeness"] == "incomplete"
    )
    call = readers["publisher"].call_args
    prepared = call.args[0]
    assert prepared._preparation_digest
    assert prepared.projection["completeness"] == "incomplete"
    assert call.kwargs["publish"] is True
    assert call.kwargs["environ"]["EXECUTION_EVIDENCE_SYNC_URL"] == caller.SYNC_URL
    assert env == original
    assert "private" not in json.dumps(result)


def test_runner_to_real_caller_uses_synthetic_transport_and_preserves_incomplete():
    opener = Mock()

    def publisher(prepared, **kwargs):
        opener.open.return_value = AckResponse(json.dumps(ack(prepared)).encode())
        return caller.publish_prepared(
            prepared, opener_factory=lambda *_a: opener, **kwargs
        )

    result, _ = run(publish=True, publisher=publisher)
    assert result["status"] == "stored_acknowledged"
    assert result["completeness"] == "incomplete"
    request = opener.open.call_args.args[0]
    body = json.loads(request.data)
    assert body["completeness"] == "incomplete"
    assert "coverage_unconfirmed" in body["read_errors"]
    assert request.get_header("X-qsl-source-binding-id")
    assert "PRIVATE-ACCOUNT" not in request.data.decode()
    opener.open.assert_called_once()


def test_publish_requires_native_identity_before_report_read_or_post():
    archive = Mock()
    publisher = Mock()
    fact_reader = Mock(return_value=verified_facts())
    identity_reader = Mock(return_value="native_identity_mismatch")

    result, readers = run(
        publish=True,
        identity_reader=identity_reader,
        fact_reader=fact_reader,
        archive_reader=archive,
        publisher=publisher,
    )

    assert result == {"status": "skipped", "reason": "native_identity_mismatch"}
    identity_reader.assert_called_once()
    assert identity_reader.call_args.kwargs["expected_account_hash"] == caller._select_identity(
        environment()
    )[0]
    fact_reader.assert_not_called()
    archive.assert_not_called()
    publisher.assert_not_called()


def test_protected_daily_target_avoids_native_get_even_when_token_would_be_expired():
    identity_reader = Mock(return_value="token_expired")
    result, readers = run(
        environ=environment_with_protected_daily_target(),
        publish=True,
        identity_reader=identity_reader,
    )

    assert result["status"] == "stored_acknowledged"
    identity_reader.assert_not_called()
    readers["fact_reader"].assert_called_once()
    readers["archive_reader"].assert_called_once()
    readers["publisher"].assert_called_once()


def test_protected_daily_target_accepts_equivalent_json_formatting_and_key_order():
    env = environment()
    target = json.loads(env["RUNTIME_TARGET_JSON"])
    env["SCHWAB_RUNTIME_DAILY_TARGET_JSON"] = json.dumps(
        target, indent=2, sort_keys=True
    )
    assert env["SCHWAB_RUNTIME_DAILY_TARGET_JSON"] != env["RUNTIME_TARGET_JSON"]
    identity_reader = Mock(return_value="token_expired")

    result, readers = run(
        environ=env,
        publish=True,
        identity_reader=identity_reader,
    )

    assert result["status"] == "stored_acknowledged"
    identity_reader.assert_not_called()
    readers["archive_reader"].assert_called_once()
    readers["publisher"].assert_called_once()


def test_publish_without_protected_daily_target_keeps_native_get():
    result, readers = run(publish=True)

    assert result["status"] == "stored_acknowledged"
    readers["identity_reader"].assert_called_once()
    assert readers["identity_reader"].call_args.kwargs["expected_account_hash"] == caller._select_identity(
        environment()
    )[0]
    readers["archive_reader"].assert_called_once()
    readers["publisher"].assert_called_once()


@pytest.mark.parametrize("private_value", ["{", "different_complete_json"])
def test_invalid_or_conflicting_protected_target_stops_before_archive_and_post(
    private_value,
):
    env = environment_with_protected_daily_target()
    if private_value == "different_complete_json":
        target = json.loads(env["SCHWAB_RUNTIME_DAILY_TARGET_JSON"])
        target["unrelated_config_field"] = "synthetic-conflict"
        env["SCHWAB_RUNTIME_DAILY_TARGET_JSON"] = json.dumps(target)
    else:
        env["SCHWAB_RUNTIME_DAILY_TARGET_JSON"] = private_value
    identity_reader = Mock()
    fact_reader = Mock(return_value=verified_facts())
    archive_reader = Mock()
    publisher = Mock()

    result, _ = run(
        environ=env,
        publish=True,
        identity_reader=identity_reader,
        fact_reader=fact_reader,
        archive_reader=archive_reader,
        publisher=publisher,
    )

    assert result == {"status": "skipped", "reason": "source_identity_unavailable"}
    identity_reader.assert_not_called()
    fact_reader.assert_not_called()
    archive_reader.assert_not_called()
    publisher.assert_not_called()


def test_native_identity_only_mode_reads_no_daily_source_or_publisher():
    env = environment()
    env.pop("SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX")
    identity_reader = Mock(return_value="verified")

    result, readers = run(
        environ=env,
        verify_native_identity_only=True,
        identity_reader=identity_reader,
    )

    assert result == {"status": "native_identity_verified"}
    identity_reader.assert_called_once()
    readers["fact_reader"].assert_not_called()
    readers["archive_reader"].assert_not_called()
    readers["publisher"].assert_not_called()


def test_native_identity_only_still_checks_broker_with_protected_target_present():
    env = environment_with_protected_daily_target()
    env.pop("SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX")
    identity_reader = Mock(return_value="verified")

    result, readers = run(
        environ=env,
        verify_native_identity_only=True,
        identity_reader=identity_reader,
    )

    assert result == {"status": "native_identity_verified"}
    identity_reader.assert_called_once()
    readers["fact_reader"].assert_not_called()
    readers["archive_reader"].assert_not_called()
    readers["publisher"].assert_not_called()


def test_protected_daily_target_does_not_admit_a_report_with_another_native_hash():
    entry = envelope()
    entry["payload"]["summary"]["account_observation"]["account_hash"] = (
        "SYNTHETIC-OTHER-NATIVE-HASH"
    )
    archive_reader = Mock(return_value=caller.ReadBatch([entry]))
    opener = Mock()

    def publisher(prepared, **kwargs):
        return caller.publish_prepared(
            prepared, opener_factory=lambda *_args: opener, **kwargs
        )

    result, readers = run(
        environ=environment_with_protected_daily_target(),
        publish=True,
        archive_reader=archive_reader,
        publisher=publisher,
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "source_identity_mismatch"
    assert result["mismatch_provenance_passed"] == 1
    readers["identity_reader"].assert_not_called()
    archive_reader.assert_called_once()
    opener.open.assert_not_called()


def test_token_load_mode_checks_only_existing_secret_and_never_reads_daily_or_broker():
    env = environment()
    env.pop("SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX")
    checker = Mock(return_value="token_payload_valid")
    identity_reader = Mock()

    result, readers = run(
        environ=env,
        check_token_load_only=True,
        token_load_checker=checker,
        identity_reader=identity_reader,
    )

    assert result == {
        "status": "token_load_checked",
        "reason": "token_payload_valid",
    }
    checker.assert_called_once()
    identity_reader.assert_not_called()
    readers["fact_reader"].assert_not_called()
    readers["archive_reader"].assert_not_called()
    readers["publisher"].assert_not_called()


def test_token_load_mode_preserves_fixed_failure_classification():
    result, readers = run(
        check_token_load_only=True,
        token_load_checker=Mock(return_value="token_permission_denied"),
    )
    assert result == {"status": "skipped", "reason": "token_permission_denied"}
    readers["fact_reader"].assert_not_called()
    readers["archive_reader"].assert_not_called()
    readers["publisher"].assert_not_called()


def test_conflicting_token_check_and_publish_modes_stop_before_any_reader():
    result, readers = run(publish=True, check_token_load_only=True)
    assert result == {"status": "skipped", "reason": "conflicting_modes"}
    readers["identity_reader"].assert_not_called()
    readers["fact_reader"].assert_not_called()
    readers["archive_reader"].assert_not_called()
    readers["publisher"].assert_not_called()


def test_publish_without_token_is_skipped_and_never_opens_transport():
    env = environment()
    env.pop("EXECUTION_EVIDENCE_SYNC_TOKEN", None)
    opener = Mock()

    def publisher(prepared, **kwargs):
        return caller.publish_prepared(
            prepared, opener_factory=lambda *_a: opener, **kwargs
        )

    result, _ = run(environ=env, publish=True, publisher=publisher)
    assert result["status"] == "skipped" and result["completeness"] == "incomplete"
    assert result["reason"] == "publish_configuration_unavailable"
    opener.open.assert_not_called()


@pytest.mark.parametrize(
    "outcome, expected",
    [
        (
            {"status": "unconfirmed", "reason": "ack_invalid"},
            ("unconfirmed", "ack_invalid"),
        ),
        (
            {"status": "skipped", "reason": "projection_changed"},
            ("skipped", "projection_changed"),
        ),
        (
            {"status": "skipped", "reason": "PRIVATE-PUBLISH-ERROR"},
            ("skipped", "publication_unconfirmed"),
        ),
    ],
)
def test_publication_reports_fixed_skip_or_uncertain_outcome_accurately(
    outcome, expected
):
    result, _ = run(publish=True, publisher=Mock(return_value=outcome))
    assert (result["status"], result["reason"]) == expected
    assert "PRIVATE" not in json.dumps(result)


@pytest.mark.parametrize(
    "mutation", ["identity", "duplicate", "prefix", "time", "mode", "revision"]
)
def test_invalid_inputs_do_not_read_reports_or_post(mutation):
    env = environment()
    if mutation == "identity":
        env["RUNTIME_TARGET_JSON"] = "{}"
    elif mutation == "duplicate":
        item = json.loads(env["RUNTIME_TARGET_JSON"])
        env["CLOUD_RUN_SERVICE_TARGETS_JSON"] = json.dumps([item, item])
    elif mutation == "prefix":
        env["SCHWAB_ACCOUNT_FACTS_REPORT_PREFIX"] = "gs://private/other/"
    elif mutation == "revision":
        env["EXPECTED_RUNTIME_REVISION"] = "different-revision"
    read_facts = Mock(return_value=verified_facts())
    archive = Mock()
    publisher = Mock()
    result = runner.run_daily(
        env,
        publish="true" if mutation == "mode" else False,
        observed_at=dt.datetime(2026, 10, 6) if mutation == "time" else NOW,
        fact_reader=read_facts,
        archive_reader=archive,
        publisher=publisher,
    )
    assert result["status"] == "skipped"
    archive.assert_not_called()
    publisher.assert_not_called()
    if mutation != "revision":
        read_facts.assert_not_called()


def test_missing_serving_revision_never_reads_a_report():
    result, readers = run(observation=facts.SourceFacts("service_unavailable"))
    assert result["status"] == "skipped"
    readers["archive_reader"].assert_not_called()
    readers["publisher"].assert_not_called()


def test_callback_can_only_produce_unevaluable_when_actual_facts_are_missing():
    env = environment()
    prepared = caller.prepare_daily(
        environ=env,
        batch=caller.ReadBatch([envelope()]),
        report_prefix=PREFIX,
        expected_runtime_revision=REVISION,
        observed_at=NOW,
        session_dates_loader=lambda *_a, **_k: {NOW.date()},
        schedule_provider=lambda *_a, **_k: None,
    )
    assert prepared.projection["records"][0]["schedule"]["state"] == "unevaluable"
    assert prepared._preparation_digest


def test_runner_errors_and_cli_arguments_never_echo_private_values(capsys):
    result, readers = run(archive_reader=Mock(side_effect=OSError("PRIVATE-ARCHIVE")))
    assert result == {"status": "skipped", "reason": "operation_failed"}
    readers["publisher"].assert_not_called()
    assert runner.main(["--PRIVATE-ARG"]) == 2
    assert "PRIVATE" not in capsys.readouterr().out


def test_cli_empty_is_prepare_and_publish_is_explicit(capsys):
    operation = Mock(
        return_value={"status": "prepared", "reason": "coverage_unconfirmed"}
    )
    assert runner.main([], environ={}, operation=operation) == 0
    assert operation.call_args.kwargs["publish"] is False
    assert runner.main(["--publish"], environ={}, operation=operation) == 0
    assert operation.call_args.kwargs["publish"] is True
    assert runner.main(["--verify-native-identity"], environ={}, operation=operation) == 0
    assert operation.call_args.kwargs["verify_native_identity_only"] is True
    token_check = Mock(
        return_value={"status": "token_load_checked", "reason": "token_payload_valid"}
    )
    assert runner.main(["--check-token-load"], environ={}, operation=token_check) == 0
    assert token_check.call_args.kwargs["check_token_load_only"] is True
    assert "coverage_unconfirmed" in capsys.readouterr().out


def test_new_workflow_is_manual_main_only_and_preparation_has_no_token():
    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    assert "workflow_dispatch:" in workflow
    for forbidden in (
        "  schedule:",
        "  push:",
        "  pull_request:",
        "  workflow_run:",
        "environment:",
    ):
        assert forbidden not in workflow
    assert "type: boolean" in workflow and "default: false" in workflow
    identity_input = workflow.split("      verify_native_identity:", 1)[1].split(
        "permissions:", 1
    )[0]
    assert "type: boolean" in identity_input
    assert "default: false" in identity_input
    token_input = workflow.split("      check_token_load:", 1)[1].split(
        "    permissions:", 1
    )[0]
    assert "type: boolean" in token_input
    assert "default: false" in token_input
    assert "github.event_name == 'workflow_dispatch'" in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
    assert (
        "RUNTIME_TARGET_JSON: ${{ secrets.SCHWAB_RUNTIME_DAILY_TARGET_JSON || "
        "vars.RUNTIME_TARGET_JSON || secrets.RUNTIME_TARGET_JSON }}"
    ) in workflow
    assert "google-github-actions/auth@v3" in workflow
    assert "uv sync --frozen --no-dev" in workflow
    assert "!inputs.publish" in workflow and "inputs.publish" in workflow
    prepare = workflow.split("- name: Prepare runtime daily", 1)[1].split(
        "- name: Publish runtime daily", 1
    )[0]
    publish_step = workflow.split("- name: Publish runtime daily", 1)[1].split(
        "- name:", 1
    )[0]
    assert "--publish" not in prepare and "SYNC_TOKEN" not in prepare
    assert "SCHWAB_RUNTIME_DAILY_TARGET_JSON" not in prepare
    assert "SCHWAB_RUNTIME_DAILY_TARGET_JSON: ${{ secrets.SCHWAB_RUNTIME_DAILY_TARGET_JSON }}" in publish_step
    assert workflow.count("secrets.EXECUTION_EVIDENCE_SYNC_TOKEN") == 1
    assert "run_runtime_daily_from_reports.py --publish" in workflow
    assert "run_runtime_daily_from_reports.py --verify-native-identity" in workflow
    assert "run_runtime_daily_from_reports.py --check-token-load" in workflow
    identity_step = workflow.split(
        "- name: Verify native Schwab account identity", 1
    )[1].split("- name:", 1)[0]
    assert "EXECUTION_EVIDENCE_SYNC_TOKEN" not in identity_step
    assert "SCHWAB_APP_SECRET" not in identity_step
    for forbidden in (
        "execution_report_heartbeat.py",
        "publish_account_facts_from_reports.py",
        "gcloud run deploy",
        "gcloud scheduler jobs run",
        "TELEGRAM_TOKEN",
        "secrets.SCHWAB_ACCOUNT_FACTS_SYNC_TOKEN",
    ):
        assert forbidden not in workflow
    assert (
        "tests/test_run_runtime_daily_from_reports.py"
        in (ROOT / ".github/workflows/ci.yml").read_text()
    )


def test_manual_daily_prefers_protected_runtime_binding_with_legacy_fallback():
    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    assignment = (
        "RUNTIME_TARGET_JSON: ${{ secrets.SCHWAB_RUNTIME_DAILY_TARGET_JSON || "
        "vars.RUNTIME_TARGET_JSON || secrets.RUNTIME_TARGET_JSON }}"
    )
    assert workflow.count(assignment) == 1


@pytest.mark.parametrize(
    "event", ["workflow_dispatch", "schedule", "push", "pull_request", "workflow_run"]
)
@pytest.mark.parametrize("ref", ["refs/heads/main", "refs/heads/private-branch"])
@pytest.mark.parametrize("publish", [None, False, True])
@pytest.mark.parametrize("diagnose", [None, False, True])
@pytest.mark.parametrize("verify", [None, False, True])
@pytest.mark.parametrize("check_token", [None, False, True])
def test_workflow_event_branch_and_modes_are_mutually_exclusive(
    event, ref, publish, diagnose, verify, check_token
):
    import ast

    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    job = re.search(r"^    if: (.+)$", workflow, re.MULTILINE).group(1)
    prepare = re.search(
        r"- name: Prepare runtime daily\n        if: \$\{\{ (.+) \}\}", workflow
    ).group(1)
    post = re.search(
        r"- name: Publish runtime daily\n        if: \$\{\{ (.+) \}\}", workflow
    ).group(1)
    diagnostic = re.search(
        r"- name: Check Schwab token Secret metadata and read permission\n"
        r"        if: \$\{\{ (.+) \}\}",
        workflow,
    ).group(1)
    identity = re.search(
        r"- name: Verify native Schwab account identity\n"
        r"        if: \$\{\{ (.+) \}\}",
        workflow,
    ).group(1)
    token_check = re.search(
        r"- name: Check existing Schwab token load status\n"
        r"        if: \$\{\{ (.+) \}\}",
        workflow,
    ).group(1)

    def evaluate(expression):
        expression = (
            expression.replace("inputs.capture_transactions_once", "False")
            .replace("inputs.renew_access_token_once", "False")
            .replace("inputs.check_renewal_credentials", "False")
            .replace("github.event_name", repr(event))
            .replace("github.ref", repr(ref))
            .replace("inputs.publish", repr(publish))
            .replace("inputs.diagnose_source_access", repr(diagnose))
            .replace("inputs.verify_native_identity", repr(verify))
            .replace("inputs.check_token_load", repr(check_token))
        )
        expression = expression.replace("&&", " and ").replace("!", "not ")
        parsed = ast.parse(expression, mode="eval")
        allowed = (
            ast.Expression,
            ast.BoolOp,
            ast.And,
            ast.Compare,
            ast.Eq,
            ast.Constant,
            ast.UnaryOp,
            ast.Not,
        )
        assert all(isinstance(node, allowed) for node in ast.walk(parsed))
        return bool(
            eval(
                compile(parsed, "<offline workflow gate>", "eval"),
                {"__builtins__": {}},
                {},
            )
        )

    runnable = evaluate(job)
    prepare_runs, publish_runs, diagnostic_runs, identity_runs, token_check_runs = (
        runnable and evaluate(prepare),
        runnable and evaluate(post),
        runnable and evaluate(diagnostic),
        runnable and evaluate(identity),
        runnable and evaluate(token_check),
    )
    valid_dispatch = event == "workflow_dispatch" and ref == "refs/heads/main"
    assert prepare_runs == (
        valid_dispatch and not publish and not diagnose and not verify and not check_token
    )
    assert publish_runs == (
        valid_dispatch and bool(publish) and not diagnose and not verify and not check_token
    )
    assert diagnostic_runs == (
        valid_dispatch and bool(diagnose) and not publish and not verify and not check_token
    )
    assert identity_runs == (
        valid_dispatch and bool(verify) and not publish and not diagnose and not check_token
    )
    assert token_check_runs == (
        valid_dispatch and bool(check_token) and not publish and not diagnose and not verify
    )
    assert sum((prepare_runs, publish_runs, diagnostic_runs, identity_runs, token_check_runs)) <= 1


def test_runner_identity_failure_adds_counts_after_original_result_with_no_reread_or_post(
    capsys,
):
    from test_publish_runtime_daily_from_reports import (
        HASH,
        diagnostic_counts,
        mismatch_entry,
    )

    entries = [mismatch_entry(account_hash=HASH.swapcase()), mismatch_entry()]
    stale = mismatch_entry()
    stale["payload"]["diagnostics"]["runtime_revision"] = "PRIVATE-OLD"
    entries.append(stale)
    archive = Mock(return_value=caller.ReadBatch(entries))
    result, readers = run(publish=True, archive_reader=archive)
    assert result == {
        "status": "skipped",
        "reason": "source_identity_mismatch",
        **diagnostic_counts(2, 1, 0, 1),
    }
    readers["fact_reader"].assert_called_once()
    archive.assert_called_once()
    readers["publisher"].assert_not_called()
    assert "PRIVATE" not in json.dumps(result)
    operation = Mock(return_value=result)
    assert runner.main([], environ={}, operation=operation) == 2
    output = capsys.readouterr().out
    assert "source_identity_mismatch" in output and "PRIVATE" not in output


@pytest.mark.parametrize(
    "fault",
    [
        "raise",
        "private_key",
        "boolean",
        "overflow",
        "total_overflow",
        "case_overflow",
        "negative",
        "zero",
        "none",
    ],
)
def test_runner_diagnostic_failure_or_malformed_counts_preserve_original_skip(
    fault, monkeypatch
):
    from test_publish_runtime_daily_from_reports import (
        diagnostic_counts,
        mismatch_entry,
    )

    counts = diagnostic_counts(passed=1)
    if fault == "private_key":
        counts["PRIVATE-KEY"] = 1
    elif fault == "boolean":
        counts["mismatch_provenance_passed"] = True
    elif fault == "overflow":
        counts["mismatch_provenance_passed"] = 21
    elif fault == "total_overflow":
        counts = diagnostic_counts(20, 1)
    elif fault == "case_overflow":
        counts["mismatch_passed_ascii_case_only"] = 2
    elif fault == "negative":
        counts["mismatch_provenance_unknown"] = -1
    elif fault == "zero":
        counts = diagnostic_counts()
    elif fault == "none":
        counts = None
    diagnostic = Mock(
        side_effect=OSError("PRIVATE-DIAGNOSTIC") if fault == "raise" else None,
        return_value=counts,
    )
    monkeypatch.setattr(caller, "diagnose_identity_mismatch", diagnostic)
    result, readers = run(
        publish=True,
        archive_reader=Mock(return_value=caller.ReadBatch([mismatch_entry()])),
    )
    assert result == {"status": "skipped", "reason": "source_identity_mismatch"}
    readers["publisher"].assert_not_called()


def test_runner_never_diagnoses_non_mismatch_results(monkeypatch):
    diagnostic = Mock(side_effect=AssertionError("diagnostic must remain gated"))
    monkeypatch.setattr(caller, "diagnose_identity_mismatch", diagnostic)
    result, _ = run()
    assert result["status"] == "prepared"
    entry = envelope()
    entry["payload"]["summary"].pop("account_observation")
    result, readers = run(
        publish=True, archive_reader=Mock(return_value=caller.ReadBatch([entry]))
    )
    assert result == {"status": "skipped", "reason": "source_observation_missing"}
    diagnostic.assert_not_called()
    readers["publisher"].assert_not_called()


@pytest.mark.parametrize("all_excluded", [False, True])
def test_runner_summary_distinguishes_preparation_from_available_projected_reports(
    all_excluded,
):
    from test_publish_runtime_daily_from_reports import excluded_mismatch

    entries = [excluded_mismatch()]
    if not all_excluded:
        entries.append(envelope())
    result, readers = run(archive_reader=Mock(return_value=caller.ReadBatch(entries)))
    assert result["status"] == "prepared"
    assert result["daily_status"] == "read_incomplete"
    assert result["projected_run_count"] == (0 if all_excluded else 1)
    assert type(result["projected_run_count"]) is int
    assert result["completeness"] == "incomplete"
    readers["fact_reader"].assert_called_once()
    readers["archive_reader"].assert_called_once()
    readers["publisher"].assert_not_called()
    assert "PRIVATE" not in json.dumps(result)


@pytest.mark.parametrize(
    "order", [(0, 1, 2), (0, 2, 1), (1, 0, 2), (1, 2, 0), (2, 0, 1), (2, 1, 0)]
)
def test_runner_qualified_prior_day_mismatch_still_skips_in_every_order(order):
    from test_publish_runtime_daily_from_reports import excluded_mismatch

    bad = envelope(stamp="20261005T200100Z")
    bad["payload"]["summary"]["account_observation"]["account_hash"] = "PRIVATE-OTHER"
    values = [bad, envelope(), excluded_mismatch()]
    result, readers = run(
        publish=True,
        archive_reader=Mock(return_value=caller.ReadBatch([values[i] for i in order])),
    )
    assert (
        result["status"] == "skipped" and result["reason"] == "source_identity_mismatch"
    )
    assert "daily_status" not in result and "projected_run_count" not in result
    readers["publisher"].assert_not_called()
    operation = Mock(return_value=result)
    assert runner.main([], environ={}, operation=operation) == 2


def test_projection_summary_reads_original_sealed_projection_without_mutation():
    from test_publish_runtime_daily_from_reports import prepare

    prepared = prepare([envelope()])
    body = copy.deepcopy(prepared.projection)
    seal = prepared._preparation_digest
    binding = prepared._source_binding_id
    assert runner._prepared_projection_summary(prepared) == {
        "daily_status": "read_incomplete",
        "projected_run_count": 1,
    }
    assert prepared.projection == body and prepared._preparation_digest == seal
    assert prepared._source_binding_id == binding


@pytest.mark.parametrize(
    "mutation",
    [
        "changed_body",
        "unknown_status",
        "healthy_status",
        "wrong_runs",
        "too_many_runs",
        "wrong_records",
        "missing_seal",
    ],
)
def test_projection_summary_rejects_unsealed_or_non_whitelisted_shape(mutation):
    from test_publish_runtime_daily_from_reports import prepare

    prepared = prepare([envelope()])
    body = copy.deepcopy(prepared.projection)
    if mutation == "changed_body":
        prepared.projection["records"][0]["status"] = "PRIVATE-STATUS"
        assert runner._prepared_projection_summary(prepared) == {}
        return
    if mutation == "unknown_status":
        body["records"][0]["status"] = "PRIVATE-STATUS"
    elif mutation == "healthy_status":
        body["records"][0]["status"] = "healthy"
    elif mutation == "wrong_runs":
        body["records"][0]["runs"] = {"private": "value"}
    elif mutation == "too_many_runs":
        body["records"][0]["runs"] *= 21
    elif mutation == "wrong_records":
        body["records"] *= 2
    seal = (
        None
        if mutation == "missing_seal"
        else caller._preparation_digest(
            caller._canonical_body(body), prepared._source_binding_id
        )
    )
    synthetic = caller.PreparedDaily(
        "prepared", body, prepared._source_binding_id, seal
    )
    assert runner._prepared_projection_summary(synthetic) == {}


@pytest.mark.parametrize(
    "kind",
    ["empty", "revision", "schema", "old_day", "mixed", "read_error", "truncated"],
)
def test_zero_run_diagnostics_use_only_original_batch_and_sealed_exclusions(kind):
    from test_publish_runtime_daily_from_reports import prefilter_case, zero_run_counts

    entries = []
    expected = zero_run_counts()
    if kind in {"revision", "mixed"}:
        entries.append(prefilter_case("revision_mismatch"))
        expected["zero_run_revision_mismatch"] += 1
    if kind == "schema":
        entries.append(prefilter_case("schema_invalid"))
        expected["zero_run_schema_invalid"] += 1
    if kind in {"old_day", "mixed"}:
        entries.append(envelope(stamp="20261005T200100Z"))
        expected["zero_run_provenance_passed"] += 1
    expected["zero_run_entries"] = len(entries)
    expected["zero_run_read_failed"] = kind == "read_error"
    expected["zero_run_truncated"] = kind == "truncated"
    expected["zero_run_other_business_date"] = int(kind in {"old_day", "mixed"})
    batch = caller.ReadBatch(
        entries, read_failed=kind == "read_error", truncated=kind == "truncated"
    )
    result, readers = run(archive_reader=Mock(return_value=batch))
    assert result["status"] == "prepared" and result["projected_run_count"] == 0
    assert {k: v for k, v in result.items() if k.startswith("zero_run_")} == expected
    assert (
        result["daily_status"] == "read_incomplete"
        and result["completeness"] == "incomplete"
    )
    readers["fact_reader"].assert_called_once()
    readers["archive_reader"].assert_called_once()
    readers["publisher"].assert_not_called()
    assert "PRIVATE" not in json.dumps(result)


def test_zero_run_diagnostics_never_run_for_nonzero_or_identity_skip(monkeypatch):
    from test_publish_runtime_daily_from_reports import mismatch_entry

    diagnostic = Mock(side_effect=AssertionError("zero-only diagnostic"))
    monkeypatch.setattr(caller, "diagnose_report_prefilter", diagnostic)
    result, _ = run()
    assert result["projected_run_count"] == 1
    result, readers = run(
        archive_reader=Mock(return_value=caller.ReadBatch([mismatch_entry()]))
    )
    assert result["reason"] == "source_identity_mismatch"
    assert not any(key.startswith("zero_run_") for key in result)
    diagnostic.assert_not_called()
    readers["publisher"].assert_not_called()


@pytest.mark.parametrize(
    "fault",
    [
        "none",
        "raise",
        "unknown_key",
        "boolean_count",
        "negative",
        "overflow",
        "total",
        "flag",
        "entries",
        "nonempty",
    ],
)
def test_zero_run_diagnostics_invalid_counts_preserve_prepare(fault, monkeypatch):
    from test_publish_runtime_daily_from_reports import zero_run_counts

    counts = zero_run_counts()
    if fault == "none":
        counts = None
    elif fault == "unknown_key":
        counts["PRIVATE-KEY"] = 1
    elif fault == "boolean_count":
        counts["zero_run_uri_invalid"] = True
    elif fault == "negative":
        counts["zero_run_uri_invalid"] = -1
    elif fault == "overflow":
        counts["zero_run_uri_invalid"] = 21
    elif fault == "total":
        counts["zero_run_uri_invalid"] = 1
    elif fault == "flag":
        counts["zero_run_read_failed"] = 1
    elif fault == "entries":
        counts["zero_run_entries"] = True
    elif fault == "nonempty":
        counts = zero_run_counts(1, provenance_passed=1)
    diagnostic = Mock(
        return_value=counts,
        side_effect=RuntimeError("PRIVATE-ERROR") if fault == "raise" else None,
    )
    monkeypatch.setattr(caller, "diagnose_report_prefilter", diagnostic)
    result, readers = run(archive_reader=Mock(return_value=caller.ReadBatch([])))
    assert result["status"] == "prepared" and result["projected_run_count"] == 0
    assert not any(key.startswith("zero_run_") for key in result)
    readers["publisher"].assert_not_called()


def test_zero_run_diagnostics_leave_publisher_body_binding_and_seal_unchanged(
    monkeypatch,
):
    entries = [envelope(stamp="20261005T200100Z")]
    calls = []

    def capture(prepared, **_kwargs):
        calls.append(
            (
                copy.deepcopy(prepared.projection),
                prepared._source_binding_id,
                prepared._preparation_digest,
            )
        )
        return {"status": "stored_acknowledged"}

    options = dict(
        archive_reader=Mock(return_value=caller.ReadBatch(entries)),
        publisher=Mock(side_effect=capture),
    )
    result, readers = run(publish=True, **options)
    assert result["zero_run_other_business_date"] == 1
    monkeypatch.setattr(caller, "diagnose_report_prefilter", Mock(return_value=None))
    control, _ = run(publish=True, **options)
    assert calls[0] == calls[1]
    assert {k: v for k, v in result.items() if not k.startswith("zero_run_")} == control
    assert readers["publisher"].call_count == 2


@pytest.mark.parametrize(
    "fault",
    [
        "body_changed",
        "seal_missing",
        "bad_reason",
        "bad_shape",
        "too_many",
        "count_exceeds_passed",
    ],
)
def test_zero_run_diagnostics_require_sealed_whitelisted_exclusion_snapshot(fault):
    from test_publish_runtime_daily_from_reports import prepare

    batch = caller.ReadBatch([envelope(stamp="20261005T200100Z")])
    prepared = prepare(batch=batch)
    body = copy.deepcopy(prepared.projection)
    excluded = body["records"][0]["excluded_reports"]
    if fault in {"body_changed", "bad_reason"}:
        excluded[0]["reason"] = "PRIVATE-REASON"
    elif fault == "bad_shape":
        body["records"][0]["excluded_reports"] = {"PRIVATE": 1}
    elif fault == "too_many":
        excluded *= 21
    elif fault == "count_exceeds_passed":
        excluded *= 2
    digest = (
        prepared._preparation_digest
        if fault == "body_changed"
        else caller._preparation_digest(
            caller._canonical_body(body), prepared._source_binding_id
        )
    )
    if fault == "seal_missing":
        digest = None
    changed = caller.PreparedDaily(
        "prepared", body, prepared._source_binding_id, digest
    )
    assert (
        runner._zero_run_diagnostics(
            changed,
            batch=batch,
            report_prefix=PREFIX,
            expected_runtime_revision=REVISION,
            observed_at=NOW,
        )
        == {}
    )


def test_zero_run_diagnostic_cli_output_is_only_fixed_counts_and_existing_classifications(
    capsys,
):
    from test_publish_runtime_daily_from_reports import prefilter_case

    result, _ = run(
        archive_reader=Mock(
            return_value=caller.ReadBatch([prefilter_case("revision_mismatch")])
        )
    )
    assert runner.main([], environ={}, operation=Mock(return_value=result)) == 0
    output = capsys.readouterr().out
    assert json.loads(output)["zero_run_revision_mismatch"] == 1
    assert not any(
        value in output
        for value in (PREFIX, REVISION, "PRIVATE", "202610", "runtime_revision")
    )


def test_scope_subcounts_reach_only_zero_run_safe_stdout_without_extra_reads(capsys):
    from test_publish_runtime_daily_from_reports import (
        production_envelope,
        production_environment,
    )
    from test_runtime_daily_report_projection import PRODUCER_REVISION, scope_stage_case

    entry = production_envelope()
    entry["payload"] = scope_stage_case("project")
    env = {**environment(), **production_environment()}
    result, readers = run(
        environ=env,
        observation=facts.SourceFacts(
            "verified", PRODUCER_REVISION, "0 16 * * 1-5", "America/New_York"
        ),
        archive_reader=Mock(return_value=caller.ReadBatch([entry])),
    )
    assert result["zero_run_scope_invalid"] == result["zero_run_scope_project"] == 1
    assert result["projected_run_count"] == 0
    assert runner.main([], environ={}, operation=Mock(return_value=result)) == 0
    assert "PRIVATE" not in capsys.readouterr().out
    readers["fact_reader"].assert_called_once()
    readers["archive_reader"].assert_called_once()
    readers["publisher"].assert_not_called()


@pytest.mark.parametrize(
    "fault",
    ["negative", "boolean", "overflow", "unknown_key", "child_sum", "missing_child"],
)
def test_scope_subcount_validation_rejects_malformed_diagnostics(fault, monkeypatch):
    from test_publish_runtime_daily_from_reports import zero_run_counts

    counts = zero_run_counts()
    key = "zero_run_scope_project"
    if fault == "negative":
        counts[key] = -1
    elif fault == "boolean":
        counts[key] = True
    elif fault == "overflow":
        counts[key] = 21
    elif fault == "unknown_key":
        counts["zero_run_scope_PRIVATE"] = 0
    elif fault == "child_sum":
        counts[key] = 1
    else:
        counts.pop(key)
    monkeypatch.setattr(caller, "diagnose_report_prefilter", Mock(return_value=counts))
    result, readers = run(archive_reader=Mock(return_value=caller.ReadBatch([])))
    assert result["status"] == "prepared" and result["projected_run_count"] == 0
    assert not any(key.startswith("zero_run_") for key in result)
    readers["publisher"].assert_not_called()


def test_capture_workflow_is_default_off_and_checks_private_refs_before_auth():
    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    capture_input = workflow.split("      capture_transactions_once:\n", 1)[1].split("permissions:", 1)[0]
    assert "type: boolean" in capture_input and "default: false" in capture_input
    guard = workflow.split("- name: Reject conflicting operation inputs", 1)[1].split("- name:", 1)[0]
    assert "CAPTURE_TRANSACTIONS_ONCE" in guard
    assert "capture_private_references_required" in guard
    assert workflow.index("capture_private_references_required") < workflow.index("- name: Authenticate to Google Cloud")
    capture_step = workflow.split("- name: Capture fixed Schwab transactions once", 1)[1].split("- name:", 1)[0]
    assert "inputs.capture_transactions_once" in capture_step
    assert "--capture-transactions-once" in capture_step
    assert "source_complete" in capture_step and "False" in capture_step
    assert "upload-artifact" not in capture_step


def _capture_workflow_python():
    import textwrap

    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    step = workflow.split("- name: Capture fixed Schwab transactions once\n", 1)[1]
    body = step.split("          uv run --no-sync python - <<'PY'\n", 1)[1]
    return compile(textwrap.dedent(body.split("          PY\n", 1)[0]),
                   "<actual capture workflow Python>", "exec")


_CAPTURE_CONTINUATION_MARKER = "capture.after-zero-39569939360.started"


@pytest.fixture
def capture_workflow_ports(tmp_path, monkeypatch, capsys):
    """Execute the workflow's exact body and real runner/SDK/collector; fake external ports only."""
    import os
    import socket
    from types import SimpleNamespace

    import httpx

    from scripts import capture_schwab_transactions_once as capture
    from scripts.publish_account_facts_from_reports import _binding_id

    prefix = "digest-candidates/schwab/_transactions_once/window-20261015-20261016/"
    bucket_name = "schwab-private-evidence-test"
    base = "gs://" + bucket_name + "/" + prefix
    account = "SYNTHETIC_PRIVATE_ACCOUNT"
    document = {
        "ok": True, "projection": "identity", "target_id": "synthetic-target",
        "source_binding_id": _binding_id(account, caller.TARGET["service"]),
        "records": [
            {"observation_date": date, "observed_finished_at": stamp,
             "target_id_matches": True, "source_binding_id_matches": True,
             "account_scope_matches": True, "record_revision": None}
            for date, stamp in zip(["2026-10-15", "2026-10-16"],
                                   [capture.FINANCIAL_START, capture.FINANCIAL_END])
        ],
    }
    env = {
        "RUNNER_TEMP": str(tmp_path), "GCP_PROJECT_ID": "charlesschwabquant",
        "CAPTURE_BUCKET": bucket_name, "CAPTURE_BINDING_URI": base + "identity.input",
        "CAPTURE_BINDING_GENERATION": "17", "CAPTURE_OUTPUT_PREFIX": base + "result/",
        "SCHWAB_API_KEY": "SYNTHETIC_PRIVATE_KEY",
        "SCHWAB_API_KEY_SECRET_NAME": "charles-schwab-api-key",
        "SCHWAB_ACCOUNT_FACTS_TARGET_ID": document["target_id"],
        "RUNTIME_TARGET_JSON": json.dumps({
            **caller.TARGET, "account_selector": ["live"],
            "runtime_risk_limits": {"binding": {"account_hash": account}},
            "market_calendar": "NYSE", "market_timezone": "America/New_York",
        }),
    }
    predecessor_key = prefix + "result/capture.started"
    predecessor_body = b"one-shot reserved; no automatic retry\n"
    state = SimpleNamespace(document=document, events=[],
                            objects={predecessor_key: predecessor_body}, requests=[],
                            fault=None, env=env, cli_receipts=[], prefix=prefix,
                            status=200, body=b"[]", bucket_name=bucket_name, bucket_names=[],
                            predecessor_key=predecessor_key, predecessor_body=predecessor_body,
                            peer_timing=None, peer_results=[], key_requests=[])

    def deny(*_args, **_kwargs):
        raise AssertionError("unconfigured external capability forbidden")

    monkeypatch.setattr(os, "environ", env)
    monkeypatch.setattr(socket, "socket", deny)
    monkeypatch.setattr("subprocess.Popen", deny)
    monkeypatch.setattr("scripts.schwab_native_identity._load_existing_token", deny)

    def run_peer():
        """Interleave a second real workflow at the fake atomic storage boundary."""
        from contextlib import redirect_stderr, redirect_stdout
        import io

        state.peer_timing = None
        previous = env.get("GITHUB_RUN_ID")
        env["GITHUB_RUN_ID"] = "synthetic-second-worker"
        stdout, stderr = io.StringIO(), io.StringIO()
        try:
            with redirect_stdout(stdout), redirect_stderr(stderr), pytest.raises(SystemExit) as stopped:
                exec(_capture_workflow_python(), {})
            assert stderr.getvalue() == "" and "SYNTHETIC_PRIVATE" not in stdout.getvalue()
            state.peer_results.append((stopped.value.code, json.loads(stdout.getvalue())))
        finally:
            if previous is None:
                env.pop("GITHUB_RUN_ID")
            else:
                env["GITHUB_RUN_ID"] = previous

    class Blob:
        def __init__(self, name, generation=None):
            self.name, self.generation, self.size = name, generation, None

        def reload(self, **options):
            assert options == {"timeout": 15, "retry": None}
            if self.name == predecessor_key:
                state.events.append("predecessor_reload")
                if state.fault == "predecessor_missing":
                    raise FileNotFoundError("SYNTHETIC_PRIVATE predecessor")
                self.generation = 1792571512420942 if state.fault == "predecessor_generation" else 1792571512420941
                self.size = 39 if state.fault == "predecessor_size" else len(state.objects[predecessor_key])
                return
            state.events.append("metadata_reload")
            self.size = 65537 if state.fault == "metadata_size" else len(json.dumps(state.document).encode())
            if state.fault == "metadata_generation":
                self.generation = 18

        def download_to_file(self, handle, **options):
            assert options == {"timeout": 15, "retry": None, "if_generation_match": 17}
            state.events.append("metadata_download")
            handle.write(json.dumps(state.document).encode())

        def upload_from_string(self, body, **options):
            assert options == {"timeout": 15, "retry": None, "if_generation_match": 0}
            assert self.name.startswith(prefix + "result/")
            state.events.append(self.name.rsplit("/", 1)[-1])
            is_claim = self.name == prefix + "result/" + _CAPTURE_CONTINUATION_MARKER
            if is_claim and state.peer_timing == "before_claim":
                run_peer()
            if self.name in state.objects:
                raise RuntimeError("SYNTHETIC_PRIVATE_STORAGE collision")
            if state.fault == self.name.rsplit("/", 1)[-1]:
                raise RuntimeError("SYNTHETIC_PRIVATE_STORAGE failure")
            state.objects[self.name] = body
            self.generation = "23"
            if state.fault == self.name.rsplit("/", 1)[-1] + ".committed":
                raise RuntimeError("SYNTHETIC_PRIVATE_STORAGE committed before lost acknowledgement")
            if is_claim and state.peer_timing == "after_commit":
                run_peer()

        def upload_from_filename(self, path, **options):
            self.upload_from_string(Path(path).read_bytes(), **options)

    class Bucket:
        def blob(self, name, generation=None):
            assert name in {prefix + value for value in (
                "identity.input", "result/capture.started", "result/response.body",
                "result/capture.receipt", "result/storage.receipt",
                "result/" + _CAPTURE_CONTINUATION_MARKER)}
            return Blob(name, generation)

    class Storage:
        def __init__(self, **options):
            assert options == {"project": "charlesschwabquant"}
            state.events.append("storage_client")

        def bucket(self, name):
            assert name == state.bucket_name
            state.bucket_names.append(name)
            return Bucket()

    class Secrets:
        def access_secret_version(self, **options):
            state.key_requests.append(options)
            assert options == {"request": {"name": "projects/401309731911/secrets/charles-schwab-api-key/versions/1"},
                               "retry": None, "timeout": 15}
            state.events.append("key_secret")
            if state.fault == "key_secret":
                raise RuntimeError("SYNTHETIC_PRIVATE_SECRET failure")
            return SimpleNamespace(payload=SimpleNamespace(data=b"SYNTHETIC_PRIVATE_KEY"))

    monkeypatch.setattr("google.cloud.storage.Client", Storage)
    monkeypatch.setattr("google.cloud.secretmanager.SecretManagerServiceClient", Secrets)
    real_capture = capture.capture

    def token_loader(*_args):
        state.events.append("token_loader")
        return json.dumps({"creation_timestamp": 1792179840,
                           "token": {"access_token": "SYNTHETIC_PRIVATE_TOKEN", "expires_at": 1792183500}})

    def transport(request):
        state.events.append("broker_get")
        state.requests.append(request)
        assert request.method == "GET"
        return httpx.Response(state.status, stream=httpx.ByteStream(state.body))

    def actual_capture(**options):
        state.events.append("capture_cli")
        if state.fault == "cli_unknown":
            raise KeyboardInterrupt("SYNTHETIC_PRIVATE_CLI")
        result = real_capture(**options, token_loader=token_loader,
                              http_transport=httpx.MockTransport(transport), now=1792179900)
        state.cli_receipts.append(result.receipt)
        return result

    monkeypatch.setattr(capture, "capture", actual_capture)

    def execute():
        with pytest.raises(SystemExit) as raised:
            exec(_capture_workflow_python(), {})
        output = capsys.readouterr()
        assert output.err == ""
        assert "SYNTHETIC_PRIVATE" not in output.out
        public = json.loads(output.out)
        assert set(public) == {"status", "reason", "request_attempts", "source_complete", "private_files_saved"}
        assert public["source_complete"] is False
        return raised.value.code, public

    state.execute = execute
    return state


def test_capture_workflow_real_sdk_and_collector_one_request_private_save_and_reservation(capture_workflow_ports):
    ports = capture_workflow_ports
    code, receipt = ports.execute()
    assert code == 0 and receipt == {"status": "captured_incomplete", "reason": None,
        "request_attempts": 1, "source_complete": False, "private_files_saved": 2}, (receipt, ports.events, ports.cli_receipts)
    assert ports.events == ["storage_client", "metadata_reload", "metadata_download", "predecessor_reload",
                            _CAPTURE_CONTINUATION_MARKER, "key_secret", "capture_cli", "token_loader", "broker_get", "response.body",
                            "capture.receipt", "storage.receipt"]
    assert len(ports.requests) == 1
    request = ports.requests[0]
    assert request.url.params["startDate"] == "2026-10-15T19:45:05Z"
    assert request.url.params["endDate"] == "2026-10-16T19:45:07Z"
    assert ports.objects[ports.prefix + "result/response.body"] == b"[]"
    private = json.loads(ports.objects[ports.prefix + "result/capture.receipt"])
    assert private["request_attempts"] == 1 and private["source_complete"] is False
    manifest = json.loads(ports.objects[ports.prefix + "result/storage.receipt"])
    assert {row["name"] for row in manifest["files"]} == {"response.body", "capture.receipt"}
    assert all(row["generation"] == "23" for row in manifest["files"])
    # Both the archived predecessor and the continuation claim remain reserved.
    code, receipt = ports.execute()
    assert code == 2 and receipt["request_attempts"] == 0
    assert ports.events.count("capture_cli") == ports.events.count("broker_get") == 1


@pytest.mark.parametrize("fault", ["references", "metadata_size", "metadata_generation", "metadata_schema",
                                       _CAPTURE_CONTINUATION_MARKER, "key_secret"])
def test_capture_workflow_pre_request_failures_never_enter_collector(capture_workflow_ports, fault):
    ports = capture_workflow_ports
    ports.fault = fault
    if fault == "references":
        ports.env["CAPTURE_BINDING_URI"] = "gs://unqualified-bucket/private"
    elif fault == "metadata_schema":
        ports.document["records"][0]["account_scope_matches"] = False
    elif fault == "key_secret":
        ports.env["SCHWAB_API_KEY_SECRET_NAME"] = "charles-schwab-api-key"
    code, receipt = ports.execute()
    assert code == 2 and receipt["status"] == "blocked" and receipt["request_attempts"] == 0
    assert not ports.requests and "capture_cli" not in ports.events
    if fault == "references":
        assert ports.events == []


def test_capture_workflow_named_existing_key_is_private_and_has_no_failed_read_fallback(capture_workflow_ports):
    ports = capture_workflow_ports
    ports.env["SCHWAB_API_KEY_SECRET_NAME"] = "charles-schwab-api-key"
    code, receipt = ports.execute()
    assert code == 0 and receipt["request_attempts"] == 1, (receipt, ports.events, ports.cli_receipts)
    assert ports.events.index(_CAPTURE_CONTINUATION_MARKER) < ports.events.index("key_secret") < ports.events.index("token_loader")


@pytest.mark.parametrize("fault", ["cli_unknown", "response.body", "capture.receipt", "storage.receipt"])
def test_capture_workflow_uncertain_or_save_failure_never_retries_capture(capture_workflow_ports, fault):
    ports = capture_workflow_ports
    ports.fault = fault
    code, receipt = ports.execute()
    assert code == 2 and receipt["status"] == "blocked"
    assert receipt["request_attempts"] == (None if fault == "cli_unknown" else 1)
    assert ports.events.count("capture_cli") == 1 and len(ports.requests) == (0 if fault == "cli_unknown" else 1)
    assert ports.prefix + "result/" + _CAPTURE_CONTINUATION_MARKER in ports.objects


def test_capture_workflow_binding_failure_keeps_known_zero_without_fabricating_response(capture_workflow_ports):
    ports = capture_workflow_ports
    ports.env["SCHWAB_ACCOUNT_FACTS_TARGET_ID"] = "different-synthetic-target"
    code, receipt = ports.execute()
    assert code == 2 and receipt["request_attempts"] == 0 and not ports.requests
    assert ports.cli_receipts[0]["reason"] == "binding_mismatch"
    assert ports.prefix + "result/response.body" not in ports.objects


def test_capture_workflow_http_error_is_private_and_not_retried(capture_workflow_ports):
    ports = capture_workflow_ports
    ports.status, ports.body = 503, b"SYNTHETIC_PRIVATE_ERROR_BODY"
    code, receipt = ports.execute()
    assert code == 2 and receipt["reason"] == "http_error" and receipt["request_attempts"] == 1
    assert len(ports.requests) == 1
    assert ports.objects[ports.prefix + "result/response.body"] == ports.body
    assert receipt["private_files_saved"] == 2


@pytest.mark.parametrize("fault,confirmed_count", [
    ("capture.receipt", None), ("storage.receipt", 2),
    ("response.body.committed", None), ("capture.receipt.committed", None),
])
def test_capture_workflow_save_receipt_preserves_confirmed_or_unknown_writes(capture_workflow_ports, fault, confirmed_count):
    ports = capture_workflow_ports
    ports.fault = fault
    code, receipt = ports.execute()
    assert code == 2 and receipt["request_attempts"] == 1
    assert receipt["private_files_saved"] == confirmed_count
    assert len(ports.requests) == 1 and ports.events.count("capture_cli") == 1
    assert ports.prefix + "result/" + _CAPTURE_CONTINUATION_MARKER in ports.objects
    if fault in {"storage.receipt", "capture.receipt.committed"}:
        assert ports.prefix + "result/response.body" in ports.objects
        assert ports.prefix + "result/capture.receipt" in ports.objects


@pytest.mark.parametrize("fault", [
    "off", "valid", "PUBLISH", "DIAGNOSE_SOURCE_ACCESS", "VERIFY_NATIVE_IDENTITY",
    "CHECK_TOKEN_LOAD", "EMIT_DIGEST_CANDIDATES", "CAPTURE_ARTIFACT", "binding_uri",
    "output_prefix", "generation_empty", "generation_zero", "generation_text", "generation_long",
    "bucket_missing", "bucket_shared", "bucket_invalid", "binding_other_bucket", "output_other_bucket",
])
def test_capture_workflow_exact_shell_preflight_rejects_conflicts_or_unbound_references(fault):
    import subprocess
    import textwrap

    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    step = workflow.split("- name: Reject conflicting operation inputs\n", 1)[1].split("      - name:", 1)[0]
    script = textwrap.dedent(step.split("        run: |\n", 1)[1])
    base = "gs://schwab-private-evidence-test/digest-candidates/schwab/_transactions_once/window-20261015-20261016/"
    env = {"CAPTURE_TRANSACTIONS_ONCE": "true", "CAPTURE_BUCKET": "schwab-private-evidence-test",
           "CAPTURE_BINDING_URI": base + "identity.input",
           "CAPTURE_BINDING_GENERATION": "17", "CAPTURE_OUTPUT_PREFIX": base + "result/"}
    if fault == "off":
        env = {"CAPTURE_TRANSACTIONS_ONCE": "false"}
    elif fault.isupper():
        env[fault] = "true"
    elif fault == "binding_uri":
        env["CAPTURE_BINDING_URI"] = "gs://unqualified-bucket/private"
    elif fault == "output_prefix":
        env["CAPTURE_OUTPUT_PREFIX"] = base + "unapproved/"
    elif fault.startswith("generation_"):
        env["CAPTURE_BINDING_GENERATION"] = {"empty": "", "zero": "0", "text": "PRIVATE", "long": "1" * 21}[fault[11:]]
    elif fault == "bucket_missing":
        env.pop("CAPTURE_BUCKET")
    elif fault in {"bucket_shared", "bucket_invalid"}:
        name = "qsl-runtime-logs-shared" if fault == "bucket_shared" else "bad/bucket"
        env["CAPTURE_BUCKET"] = name
        changed = base.replace("schwab-private-evidence-test", name)
        env["CAPTURE_BINDING_URI"] = changed + "identity.input"
        env["CAPTURE_OUTPUT_PREFIX"] = changed + "result/"
    elif fault == "binding_other_bucket":
        env["CAPTURE_BINDING_URI"] = base.replace("schwab-private-evidence-test", "another-private-bucket") + "identity.input"
    elif fault == "output_other_bucket":
        env["CAPTURE_OUTPUT_PREFIX"] = base.replace("schwab-private-evidence-test", "another-private-bucket") + "result/"
    result = subprocess.run(["/bin/bash", "--noprofile", "--norc", "-c", script],
                            cwd=ROOT, env=env, timeout=5, capture_output=True, text=True, check=False)
    assert result.stderr == ""
    if fault in {"off", "valid"}:
        assert result.returncode == 0 and result.stdout == ""
    else:
        assert result.returncode == 2
        reason = "conflicting_operation_inputs" if fault.isupper() else "capture_private_references_required"
        assert result.stdout == "skipped:" + reason + "\n"


@pytest.mark.parametrize("name", ["schwab-private-evidence-test", "schwab-private-evidence-alt"])
def test_capture_workflow_uses_the_configured_private_bucket(capture_workflow_ports, name):
    ports = capture_workflow_ports
    ports.bucket_name = name
    ports.env["CAPTURE_BUCKET"] = name
    base = "gs://" + name + "/" + ports.prefix
    ports.env["CAPTURE_BINDING_URI"] = base + "identity.input"
    ports.env["CAPTURE_OUTPUT_PREFIX"] = base + "result/"
    code, receipt = ports.execute()
    assert code == 0 and receipt["private_files_saved"] == 2
    assert ports.bucket_names == [name]
    assert len(ports.requests) == 1
    assert ports.events.index(_CAPTURE_CONTINUATION_MARKER) < ports.events.index("capture_cli")


@pytest.mark.parametrize("fault", ["missing", "shared", "invalid", "binding_other_bucket", "output_other_bucket"])
def test_capture_workflow_bucket_failures_precede_storage_and_credentials(capture_workflow_ports, fault):
    ports = capture_workflow_ports
    if fault == "missing":
        ports.env.pop("CAPTURE_BUCKET")
    elif fault in {"shared", "invalid"}:
        name = "qsl-runtime-logs-shared" if fault == "shared" else "bad/bucket"
        ports.env["CAPTURE_BUCKET"] = name
        base = "gs://" + name + "/" + ports.prefix
        ports.env["CAPTURE_BINDING_URI"] = base + "identity.input"
        ports.env["CAPTURE_OUTPUT_PREFIX"] = base + "result/"
    elif fault == "binding_other_bucket":
        ports.env["CAPTURE_BINDING_URI"] = "gs://another-private-bucket/" + ports.prefix + "identity.input"
    else:
        ports.env["CAPTURE_OUTPUT_PREFIX"] = "gs://another-private-bucket/" + ports.prefix + "result/"
    code, receipt = ports.execute()
    assert code == 2 and receipt["request_attempts"] == 0 and receipt["private_files_saved"] == 0
    assert ports.events == [] and not ports.requests
    assert ports.objects == {ports.predecessor_key: ports.predecessor_body}


def test_capture_continuation_keeps_old_claim_and_has_one_new_fixed_claim(capture_workflow_ports):
    ports = capture_workflow_ports
    code, receipt = ports.execute()
    assert code == 0 and receipt["request_attempts"] == 1
    assert ports.objects[ports.predecessor_key] == ports.predecessor_body
    assert ports.events.count("capture.started") == 0
    assert ports.prefix + "result/" + _CAPTURE_CONTINUATION_MARKER in ports.objects
    assert ports.events.index("predecessor_reload") < ports.events.index(_CAPTURE_CONTINUATION_MARKER)
    assert ports.events.index(_CAPTURE_CONTINUATION_MARKER) < ports.events.index("key_secret")
    assert ports.key_requests == [{"request": {
        "name": "projects/401309731911/secrets/charles-schwab-api-key/versions/1"},
        "timeout": 15, "retry": None}]
    code, receipt = ports.execute()
    assert code == 2 and receipt["request_attempts"] == 0
    assert len(ports.requests) == len(ports.key_requests) == 1
    assert ports.objects[ports.predecessor_key] == ports.predecessor_body


@pytest.mark.parametrize("fault", ["predecessor_missing", "predecessor_generation", "predecessor_size"])
def test_capture_continuation_rejects_unmatched_live_predecessor(capture_workflow_ports, fault):
    ports = capture_workflow_ports
    ports.fault = fault
    code, receipt = ports.execute()
    assert code == 2 and receipt["request_attempts"] == 0
    assert "predecessor_reload" in ports.events
    assert _CAPTURE_CONTINUATION_MARKER not in ports.events
    assert not ports.key_requests and not ports.requests and "capture_cli" not in ports.events
    assert ports.objects == {ports.predecessor_key: ports.predecessor_body}


@pytest.mark.parametrize("timing", ["before_claim", "after_commit"])
def test_capture_continuation_interleaved_workers_share_one_atomic_claim(capture_workflow_ports, timing):
    ports = capture_workflow_ports
    ports.env["GITHUB_RUN_ID"] = "synthetic-first-worker"
    ports.peer_timing = timing
    result = ports.execute()
    assert len(ports.peer_results) == 1
    outcomes = [result, ports.peer_results[0]]
    assert sorted(code for code, _ in outcomes) == [0, 2]
    assert sorted(receipt["request_attempts"] for _, receipt in outcomes) == [0, 1]
    assert len(ports.requests) == len(ports.key_requests) == 1
    assert ports.events.count("capture_cli") == ports.events.count("broker_get") == 1
    assert ports.objects[ports.predecessor_key] == ports.predecessor_body
    assert {key for key in ports.objects if key.endswith(".started")} == {
        ports.predecessor_key, ports.prefix + "result/" + _CAPTURE_CONTINUATION_MARKER}


def test_capture_continuation_uncertain_claim_never_reopens_or_calls(capture_workflow_ports):
    ports = capture_workflow_ports
    ports.fault = _CAPTURE_CONTINUATION_MARKER + ".committed"
    code, receipt = ports.execute()
    assert code == 2 and receipt["request_attempts"] == 0
    assert ports.prefix + "result/" + _CAPTURE_CONTINUATION_MARKER in ports.objects
    ports.fault = None
    code, receipt = ports.execute()
    assert code == 2 and receipt["request_attempts"] == 0
    assert not ports.key_requests and not ports.requests and "capture_cli" not in ports.events
    assert ports.objects[ports.predecessor_key] == ports.predecessor_body


@pytest.mark.parametrize("reference", [None, "", "synthetic-other-key", "projects/other/secrets/key"])
def test_capture_continuation_never_switches_the_selected_key_reference(capture_workflow_ports, reference):
    ports = capture_workflow_ports
    if reference is None:
        ports.env.pop("SCHWAB_API_KEY_SECRET_NAME")
    else:
        ports.env["SCHWAB_API_KEY_SECRET_NAME"] = reference
    code, receipt = ports.execute()
    assert code == 2 and receipt["request_attempts"] == 0
    assert not ports.events and not ports.key_requests and not ports.requests
    assert ports.objects == {ports.predecessor_key: ports.predecessor_body}


@pytest.mark.parametrize("conflict", [None, "CAPTURE_TRANSACTIONS_ONCE", "PUBLISH", "DIAGNOSE_SOURCE_ACCESS",
                                       "VERIFY_NATIVE_IDENTITY", "CHECK_TOKEN_LOAD", "EMIT_DIGEST_CANDIDATES", "CAPTURE_ARTIFACT"])
def test_standard_refresh_actual_shell_gate_is_exclusive_before_cloud_auth(conflict):
    import os
    import subprocess

    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    step = workflow.split("- name: Reject conflicting operation inputs\n", 1)[1].split("      - name:", 1)[0]
    script = step.split("        run: |\n", 1)[1]
    script = "\n".join(line[10:] for line in script.splitlines())
    env = {"PATH": os.environ["PATH"], "RENEW_ACCESS_TOKEN_ONCE": "true"}
    if conflict:
        env[conflict] = "true"
    result = subprocess.run(["/bin/bash", "-c", script], env=env, capture_output=True, text=True)
    assert result.returncode == (2 if conflict else 0)
    assert result.stdout == ("skipped:conflicting_operation_inputs\n" if conflict else "")
    assert result.stderr == ""


def test_standard_refresh_is_separate_default_off_and_has_no_data_or_login_path():
    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    section = workflow.split("      renew_access_token_once:\n", 1)[1].split("      emit_digest_candidates:", 1)[0]
    assert "type: boolean" in section and "default: false" in section
    step = workflow.split("      - name: Renew cached Schwab access token once\n", 1)[1].split("      - name:", 1)[0]
    assert "python -m scripts.schwab_native_identity --renew-access-token-once" in step
    assert 'SCHWAB_RENEW_ACCESS_TOKEN_ONCE: "true"' in step
    assert not any(x in step for x in ("SCHWAB_PASSWORD", "TOTP", "GCP_SA_KEY", "capture.started", "upload-artifact"))
    for other in workflow.split("      - name:")[1:]:
        if other.startswith((" Renew cached Schwab access token once", " Capture fixed Schwab transactions once")):
            continue
        conditions = re.findall(r"^        if: (.+)$", other, re.MULTILINE)
        assert all("!inputs.renew_access_token_once" in x for x in conditions)


@pytest.mark.parametrize("conflict", [None, "CAPTURE_TRANSACTIONS_ONCE", "PUBLISH", "DIAGNOSE_SOURCE_ACCESS",
                                       "VERIFY_NATIVE_IDENTITY", "CHECK_TOKEN_LOAD", "EMIT_DIGEST_CANDIDATES", "CAPTURE_ARTIFACT"])
def test_renewal_job_rejects_conflicts_without_loading_private_inventory(conflict):
    import os
    import subprocess

    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    daily, renewal = workflow.split("  renew-token:\n", 1)
    daily_condition = re.search(r"^    if: (.+)$", daily, re.MULTILINE).group(1)
    renewal_condition = re.search(r"^    if: (.+)$", renewal, re.MULTILINE).group(1)
    for enabled in (False, True):
        bindings = {"github.event_name": "'workflow_dispatch'", "github.ref": "'refs/heads/main'",
                    "inputs.renew_access_token_once": str(enabled), "inputs.check_renewal_credentials": "False"}
        conditions = []
        for condition in (daily_condition, renewal_condition):
            for key, value in bindings.items():
                condition = condition.replace(key, value)
            conditions.append(eval(condition.replace("&&", " and ").replace("||", " or ").replace("!", "not ")))
        assert conditions == [not enabled, enabled]
    # This job must not load inventory, account references, login credentials or artifacts.
    assert not any(x in renewal for x in ("vars.", "secrets.", "upload-artifact", "RUNTIME_TARGET_JSON",
                                         "CLOUD_RUN_SERVICE", "ACCOUNT_FACTS", "TOTP", "PASSWORD"))
    assert renewal.index("Reject renewal with other operation inputs") < renewal.index("Authenticate to Google Cloud")
    guard = renewal.split("        run: |\n", 1)[1].split("      - name:", 1)[0]
    script = "\n".join(line[10:] for line in guard.splitlines())
    env = {"PATH": os.environ["PATH"]}
    if conflict:
        env[conflict] = "true"
    result = subprocess.run(["/bin/bash", "-c", script], env=env, capture_output=True, text=True)
    assert result.returncode == (2 if conflict else 0)
    assert result.stdout == ("skipped:conflicting_operation_inputs\n" if conflict else "")
    assert result.stderr == ""


@pytest.mark.parametrize("renew,check", [(False, False), (True, False), (False, True), (True, True)])
def test_renewal_readiness_mode_cannot_fall_through_to_a_grant(renew, check):
    import os
    import subprocess

    workflow = (ROOT / ".github/workflows/runtime-daily-sync.yml").read_text()
    daily, isolated = workflow.split("  renew-token:\n", 1)
    assignment = {"github.event_name": "'workflow_dispatch'", "github.ref": "'refs/heads/main'",
                  "inputs.renew_access_token_once": str(renew), "inputs.check_renewal_credentials": str(check)}

    def evaluate(condition):
        for key, value in assignment.items():
            condition = condition.replace(key, value)
        return eval(condition.replace("${{", "").replace("}}", "").replace("&&", " and ").replace("||", " or ").replace("!", "not "))

    assert evaluate(re.search(r"^    if: (.+)$", daily, re.MULTILINE).group(1)) == (not renew and not check)
    assert evaluate(re.search(r"^    if: (.+)$", isolated, re.MULTILINE).group(1)) == (renew or check)
    readiness = isolated.split("      - name: Check fixed renewal credential readiness\n", 1)[1].split("      - name:", 1)[0]
    renewal = isolated.split("      - name: Renew cached Schwab access token once\n", 1)[1]
    assert evaluate(re.search(r"^        if: (.+)$", readiness, re.MULTILINE).group(1)) == (check and not renew)
    assert evaluate(re.search(r"^        if: (.+)$", renewal, re.MULTILINE).group(1)) == (renew and not check)
    assert "--check-renewal-credentials" in readiness
    assert "SCHWAB_RENEW_ACCESS_TOKEN_ONCE" not in readiness
    guard = isolated.split("        run: |\n", 1)[1].split("      - name:", 1)[0]
    script = "\n".join(line[10:] for line in guard.splitlines())
    result = subprocess.run(["/bin/bash", "-c", script], capture_output=True, text=True,
                            env={"PATH": os.environ["PATH"], "RENEW_ACCESS_TOKEN_ONCE": str(renew).lower(),
                                 "CHECK_RENEWAL_CREDENTIALS": str(check).lower()})
    assert result.returncode == (2 if renew and check else 0)
