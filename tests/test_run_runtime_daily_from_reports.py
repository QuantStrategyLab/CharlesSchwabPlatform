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


def run(*, environ=None, publish=False, observation=None, **overrides):
    readers = {
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
    assert "github.event_name == 'workflow_dispatch'" in workflow
    assert "github.ref == 'refs/heads/main'" in workflow
    assert "google-github-actions/auth@v3" in workflow
    assert "uv sync --frozen --no-dev" in workflow
    assert "!inputs.publish" in workflow and "inputs.publish" in workflow
    prepare = workflow.split("- name: Prepare runtime daily", 1)[1].split(
        "- name: Publish runtime daily", 1
    )[0]
    assert "--publish" not in prepare and "SYNC_TOKEN" not in prepare
    assert workflow.count("secrets.EXECUTION_EVIDENCE_SYNC_TOKEN") == 1
    assert "run_runtime_daily_from_reports.py --publish" in workflow
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


@pytest.mark.parametrize(
    "event", ["workflow_dispatch", "schedule", "push", "pull_request", "workflow_run"]
)
@pytest.mark.parametrize("ref", ["refs/heads/main", "refs/heads/private-branch"])
@pytest.mark.parametrize("publish", [None, False, True])
@pytest.mark.parametrize("diagnose", [None, False, True])
def test_workflow_event_branch_and_modes_are_mutually_exclusive(
    event, ref, publish, diagnose
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

    def evaluate(expression):
        expression = (
            expression.replace("github.event_name", repr(event))
            .replace("github.ref", repr(ref))
            .replace("inputs.publish", repr(publish))
            .replace("inputs.diagnose_source_access", repr(diagnose))
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
    prepare_runs, publish_runs, diagnostic_runs = (
        runnable and evaluate(prepare),
        runnable and evaluate(post),
        runnable and evaluate(diagnostic),
    )
    valid_dispatch = event == "workflow_dispatch" and ref == "refs/heads/main"
    assert prepare_runs == (valid_dispatch and not publish and not diagnose)
    assert publish_runs == (valid_dispatch and bool(publish) and not diagnose)
    assert diagnostic_runs == (valid_dispatch and bool(diagnose) and not publish)
    assert sum((prepare_runs, publish_runs, diagnostic_runs)) <= 1


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
