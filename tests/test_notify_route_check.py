from __future__ import annotations

import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("readonly_notify_route_check", ROOT / "scripts/notify_route_check.py")
route = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(route)

PROJECT = "synthetic-project"
SERVICE = "private-service"
REVISION = "private-serving"
VERSION = "97007"
BOUND_SECRET = "private-bound-secret"
CONTRACT_TOKEN = "98765001:PRIVATE_CONTRACT_TOKEN"
BOUND_TOKEN = "98765002:PRIVATE_BOUND_TOKEN"
OTHER_TOKEN = "98765003:PRIVATE_OTHER_TOKEN"
CONTRACT_BOT = 98765001
OTHER_BOT = 98765002
SOURCE_SHA = "a" * 40


def secret_env(name=BOUND_SECRET, version=VERSION):
    return {"name": "TELEGRAM_TOKEN", "valueFrom": {"secretKeyRef": {"name": name, "key": version}}}


def revision(env=None, containers=None, annotations=None):
    return {"metadata": {"annotations": annotations or {}},
            "spec": {"serviceAccountName": "private-sa@example.test",
                     "containers": containers if containers is not None else [{"env": env if env is not None else [
                         secret_env(), {"name": "QSL_GLOBAL_TELEGRAM_CHAT_ID", "value": "PRIVATE_CHAT_QSL"},
                         {"name": "GLOBAL_TELEGRAM_CHAT_ID", "value": "PRIVATE_CHAT_GLOBAL"},
                     ]}]}}


class FakeCloud:
    def __init__(self):
        self.calls = []
        self.service = {
            "spec": {"template": revision([secret_env(route.CONTRACT_SECRET, "latest")])},
            "status": {"latestReadyRevisionName": "private-ready",
                       "traffic": [{"revisionName": REVISION, "percent": 100}]},
        }
        self.revisions = {REVISION: revision()}
        self.secrets = {
            (PROJECT, route.CONTRACT_SECRET, "latest"): CONTRACT_TOKEN,
            (PROJECT, BOUND_SECRET, VERSION): BOUND_TOKEN,
            (PROJECT, BOUND_SECRET, "latest"): CONTRACT_TOKEN,
        }
        self.fail = set()

    def __call__(self, *args):
        self.calls.append(args)
        if args[:3] == ("run", "services", "describe"):
            key = ("service", args[3])
            value = self.service
        elif args[:3] == ("run", "revisions", "describe"):
            key = ("revision", args[3])
            value = self.revisions[args[3]]
        elif args[:3] == ("secrets", "versions", "access"):
            project = next(arg.removeprefix("--project=") for arg in args if arg.startswith("--project="))
            secret = next(arg.removeprefix("--secret=") for arg in args if arg.startswith("--secret="))
            key = ("secret", project, secret, args[3])
            value = self.secrets[(project, secret, args[3])]
        else:
            raise AssertionError("Unexpected cloud operation")
        if key in self.fail:
            raise subprocess.CalledProcessError(1, ["PRIVATE_COMMAND"], output="PRIVATE_STDOUT", stderr="PRIVATE_STDERR")
        return value if isinstance(value, str) else json.dumps(value)


class FakeReply(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class FakeTelegram:
    def __init__(self):
        self.calls = []
        self.results = {
            CONTRACT_TOKEN: {"ok": True, "result": {"id": CONTRACT_BOT, "is_bot": True,
                                                   "username": "PRIVATE_BOT_NAME"}},
            BOUND_TOKEN: {"ok": True, "result": {"id": OTHER_BOT, "is_bot": True}},
            OTHER_TOKEN: {"ok": True, "result": {"id": CONTRACT_BOT, "is_bot": True}},
        }

    def __call__(self, request, **kwargs):
        url = request.full_url if hasattr(request, "full_url") else request
        method = request.get_method() if hasattr(request, "get_method") else "GET"
        assert method == "GET"
        assert url.startswith("https://api.telegram.org/bot") and url.endswith("/getMe")
        token = url.removeprefix("https://api.telegram.org/bot").removesuffix("/getMe")
        self.calls.append(token)
        value = self.results[token]
        if isinstance(value, Exception):
            raise value
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        return FakeReply(raw)


def observe(tmp_path, cloud=None, telegram=None, **env_overrides):
    cloud = cloud or FakeCloud()
    telegram = telegram or FakeTelegram()
    env = {"GCP_PROJECT_ID": PROJECT, "CLOUD_RUN_REGION": "private-region", "CLOUD_RUN_SERVICE": SERVICE,
           "SOURCE_SHA": SOURCE_SHA, "GITHUB_STEP_SUMMARY": str(tmp_path / "summary"), **env_overrides}
    output = io.StringIO()
    errors = io.StringIO()
    with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(route, "_gcloud", cloud), \
            mock.patch.object(route.urllib.request, "urlopen", telegram), \
            mock.patch("sys.stdout", output), mock.patch("sys.stderr", errors):
        code = route.main()
    text = output.getvalue()
    summary = (tmp_path / "summary").read_text()
    for private in [PROJECT, SERVICE, REVISION, VERSION, BOUND_SECRET, route.CONTRACT_SECRET,
                    CONTRACT_TOKEN, BOUND_TOKEN, "private-ready", "private-sa@example.test", "private-region",
                    "PRIVATE_CHAT", "PRIVATE_STDOUT", "PRIVATE_STDERR", "PRIVATE_BOT", "PRIVATE_COMMAND",
                    "api.telegram.org", str(CONTRACT_BOT), str(OTHER_BOT), "::add-mask::"]:
        assert private not in text + errors.getvalue() + summary
    assert errors.getvalue() == ""
    assert code == 0
    report = json.loads(text)
    assert report["schema_version"] == "qsl.notify_route_identity.v1"
    assert report["reference_scope"] == "project_contract_only"
    assert report["canonical_route_status"] == "unknown"
    for service in report["services"]:
        for row in [service, *service["revisions"]]:
            assert row["same_chat_as_canonical"] is None
            assert row["same_topic_as_canonical"] is None
            assert row["chat_status"] == row["topic_status"] == "unknown"
    return report, cloud, telegram


def test_template_and_secret_latest_are_not_serving_evidence(tmp_path):
    report, cloud, _ = observe(tmp_path)
    row = report["services"][0]
    assert row["same_bot_as_contract"] is False
    assert row["bot_status"] == "mismatch"
    assert report["source_sha"] == SOURCE_SHA
    assert ("run", "revisions", "describe", REVISION, f"--project={PROJECT}",
            "--region=private-region", "--format=json") in cloud.calls
    assert ("secrets", "versions", "access", VERSION, f"--secret={BOUND_SECRET}",
            f"--project={PROJECT}") in cloud.calls
    assert not any(args[:4] == ("secrets", "versions", "access", "latest")
                   and f"--secret={BOUND_SECRET}" in args for args in cloud.calls)


def test_fixed_version_can_match_while_latest_and_template_differ(tmp_path):
    cloud = FakeCloud()
    cloud.secrets[(PROJECT, BOUND_SECRET, VERSION)] = OTHER_TOKEN
    cloud.secrets[(PROJECT, BOUND_SECRET, "latest")] = BOUND_TOKEN
    report, _, _ = observe(tmp_path, cloud)
    assert report["services"][0]["same_bot_as_contract"] is True
    assert report["services"][0]["bot_status"] == "match"


def test_same_secret_name_does_not_prove_same_bot(tmp_path):
    cloud = FakeCloud()
    cloud.revisions[REVISION] = revision([secret_env(route.CONTRACT_SECRET)])
    cloud.secrets[(PROJECT, route.CONTRACT_SECRET, VERSION)] = BOUND_TOKEN
    report, _, _ = observe(tmp_path, cloud)
    assert report["services"][0]["same_bot_as_contract"] is False


@pytest.mark.parametrize("traffic", [
    None, {}, [], [{"revisionName": REVISION, "percent": 0}], [None],
    [{"revisionName": REVISION}], [{"revisionName": REVISION, "percent": True}],
    [{"revisionName": REVISION, "percent": "100"}], [{"revisionName": REVISION, "percent": -1}],
    [{"revisionName": REVISION, "percent": 101}], [{"revisionName": REVISION, "percent": 50}],
    [{"latestRevision": True, "percent": 100}], [{"revisionName": "", "percent": 100}],
    [{"revisionName": REVISION, "percent": 50}, {"revisionName": REVISION, "percent": 50}],
])
def test_missing_partial_invalid_and_ambiguous_traffic_is_unknown(tmp_path, traffic):
    cloud = FakeCloud()
    cloud.service["status"]["traffic"] = traffic
    report, _, _ = observe(tmp_path, cloud)
    row = report["services"][0]
    assert row["same_bot_as_contract"] is None
    assert row["bot_status"] == "unknown"
    assert not any(args[:3] == ("run", "revisions", "describe") for args in cloud.calls)


def test_split_traffic_reads_each_revision_and_remains_unknown(tmp_path):
    cloud = FakeCloud()
    cloud.service["status"]["traffic"] = [
        {"revisionName": REVISION, "percent": 50}, {"revisionName": "private-serving-two", "percent": 50},
        {"revisionName": "private-zero-traffic", "percent": 0},
    ]
    cloud.revisions["private-serving-two"] = revision([secret_env(version="97008")])
    cloud.secrets[(PROJECT, BOUND_SECRET, "97008")] = OTHER_TOKEN
    report, _, _ = observe(tmp_path, cloud)
    row = report["services"][0]
    assert row["traffic_status"] == "split"
    assert row["same_bot_as_contract"] is None
    assert len(row["revisions"]) == 2
    assert all(item["same_bot_as_contract"] is None for item in row["revisions"])
    described = [args[3] for args in cloud.calls if args[:3] == ("run", "revisions", "describe")]
    assert described == [REVISION, "private-serving-two"]
    assert not any("private-zero-traffic" in args for args in cloud.calls)


@pytest.mark.parametrize("containers", [[], [{}, {}], None])
def test_missing_or_multiple_containers_do_not_guess(tmp_path, containers):
    cloud = FakeCloud()
    cloud.revisions[REVISION] = {"spec": {"containers": containers}}
    report, _, _ = observe(tmp_path, cloud)
    assert report["services"][0]["same_bot_as_contract"] is None
    assert not any(f"--secret={BOUND_SECRET}" in args for args in cloud.calls)


@pytest.mark.parametrize("env", [
    [], [{"name": "TG_TOKEN", "value": OTHER_TOKEN}], [secret_env(), secret_env()],
    [{"name": "TELEGRAM_TOKEN"}], [{"name": "TELEGRAM_TOKEN", "value": ""}],
    [{"name": "TELEGRAM_TOKEN", "value": 7}],
    [{"name": "TELEGRAM_TOKEN", "value": OTHER_TOKEN,
      "valueFrom": {"secretKeyRef": {"name": BOUND_SECRET, "key": VERSION}}}],
    [secret_env(version="latest")], [secret_env(version="current")], [secret_env(version=None)],
    [secret_env(version=7)], [secret_env(version="0")], [secret_env(version="-1")],
    [secret_env(name="")], [secret_env(name="PRIVATE_BAD_SECRET/path")],
])
def test_missing_ambiguous_or_unpinned_winner_is_unknown_without_fallback(tmp_path, env):
    cloud = FakeCloud()
    cloud.revisions[REVISION] = revision(env)
    report, _, telegram = observe(tmp_path, cloud)
    assert report["services"][0]["same_bot_as_contract"] is None
    assert telegram.calls == [CONTRACT_TOKEN]


@pytest.mark.parametrize("name,annotations,secret_project", [
    (f"projects/{PROJECT}/secrets/{BOUND_SECRET}", {}, PROJECT),
    ("private-alias", {"run.googleapis.com/secrets": f"private-alias:projects/{PROJECT}/secrets/{BOUND_SECRET}"}, PROJECT),
    ("private-alias", {"run.googleapis.com/secrets": f"private-alias:projects/synthetic-other/secrets/{BOUND_SECRET}"},
     "synthetic-other"),
])
def test_qualified_and_annotated_secret_reference_keeps_exact_project_version(tmp_path, name, annotations, secret_project):
    cloud = FakeCloud()
    cloud.revisions[REVISION] = revision([secret_env(name=name)], annotations=annotations)
    cloud.secrets[(secret_project, BOUND_SECRET, VERSION)] = OTHER_TOKEN
    report, _, _ = observe(tmp_path, cloud)
    assert report["services"][0]["same_bot_as_contract"] is True
    assert ("secrets", "versions", "access", VERSION, f"--secret={BOUND_SECRET}",
            f"--project={secret_project}") in cloud.calls


@pytest.mark.parametrize("annotations", [
    {"run.googleapis.com/secrets": "private-alias:PRIVATE_BAD_PATH"},
    {"run.googleapis.com/secrets": "private-alias:projects/synthetic-project/secrets/a,private-alias:projects/synthetic-project/secrets/b"},
    {"run.googleapis.com/secrets": {"private-alias": "PRIVATE_BAD_VALUE"}},
])
def test_invalid_or_duplicate_secret_alias_cannot_fall_back_to_same_project_name(tmp_path, annotations):
    cloud = FakeCloud()
    cloud.secrets[(PROJECT, "PRIVATE_BAD_PATH", VERSION)] = OTHER_TOKEN
    cloud.revisions[REVISION] = revision([secret_env(name="private-alias")], annotations=annotations)
    report, _, telegram = observe(tmp_path, cloud)
    assert report["services"][0]["same_bot_as_contract"] is None
    assert telegram.calls == [CONTRACT_TOKEN]


@pytest.mark.parametrize("stage", ["service", "revision", "bound_secret", "contract_secret", "bound_getme", "contract_getme"])
def test_private_command_and_network_failures_are_nullable_and_redacted(tmp_path, stage):
    cloud, telegram = FakeCloud(), FakeTelegram()
    if stage == "service":
        cloud.fail.add(("service", SERVICE))
    elif stage == "revision":
        cloud.fail.add(("revision", REVISION))
    elif stage == "bound_secret":
        cloud.fail.add(("secret", PROJECT, BOUND_SECRET, VERSION))
    elif stage == "contract_secret":
        cloud.fail.add(("secret", PROJECT, route.CONTRACT_SECRET, "latest"))
    else:
        telegram.results[BOUND_TOKEN if stage == "bound_getme" else CONTRACT_TOKEN] = RuntimeError("PRIVATE_EXCEPTION_URL")
    report, _, _ = observe(tmp_path, cloud, telegram)
    assert report["services"][0]["same_bot_as_contract"] is None


@pytest.mark.parametrize("body", [
    b"{broken", [], {}, {"ok": False}, {"ok": True, "result": None},
    {"ok": True, "result": {"id": True, "is_bot": True}},
    {"ok": True, "result": {"id": "98765001", "is_bot": True}},
    {"ok": True, "result": {"id": -1, "is_bot": True}},
    {"ok": True, "result": {"id": CONTRACT_BOT, "is_bot": False}},
    {"ok": True, "result": {"id": CONTRACT_BOT}},
    b"x" * (64 * 1024 + 1),
])
def test_invalid_getme_payload_is_unknown_not_false_or_match(tmp_path, body):
    telegram = FakeTelegram()
    telegram.results[BOUND_TOKEN] = body
    report, _, _ = observe(tmp_path, telegram=telegram)
    assert report["services"][0]["same_bot_as_contract"] is None


def test_plain_env_single_container_can_be_compared_without_secret_value_output(tmp_path):
    cloud = FakeCloud()
    cloud.revisions[REVISION] = revision([{"name": "TELEGRAM_TOKEN", "value": OTHER_TOKEN}])
    report, _, _ = observe(tmp_path, cloud)
    assert report["services"][0]["same_bot_as_contract"] is True
    assert report["services"][0]["revisions"][0]["token_source"] == "plain_env"


def test_primary_token_failure_is_not_rescued_by_secondary_token(tmp_path):
    cloud, telegram = FakeCloud(), FakeTelegram()
    cloud.revisions[REVISION] = revision([
        {"name": "TELEGRAM_TOKEN", "value": BOUND_TOKEN}, {"name": "TG_TOKEN", "value": OTHER_TOKEN},
    ])
    telegram.results[BOUND_TOKEN] = RuntimeError("PRIVATE_WINNER_FAILED")
    report, _, _ = observe(tmp_path, cloud, telegram)
    assert report["services"][0]["same_bot_as_contract"] is None
    assert OTHER_TOKEN not in telegram.calls


@pytest.mark.parametrize("env_overrides", [
    {"CLOUD_RUN_SERVICE": ""}, {"CLOUD_RUN_SERVICE_TARGETS_JSON": "{broken"},
    {"CLOUD_RUN_SERVICE_TARGETS_JSON": '{"targets":[{"runtime_target":7}]}'},
    {"CLOUD_RUN_SERVICE_TARGETS_JSON": '{"targets":[{}]}'},
    {"CLOUD_RUN_SERVICE_TARGETS_JSON": '{"targets":"PRIVATE_BAD_TARGETS"}'},
    {"GCP_PROJECT_ID": ""}, {"GCP_PROJECT_ID": "PRIVATE_BAD_PROJECT/path"},
])
def test_missing_invalid_config_and_target_mapping_are_fixed_unknown(tmp_path, env_overrides):
    report, cloud, _ = observe(tmp_path, **env_overrides)
    assert report["status"] == "config_unknown"
    assert report["services"] == []
    assert cloud.calls == []


def test_target_selection_keeps_existing_sources_and_deduplicates_without_disclosing_names(tmp_path):
    report, cloud, _ = observe(
        tmp_path, CLOUD_RUN_SERVICES=SERVICE, RUNTIME_GUARD_CLOUD_RUN_SERVICES=SERVICE,
        CLOUD_RUN_SERVICE_TARGETS_JSON=json.dumps({"targets": [
            {"runtime_target": {"service_name": SERVICE}}, SERVICE,
        ]}),
    )
    assert len(report["services"]) == 1
    assert sum(args[:3] == ("run", "services", "describe") for args in cloud.calls) == 1


@pytest.mark.parametrize("stage", ["service", "revision"])
def test_malformed_metadata_cannot_escape_as_private_traceback(tmp_path, stage):
    cloud = FakeCloud()
    if stage == "service":
        cloud.service = "{PRIVATE_BAD_SERVICE_JSON"
    else:
        cloud.revisions[REVISION] = "{PRIVATE_BAD_REVISION_JSON"
    report, _, _ = observe(tmp_path, cloud)
    assert report["services"][0]["same_bot_as_contract"] is None


def test_subprocess_cloud_command_captures_both_streams_and_has_bounded_timeout():
    with mock.patch.object(route.subprocess, "run", return_value=subprocess.CompletedProcess(
            ["gcloud"], 0, stdout="PRIVATE_RESULT", stderr="PRIVATE_ERROR")) as run:
        assert route._gcloud("run", "services", "describe", SERVICE) == "PRIVATE_RESULT"
    kwargs = run.call_args.kwargs
    assert kwargs["capture_output"] is True
    assert kwargs["text"] is True
    assert 0 < kwargs["timeout"] <= 60


def test_invalid_source_sha_does_not_publish_untrusted_metadata(tmp_path):
    report, _, _ = observe(tmp_path, SOURCE_SHA="PRIVATE_SOURCE_VALUE")
    assert report["source_sha"] is None


def test_workflow_protects_existing_inputs_before_headers_and_keeps_manual_main_only():
    caller = (ROOT / ".github/workflows/notify-route-check.yml").read_text()
    protected_path = ROOT / ".github/workflows/notify-route-check-protected.yml"
    assert protected_path.exists()
    protected = protected_path.read_text()
    assert "workflow_dispatch:" in caller and "schedule:" not in caller
    assert "uses: ./.github/workflows/notify-route-check-protected.yml" in caller
    assert "github.ref == 'refs/heads/main'" in caller
    assert "github.event_name == 'workflow_dispatch'" in caller
    assert "\n    env:" not in caller and "\n    with:" not in caller
    assert "persist-credentials: false" in protected
    for name in ["GCP_PROJECT_ID", "GCP_WORKLOAD_IDENTITY_PROVIDER", "GCP_WORKLOAD_IDENTITY_SERVICE_ACCOUNT",
                 "RUNTIME_GUARD_CLOUD_RUN_SERVICES", "CLOUD_RUN_SERVICE", "CLOUD_RUN_SERVICES",
                 "CLOUD_RUN_SERVICE_TARGETS_JSON", "CLOUD_RUN_REGION", "TELEGRAM_TOKEN_SECRET_NAME"]:
        assert re.search(r"^      " + name + r":", caller, re.MULTILINE)
        assert re.search(r"^      " + name + r":", protected, re.MULTILINE)
        assert "      " + name + ": ${{ secrets." + name + " }}" in protected
    assert "SOURCE_SHA: ${{ github.sha }}" in protected
    assert "::add-mask::" not in protected
    assert "run: python3 scripts/notify_route_check.py" in protected
