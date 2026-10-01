import importlib.util
import json
from pathlib import Path

import pytest


path = Path(__file__).resolve().parents[1] / "scripts" / "verify_deployed_runtime_target_admission.py"
spec = importlib.util.spec_from_file_location("deployed_target_admission", path)
assert spec is not None and spec.loader is not None
admission = importlib.util.module_from_spec(spec)
spec.loader.exec_module(admission)


def payload(target, profile, dry_run="true"):
    return {"spec": {"template": {"spec": {"containers": [{"env": [
        {"name": "RUNTIME_TARGET_JSON", "value": json.dumps(target)},
        {"name": "STRATEGY_PROFILE", "value": profile},
        {"name": "SCHWAB_DRY_RUN_ONLY", "value": dry_run},
        {"name": "RUNTIME_TARGET_ENABLED", "value": "true"},
    ]}]}}}}


def target(profile="soxl_soxx_trend_income", dry_run=True):
    return {"platform_id": "schwab", "service_name": "paper-service", "strategy_profile": profile, "execution_mode": "paper" if dry_run else "live", "dry_run_only": dry_run}


def test_admitted_shadow_target_passes():
    result = admission.verify_service(service="paper-service", service_json=payload(target(), "soxl_soxx_trend_income"))
    assert result["profile"] == "soxl_soxx_trend_income"


def test_paper_broker_submission_target_passes():
    configured = target(dry_run=False) | {"execution_mode": "paper"}
    assert admission.verify_service(service="paper-service", service_json=payload(configured, "soxl_soxx_trend_income", "false"))["dry_run_only"] is False


@pytest.mark.parametrize(
    ("configured", "profile", "message"),
    [
        (target(), "different_profile", "STRATEGY_PROFILE does not match"),
        (target() | {"execution_mode": "live"}, "soxl_soxx_trend_income", "dry-run/shadow target"),
        (target("retired_profile"), "retired_profile", "not admitted"),
    ],
)
def test_target_drift_fails_closed(configured, profile, message):
    with pytest.raises(admission.AdmissionError, match=message):
        admission.verify_service(service="paper-service", service_json=payload(configured, profile))


@pytest.mark.parametrize("mismatch", [None, "image", "source", "traffic", "scheduler", "iam", "configuration"])
def test_no_traffic_readback_projects_container_array_and_checks_identity(tmp_path, monkeypatch, mismatch):
    from types import SimpleNamespace
    from scripts import verify_cloud_run_no_traffic_deploy as readback
    sha, digest = "a" * 40, "sha256:" + "b" * 64
    baseline = {key: "synthetic" for key in ("traffic", "configuration", "iam", "scheduler")}
    before = tmp_path / "before.json"
    before.write_text(json.dumps(baseline))
    args = SimpleNamespace(before=before, project="synthetic", region="synthetic",
                           service="synthetic", expected_sha=sha, expected_image_digest=digest)
    observed = baseline | ({mismatch: "changed"} if mismatch in baseline else {})
    monkeypatch.setattr(readback, "_snapshot", lambda _args: observed)
    monkeypatch.setattr(readback.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout="synthetic-revision", stderr=""))
    def projected(command):
        result = {"metadata": {"labels": {"commit-sha": "c" * 40 if mismatch == "source" else sha}}}
        # Verified gcloud behavior: missing [] omits the whole containers subtree.
        if "spec.containers[].image" in command[-1]:
            value = "sha256:" + "d" * 64 if mismatch == "image" else digest
            result["spec"] = {"containers": [{"image": "synthetic/image@" + value}]}
        return result
    monkeypatch.setattr(readback, "_run_json", projected)
    if mismatch:
        message = f"{mismatch} changed" if mismatch in baseline else "commit SHA" if mismatch == "source" else "image digest"
        with pytest.raises(RuntimeError, match=message):
            readback._verify(args)
    else:
        readback._verify(args)


def test_no_traffic_capture_keeps_resource_secret_reference_and_iam_arrays(monkeypatch):
    from types import SimpleNamespace
    from scripts import verify_cloud_run_no_traffic_deploy as readback
    service_spec = {"template": {"spec": {"containers": [{
        "resources": {"limits": {"cpu": "1", "memory": "512Mi"}},
        "env": [{"name": "SYNTHETIC_SECRET", "valueFrom": {"secretKeyRef": {"name": "synthetic", "key": "1"}}}],
    }]}}}
    policy = {"bindings": [{"role": "roles/run.invoker", "members": ["serviceAccount:synthetic"]}]}
    def projected(command):
        expression = command[-1]
        assert 'env.value,' not in expression and 'env[].value,' not in expression
        if "get-iam-policy" in command:
            return policy if "bindings[].members" in expression else None
        if "scheduler" in command:
            return []
        result = {"status": {"traffic": [{"revisionName": "synthetic", "percent": 100}]}}
        if all(value in expression for value in ("containers[].resources", "containers[].env[].name", "containers[].env[].valueFrom.secretKeyRef")):
            result["spec"] = service_spec
        return result
    monkeypatch.setattr(readback, "_run_json", projected)
    result = readback._snapshot(SimpleNamespace(service="synthetic", project="synthetic", region="synthetic", scheduler_location="synthetic"))
    assert result["configuration"] == readback._digest(service_spec)
    assert result["iam"] == readback._digest(policy)


@pytest.mark.parametrize(
    ("baseline_currency", "service_currency", "revision_currency", "passes"),
    [
        (None, "USD", "USD", True),
        ("USD", "USD", "USD", True),
        ("EUR", "USD", "USD", False),
        (None, "EUR", "USD", False),
        (None, None, "USD", False),
        (None, "USD", "EUR", False),
        (None, "USD", None, False),
    ],
)
def test_explicit_cash_declaration_allows_only_absent_to_usd(baseline_currency, service_currency, revision_currency, passes, tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts import verify_cloud_run_no_traffic_deploy as readback

    sha, digest = "a" * 40, "sha256:" + "b" * 64
    before = tmp_path / "before.json"
    before.write_text(json.dumps({
        "traffic": "traffic",
        "scheduler": "scheduler",
        "iam": "iam",
        "configuration": "configuration",
        "cash_currency": baseline_currency,
    }))
    args = SimpleNamespace(
        before=before, project="synthetic", region="synthetic", service="synthetic",
        scheduler_location="synthetic", expected_sha=sha,
        expected_image_digest=digest, cash_currency="USD",
    )
    monkeypatch.setattr(readback, "_snapshot", lambda _args: {
        "traffic": "traffic", "scheduler": "scheduler", "iam": "iam",
        "configuration": "configuration", "cash_currency": service_currency,
    })
    monkeypatch.setattr(readback, "_created_revision", lambda _args: {
        "metadata": {"name": "synthetic-revision", "labels": {"commit-sha": sha}},
        "spec": {"containers": [{"image": "synthetic/image@" + digest}]},
    })
    monkeypatch.setattr(readback, "_cash_currency", lambda _args, revision=False: revision_currency if revision else service_currency)
    monkeypatch.setattr(readback, "print", lambda *_args, **_kwargs: None, raising=False)

    if passes:
        readback._verify(args)
    else:
        with pytest.raises(RuntimeError):
            readback._verify(args)


def test_explicit_cash_declaration_does_not_mask_other_service_configuration(monkeypatch):
    from types import SimpleNamespace
    from scripts import verify_cloud_run_no_traffic_deploy as readback

    baseline = {"template": {"spec": {"containers": [{
        "resources": {"limits": {"cpu": "1"}}, "env": [{"name": "OTHER_SETTING"}],
    }]}}}
    cash_added = {"template": {"spec": {"containers": [{
        "resources": {"limits": {"cpu": "1"}}, "env": [
            {"name": "OTHER_SETTING"}, {"name": "SCHWAB_CASH_CURRENCY"},
        ],
    }]}}}
    assert readback._digest(readback._configuration_projection(baseline, allow_cash_currency=True)) == readback._digest(
        readback._configuration_projection(cash_added, allow_cash_currency=True)
    )
    after = {"template": {"spec": {"containers": [{
        "resources": {"limits": {"cpu": "2"}}, "env": [
            {"name": "OTHER_SETTING"}, {"name": "SCHWAB_CASH_CURRENCY"},
        ],
    }]}}}
    assert readback._digest(readback._configuration_projection(baseline, allow_cash_currency=True)) != readback._digest(
        readback._configuration_projection(after, allow_cash_currency=True)
    )
    after = {"template": {"spec": {"containers": [{
        "resources": {"limits": {"cpu": "1"}}, "env": [
            {"name": "SCHWAB_CASH_CURRENCY"}, {"name": "OTHER_SETTING_CHANGED"},
        ],
    }]}}}
    assert readback._digest(readback._configuration_projection(baseline, allow_cash_currency=True)) != readback._digest(
        readback._configuration_projection(after, allow_cash_currency=True)
    )
    assert readback._digest(readback._configuration_projection(baseline, allow_cash_currency=False)) != readback._digest(
        readback._configuration_projection(after, allow_cash_currency=False)
    )


@pytest.mark.parametrize(
    "cash_entries",
    [
        [{"name": "SCHWAB_CASH_CURRENCY", "valueFrom": {"secretKeyRef": {"name": "synthetic", "key": "1"}}}],
        [{"name": "SCHWAB_CASH_CURRENCY"}, {"name": "SCHWAB_CASH_CURRENCY"}],
    ],
)
def test_explicit_cash_declaration_rejects_secret_or_duplicate_entry(cash_entries):
    from scripts import verify_cloud_run_no_traffic_deploy as readback

    spec = {"template": {"spec": {"containers": [{"env": cash_entries}]}}}
    with pytest.raises(RuntimeError, match="cash currency environment entry"):
        readback._configuration_projection(spec, allow_cash_currency=True)


def test_no_cash_declaration_keeps_original_success_message(tmp_path, monkeypatch, capsys):
    from types import SimpleNamespace
    from scripts import verify_cloud_run_no_traffic_deploy as readback

    sha, digest = "a" * 40, "sha256:" + "b" * 64
    before = tmp_path / "before.json"
    baseline = {key: "synthetic" for key in ("traffic", "configuration", "iam", "scheduler")}
    before.write_text(json.dumps(baseline))
    args = SimpleNamespace(
        before=before, project="synthetic", region="synthetic", service="synthetic",
        scheduler_location="synthetic", expected_sha=sha, expected_image_digest=digest,
    )
    monkeypatch.setattr(readback, "_snapshot", lambda _args: baseline)
    monkeypatch.setattr(readback, "_created_revision", lambda _args: {
        "metadata": {"labels": {"commit-sha": sha}},
        "spec": {"containers": [{"image": "synthetic/image@" + digest}]},
    })

    readback._verify(args)

    output = capsys.readouterr().out
    assert "traffic, scheduler, IAM, and configuration digests are unchanged." in output
    assert "cash currency matches" not in output


@pytest.mark.parametrize(
    ("revision", "payload", "expected_format"),
    [
        (False, {"spec": {"template": {"spec": {"containers": [{"env": [["USD"]]}]}}}}, "service"),
        (True, {"spec": {"containers": [{"env": [["USD"]]}]}}, "revision"),
        (False, None, "service"),
        (True, None, "revision"),
    ],
)
def test_cash_currency_projection_is_targeted_and_never_persists_other_env_values(monkeypatch, revision, payload, expected_format):
    from types import SimpleNamespace
    from scripts import verify_cloud_run_no_traffic_deploy as readback

    commands = []

    def run_json(command):
        commands.append(command)
        return payload

    monkeypatch.setattr(readback, "_run_json", run_json)
    args = SimpleNamespace(
        cash_currency="USD", service="synthetic", project="synthetic", region="synthetic",
        revision_name="synthetic-revision",
    )
    assert readback._cash_currency(args, revision=revision) == ("USD" if payload else None)
    expected = readback.CASH_REVISION_FORMAT if expected_format == "revision" else readback.CASH_CURRENCY_FORMAT
    assert commands[0][-1] == f"--format={expected}"
    assert "env.always().filter(\"name=SCHWAB_CASH_CURRENCY\").map().extract(value)" in commands[0][-1]
    assert "SYNTHETIC_OTHER" not in json.dumps(payload)
    assert "PRIVATE" not in json.dumps(payload)
    assert "env[].value" not in commands[0][-1]
