import json
from pathlib import Path

import pytest

from scripts import verify_image_ues_release_binding as guard

OLD = "4a3943883cd6b5bbfe32a559e56a91b40a81b7ce"
NEW = "9d1544d1476dd19b1da61b16f37cee7f9126caf4"


def _target(service, *, release=None, binding=None):
    rt = {"service_name": service}
    if release:
        rt["strategy_release"] = {"release_id": f"{service}-rel", "strategy_revision": release}
    if binding:
        rt["runtime_risk_limits"] = {"binding": {"ues_revision": binding}}
    return rt


def _plan(*targets):
    return json.dumps({"targets": [{"service_name": t["service_name"], "env": {"RUNTIME_TARGET_JSON": json.dumps(t)}}
                                   for t in targets]})


def test_lockfile_pin_is_read_from_repo_lock():
    assert guard.SHA_RE.match(guard.revision_from_lockfile(Path("uv.lock")))


def test_matching_release_and_binding_pass():
    env = {"SYNC_PLAN_JSON": _plan(_target("soxl", release=OLD, binding=OLD), _target("tqqq", release=OLD))}
    assert guard.check({"image": OLD}, guard.runtime_targets_from_env(env)) == []


@pytest.mark.parametrize("release,binding", [(NEW, OLD), (OLD, NEW), (OLD, OLD)])
def test_any_mismatch_blocks(release, binding):
    env = {"SYNC_PLAN_JSON": _plan(_target("soxl", release=release, binding=binding))}
    installed = NEW if (release, binding) == (OLD, OLD) else OLD
    failures = guard.check({"image": installed}, guard.runtime_targets_from_env(env))
    assert failures and all("soxl" in line for line in failures)


def test_rebuild_from_newer_pin_without_release_update_is_blocked():
    env = {"SYNC_PLAN_JSON": _plan(_target("soxl", release=OLD, binding=OLD))}
    failures = guard.check({"lockfile": NEW, "image": NEW}, guard.runtime_targets_from_env(env))
    assert len(failures) == 2


def test_targets_without_release_are_not_blocked():
    env = {"SYNC_PLAN_JSON": _plan(_target("russell"))}
    assert guard.check({"image": NEW}, guard.runtime_targets_from_env(env)) == []


def test_installed_sources_must_agree():
    env = {"SYNC_PLAN_JSON": _plan(_target("soxl", release=OLD))}
    assert guard.check({"lockfile": NEW, "image": OLD}, guard.runtime_targets_from_env(env))


def test_no_target_fails_closed():
    assert guard.check({"image": OLD}, guard.runtime_targets_from_env({}))


def test_fallback_to_service_targets_json_and_single_runtime_target():
    targets = {"targets": [{"service_name": "soxl", "runtime_target": _target("soxl", release=OLD, binding=OLD)}]}
    resolved = guard.runtime_targets_from_env({"CLOUD_RUN_SERVICE_TARGETS_JSON": json.dumps(targets)})
    assert resolved[0][0] == "soxl"
    assert guard.check({"image": NEW}, resolved)
    single = guard.runtime_targets_from_env({"RUNTIME_TARGET_JSON": json.dumps(_target("svc", release=OLD))})
    assert guard.check({"image": OLD}, single) == []


def test_main_exit_codes(monkeypatch, capsys):
    monkeypatch.setenv("SYNC_PLAN_JSON", _plan(_target("soxl", release=OLD, binding=OLD)))
    monkeypatch.delenv("CLOUD_RUN_SERVICE_TARGETS_JSON", raising=False)
    assert guard.main(["--installed-revision", OLD]) == 0
    assert guard.main(["--installed-revision", NEW]) == 1
    assert guard.main([]) == 1
    assert "Deploy blocked" in capsys.readouterr().err


def test_deploy_workflow_runs_guard_before_push_and_deploy():
    workflow = Path(".github/workflows/sync-cloud-run-env.yml").read_text(encoding="utf-8")
    guard_at = workflow.index("scripts/verify_image_ues_release_binding.py --image")
    assert workflow.index("docker build --pull") < guard_at < workflow.index('docker push "${image}"')
    assert guard_at < workflow.index('gcloud run services update "${CLOUD_RUN_SERVICE}"')
