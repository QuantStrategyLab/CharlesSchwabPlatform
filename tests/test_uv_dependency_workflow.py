from pathlib import Path

import base64
import hashlib
import importlib.metadata
import importlib.util
import json
import sys
from types import SimpleNamespace

import pytest


def test_pyproject_declares_runtime_and_test_dependencies() -> None:
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")

    assert "dependencies = [" in pyproject
    assert "quant-platform-kit @ git+https://github.com/QuantStrategyLab/QuantPlatformKit.git@" in pyproject
    assert "us-equity-strategies @ git+https://github.com/QuantStrategyLab/UsEquityStrategies.git@" in pyproject
    assert "[project.optional-dependencies]" in pyproject
    assert "test = [" in pyproject


def test_ci_and_docker_use_uv_lock() -> None:
    ci = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    dockerfile = Path("Dockerfile").read_text(encoding="utf-8")
    env_sync = Path(".github/workflows/sync-cloud-run-env.yml").read_text(encoding="utf-8")
    lockfile = Path("uv.lock").read_text(encoding="utf-8")

    assert lockfile.startswith("version = ")
    assert "uv sync --frozen --extra test" in ci
    assert "uv run --no-sync ruff check --exclude external ." in ci
    assert (
        "uv run --no-sync python external/QuantPlatformKit/scripts/check_qpk_pin_consistency.py"
        in ci
    )
    assert "uv sync --frozen --no-dev" in env_sync
    assert "uv run --no-sync python scripts/build_cloud_run_env_sync_plan.py --json" in env_sync
    assert "COPY . ." in dockerfile
    assert dockerfile.index("COPY . .") < dockerfile.index("uv sync --frozen --no-dev")
    assert "uv sync --frozen --no-dev" in dockerfile
    assert "python -m pip install -r requirements.txt" not in dockerfile
    assert "--no-install-project" not in ci
    assert "--no-install-project" not in env_sync
    assert "--no-install-project" not in dockerfile


def test_ci_runs_fail_closed_paper_admission_contract_tests() -> None:
    ci = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "- name: Run PAPER admission integration contract tests" in ci
    assert "uv run --no-sync pytest -q" in ci
    for test_target in (
        "tests/test_runtime_config_support.py::RuntimeConfigSupportTests::test_reads_schwab_dry_run_only_flag",
        "tests/test_runtime_config_support.py::RuntimeConfigSupportTests::test_reads_runtime_target_enabled_flag",
        "tests/test_runtime_composer.py",
        "tests/test_rebalance_service.py::RebalanceServiceTests::test_run_strategy_core_dry_run_skips_submit_and_marks_message",
        "tests/test_rebalance_service.py::RebalanceServiceTests::test_run_strategy_skips_when_execution_marker_already_exists",
        "tests/test_request_handling.py::RequestHandlingTests::test_handle_schwab_dry_run_uses_dry_run_override",
        "tests/test_request_handling.py::RequestHandlingTests::test_handle_schwab_dry_run_stays_silent_when_market_closed",
        "tests/test_runtime_broker_adapters.py",
    ):
        assert test_target in ci
    assert "|| true" not in ci


def _artifact_checker():
    spec = importlib.util.spec_from_file_location(
        "frozen_shared_artifacts_for_test", "scripts/verify_frozen_shared_artifacts.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def recorded_artifact(tmp_path):
    """Synthetic RECORD fixture for rejection tests, never installation evidence."""
    site = tmp_path / "site-packages"
    package = site / "fixture_package"
    info = site / "fixture_package-1.dist-info"
    package.mkdir(parents=True)
    info.mkdir()
    (package / "__init__.py").write_text("# synthetic artifact\n")
    (package / "config.json").write_text('{"role": "synthetic"}')
    (info / "METADATA").write_text("Name: fixture-package\nVersion: 1\n")
    rows = []
    for path in (package / "__init__.py", package / "config.json", info / "METADATA"):
        raw = path.read_bytes()
        digest = base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).decode().rstrip("=")
        rows.append(f"{path.relative_to(site)},sha256={digest},{len(raw)}")
    (info / "RECORD").write_text("\n".join(rows) + "\n")
    return importlib.metadata.PathDistribution(info), package, (site,)


@pytest.mark.parametrize("change,reason", [
    ({"vcs_info": {"vcs": "git", "commit_id": "2" * 40}}, "commit_mismatch"),
    ({"dir_info": {"editable": True}}, "editable_dependency"),
    ({"url": "https://github.com/another/source.git"}, "source_url_mismatch"),
])
def test_frozen_artifact_rejects_wrong_provenance(change, reason):
    module = _artifact_checker()
    direct = {
        "url": "https://github.com/QuantStrategyLab/QuantPlatformKit.git",
        "vcs_info": {"vcs": "git", "commit_id": "1" * 40},
    }
    direct.update(change)
    with pytest.raises(module.ArtifactError, match=reason):
        module.check_provenance(direct, direct_url=(
            "https://github.com/QuantStrategyLab/QuantPlatformKit.git"
        ), revision="1" * 40)


def test_frozen_artifact_rejects_source_shadow(recorded_artifact, tmp_path, monkeypatch):
    module = _artifact_checker()
    dist, package, roots = recorded_artifact
    shadow = tmp_path / "source_checkout.py"
    shadow.write_text("# not an installed dependency\n")
    loaded = SimpleNamespace(__file__=str(shadow), __spec__=SimpleNamespace(origin=str(shadow)))
    monkeypatch.setattr(module.importlib, "import_module", lambda _: loaded)
    with pytest.raises(module.ArtifactError, match="outside_site_packages"):
        module.verified_module(dist, "fixture_package", roots)
    assert (package / "__init__.py").exists()


def test_frozen_artifact_rejects_record_drift(recorded_artifact):
    module = _artifact_checker()
    dist, package, roots = recorded_artifact
    (package / "__init__.py").write_text("# changed after install\n")
    with pytest.raises(module.ArtifactError, match="record_hash_mismatch"):
        module.checked_file(dist, package / "__init__.py", roots)


def test_frozen_artifact_rejects_missing_resource(recorded_artifact):
    module = _artifact_checker()
    dist, package, roots = recorded_artifact
    (package / "config.json").unlink()
    loaded = SimpleNamespace(__file__=str(package / "__init__.py"))
    with pytest.raises(module.ArtifactError, match="missing_artifact_file"):
        module.verified_resource(dist, loaded, "config.json", ("role",), roots)


def test_frozen_artifact_rejects_interface_drift():
    module = _artifact_checker()

    def changed_sender(self, payload, *, new_required):
        raise AssertionError("must inspect, never call")

    with pytest.raises(module.ArtifactError, match="interface_mismatch"):
        module.check_signature(changed_sender, ("self", "message"))


def test_frozen_artifact_cli_reports_failure_exit(monkeypatch, capsys):
    module = _artifact_checker()

    def fail(_):
        raise module.ArtifactError("synthetic_rejection")

    monkeypatch.setattr(module, "verify", fail)
    assert module.main(["--lockfile", "uv.lock"]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "FAIL"


def test_ci_executes_frozen_artifact_gate_before_editable(monkeypatch):
    """Exercise the verifier entry point with argv extracted from the CI step.

    Only uv's installation is not replayed here. Its separate real no-dev
    environment is checked by CI; this test exercises wiring and fail-closed CLI.
    """
    workflow = Path(".github/workflows/ci.yml").read_text()
    name = "- name: Verify frozen shared artifacts"
    start = workflow.index(name)
    end = workflow.index("\n      - name:", start + len(name))
    step = workflow[start:end]
    assert start < workflow.index("- name: Install dependencies")
    assert start < workflow.index("- name: Install editable shared repositories")
    assert "UV_PROJECT_ENVIRONMENT: ${{ runner.temp }}/frozen-shared-artifacts" in step
    assert "uv sync --frozen --no-dev" in step
    assert "--extra" not in step and "--no-deps" not in step
    assert "--no-install-project" not in step and "continue-on-error" not in step
    assert "|| true" not in step and "set -euo pipefail" in step
    command = next(line.strip() for line in step.splitlines()
                   if line.strip().startswith("uv run --no-sync python "))
    args = command.split()
    assert args[:5] == ["uv", "run", "--no-sync", "python", "-I"]
    assert args[5:] == ["scripts/verify_frozen_shared_artifacts.py", "--lockfile", "uv.lock"]
    module = _artifact_checker()
    called = []

    def rejected(lockfile):
        called.append(lockfile)
        raise module.ArtifactError("synthetic_rejection")

    monkeypatch.setattr(module, "verify", rejected)
    monkeypatch.setattr(sys, "argv", args[5:])
    assert module.main() == 1
    assert called == [Path("uv.lock")]
