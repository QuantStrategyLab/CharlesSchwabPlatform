"""Disposable source/metadata fixtures; never mutate a real installed dependency."""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from test_support.provenance import Audit, ProvenanceError, SPECS, locked_sources


class Entry:
    def __init__(self, path, content):
        self.path = path
        self.hash = SimpleNamespace(mode="sha256", value=base64.urlsafe_b64encode(
            hashlib.sha256(content).digest()).decode().rstrip("="))


class Distribution:
    def __init__(self, site, namespace, expected):
        self.site = site
        self.version = expected["version"]
        self.direct = {"url": expected["url"], "vcs_info": {
            "vcs": "git", "commit_id": expected["revision"]}}
        path = Path(namespace) / "__init__.py"
        self.files = [Entry(path, (site / path).read_bytes())]

    def locate_file(self, entry):
        return self.site / (entry.path if isinstance(entry, Entry) else entry)

    def read_text(self, name):
        assert name == "direct_url.json"
        return json.dumps(self.direct)


def module_at(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def fixture(tmp_path):
    site = tmp_path / "site"
    locked, distributions, modules = {}, {}, {}
    for namespace, (name, repo) in SPECS.items():
        path = site / namespace / "__init__.py"
        path.parent.mkdir(parents=True)
        path.write_text('SOURCE = "installed"\n')
        locked[namespace] = dict(name=name, url=f"https://github.com/QuantStrategyLab/{repo}.git",
                                 revision="a" * 40, version="1.0")
        distributions[name] = Distribution(site, namespace, locked[namespace])
        modules[namespace] = module_at(namespace, path)
    audit = Audit(locked, "installed-lock", distributions=distributions.__getitem__, sites=[site])
    return SimpleNamespace(site=site, locked=locked, dists=distributions, modules=modules,
                           audit=audit, tmp=tmp_path)


def test_installed_metadata_and_module_origins_pass(fixture):
    assert fixture.audit.audit(fixture.modules) == set(SPECS)


@pytest.mark.parametrize("damage", ["wrong_revision", "missing_revision", "missing_metadata",
                                   "wrong_url", "wrong_version", "editable"])
def test_metadata_is_required(fixture, damage):
    dist = fixture.dists["quant-platform-kit"]
    if damage == "wrong_revision":
        dist.direct["vcs_info"]["commit_id"] = "b" * 40
    elif damage == "missing_revision":
        del dist.direct["vcs_info"]["commit_id"]
    elif damage == "missing_metadata":
        dist.direct = {}
    elif damage == "wrong_url":
        dist.direct["url"] = "https://example.invalid/other.git"
    elif damage == "wrong_version":
        dist.version = "2.0"
    else:
        dist.direct["dir_info"] = {"editable": True}
    with pytest.raises(ProvenanceError, match="installed_metadata_mismatch"):
        fixture.audit.audit(fixture.modules)


def test_sibling_shadow_gap_and_rejection(fixture):
    path = fixture.tmp / "sibling" / "quant_platform_kit" / "__init__.py"
    path.parent.mkdir(parents=True)
    path.write_text('SOURCE = "sibling"\n')
    shadow = module_at("quant_platform_kit", path)
    # Without a guard, Python successfully executes the wrong source despite correct dist metadata.
    assert shadow.SOURCE == "sibling"
    fixture.modules["quant_platform_kit"] = shadow
    with pytest.raises(ProvenanceError, match="outside_distribution"):
        fixture.audit.audit(fixture.modules)


@pytest.mark.parametrize("damage", ["file_spec", "no_spec", "namespace", "none", "search_path",
                                   "missing_record", "changed_bytes", "symlink"])
def test_unproven_origins_fail(fixture, damage):
    module = fixture.modules["quant_platform_kit"]
    if damage == "file_spec":
        module.__spec__.origin = str(fixture.site / "other.py")
    elif damage == "no_spec":
        module.__spec__ = None
    elif damage == "namespace":
        module.__file__ = None
    elif damage == "none":
        fixture.modules["quant_platform_kit"] = None
    elif damage == "search_path":
        module.__path__.append(str(fixture.tmp / "sibling"))
    elif damage == "missing_record":
        fixture.dists["quant-platform-kit"].files = []
    elif damage == "changed_bytes":
        Path(module.__file__).write_text("SOURCE = 'changed'\n")
    else:
        path = Path(module.__file__)
        outside = fixture.tmp / "outside.py"
        outside.write_bytes(path.read_bytes())
        path.unlink()  # Disposable synthetic file only.
        path.symlink_to(outside)
    with pytest.raises((ProvenanceError, OSError)):
        fixture.audit.audit(fixture.modules)


def test_stale_preloaded_module_cannot_evade(fixture, monkeypatch):
    from types import ModuleType
    for name in list(sys.modules):
        if name.split(".")[0] in SPECS:
            monkeypatch.delitem(sys.modules, name)
    for name, module in fixture.modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setitem(sys.modules, "quant_platform_kit.stale", ModuleType("quant_platform_kit.stale"))
    with pytest.raises(ProvenanceError, match="unproven_module"):
        fixture.audit.audit()


def test_incomplete_inventory_cannot_complete(fixture, monkeypatch):
    for name in list(sys.modules):
        if name.split(".")[0] in SPECS:
            monkeypatch.delitem(sys.modules, name)
    for name, module in fixture.modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    with pytest.raises(ProvenanceError, match="incomplete_real_inventory"):
        fixture.audit.receipt()


def test_scoped_and_leaked_doubles(fixture, monkeypatch):
    from types import ModuleType
    for name in list(sys.modules):
        if name.split(".")[0] in SPECS:
            monkeypatch.delitem(sys.modules, name)
    for name, module in fixture.modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    name = "quant_platform_kit"
    original = sys.modules[name]
    double = ModuleType(name)
    with fixture.audit.scoped_double(name, double):
        assert name not in fixture.audit.audit()
        with pytest.raises(ProvenanceError, match="incomplete_real_inventory"):
            fixture.audit.receipt()
    assert sys.modules[name] is original
    assert name in fixture.audit.audit()
    monkeypatch.setitem(sys.modules, name, double)
    with pytest.raises(ProvenanceError, match="unproven_module"):
        fixture.audit.audit()


def run_git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout


@pytest.fixture
def checkout_fixture(fixture):
    roots = {}
    for namespace in SPECS:
        root = fixture.tmp / namespace
        path = root / "src" / namespace / "__init__.py"
        path.parent.mkdir(parents=True)
        path.write_text("SOURCE = 'checkout'\n")
        (root / ".gitignore").write_text("__pycache__/\n")
        run_git(root, "init", "-q")
        run_git(root, "add", ".")
        run_git(root, "-c", "user.name=Synthetic", "-c", "user.email=example@example.invalid",
                "commit", "-qm", "test: freeze synthetic checkout")
        fixture.locked[namespace]["revision"] = run_git(root, "rev-parse", "HEAD").decode().strip()
        fixture.modules[namespace] = module_at(namespace, path)
        roots[namespace] = root
    fixture.roots = roots
    fixture.audit = Audit(fixture.locked, "pinned-checkout", checkouts=roots)
    return fixture


def test_exact_clean_checkout_passes_declared_mode_only(checkout_fixture):
    f = checkout_fixture
    assert f.audit.audit(f.modules) == set(SPECS)
    installed = Audit(f.locked, "installed-lock", distributions=f.dists.__getitem__, sites=[f.site])
    with pytest.raises(ProvenanceError):
        installed.audit(f.modules)


@pytest.mark.parametrize("damage", ["dirty", "untracked", "wrong_revision", "same_name_root",
                                   "symlink_escape", "ignored_source"])
def test_checkout_rejections(checkout_fixture, damage):
    f = checkout_fixture
    root = f.roots["quant_platform_kit"]
    module = f.modules["quant_platform_kit"]
    if damage == "dirty":
        Path(module.__file__).write_text("SOURCE = 'dirty'\n")
    elif damage == "untracked":
        (root / "unexpected.py").write_text("# synthetic\n")
    elif damage == "wrong_revision":
        f.locked["quant_platform_kit"]["revision"] = "0" * 40
    elif damage == "same_name_root":
        path = f.tmp / "unexpected" / root.name / "__init__.py"
        path.parent.mkdir(parents=True)
        path.write_text("SOURCE = 'unexpected'\n")
        f.modules["quant_platform_kit"] = module_at("quant_platform_kit", path)
    elif damage == "symlink_escape":
        outside = f.tmp / "outside.py"
        outside.write_bytes(Path(module.__file__).read_bytes())
        Path(module.__file__).unlink()
        Path(module.__file__).symlink_to(outside)
    else:
        (root / "src" / "quant_platform_kit" / "__pycache__").mkdir(exist_ok=True)
        path = root / "src" / "quant_platform_kit" / "__pycache__" / "hidden.py"
        path.write_text("SOURCE = 'untracked ignored'\n")
        f.modules["quant_platform_kit.hidden"] = module_at("quant_platform_kit.hidden", path)
    with pytest.raises(ProvenanceError):
        f.audit.audit(f.modules)


def test_no_implicit_mode_or_checkout(fixture):
    with pytest.raises(ProvenanceError, match="explicit_mode"):
        Audit(fixture.locked, None)
    with pytest.raises(ProvenanceError, match="explicit_checkouts"):
        Audit(fixture.locked, "pinned-checkout")
    with pytest.raises(ProvenanceError, match="incomplete_lock"):
        Audit({}, "installed-lock")


def test_lock_missing_or_moving_revision_fails(tmp_path):
    lock = tmp_path / "uv.lock"
    lock.write_text('package = []\n')
    with pytest.raises(ProvenanceError, match="ambiguous_lock"):
        locked_sources(lock)
    content = []
    for namespace, (name, repo) in SPECS.items():
        content.append(f'[[package]]\nname = "{name}"\nversion = "1"\n'
                       f'source = {{git = "https://github.com/QuantStrategyLab/{repo}.git#main"}}\n')
    lock.write_text("".join(content))
    with pytest.raises(ProvenanceError, match="invalid_lock"):
        locked_sources(lock)


def test_subprocess_shadow_is_observed_failure(fixture):
    # Execute the failing guard in a real child process; do not invent failed CI.
    shadow = fixture.tmp / "sibling" / "quant_platform_kit" / "__init__.py"
    shadow.parent.mkdir(parents=True)
    shadow.write_text("SOURCE = 'sibling'\n")
    helper = Path(__file__).resolve()
    code = ("import runpy, sys; from pathlib import Path; "
            f"h=runpy.run_path({str(helper)!r}); "
            f"module=h['module_at']('quant_platform_kit', Path({str(shadow)!r})); "
            "print('UNGUARDED_SIBLING_IMPORTED', flush=True); "
            f"locked={fixture.locked!r}; site=Path({str(fixture.site)!r}); "
            "dists={v['name']:h['Distribution'](site,k,v) for k,v in locked.items()}; "
            "h['Audit'](locked,'installed-lock',distributions=dists.__getitem__,sites=[site])"
            ".audit({'quant_platform_kit':module}); print('SHARED_IMPORT_PROVENANCE_COMPLETE')")
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode != 0
    assert "UNGUARDED_SIBLING_IMPORTED" in result.stdout
    assert "outside_distribution" in result.stderr
    assert "SHARED_IMPORT_PROVENANCE_COMPLETE" not in result.stdout



@pytest.mark.parametrize("state", ["not_collected", "not_executed", "collect_only", "failed", "exit_error"])
def test_incomplete_pytest_execution_cannot_emit_completion(state):
    from test_support import pytest_provenance as plugin
    lines = []
    config = SimpleNamespace(_shared_failed=state == "failed",
                             _shared_collected=state != "not_collected",
                             _shared_executed=set() if state in {"not_executed", "collect_only"}
                             else plugin.EXPECTED,
                             pluginmanager=SimpleNamespace(getplugin=lambda name:
                                 SimpleNamespace(write_line=lines.append)))
    session = SimpleNamespace(config=config, exitstatus=0)
    plugin.pytest_sessionfinish(session, 1 if state == "exit_error" else 0)
    assert not lines


def test_incomplete_pytest_collection_is_rejected(tmp_path):
    from test_support import pytest_provenance as plugin
    config = SimpleNamespace(rootpath=tmp_path, _shared_failed=False)
    item = SimpleNamespace(name="test_pinned_uri_parser",
                           path=tmp_path / "tests/test_shared_import_contract.py")
    with pytest.raises(pytest.UsageError, match="incomplete_audited_selection"):
        plugin.pytest_collection_finish(SimpleNamespace(config=config, items=[item]))
    assert config._shared_failed
