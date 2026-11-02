"""Test-only shared import checks. Receipts describe a bounded pytest process."""
from __future__ import annotations

import base64
from contextlib import contextmanager
import hashlib
from importlib import metadata
import json
from pathlib import Path
import re
import subprocess
import sys
import sysconfig
import tomllib
from types import ModuleType
from urllib.parse import urlsplit, urlunsplit

SPECS = {
    "quant_platform_kit": ("quant-platform-kit", "QuantPlatformKit"),
    "us_equity_strategies": ("us-equity-strategies", "UsEquityStrategies"),
}
REQUIRED = {
    "quant_platform_kit", "quant_platform_kit.common.strategy_plugin_artifacts",
    "us_equity_strategies", "us_equity_strategies.catalog",
}


class ProvenanceError(RuntimeError):
    """Fixed reason codes only; never include a private path or metadata value."""


def locked_sources(lockfile):
    packages = tomllib.loads(Path(lockfile).read_text())["package"]
    result = {}
    for namespace, (name, repo) in SPECS.items():
        matches = [p for p in packages if p["name"] == name]
        if len(matches) != 1:
            raise ProvenanceError("ambiguous_lock")
        package = matches[0]
        parsed = urlsplit(package.get("source", {}).get("git", ""))
        url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        if (url != f"https://github.com/QuantStrategyLab/{repo}.git"
                or not re.fullmatch(r"[0-9a-f]{40}", parsed.fragment)):
            raise ProvenanceError("invalid_lock")
        result[namespace] = dict(name=name, url=url, revision=parsed.fragment,
                                 version=package["version"])
    return result


def git(root, *args):
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True)
    if result.returncode:
        raise ProvenanceError("checkout_unproven")
    return result.stdout


class Audit:
    def __init__(self, locked, mode, *, checkouts=None, distributions=metadata.distribution,
                 sites=None, source_modes=None):
        if mode not in {"installed-lock", "pinned-checkout"}:
            raise ProvenanceError("explicit_mode_required")
        if set(locked) != set(SPECS):
            raise ProvenanceError("incomplete_lock")
        self.locked = locked
        self.mode = mode
        self.source_modes = dict.fromkeys(SPECS, mode) if source_modes is None else source_modes
        if (set(self.source_modes) != set(SPECS)
                or not set(self.source_modes.values()) <= {"installed-lock", "pinned-checkout"}
                or (mode == "installed-lock" and set(self.source_modes.values()) != {mode})):
            raise ProvenanceError("explicit_sources_required")
        self.checkouts = {k: Path(v).resolve() for k, v in (checkouts or {}).items()}
        if set(self.checkouts) != {k for k, v in self.source_modes.items() if v == "pinned-checkout"}:
            raise ProvenanceError("explicit_checkouts_required")
        self.distributions = distributions
        self.sites = tuple(Path(p).resolve() for p in (sites if sites is not None else {
            sysconfig.get_path("purelib"), sysconfig.get_path("platlib")}))
        self.doubles = {}
        self.double_observations = 0

    def _distribution(self, namespace):
        expected = self.locked[namespace]
        dist = self.distributions(expected["name"])
        direct = json.loads(dist.read_text("direct_url.json") or "{}")
        if (dist.version != expected["version"] or direct.get("dir_info", {}).get("editable")
                or direct.get("url") != expected["url"]
                or direct.get("vcs_info", {}).get("vcs") != "git"
                or direct.get("vcs_info", {}).get("commit_id") != expected["revision"]):
            raise ProvenanceError("installed_metadata_mismatch")
        return dist

    def _checkout(self, namespace):
        root = self.checkouts[namespace]
        if (Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve() != root
                or git(root, "rev-parse", "HEAD").decode().strip()
                != self.locked[namespace]["revision"]
                or git(root, "status", "--porcelain", "--untracked-files=all")):
            raise ProvenanceError("checkout_not_clean_locked_root")
        return root

    def _file(self, path, namespace, source):
        resolved = path.resolve(strict=True)
        if self.source_modes[namespace] == "installed-lock":
            if not any(resolved.is_relative_to(root) for root in self.sites):
                raise ProvenanceError("outside_distribution")
            entries = [entry for entry in (source.files or ())
                       if Path(source.locate_file(entry)).absolute() == path.absolute()]
            if (len(entries) != 1 or entries[0].hash is None
                    or entries[0].hash.mode != "sha256"):
                raise ProvenanceError("missing_record_hash")
            digest = base64.urlsafe_b64encode(hashlib.sha256(path.read_bytes()).digest())
            if digest.decode().rstrip("=") != entries[0].hash.value:
                raise ProvenanceError("record_hash_mismatch")
            package_root = Path(source.locate_file(namespace)).resolve()
        else:
            package_root = source / "src" / namespace
            if not resolved.is_relative_to(package_root):
                raise ProvenanceError("outside_declared_checkout")
            relative = path.absolute().relative_to(source).as_posix()
            # The Git object supplies the expected bytes, including for ignored files.
            if git(source, "show", f"HEAD:{relative}") != path.read_bytes():
                raise ProvenanceError("checkout_content_mismatch")
        if not resolved.is_relative_to(package_root):
            raise ProvenanceError("symlink_escape")
        return resolved

    def require_fresh_process(self, modules=None):
        # Files on disk cannot prove what an earlier module object executed.
        modules = sys.modules if modules is None else modules
        if any(name.split(".")[0] in SPECS for name in modules):
            raise ProvenanceError("preloaded_shared_modules_unverified")

    def audit(self, modules=None):
        modules = dict(sys.modules if modules is None else modules)
        sources = {namespace: (self._distribution(namespace) if self.source_modes[namespace] == "installed-lock"
                              else self._checkout(namespace)) for namespace in SPECS}
        observed = set()
        for name, module in modules.items():
            namespace = name.split(".")[0]
            if namespace not in SPECS:
                continue
            if name in self.doubles and module is self.doubles[name]:
                self.double_observations += 1
                continue
            spec = getattr(module, "__spec__", None)
            filename = getattr(module, "__file__", None)
            origin = getattr(spec, "origin", None)
            if (not isinstance(module, ModuleType) or module.__name__ != name
                    or getattr(spec, "name", None) != name or not filename or not origin):
                raise ProvenanceError("unproven_module")
            file = Path(filename)
            if not file.is_absolute() or file.resolve() != Path(origin).resolve():
                raise ProvenanceError("origin_disagreement")
            checked = self._file(file, namespace, sources[namespace])
            if hasattr(module, "__path__"):
                paths = tuple(Path(p).resolve() for p in module.__path__)
                locations = tuple(Path(p).resolve() for p in (spec.submodule_search_locations or ()))
                if paths != (checked.parent,) or locations != paths:
                    raise ProvenanceError("package_search_path_mismatch")
            observed.add(name)
        return observed

    @contextmanager
    def scoped_double(self, name, double):
        # Only an exact, already verified module can be temporarily replaced.
        if name not in self.audit() or name in self.doubles:
            raise ProvenanceError("double_boundary_unproven")
        original = sys.modules[name]
        self.doubles[name] = double
        sys.modules[name] = double
        try:
            self.audit()
            yield double
        finally:
            leaked = sys.modules.get(name) is not double
            sys.modules[name] = original
            del self.doubles[name]
            self.audit()
            if leaked:
                raise ProvenanceError("double_boundary_changed")

    def receipt(self):
        observed = self.audit()
        if self.doubles or not REQUIRED <= observed:
            raise ProvenanceError("incomplete_real_inventory")
        return {"mode": self.mode, "scope": "tests/test_shared_import_contract.py",
                "origin_classes": self.source_modes, "real_modules": sorted(observed),
                "revisions": {k: v["revision"] for k, v in self.locked.items()},
                "scoped_double_observations": self.double_observations}
