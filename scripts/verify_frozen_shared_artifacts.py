#!/usr/bin/env python3
"""Check normally installed shared artifacts before CI's editable source overlay.

Uses only stdlib and safe catalog/port definitions, never strategy or broker
entrypoints. This is an installed-artifact contract, not deployment attestation.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import importlib
from importlib import metadata
import inspect
import json
from pathlib import Path
import re
import sysconfig
import tomllib
from urllib.parse import urlsplit, urlunsplit


class ArtifactError(RuntimeError):
    pass


SPECS = (
    ("quant-platform-kit", "quant_platform_kit", "QuantPlatformKit",
     (("quant_platform_kit.common.port_adapters", "CallableNotificationPort.send_text", ("self", "message")),
      ("quant_platform_kit.common.port_adapters", "CallablePortfolioPort.get_portfolio_snapshot", ("self",))),
     "schemas/source-receipt.v1.schema.json", ("$schema", "$id", "type", "properties", "required")),
    ("us-equity-strategies", "us_equity_strategies", "UsEquityStrategies",
     (("us_equity_strategies", "resolve_canonical_profile", ("profile",)),),
     "configs/tech_communication_pullback_enhancement.json", ("role", "strategy", "universe")),
)


def locked_sources(lockfile: Path) -> dict:
    packages = tomllib.loads(lockfile.read_text(encoding="utf-8"))["package"]
    result = {}
    for name, _, repo, *_ in SPECS:
        matches = [p for p in packages if p["name"] == name]
        if len(matches) != 1:
            raise ArtifactError(f"{name}: ambiguous_locked_package")
        package = matches[0]
        raw = package["source"].get("git", "")
        parsed = urlsplit(raw)
        expected = f"https://github.com/QuantStrategyLab/{repo}.git"
        source = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        if source != expected or not re.fullmatch("[0-9a-f]{40}", parsed.fragment):
            raise ArtifactError(f"{name}: invalid_locked_source")
        result[name] = {"url": expected, "revision": parsed.fragment, "version": package["version"]}
    return result


def check_provenance(direct: dict, *, direct_url: str, revision: str) -> None:
    if direct.get("dir_info", {}).get("editable"):
        raise ArtifactError("editable_dependency")
    if direct.get("url") != direct_url:
        raise ArtifactError("source_url_mismatch")
    vcs = direct.get("vcs_info", {})
    if vcs.get("vcs") != "git" or vcs.get("commit_id") != revision:
        raise ArtifactError("commit_mismatch")


def checked_file(dist, path: Path, roots: tuple[Path, ...]) -> str:
    path = path.resolve()
    if not any(path.is_relative_to(root.resolve()) for root in roots):
        raise ArtifactError("outside_site_packages")
    if not path.is_file():
        raise ArtifactError("missing_artifact_file")
    entries = [entry for entry in (dist.files or ())
               if Path(dist.locate_file(entry)).resolve() == path]
    if len(entries) != 1 or entries[0].hash is None or entries[0].hash.mode != "sha256":
        raise ArtifactError("missing_record_hash")
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).digest()
    if base64.urlsafe_b64encode(digest).decode().rstrip("=") != entries[0].hash.value:
        raise ArtifactError("record_hash_mismatch")
    return digest.hex()


def verified_module(dist, name: str, roots: tuple[Path, ...]):
    module = importlib.import_module(name)
    filename = getattr(module, "__file__", None)
    origin = getattr(getattr(module, "__spec__", None), "origin", None)
    if not filename or not origin or Path(filename).resolve() != Path(origin).resolve():
        raise ArtifactError("module_origin_unknown")
    checked_file(dist, Path(filename), roots)
    return module


def check_signature(function, names: tuple[str, ...]) -> None:
    parameters = tuple(inspect.signature(function).parameters.values())
    if (tuple(p.name for p in parameters) != names
            or any(p.kind is not inspect.Parameter.POSITIONAL_OR_KEYWORD
                   or p.default is not inspect.Parameter.empty for p in parameters)):
        raise ArtifactError("interface_mismatch")


def verified_resource(dist, module, relative: str, required: tuple[str, ...],
                      roots: tuple[Path, ...]) -> dict:
    path = Path(module.__file__).parent / relative
    digest = checked_file(dist, path, roots)
    content = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(content, dict) or not content or not set(required) <= content.keys():
        raise ArtifactError("resource_contract_mismatch")
    return {"path": relative, "sha256": digest}


def verify(lockfile: Path) -> list[dict]:
    locked = locked_sources(lockfile)
    roots = tuple({Path(sysconfig.get_path(key)).resolve() for key in ("purelib", "platlib")})
    results = []
    for name, package, _, probes, resource, keys in SPECS:
        dist = metadata.distribution(name)
        expected = locked[name]
        if dist.version != expected["version"]:
            raise ArtifactError(f"{name}: version_mismatch")
        direct_files = [entry for entry in (dist.files or ())
                        if entry.name == "direct_url.json" and entry.parent.name.endswith(".dist-info")]
        if len(direct_files) != 1:
            raise ArtifactError(f"{name}: missing_direct_url")
        direct_path = Path(dist.locate_file(direct_files[0]))
        checked_file(dist, direct_path, roots)
        checked_file(dist, direct_path.with_name("METADATA"), roots)
        direct = json.loads(direct_path.read_text(encoding="utf-8"))
        check_provenance(direct, direct_url=expected["url"], revision=expected["revision"])
        root_module = verified_module(dist, package, roots)
        modules = {package: str(Path(root_module.__file__).resolve())}
        interfaces = []
        for module_name, attribute, names in probes:
            module = verified_module(dist, module_name, roots)
            modules[module_name] = str(Path(module.__file__).resolve())
            function = module
            for part in attribute.split("."):
                function = getattr(function, part)
            check_signature(function, names)
            # Lazy exports must resolve to actual code owned by this artifact too.
            code_file = inspect.getsourcefile(function)
            if not code_file:
                raise ArtifactError("callable_origin_unknown")
            code_digest = checked_file(dist, Path(code_file), roots)
            interfaces.append({"name": module_name + "." + attribute, "parameters": list(names),
                               "code_origin": str(Path(code_file).resolve()), "sha256": code_digest})
        results.append({"distribution": name, "version": dist.version, "commit": expected["revision"],
                        "source": expected["url"], "module_origins": modules,
                        "interfaces": interfaces,
                        "resource": verified_resource(dist, root_module, resource, keys, roots)})
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--lockfile", type=Path, default=Path("uv.lock"))
    args = parser.parse_args(argv)
    try:
        result = verify(args.lockfile)
    except (ArtifactError, OSError, ValueError, KeyError, TypeError, AttributeError,
            ImportError, metadata.PackageNotFoundError) as exc:
        reason = str(exc) if isinstance(exc, ArtifactError) else type(exc).__name__
        print(json.dumps({"status": "FAIL", "reason": reason}))
        return 1
    print(json.dumps({"status": "PASS_FROZEN_SHARED_ARTIFACT_CONTRACT", "artifacts": result},
                     sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
