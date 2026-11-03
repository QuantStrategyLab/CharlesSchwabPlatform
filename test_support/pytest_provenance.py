"""Explicit strict lane; never auto-loaded by ordinary pytest runs."""
import json
from pathlib import Path

import pytest

from .provenance import Audit, ProvenanceError, locked_sources

MODES = ("pinned-checkout",)

EXPECTED = {"test_pinned_uri_parser", "test_pinned_profile_parser", "test_scoped_double_restores"}


def pytest_addoption(parser):
    group = parser.getgroup("shared import provenance")
    group.addoption("--shared-import-mode", choices=MODES, required=True)
    group.addoption("--qpk-source", choices=("installed-lock", "pinned-checkout"), required=True)
    group.addoption("--ues-source", choices=("installed-lock", "pinned-checkout"), required=True)
    group.addoption("--qpk-checkout")
    group.addoption("--ues-checkout")


def pytest_configure(config):
    roots = {}
    for namespace, option in (("quant_platform_kit", "qpk_checkout"),
                              ("us_equity_strategies", "ues_checkout")):
        value = config.getoption(option)
        if value:
            roots[namespace] = value
    config._shared_audit = Audit(locked_sources(config.rootpath / "uv.lock"),
                                 config.getoption("shared_import_mode"), checkouts=roots,
                                 source_modes={"quant_platform_kit": config.getoption("qpk_source"),
                                               "us_equity_strategies": config.getoption("ues_source")})
    config._shared_executed = set()
    config._shared_collected = False
    config._shared_failed = False


def checked(config):
    try:
        config._shared_audit.audit()
    except (ProvenanceError, OSError, ValueError, ImportError) as exc:
        config._shared_failed = True
        reason = str(exc) if isinstance(exc, ProvenanceError) else type(exc).__name__
        raise pytest.UsageError("shared import provenance: " + reason) from None


def pytest_sessionstart(session):
    try:
        session.config._shared_audit.require_fresh_process()
    except ProvenanceError:
        session.config._shared_failed = True
        raise pytest.UsageError("shared import provenance: preloaded_shared_modules_unverified") from None
    checked(session.config)


def pytest_collection_finish(session):
    config = session.config
    expected_path = (config.rootpath / "tests/test_shared_import_contract.py").resolve()
    if ({item.name for item in session.items} != EXPECTED
            or len(session.items) != len(EXPECTED)
            or any(Path(item.path).resolve() != expected_path for item in session.items)):
        config._shared_failed = True
        raise pytest.UsageError("shared import provenance: incomplete_audited_selection")
    checked(config)  # Same process, after test modules/conftests have been imported.
    config._shared_collected = True


def boundary(item):
    checked(item.config)
    outcome = yield
    checked(item.config)
    if outcome.excinfo:
        item.config._shared_failed = True


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_setup(item):
    yield from boundary(item)


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_call(item):
    yield from boundary(item)


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_teardown(item):
    yield from boundary(item)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    report = (yield).get_result()
    if report.failed or report.skipped:
        item.config._shared_failed = True
    if report.when == "call" and report.passed:
        item.config._shared_executed.add(item.name)


def pytest_sessionfinish(session, exitstatus):
    config = session.config
    if (exitstatus or config._shared_failed or not config._shared_collected
            or config._shared_executed != EXPECTED):
        return
    try:
        receipt = config._shared_audit.receipt()
    except (ProvenanceError, OSError, ValueError, ImportError):
        session.exitstatus = 1
        return
    reporter = config.pluginmanager.getplugin("terminalreporter")
    reporter.write_line("SHARED_IMPORT_PROVENANCE_COMPLETE " + json.dumps(receipt, sort_keys=True))
