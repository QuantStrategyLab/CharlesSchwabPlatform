"""Verify the coverage source layout measures shipped production code.

The denominator is the repository tree minus explicitly excluded categories
(tests, external dependency checkouts, virtual environments, build outputs,
and the research/release-overlay trees that are not shipped platform code).
A hermetic synthetic tree proves that unexecuted in-scope lines and files
remain in the denominator while excluded code cannot inflate the measurement.
"""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[1]

SYNTHETIC_MODULE = (
    "def executed():\n"
    "    return 1\n"
    "\n"
    "\n"
    "def never_executed():\n"
    "    value = 1\n"
    "    return value\n"
)

CANONICAL_OMIT_CATEGORIES = (
    "tests/",
    "external/",
    ".venv/",
    "build/",
    "dist/",
    "release-overlays/",
    "research/",
)

MEASUREMENT_STEP = "Measure coverage against the production source layout"


def pyproject():
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def coverage_run_config():
    return pyproject()["tool"]["coverage"]["run"]


def coverage_report_config():
    return pyproject()["tool"]["coverage"]["report"]


def declared_production_sources():
    data = pyproject()
    modules = data["tool"]["setuptools"]["py-modules"]
    include = data["tool"]["setuptools"]["packages"]["find"]["include"]
    packages = sorted({p.name for pattern in include for p in ROOT.glob(pattern) if p.is_dir()})
    return modules, packages


def matches_any_omit(path, omit_patterns):
    for pattern in omit_patterns:
        regex = "".join(".*" if char == "*" else re.escape(char) for char in pattern)
        if re.fullmatch(regex, path):
            return True
    return False


def measurement_step_block():
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    lines = workflow.splitlines()
    start = next(
        index
        for index, line in enumerate(lines)
        if line == f"      - name: {MEASUREMENT_STEP}"
    )
    end = len(lines)
    for index in range(start + 1, len(lines)):
        if lines[index].startswith("      - name:") or lines[index].startswith("      - uses:"):
            end = index
            break
    return "\n".join(lines[start:end])


class CoverageSourceLayoutTests(unittest.TestCase):
    def test_source_is_repository_root_with_explicit_category_exclusions(self):
        run = coverage_run_config()
        report = coverage_report_config()
        self.assertEqual(run["source"], ["."])
        self.assertTrue(report.get("include_namespace_packages"))
        self.assertFalse((ROOT / "src").exists())
        self.assertNotIn("fail_under", str(pyproject()["tool"]["coverage"]))
        omit = " ".join(run["omit"])
        for category in CANONICAL_OMIT_CATEGORIES:
            self.assertIn(category, omit)

    def test_declared_production_paths_are_measured(self):
        modules, packages = declared_production_sources()
        omit = coverage_run_config()["omit"]
        for module in modules:
            relative = f"{module}.py"
            self.assertTrue((ROOT / relative).exists(), relative)
            self.assertFalse(matches_any_omit(relative, omit), relative)
        for package in packages:
            self.assertFalse(matches_any_omit(package, omit), package)

    def test_unexecuted_in_scope_files_remain_in_denominator(self):
        modules, packages = declared_production_sources()
        run = coverage_run_config()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for module in modules:
                (root / f"{module}.py").write_text(SYNTHETIC_MODULE, encoding="utf-8")
            for package in packages:
                (root / package).mkdir()
            (root / packages[0] / "synthetic_service.py").write_text(
                SYNTHETIC_MODULE, encoding="utf-8"
            )
            excluded = "def never_executed():\n    value = 1\n    return value\n"
            for directory in (
                "tests",
                "external",
                ".venv",
                "build",
                "dist",
                "release-overlays",
                "research",
            ):
                (root / directory).mkdir(parents=True)
                (root / directory / "excluded_module.py").write_text(excluded, encoding="utf-8")
            imported = modules[:-1]
            imports = [
                f"importlib.import_module({module!r}).executed()" for module in imported
            ]
            (root / "runner.py").write_text(
                "import importlib\n" + "\n".join(imports) + "\nprint('done')\n",
                encoding="utf-8",
            )
            (root / ".coveragerc").write_text(
                "[run]\n"
                "branch = true\n"
                "source =\n"
                + "".join(f"    {entry}\n" for entry in run["source"])
                + "omit =\n"
                + "".join(f"    {pattern}\n" for pattern in run["omit"])
                + "[report]\n"
                f"include_namespace_packages = {coverage_report_config().get('include_namespace_packages', False)}\n",
                encoding="utf-8",
            )
            collected = subprocess.run(
                [sys.executable, "-m", "coverage", "run", "runner.py"],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            self.assertEqual(collected.returncode, 0, collected.stderr)
            report = subprocess.run(
                [sys.executable, "-m", "coverage", "report", "--show-missing"],
                cwd=root,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            self.assertEqual(report.returncode, 0, report.stderr)
            output = report.stdout
            self.assertNotIn("No data to report", output)
            lines = {
                candidate.split()[0]: candidate
                for candidate in output.splitlines()
                if candidate and not candidate.startswith(("Name", "-", "TOTAL"))
            }
            for module in imported:
                self.assertIn(module + ".py", lines)
                self.assertIn("6-7", lines[module + ".py"])
            never_imported = modules[-1]
            self.assertIn(never_imported + ".py", lines)
            self.assertIn("0%", lines[never_imported + ".py"])
            service_path = f"{packages[0]}/synthetic_service.py"
            self.assertIn(service_path, lines)
            self.assertIn("0%", lines[service_path])
            for directory in (
                "tests",
                "external",
                ".venv",
                "build",
                "dist",
                "release-overlays",
                "research",
            ):
                self.assertNotIn(f"{directory}/excluded_module.py", output, directory)

    def test_ci_measurement_step_runs_the_suite_instrumented(self):
        block = measurement_step_block()
        self.assertIn("PYTEST_DISABLE_PLUGIN_AUTOLOAD", block)
        self.assertIn("PYTHONPATH", block)
        self.assertIn("coverage run -m pytest -q tests", block)
        self.assertIn("coverage report --show-missing", block)
        self.assertIn("coverage measured no files", block)


if __name__ == "__main__":
    unittest.main()
