"""Execute the CI scan with controlled tools and synthetic workflow files."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
STEP = "      - name: Reject public runtime configuration bindings\n        run: |\n"


def scan_script() -> str:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    if workflow.count(STEP) != 1:
        raise AssertionError("Expected exactly one public configuration scan step")
    lines = workflow.split(STEP, 1)[1].splitlines(keepends=True)
    body = []
    for line in lines:
        if not line.strip():
            body.append(line)
        elif line.startswith("          "):
            body.append(line[10:])
        else:
            break
    return "".join(body)


class PublicRuntimeConfigurationScanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bash = shutil.which("bash")
        cls.ripgrep = shutil.which("rg")
        if not cls.bash or not cls.ripgrep:
            raise RuntimeError("The scan regression tests require bash and ripgrep")
        cls.script = scan_script()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.tools = self.root / "tools"
        self.tools.mkdir()
        self.workflows = self.root / ".github/workflows"
        self.workflows.mkdir(parents=True)

    def run_scan(self):
        return subprocess.run(
            [self.bash, "--noprofile", "--norc", "-eo", "pipefail", "-c", self.script],
            cwd=self.root,
            env={"PATH": str(self.tools), "LC_ALL": "C"},
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def fake_scanner(self, exit_code, output=""):
        executable = self.tools / "rg"
        executable.write_text(
            f"#!{self.bash}\nprintf '%s\\n' '{output}' >&2\nexit {exit_code}\n",
            encoding="utf-8",
        )
        executable.chmod(0o755)

    def real_scanner(self):
        os.symlink(self.ripgrep, self.tools / "rg")

    def workflow(self, content):
        (self.workflows / "synthetic.yml").write_text(content, encoding="utf-8")

    def test_missing_executable_fails(self):
        result = self.run_scan()
        self.assertNotEqual(result.returncode, 0)

    def test_scanner_errors_fail(self):
        for status in (2, 7, 126, 127, 130):
            with self.subTest(status=status):
                self.fake_scanner(status)
                result = self.run_scan()
                self.assertEqual(result.returncode, status)

    def test_non_executable_scanner_fails(self):
        self.fake_scanner(1)
        (self.tools / "rg").chmod(0o644)
        self.assertNotEqual(self.run_scan().returncode, 0)

    def test_clean_no_match_succeeds(self):
        self.fake_scanner(1)
        self.assertEqual(self.run_scan().returncode, 0)

    def test_match_fails(self):
        self.fake_scanner(0)
        result = self.run_scan()
        self.assertEqual(result.returncode, 1)
        self.assertIn("must use GitHub Secrets", result.stderr)

    def test_error_with_output_fails(self):
        self.fake_scanner(2, "synthetic scanner read error")
        self.assertEqual(self.run_scan().returncode, 2)

    def test_real_clean_workflow_succeeds(self):
        self.real_scanner()
        self.workflow("name: Synthetic workflow\n")
        self.assertEqual(self.run_scan().returncode, 0)

    def test_real_sensitive_variable_bindings_fail(self):
        self.real_scanner()
        names = (
            "GLOBAL_TELEGRAM_CHAT_ID",
            "CLOUD_RUN_SERVICES",
            "CLOUD_RUN_SERVICE",
            "RUNTIME_HEARTBEAT_REQUIRED_SERVICES",
            "RUNTIME_GUARD_SCHEDULER_JOB_PATTERN",
        )
        suffixes = (" }}", "}}", "|| 'synthetic' }}")
        for name in names:
            for suffix in suffixes:
                with self.subTest(name=name, suffix=suffix):
                    self.workflow("value: " + "${{ vars." + name + suffix + "\n")
                    self.assertEqual(self.run_scan().returncode, 1)

    def test_real_secret_and_unrelated_variable_bindings_succeed(self):
        self.real_scanner()
        self.workflow(
            "secret: ${{ secrets.CLOUD_RUN_SERVICE }}\n"
            "unrelated: ${{ vars.CLOUD_RUN_SERVICE_SUFFIX }}\n"
            "generic: ${{ vars.SYNTHETIC_NON_SENSITIVE_SETTING }}\n"
        )
        self.assertEqual(self.run_scan().returncode, 0)

    def test_real_missing_scan_directory_fails(self):
        self.real_scanner()
        self.workflows.rmdir()
        self.assertEqual(self.run_scan().returncode, 2)

    def test_scan_remains_limited_to_workflows(self):
        self.real_scanner()
        self.workflow("name: Synthetic workflow\n")
        (self.root / "out-of-scope.txt").write_text(
            "${{ vars.CLOUD_RUN_SERVICE }}\n", encoding="utf-8"
        )
        self.assertEqual(self.run_scan().returncode, 0)


if __name__ == "__main__":
    unittest.main()
