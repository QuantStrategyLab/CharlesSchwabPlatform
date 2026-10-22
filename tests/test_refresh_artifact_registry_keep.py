"""Synthetic repository and Cloud Run inventory for the deploy KEEP guard."""

import copy
import json
import unittest

from scripts.refresh_artifact_registry_keep import KEEP_ID, refresh


PROJECT = "charlesschwabquant"
REGION = "us-central1"
REPOSITORY = "cloud-run-source-deploy"
IMAGE_REPO = f"{REGION}-docker.pkg.dev/{PROJECT}/{REPOSITORY}/charlesschwabplatform/old-service"


def image(letter: str) -> str:
    return f"{IMAGE_REPO}@sha256:{letter * 64}"


def policy_fixture():
    keep = {"id": KEEP_ID, "action": "KEEP", "condition": {
        "tagState": "ANY", "versionNamePrefixes": ["sha256:" + "a" * 56]}}
    return {
        "name": f"projects/{PROJECT}/locations/{REGION}/repositories/{REPOSITORY}",
        "updateTime": "2026-10-22T00:00:00Z",
        "cleanupPolicyDryRun": True,
        "cleanupPolicies": {
            KEEP_ID: keep,
            "keep-newest-ten-per-package": {"id": "keep-newest-ten-per-package", "action": "KEEP",
                                            "mostRecentVersions": {"keepCount": 10}},
            "keep-recent-2-per-package": {"id": "keep-recent-2-per-package", "action": "KEEP",
                                           "mostRecentVersions": {"keepCount": 2}},
            **{name: {"id": name, "action": "DELETE", "condition": {
                "tagState": "UNTAGGED", "olderThan": "604800s"}}
               for name in ("delete-aged-tagged", "delete-aged-untagged", "delete-untagged-after-14d")},
        },
    }


class FakeGcloud:
    def __init__(self, mode=True):
        self.repository = policy_fixture()
        self.repository["cleanupPolicyDryRun"] = mode
        self.calls = []
        self.fail_on = None
        self.repository_reads = 0
        self.fail_read_at = None
        self.mode_changes = {}
        self.service = {"status": {"traffic": [{"revisionName": "old-serving", "percent": 100}],
                                   "latestReadyRevisionName": "old-serving"}}
        self.revision = {"spec": {"containers": [{"image": IMAGE_REPO + ":old"}]},
                         "status": {"imageDigest": "sha256:" + "9" * 64}}
        self.jobs = [{"metadata": {"name": "existing-job"}}]
        self.job = {"template": {"template": {"containers": [{"image": image("b")}]}}}
        self.executions = []
        self.execution = {"spec": {"template": {"spec": {"containers": [{"image": image("f")}]}}},
                          "status": {"runningCount": 1}}
        self.pending = None

    def __call__(self, args):
        self.calls.append(args)
        command = tuple(args[:3])
        if command == self.fail_on:
            raise RuntimeError("simulated API failure")
        if command == ("run", "services", "list"):
            return json.dumps([{"metadata": {"name": "old-service"}}])
        if command == ("run", "services", "describe"):
            return json.dumps(self.service)
        if command == ("run", "revisions", "describe"):
            return json.dumps(self.revision)
        if command == ("run", "jobs", "list"):
            return json.dumps(self.jobs)
        if command == ("run", "jobs", "describe"):
            return json.dumps(self.job)
        if command == ("run", "jobs", "executions"):
            if args[3] == "list":
                return json.dumps(self.executions)
            if args[3] == "describe":
                return json.dumps(self.execution)
        if command == ("artifacts", "repositories", "describe"):
            self.repository_reads += 1
            if self.repository_reads == self.fail_read_at:
                raise RuntimeError("simulated readback failure")
            if self.repository_reads in self.mode_changes:
                self.repository["cleanupPolicyDryRun"] = self.mode_changes[self.repository_reads]
            return json.dumps(self.repository)
        if command == ("artifacts", "repositories", "set-cleanup-policies"):
            path = next(arg.split("=", 1)[1] for arg in args if arg.startswith("--policy="))
            with open(path, encoding="utf-8") as handle:
                self.pending = json.load(handle)
            expected = ["artifacts", "repositories", "set-cleanup-policies", REPOSITORY,
                        f"--project={PROJECT}", f"--location={REGION}", f"--policy={path}", "--dry-run", "--quiet"]
            if args != expected or len(self.pending) != 6:
                raise AssertionError("Only the complete policy set may be submitted; dry-run must remain enabled")
            for row in self.pending:
                source = self.repository["cleanupPolicies"][row["name"]]
                source["action"] = row["action"]["type"].upper()
                if "condition" in row:
                    source["condition"] = copy.deepcopy(row["condition"])
                    source["condition"]["tagState"] = source["condition"]["tagState"].upper()
            return ""
        raise AssertionError(f"Unexpected command: {args}")


def run(fake, *, candidate=None, rollback=None):
    return refresh(fake, project=PROJECT, region=REGION, repository_id=REPOSITORY,
                   image_repo=IMAGE_REPO, candidate=candidate or image("c"),
                   rollback=rollback if rollback is not None else ["sha256:" + c * 64 for c in "de"])


class KeepGuardTests(unittest.TestCase):
    def test_older_than_newest_ten_serving_and_job_are_protected(self):
        for mode in (True,):
            with self.subTest(mode=mode):
                fake = FakeGcloud(mode)
                original = copy.deepcopy(fake.repository["cleanupPolicies"])
                required = run(fake)
                self.assertEqual(required, {"sha256:" + char * 64 for char in "bcde9"})
                updated = fake.repository["cleanupPolicies"]
                self.assertEqual(updated[KEEP_ID]["condition"]["versionNamePrefixes"],
                                 ["sha256:" + "a" * 56, *sorted(required)])
                self.assertIs(fake.repository["cleanupPolicyDryRun"], mode)
                self.assertEqual({k: v for k, v in updated.items() if k != KEEP_ID},
                                 {k: v for k, v in original.items() if k != KEEP_ID})
                self.assertEqual(len(fake.pending), 6)

    def test_merge_is_idempotent(self):
        for mode in (True,):
            with self.subTest(mode=mode):
                fake = FakeGcloud(mode)
                run(fake)
                fake.calls.clear()
                fake.pending = None
                run(fake)
                self.assertIsNone(fake.pending)
                self.assertFalse(any(call[:3] == ["artifacts", "repositories", "set-cleanup-policies"]
                                     for call in fake.calls))
                self.assertIs(fake.repository["cleanupPolicyDryRun"], mode)

    def test_non_dry_run_mode_is_rejected_before_write(self):
        for mode in (False, None, "true", 0, 1):
            with self.subTest(mode=mode):
                fake = FakeGcloud(mode)
                with self.assertRaises(ValueError):
                    run(fake)
                self.assertIsNone(fake.pending)
        fake = FakeGcloud()
        del fake.repository["cleanupPolicyDryRun"]
        with self.assertRaises(ValueError):
            run(fake)
        self.assertIsNone(fake.pending)

    def test_other_package_in_same_repository_is_protected(self):
        fake = FakeGcloud()
        fake.job["template"]["template"]["containers"][0]["image"] = (
            f"{REGION}-docker.pkg.dev/{PROJECT}/{REPOSITORY}/other-package@sha256:" + "e" * 64)
        self.assertIn("sha256:" + "e" * 64, run(fake))

    def test_active_execution_retains_old_job_image(self):
        fake = FakeGcloud()
        fake.executions = [{"metadata": {"name": "existing-job-run-1"}, "status": {"runningCount": 1}}]
        self.assertIn("sha256:" + "f" * 64, run(fake))

    def test_multi_container_revision_protects_sidecar(self):
        fake = FakeGcloud()
        fake.revision["spec"]["containers"].append({"image": image("f")})
        self.assertIn("sha256:" + "f" * 64, run(fake))

    def test_unresolved_sidecar_tag_fails_before_policy_write(self):
        fake = FakeGcloud()
        fake.revision["spec"]["containers"].append({"image": IMAGE_REPO + ":sidecar"})
        with self.assertRaisesRegex(ValueError, "immutable"):
            run(fake)
        self.assertIsNone(fake.pending)

    def test_malformed_resolved_digest_fails_before_policy_write(self):
        fake = FakeGcloud()
        fake.revision["status"]["imageDigest"] = "sha256:" + "g" * 64
        with self.assertRaisesRegex(ValueError, "invalid"):
            run(fake)
        self.assertIsNone(fake.pending)

    def test_missing_traffic_or_tagged_job_fails_before_policy_write(self):
        for change in ("traffic", "job"):
            with self.subTest(change=change):
                fake = FakeGcloud()
                if change == "traffic":
                    fake.service["status"]["traffic"] = [{"latestRevision": True, "percent": 100}]
                else:
                    fake.job["template"]["template"]["containers"][0]["image"] = IMAGE_REPO + ":latest"
                with self.assertRaises(ValueError):
                    run(fake)
                self.assertFalse(any(call[:3] == ["artifacts", "repositories", "set-cleanup-policies"]
                                     for call in fake.calls))

    def test_inventory_api_failure_does_not_update_policy(self):
        for mode in (True,):
            with self.subTest(mode=mode):
                fake = FakeGcloud(mode)
                fake.fail_on = ("run", "jobs", "list")
                with self.assertRaises(RuntimeError):
                    run(fake)
                self.assertEqual(fake.calls[-1][:3], ["run", "jobs", "list"])
                self.assertIsNone(fake.pending)

    def test_policy_update_api_failure_stops_before_deploy(self):
        for mode in (True,):
            with self.subTest(mode=mode):
                fake = FakeGcloud(mode)
                fake.fail_on = ("artifacts", "repositories", "set-cleanup-policies")
                with self.assertRaises(RuntimeError):
                    run(fake)
                self.assertEqual(tuple(fake.calls[-1][:3]), fake.fail_on)
                self.assertEqual(fake.repository_reads, 2)
                self.assertIsNone(fake.pending)

    def test_repository_read_failure_stops_refresh(self):
        for mode in (True,):
            for read in (1, 2, 3):
                with self.subTest(mode=mode, read=read):
                    fake = FakeGcloud(mode)
                    fake.fail_read_at = read
                    with self.assertRaisesRegex(RuntimeError, "readback failure"):
                        run(fake)
                    self.assertEqual(fake.repository_reads, read)
                    self.assertEqual(fake.calls[-1][:3], ["artifacts", "repositories", "describe"])
                    self.assertEqual(fake.pending is not None, read == 3)

    def test_fresh_mode_drift_stops_write(self):
        for mode in (True,):
            with self.subTest(mode=mode):
                fake = FakeGcloud(mode)
                fake.mode_changes[2] = not mode
                with self.assertRaisesRegex(ValueError, "cleanupPolicyDryRun=true"):
                    run(fake)
                self.assertIsNone(fake.pending)
                self.assertEqual(fake.repository_reads, 2)

    def test_repository_drift_before_update_stops_write(self):
        class DriftingRepository(FakeGcloud):
            reads = 0

            def __call__(self, args):
                if args[:3] == ["artifacts", "repositories", "describe"]:
                    self.reads += 1
                    if self.reads == 2:
                        self.repository["updateTime"] = "2026-10-22T00:00:01Z"
                return super().__call__(args)

        for mode in (True,):
            with self.subTest(mode=mode):
                fake = DriftingRepository(mode)
                with self.assertRaisesRegex(ValueError, "changed"):
                    run(fake)
                self.assertIsNone(fake.pending)

    def test_failed_or_drifting_readback_stops_deploy(self):
        class WrongReadback(FakeGcloud):
            def __call__(self, args):
                result = super().__call__(args)
                if args[:3] == ["artifacts", "repositories", "set-cleanup-policies"]:
                    self.repository["cleanupPolicies"][KEEP_ID]["condition"]["versionNamePrefixes"].pop()
                return result

        for mode in (True,):
            with self.subTest(mode=mode):
                fake = WrongReadback(mode)
                with self.assertRaisesRegex(ValueError, "readback"):
                    run(fake)
                self.assertEqual(fake.repository_reads, 3)

    def test_repository_mode_change_fails_closed(self):
        for mode in (True,):
            for needs_update in (True, False):
                with self.subTest(mode=mode, needs_update=needs_update):
                    fake = FakeGcloud(mode)
                    if not needs_update:
                        run(fake)
                        fake.calls.clear()
                        fake.pending = None
                        fake.repository_reads = 0
                    final_read = 3 if needs_update else 2
                    fake.mode_changes[final_read] = not mode
                    with self.assertRaisesRegex(ValueError, "cleanupPolicyDryRun=true"):
                        run(fake)
                    self.assertEqual(fake.repository_reads, final_read)
                    self.assertEqual(fake.pending is not None, needs_update)
                    self.assertEqual(fake.calls[-1][:3], ["artifacts", "repositories", "describe"])

    def test_invalid_mode_type_fails_closed(self):
        for value in (None, "true", "false", 0, 1, [], {}):
            for needs_update in (True, False):
                for read in ((1, 2, 3) if needs_update else (1, 2)):
                    with self.subTest(value=value, needs_update=needs_update, read=read):
                        fake = FakeGcloud()
                        if not needs_update:
                            run(fake)
                            fake.pending = None
                            fake.calls.clear()
                            fake.repository_reads = 0
                        fake.mode_changes[read] = value
                        with self.assertRaisesRegex(ValueError, "mode must be a boolean"):
                            run(fake)
                        self.assertEqual(fake.repository_reads, read)
                        self.assertEqual(fake.pending is not None, needs_update and read == 3)

    def test_wrong_repository_or_policy_shape_fails_before_write(self):
        for mode in (True,):
            for change in ("identity", "missing_policy", "action", "tag_state", "prefixes"):
                with self.subTest(mode=mode, change=change):
                    fake = FakeGcloud(mode)
                    keep = fake.repository["cleanupPolicies"][KEEP_ID]
                    if change == "identity":
                        fake.repository["name"] += "-other"
                    elif change == "missing_policy":
                        del fake.repository["cleanupPolicies"]["keep-newest-ten-per-package"]
                    elif change == "action":
                        keep["action"] = "DELETE"
                    elif change == "tag_state":
                        keep["condition"]["tagState"] = "TAGGED"
                    else:
                        keep["condition"]["versionNamePrefixes"] = []
                    with self.assertRaises(ValueError):
                        run(fake)
                    self.assertIsNone(fake.pending)

    def test_missing_designated_rollback_fails_before_inventory(self):
        fake = FakeGcloud()
        with self.assertRaisesRegex(ValueError, "rollback"):
            run(fake, rollback=[])
        self.assertEqual(fake.calls, [])

    def test_narrowed_keep_condition_is_rejected_before_write(self):
        for key, value in (("packageNamePrefixes", ["other-package"]),
                           ("tagPrefixes", ["release-"]),
                           ("olderThan", "604800s"), ("newerThan", "3600s")):
            with self.subTest(key=key):
                fake = FakeGcloud()
                fake.repository["cleanupPolicies"][KEEP_ID]["condition"][key] = value
                with self.assertRaisesRegex(ValueError, "KEEP policy shape changed"):
                    run(fake)
                self.assertIsNone(fake.pending)


if __name__ == "__main__":
    unittest.main()
