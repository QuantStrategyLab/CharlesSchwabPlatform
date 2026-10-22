"""Owner designation, rollback revision retention and both publish boundaries."""
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.refresh_artifact_registry_keep import (
    collect_digests, protected_revisions, prune_revisions, rollback_contract, verify_rollbacks,
)
from test_refresh_artifact_registry_keep import FakeGcloud, IMAGE_REPO, PROJECT, REGION, image, run

ROOT = Path(__file__).resolve().parents[1]
SERVICE = "old-service"


def manifest():
    return {"schema_version": 1, "source": {"resource": "owner-release-record", "version": "v1"},
            "services": {SERVICE: {"image_repo": IMAGE_REPO, "rollbacks": [
                {"level": level, "revision": revision, "digest": "sha256:" + char * 64,
                 "release": "release-" + level, "designated_by": "release-owner",
                 "acceptance_evidence": "owner-record/acceptance-" + level,
                 "retirement_condition": "owner explicitly retires this release"}
                for level, revision, char in [("R1", "rollback-one", "d"), ("R2", "rollback-two", "e")]]}}}


def contract(data=None):
    return rollback_contract(json.dumps(data if data is not None else manifest()),
                             service=SERVICE, image_repo=IMAGE_REPO)


class ReleaseContractTests(unittest.TestCase):
    def test_owner_designations_remain_bound_to_service_and_package(self):
        self.assertEqual([r["level"] for r in contract()], ["R1", "R2"])
        changes = [
            lambda m: m["source"].pop("version"),
            lambda m: m["source"].update(resource="<owner source>"),
            lambda m: m["services"].pop(SERVICE),
            lambda m: m["services"][SERVICE].update(image_repo="other-package"),
            lambda m: m["services"][SERVICE]["rollbacks"].pop(),
            lambda m: m["services"][SERVICE]["rollbacks"][1].update(level="R1"),
            lambda m: m["services"][SERVICE]["rollbacks"][1].update(digest="sha256:" + "d" * 64),
            lambda m: m["services"][SERVICE]["rollbacks"][1].update(revision="rollback-one"),
            lambda m: m["services"][SERVICE]["rollbacks"][0].update(digest="sha256:abcd"),
            lambda m: m["services"][SERVICE]["rollbacks"][0].update(acceptance_evidence=""),
            lambda m: m["services"][SERVICE]["rollbacks"][0].update(retirement_condition="NEED_USER"),
        ]
        for change in changes:
            data = manifest()
            change(data)
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "NEED_USER"):
                contract(data)
        for raw in ("", "{}", "null", "[]", "invalid"):
            with self.subTest(raw=raw), self.assertRaisesRegex(ValueError, "NEED_USER"):
                rollback_contract(raw, service=SERVICE, image_repo=IMAGE_REPO)

    def test_zero_traffic_tags_and_latest_created_are_included(self):
        fake = FakeGcloud()
        fake.service["status"]["traffic"].append({"revisionName": "tagged-candidate", "tag": "candidate"})
        fake.service["status"]["latestCreatedRevisionName"] = "pending-created"
        run(fake)
        names = {c[3] for c in fake.calls if c[:3] == ["run", "revisions", "describe"]}
        self.assertEqual(names, {"old-serving", "tagged-candidate", "pending-created"})
        fake.service["spec"] = {"traffic": [{"tag": "pending", "latestRevision": True}]}
        with self.assertRaisesRegex(ValueError, "unresolved"):
            collect_digests(fake, project=PROJECT, region=REGION, image_repo=IMAGE_REPO)
        fake.service["spec"] = {"traffic": [{"tag": "pending", "revisionName": "named-pending"}]}
        self.assertIn("named-pending", protected_revisions(fake.service))

    def test_live_rollback_revision_mapping_and_image_existence(self):
        for fault in (None, "service", "digest", "missing-image"):
            calls = []
            def reader(args):
                calls.append(args)
                digest = "sha256:" + ("d" if "rollback-one" in args or "d" * 64 in str(args) else "e") * 64
                if args[:3] == ["run", "revisions", "describe"]:
                    return json.dumps({"metadata": {"labels": {"serving.knative.dev/service": "wrong" if fault == "service" else SERVICE}},
                                       "spec": {"containers": [{"image": IMAGE_REPO + "@" + digest}]},
                                       "status": {"imageDigest": "sha256:" + "f" * 64 if fault == "digest" else digest}})
                if args[:4] == ["artifacts", "docker", "images", "describe"]:
                    if fault == "missing-image":
                        raise RuntimeError("image missing")
                    return json.dumps({"image_summary": {"digest": digest}})
                raise AssertionError(args)
            with self.subTest(fault=fault):
                if fault:
                    with self.assertRaises((ValueError, RuntimeError)):
                        verify_rollbacks(reader, project=PROJECT, region=REGION, service=SERVICE,
                                         image_repo=IMAGE_REPO, rows=contract())
                else:
                    verify_rollbacks(reader, project=PROJECT, region=REGION, service=SERVICE,
                                     image_repo=IMAGE_REPO, rows=contract())
                    self.assertEqual(len(calls), 4)

    def test_revision_prune_keeps_both_rollback_levels_tags_and_latest(self):
        for fault in (None, "list", "missing-protected", "drift", "delete"):
            fake = FakeGcloud()
            fake.service["status"]["latestCreatedRevisionName"] = "new-created"
            fake.service["status"]["traffic"].append({"revisionName": "zero-tag", "tag": "candidate"})
            names = ["old-serving", "new-created", "zero-tag", "rollback-one", "rollback-two", "stale-one", "stale-two"]
            calls, deleted = [], []
            def reader(args):
                calls.append(args)
                command = args[:3]
                if command == ["run", "services", "describe"]:
                    row = copy.deepcopy(fake.service)
                    if fault == "drift" and len(calls) > 1:
                        row["metadata"] = {"resourceVersion": "changed"}
                    return json.dumps(row)
                if command == ["run", "revisions", "list"]:
                    if fault == "list":
                        raise RuntimeError("inventory unavailable")
                    return json.dumps([{"metadata": {"name": n}} for n in names if fault != "missing-protected" or n != "rollback-two"])
                if command == ["run", "revisions", "delete"]:
                    if fault == "delete":
                        raise RuntimeError("delete failed")
                    deleted.append(args[3])
                    return ""
                raise AssertionError(args)
            with self.subTest(fault=fault):
                if fault:
                    with self.assertRaises((ValueError, RuntimeError)):
                        prune_revisions(reader, project=PROJECT, region=REGION, service=SERVICE, rollback=contract())
                    self.assertEqual(deleted, [])
                    if fault == "delete":
                        self.assertEqual(calls[-1][3], "stale-one")
                else:
                    prune_revisions(reader, project=PROJECT, region=REGION, service=SERVICE, rollback=contract())
                    self.assertEqual(deleted, ["stale-one", "stale-two"])

    def test_actual_workflow_stops_both_publishes_when_keep_refresh_fails(self):
        workflow = ROOT / ".github/workflows/sync-cloud-run-env.yml"
        wf = json.loads(subprocess.check_output(["ruby", "-ryaml", "-rjson", "-e", "puts JSON.generate(YAML.load_file(ARGV[0]))", str(workflow)]))
        steps = wf["jobs"]["sync-cloud-run-env"]["steps"]
        blocks = [next(s for s in steps if s.get("id") == "deploy"),
                  next(s for s in steps if s.get("name", "").startswith("Publish one native"))]
        for step in blocks:
            self.assertIn("deploy_enabled", step["if"])
            self.assertIn("vars.SCHWAB_ARTIFACT_REGISTRY_ROLLBACK_MANIFEST_JSON", step["env"]["SCHWAB_ARTIFACT_REGISTRY_ROLLBACK_MANIFEST_JSON"])
            with self.subTest(step=step["name"]), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                shim = root / "shim"
                shim.write_text('''#!/bin/bash
printf '%s %s\\n' "${0##*/}" "$*" >> "$CALLS"
case "${0##*/} $*" in
  'python3 scripts/refresh_artifact_registry_keep.py '*) exit 42 ;;
  'gcloud artifacts docker images describe '*) printf 'sha256:%064d\\n' 0 ;;
esac
''')
                shim.chmod(0o700)
                for name in ("python3", "gcloud", "docker", "git"):
                    (root / name).symlink_to(shim)
                env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}", CALLS=str(root / "calls"),
                           GCP_PROJECT_ID=PROJECT, CLOUD_RUN_REGION=REGION, CLOUD_RUN_SERVICE=SERVICE,
                           GCP_ARTIFACT_REGISTRY_REPOSITORY="cloud-run-source-deploy", GITHUB_SHA="synthetic",
                           GITHUB_OUTPUT=str(root / "output"), NOTIFICATION_IMAGE=image("c"))
                result = subprocess.run(["bash", "-c", step["run"]], env=env, capture_output=True)
                self.assertEqual(result.returncode, 42, result.stderr)
                calls = (root / "calls").read_text()
                self.assertNotIn("gcloud run deploy", calls)
                self.assertNotIn("native-publish", calls)
                self.assertIn("--candidate=", calls)
        text = workflow.read_text()
        self.assertNotIn("gcloud artifacts docker images delete", text)
        self.assertNotIn("gcloud run revisions delete", text)
        self.assertIn("--prune-revisions", text)
