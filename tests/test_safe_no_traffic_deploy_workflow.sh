#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
workflow="$repo_dir/.github/workflows/sync-cloud-run-env.yml"
readback="$repo_dir/scripts/verify_cloud_run_no_traffic_deploy.py"

ruby -e 'require "yaml"; YAML.load_file(ARGV.fetch(0))' "$workflow" >/dev/null

for input in approved_ref expected_sha approve_image_deploy approve_env_secret_sync approve_scheduler_iam_sync approve_traffic_shift approve_cleanup; do
  grep -Fq "      ${input}:" "$workflow"
done
grep -Fq '[ "${APPROVED_REF}" != "${GITHUB_REF}" ]' "$workflow"
grep -Fq '[ "${EXPECTED_SHA}" != "${GITHUB_SHA}" ] || [ "${EXPECTED_SHA}" != "${checked_out_sha}" ]' "$workflow"
grep -Fq '[ "${ENABLE_GITHUB_CLOUD_RUN_DEPLOY:-}" = "true" ] && [ "${APPROVE_IMAGE_DEPLOY:-false}" = "true" ]' "$workflow"
grep -Fq '[ "${ENABLE_GITHUB_ENV_SYNC:-}" = "true" ] && [ "${APPROVE_ENV_SECRET_SYNC:-false}" = "true" ]' "$workflow"
grep -Fq '[ "${ENABLE_GITHUB_ENV_SYNC:-}" = "true" ] && [ "${APPROVE_SCHEDULER_IAM_SYNC:-false}" = "true" ]' "$workflow"
grep -Fq '[ "${ENABLE_GITHUB_ENV_SYNC:-}" = "true" ] && [ "${APPROVE_TRAFFIC_SHIFT:-false}" = "true" ]' "$workflow"
grep -Fq '[ "${APPROVE_CLEANUP:-false}" = "true" ]' "$workflow"
grep -Fq 'steps.config.outputs.env_sync_enabled == '\''true'\''' "$workflow"
grep -Fq 'steps.config.outputs.scheduler_iam_sync_enabled == '\''true'\''' "$workflow"
grep -Fq 'steps.config.outputs.traffic_shift_enabled == '\''true'\''' "$workflow"
grep -Fq 'steps.config.outputs.cleanup_enabled == '\''true'\''' "$workflow"

deploy_block="$(awk '/gcloud run services update "\$\{CLOUD_RUN_SERVICE\}"/ {found=1} found {print} found && /--quiet/ {exit}' "$workflow")"
grep -Fq 'immutable_image="${image_repo}@${image_digest}"' "$workflow"
grep -Fq -- '--image="${immutable_image}"' <<<"$deploy_block"
grep -Fq -- '--no-traffic' <<<"$deploy_block"
for option in --update-env-vars --set-env-vars --service-account --ingress --max-instances --concurrency --memory --cpu --timeout --startup-probe; do
  if grep -Fq -- "$option" <<<"$deploy_block"; then
    echo "image-only update must not change runtime configuration" >&2
    exit 1
  fi
done
if grep -Fq -- '--to-latest' <<<"$deploy_block"; then
  echo "image deploy must not shift traffic" >&2
  exit 1
fi

grep -Fq 'Capture no-traffic deployment baseline' "$workflow"
grep -Fq 'Verify no-traffic deployment readback' "$workflow"
grep -Fq 'scripts/verify_cloud_run_no_traffic_deploy.py capture' "$workflow"
grep -Fq 'scripts/verify_cloud_run_no_traffic_deploy.py verify' "$workflow"
grep -Fq -- '--expected-image-digest="${EXPECTED_IMAGE_DIGEST}"' "$workflow"
grep -Fq 'image_summary.digest' "$workflow"

grep -Fq 'for key in ("traffic", "scheduler", "iam", "configuration"):' "$readback"
grep -Fq 'raise RuntimeError(f"{key} changed during no-traffic deployment")' "$readback"
grep -Fq 'Compare only effective traffic' "$readback"
grep -Fq 'if percent > 0:' "$readback"
grep -Fq 'json(spec.template.spec.containers[].env.always().filter("name=SCHWAB_CASH_CURRENCY").map().extract(value))' "$readback"
grep -Fq 'json(spec.containers[].env.always().filter("name=SCHWAB_CASH_CURRENCY").map().extract(value))' "$readback"
grep -Fq 'deployment baseline cash currency is not an allowed value' "$readback"
if grep -Fq 'secrets versions access' "$readback"; then
  echo "readback must not access Secret Manager values or plaintext environment values" >&2
  exit 1
fi

# Execute the real build command against a synthetic checkout. Authentication
# files created after checkout must never enter the container build context.
python3 - "$workflow" <<'PY'
import os
from pathlib import Path
import subprocess
import sys
import tempfile

build_command = next(line.strip() for line in Path(sys.argv[1]).read_text().splitlines() if "docker build --pull" in line)
with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    control = root / "checkout"
    control.mkdir()
    checkout = control
    env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    def git(*args):
        subprocess.run(["git", *args], cwd=checkout, env=env, check=True, capture_output=True)
    git("init", "-q")
    (checkout / "Dockerfile").write_text("FROM scratch\nCOPY . /app/\n")
    (checkout / "tracked.txt").write_text("synthetic source\n")
    (checkout / "uv.lock").write_text("synthetic lock\n")
    git("add", "Dockerfile", "tracked.txt", "uv.lock")
    git("-c", "user.name=synthetic", "-c", "user.email=synthetic@example.invalid", "-c", "commit.gpgSign=false", "commit", "-qm", "synthetic")
    (checkout / "gha-creds-synthetic.json").write_text('{"synthetic":true}\n')
    (control / "gha-creds-control.json").write_text('{"synthetic":true}\n')
    (control / "tracked-maintenance-only.txt").write_text("not the approved runtime source\n")
    bin_dir = root / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(f"#!{sys.executable}\n" + '''import sys, tarfile
if sys.argv[-1] != "-":
    raise SystemExit("workspace build context can include generated credentials")
with tarfile.open(fileobj=sys.stdin.buffer, mode="r|*") as archive:
    names = {member.name for member in archive}
if names != {"Dockerfile", "tracked.txt", "uv.lock"}:
    raise SystemExit("build context must contain only the approved tracked source")
''')
    docker.chmod(0o700)
    env.update(PATH=f"{bin_dir}:{os.environ['PATH']}", image="synthetic:test", EXPECTED_SHA=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, env=env, text=True).strip())
    result = subprocess.run(["bash", "-c", "set -euo pipefail\n" + build_command], cwd=control, env=env, capture_output=True)
    if result.returncode:
        raise SystemExit("FAIL: generated authentication file is not excluded by the build boundary")
    guard = 'test "$(git rev-parse HEAD)" = "${EXPECTED_SHA}"\ngit show "${EXPECTED_SHA}:uv.lock" | cmp -s - uv.lock'
    def check_guard():
        return subprocess.run(["bash", "-c", "set -euo pipefail\n" + guard], cwd=control, env=env).returncode
    assert check_guard() == 0
    (checkout / "uv.lock").write_text("tampered lock\n")
    assert check_guard() != 0
    (checkout / "uv.lock").write_text("synthetic lock\n")
    env["EXPECTED_SHA"] = "0" * 40
    assert check_guard() != 0
print("PASS: tracked-source container build excludes generated authentication files")
PY

# The image, dependency lock, label and readback must all use the approved checkout.
python3 - "$workflow" <<'PY_SOURCE'
import json, subprocess, sys
wf = json.loads(subprocess.check_output(["ruby", "-rjson", "-ryaml", "-e", "puts JSON.generate(YAML.load_file(ARGV[0]))", sys.argv[1]]))
steps = wf["jobs"]["sync-cloud-run-env"]["steps"]
build = next(s["run"] for s in steps if s.get("name") == "Build, push, and deploy Cloud Run image")
readback = next(s["run"] for s in steps if s.get("name") == "Verify no-traffic deployment readback")
assert 'test "$(git rev-parse HEAD)" = "${EXPECTED_SHA}"' in build
assert 'git show "${EXPECTED_SHA}:uv.lock" | cmp -s - uv.lock' in build
assert 'git archive "${EXPECTED_SHA}" | docker build' in build
assert '--lockfile uv.lock' in build
assert 'commit-sha=${EXPECTED_SHA}' in build and '--expected-sha="${EXPECTED_SHA}"' in readback
assert 'WEBSITE_OBSERVATION_SOURCE' not in str(wf)
print("PASS: source, lock, image label and readback bind to the approved SHA")
PY_SOURCE

# Execute the actual mode selector with no connected services. Mixed-operation
# approvals must stop before checkout/auth; default HTTP remains available.
python3 - "$workflow" <<'PY'
import json, os, subprocess, sys, tempfile
from pathlib import Path
text = Path(sys.argv[1]).read_text()
# Ruby is already required by this test. Use JSON to avoid a new Python package.
wf = json.loads(subprocess.check_output(["ruby", "-rjson", "-ryaml", "-e", "puts JSON.generate(YAML.load_file(ARGV[0]))", sys.argv[1]]))
steps = wf['jobs']['sync-cloud-run-env']['steps']
config_step = next(x for x in steps if x.get('id') == 'config')
config = config_step['run']
# Notification dispatch inputs stay local to their steps, preserving the
# existing sync/drift job-env mirror and actual candidate input forwarding.
job_env = wf['jobs']['sync-cloud-run-env']['env']
assert 'NOTIFICATION_ONLY' not in job_env and 'NOTIFICATION_CANDIDATE_REVISION' not in job_env
assert config_step['env']['NOTIFICATION_ONLY'] == '${{ inputs.notification_only }}'
assert config_step['env']['NOTIFICATION_CANDIDATE_REVISION'] == '${{ inputs.notification_candidate_revision }}'
for step in [x for x in steps if x.get('id') == 'notification_image' or x.get('name','').startswith('Publish one native')]:
    assert step['env']['NOTIFICATION_CANDIDATE_REVISION'] == '${{ inputs.notification_candidate_revision }}'
for conflicting in [None, 'APPROVE_ENV_SECRET_SYNC', 'APPROVE_SCHEDULER_IAM_SYNC', 'APPROVE_TRAFFIC_SHIFT', 'APPROVE_CLEANUP']:
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory) / 'output'
        env = dict(os.environ, GITHUB_OUTPUT=str(output), NOTIFICATION_ONLY='true',
                   NOTIFICATION_CANDIDATE_REVISION='charles-schwab-quant-service-notification-test',
                   GITHUB_EVENT_NAME='workflow_dispatch', ENABLE_MAIN_PUSH_CLOUD_RUN_AUTOMATION='false', ENABLE_GITHUB_CLOUD_RUN_DEPLOY='true',
                   APPROVE_IMAGE_DEPLOY='true', ENABLE_GITHUB_ENV_SYNC='true', CLOUD_RUN_CLEANUP_ENABLED='true')
        for key in ['APPROVE_ENV_SECRET_SYNC', 'APPROVE_SCHEDULER_IAM_SYNC', 'APPROVE_TRAFFIC_SHIFT', 'APPROVE_CLEANUP']:
            env[key] = str(key == conflicting).lower()
        result = subprocess.run(['bash', '-c', config], env=env, capture_output=True)
        assert (result.returncode == 0) == (conflicting is None)
        if conflicting is None:
            values = dict(line.split('=', 1) for line in output.read_text().splitlines())
            assert values['notification_only'] == values['deploy_enabled'] == 'true'
            assert all(values[k] == 'false' for k in ['env_sync_enabled','scheduler_iam_sync_enabled','traffic_shift_enabled','cleanup_enabled'])
source = next(x for x in steps if x.get('name') == 'Checkout reviewed notification source')
assert source['with']['ref'] == 'e159e6cc07f9a9eea417b810674f934eb5291e6f'
assert source['with']['path'] == 'notification-source'
build = next(x['run'] for x in steps if x.get('id') == 'notification_image')
assert 'prepare-overlay' in build and '--lockfile", "notification-source/uv.lock"' in build
assert 'git archive HEAD' not in build and 'uv sync' not in build and 'pip install' not in build
publish = next(x['run'] for x in steps if x.get('name','').startswith('Publish one native'))
assert 'native-publish' in publish and '--baseline-revision=charles-schwab-quant-service-00781-lel' in publish
assert all(term not in publish for term in ['gcloud run deploy', 'services replace', '/probe', '/run', 'curl '])
for name in ['Install strategy status dependencies', 'Capture no-traffic deployment baseline', 'Build, push, and deploy Cloud Run image', 'Verify no-traffic deployment readback']:
    assert 'inputs.notification_only != true' in next(x['if'] for x in steps if x.get('name') == name)
print('PASS: native selector rejects four mixed approvals; exact source and separate HTTP/native paths verified')
PY
