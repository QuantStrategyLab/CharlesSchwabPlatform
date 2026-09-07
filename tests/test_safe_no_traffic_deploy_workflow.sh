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

deploy_block="$(sed -n '/gcloud run deploy "${CLOUD_RUN_SERVICE}"/,/--quiet/p' "$workflow")"
grep -Fq 'immutable_image="${image_repo}@${image_digest}"' "$workflow"
grep -Fq -- '--image="${immutable_image}"' <<<"$deploy_block"
grep -Fq -- '--no-traffic' <<<"$deploy_block"
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
if grep -Fq 'secrets versions access' "$readback" || grep -Fq 'containers.env.value,' "$readback"; then
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
    checkout = root / "checkout"
    checkout.mkdir()
    env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    def git(*args):
        subprocess.run(["git", *args], cwd=checkout, env=env, check=True, capture_output=True)
    git("init", "-q")
    (checkout / "Dockerfile").write_text("FROM scratch\nCOPY . /app/\n")
    (checkout / "tracked.txt").write_text("synthetic source\n")
    git("add", "Dockerfile", "tracked.txt")
    git("-c", "user.name=synthetic", "-c", "user.email=synthetic@example.invalid", "-c", "commit.gpgSign=false", "commit", "-qm", "synthetic")
    (checkout / "gha-creds-synthetic.json").write_text('{"synthetic":true}\n')
    bin_dir = root / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(f"#!{sys.executable}\n" + '''import sys, tarfile
if sys.argv[-1] != "-":
    raise SystemExit("workspace build context can include generated credentials")
with tarfile.open(fileobj=sys.stdin.buffer, mode="r|*") as archive:
    names = {member.name for member in archive}
if names != {"Dockerfile", "tracked.txt"}:
    raise SystemExit("build context must contain only the approved tracked source")
''')
    docker.chmod(0o700)
    env.update(PATH=f"{bin_dir}:{os.environ['PATH']}", image="synthetic:test")
    result = subprocess.run(["bash", "-c", "set -euo pipefail\n" + build_command], cwd=checkout, env=env, capture_output=True)
    if result.returncode:
        raise SystemExit("FAIL: generated authentication file is not excluded by the build boundary")
print("PASS: tracked-source container build excludes generated authentication files")
PY
