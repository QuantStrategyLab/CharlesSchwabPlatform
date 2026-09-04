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

grep -Fq 'traffic changed during no-traffic deployment' "$readback"
grep -Fq 'scheduler changed during no-traffic deployment' "$readback"
grep -Fq 'iam changed during no-traffic deployment' "$readback"
grep -Fq 'configuration changed during no-traffic deployment' "$readback"
grep -Fq 'Compare only effective traffic' "$readback"
grep -Fq 'if percent > 0:' "$readback"
if grep -Fq 'secrets versions access' "$readback" || grep -Fq 'containers.env.value,' "$readback"; then
  echo "readback must not access Secret Manager values or plaintext environment values" >&2
  exit 1
fi
