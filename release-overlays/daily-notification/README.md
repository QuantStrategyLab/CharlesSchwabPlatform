# Daily notification runtime overlay

This patch backports the daily-notification behavior from #623 and #624 onto an
existing runtime source baseline with the notification boundary facade. It keeps
the baseline strategy and trading dependencies and must be applied only after
verifying every affected source file against the selected immutable image.
Apply it with `git apply --check --unidiff-zero` followed by
`git apply --unidiff-zero` only after that verification. The patch alone does
not authorize deployment or change runtime settings.

Run the notification contract, receipt, storage and routing tests against the
exact baseline dependencies, including `test_boundary_fallback.py`. Verify the
resulting image layers and runtime configuration before any rollout.
