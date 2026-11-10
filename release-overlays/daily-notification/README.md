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

## Completed candidate projection

`projector.patch` aligns completed report projection and candidate storage
with the daily publication Job. It applies to the materialized runtime
after the notification overlay. Read the affected files from the full
serving image, including its upper layers, and run `git apply --check`
against that baseline before applying it. Build from the existing image
by copying only the five changed files. Retain its dependencies, runtime
entry point and trading code.

Validate the candidate writer and the Job reader against the same synthetic
archive in private isolated storage. The Job must read the existing candidate
at the same generation, and a contradictory candidate must retain its
bytes and generation. Keep the previous runtime image for rollback.
