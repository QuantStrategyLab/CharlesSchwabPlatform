# Shared dependency test imports

The ordinary pytest command keeps its existing import order and test doubles.
It does not certify shared dependency provenance. Select the separate strict lane
explicitly after installing the unchanged `uv.lock` with `uv sync --frozen --extra test`.
Dependency installation is preparation; run tests without network or credential access.

CI first runs the existing frozen-artifact checker in its separate non-editable
environment. Its metadata, file hashes, interfaces and resources remain unchanged.
The editable-lane command remains `uv pip install --no-deps -e external/QuantPlatformKit
-e external/UsEquityStrategies`. With the repository's QPK override, QPK remains the
locked installed distribution; UES resolves to its locked editable checkout. The
strict lane declares and verifies those two origins explicitly. It fails if tool
behavior changes that mapping. It does not silently infer a mode from local paths.

Literal strict/full-suite CI values are `PYTEST_DISABLE_PLUGIN_AUTOLOAD: "1"` and
`PYTHONPATH: .`. Existing full-suite and coverage settings retain those values.
Checkouts are provisioned from the lock, never upstream main. For local preparation,
use the existing CI checkout/install steps with the exact lock revisions.

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=. \
  uv run --no-sync python -m pytest -q -p test_support.pytest_provenance \
  --shared-import-mode pinned-checkout \
  --qpk-source installed-lock --ues-source pinned-checkout \
  --ues-checkout external/UsEquityStrategies tests/test_shared_import_contract.py
```

The audited selection is exactly the three tests in
`tests/test_shared_import_contract.py`: safe pinned URI/profile parsers and one
explicit, restored test double. Checks run in that pytest process at session start,
after collection, before and after setup/call/teardown, and at successful completion.
They include preloaded `sys.modules` entries and all shared modules present at each
boundary, including parser implementation modules. No shared namespace is exempted.
A scoped double replaces one previously verified module, is counted separately,
cannot count toward real-source completion, and must restore the original identity.
An undeclared or leaked double fails the strict lane. The lane never imports broker
or cloud entry points, fetches remote artifacts or reads real credentials.

Completion requires the exact full collection, all three calls passed, no failed or
skipped phases, final restored state and the complete required real-module inventory.
Collection-only, deselection, interrupted runs and failed checks cannot emit
`SHARED_IMPORT_PROVENANCE_COMPLETE`. Receipts contain public source revisions,
module names and declared origin classes, without absolute paths or environment dumps.
They are engineering observations, not authenticated review or deployment attestations.

Pinned-checkout validation requires an explicit root, exact locked Git HEAD, a clean
worktree, tracked source bytes equal to the Git object, and origins inside that root.
An unexpected same-named root, dirty content, ignored/untracked source, unknown origin,
file/spec disagreement or symlink escape fails. Installed-lock mode rejects editable
sources even if revision metadata appears correct.

Run synthetic negative controls with
`uv run --no-sync python -m pytest -q tests/test_import_provenance.py`.
They use disposable modules/metadata/Git repositories, never modify real dependencies,
and observe a child process successfully importing a sibling before the guard rejects
it with a nonzero exit. Repeat/reorder the strict selection to check cleanup.

This is a bounded parser lane. It does not certify the ordinary whole suite or
transient modules loaded and removed between boundaries by arbitrary third-party hooks.
Existing test-specific path edits and mocks remain in place, so legacy whole-suite
source ambiguity remains characterized rather than eliminated. The test support is
excluded from the setuptools production package selection; pins, production source,
existing scanner/completion checks and coverage configuration are unchanged.
