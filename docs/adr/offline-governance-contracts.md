# ADR: offline governance contract checks

Status: Proposed.

## Context

The offline Gitleaks and AI-review inspectors validate supplied declarations.
They deliberately return only `BLOCKED` or `FAIL`: a well-formed local file
cannot establish that a scanner ran over complete inputs, that required checks
were observed, or that an independent reviewer examined the exact candidate.
Matching revision strings constrain a claim without authenticating it.

The existing native Gitleaks job already performs the actual scan and preserves
its process result. Running an inspector with no acceptance path in that job
would make every otherwise clean scan fail for an unrelated missing provenance
provider. Treating its blocked exit as successful acceptance would be equally
incorrect.

## Decision

Add a separate `Offline governance contracts` CI job. It runs the actual
standard-library regression suites for the inspectors and their aggregate
adapter, using synthetic bounded inputs. The job tests parser limits, exact
revision binding, privacy boundaries and preservation of unknown and failure
states. Its name and documentation describe engineering contract verification.

Keep the native Gitleaks and commitlint jobs byte-for-byte unchanged. Their
scanner pin, historical range, exit handling and checks remain intact. Retain
the workflow triggers, token permissions and trusted risk gate. The new job
does not load code into `pull_request_target`, publish statuses through an API,
change labels, retrieve credentials or perform runtime operations.

The aggregate adapter retains the existing non-acceptance contract. Consistent
declarations remain `BLOCKED`; contradictions and observed errors remain `FAIL`.
All execution, production, merge and owner-approval flags remain false. Tests
assert those values rather than translating the adapter's exit code into an
acceptance result. A green contract-test job does not authenticate any real
evidence document or satisfy an owner approval requirement.

## Remaining provider and input gap

Authenticated evidence acceptance is a separate, incomplete integration. It
requires a trusted provider or attestation path for the complete current and
previous file inventory, effective required-check policy and observed check
results, and independently established reviewer provenance and review scope.
Each observation must bind to the repository and exact base and head revisions;
stale, missing, pending or skipped evidence cannot be substituted with success.

An agent name, shared GitHub login, PR checkbox or caller-written JSON field does
not establish independence. This proposal does not create a provider, invent
review evidence or change permissions to obtain one. Until those inputs and the
acceptance implementation are available, the standalone inspectors and adapter
remain blocked or failed. Contract-test wiring is complete only for engineering
correctness; authenticated acceptance wiring is not claimed.

## Native scan evidence in the job log

The native Gitleaks step runs through `scripts/retain_native_scan.py`. It adds the
pinned CLI's JSON report options, writes the report, the scanner's original exit
code and an unsigned receipt under the runner's temporary directory, and uploads
nothing. These repositories are public, so a retained report could expose paths or
commit metadata. The job summary instead records counts only: native exit code,
number of findings and capture status, with no paths, rule names, line numbers or
commit metadata. Scan revisions come only from the pull request or push event; no
fixed commit range is embedded in the workflow.

Before scanning, the helper requires both revisions to be full nonzero commit
identifiers that resolve in the checkout, because the pinned scanner can return zero
with an empty report after a Git range error. Each collection uses a new directory.
Native nonzero exits keep their original code even when collection fails, a zero
exit with an absent, malformed or oversized report fails collection, and a summary
write failure never changes the result. Collector success is not authenticated
scanner or review acceptance. The versioned
[review-v1](../evidence/offline-governance-review-v1.json) document and its
[provenance record](../evidence/offline-governance-review-provenance.md) stay
source-only and blocked; the required-check policy remains unavailable.

## Alternatives and consequences

Putting the non-accepting adapter after every native scan would permanently fail
the existing scanner job. Ignoring its blocked result and presenting an evidence
acceptance success would hide missing inputs. Both designs are rejected. Keeping
the native scan and contract checks as separate jobs preserves their distinct
claims and provides a useful regression gate without relaxing either boundary.

Workflow changes still encounter the existing high-risk path gate. The review
bot acts as code reviewer under the repository owner's authorization and may add
`owner-approved` only on that explicit or standing authorization, recording each
addition in the ledger. Without that authorization, a gated PR stays a draft and
its advancement stops.

## Rollback

Restore the original native scan command and remove the helper and its tests. To
revert the contract-wiring job, remove it and the bounded adapter changes. The
commit-message and risk gates continue unchanged. No dependency, trading,
account, risk-control, production configuration or deployment rollback is needed.
