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

The initial contract-wiring change kept the native Gitleaks and commitlint jobs
byte-for-byte unchanged. The proposed retention addition below preserves the
scanner pin, event range, native failure handling and commit-message checks.
Retain the workflow triggers, token permissions and trusted risk gate. The new job
does not load code into `pull_request_target`, publish statuses through an API,
change labels, retrieve credentials or perform runtime operations.

The aggregate adapter retains the existing non-acceptance contract. Consistent
declarations remain `BLOCKED`; contradictions and observed errors remain `FAIL`.
All execution, production, merge and owner-approval flags remain false. Tests
assert those values rather than translating the adapter's exit code into an
acceptance result. A green contract-test job does not authenticate any real
evidence document or satisfy an owner-only approval requirement.

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

## Proposed native evidence retention

The repository can produce the following inputs without changing the inspectors
or granting acceptance:

| Input | Repository implementation | Remaining boundary |
| --- | --- | --- |
| Native scan JSON | Add the pinned CLI's JSON report options to the existing redacted scan. Retain the actual report bytes in an Actions artifact. | A stored report is not an authenticated coverage claim. No empty report is synthesized. |
| Original process exit | Record `subprocess`'s actual return code in `scanner-exit.txt`, separately from collection status. | If the executable cannot start, no process exit file is created. A missing binary still fails with 127. |
| Invocation and receipt | Record the declared event range, exact arguments, configured version, actual executable digest, run identifiers and report digest. | The configured version is a workflow pin, not a second version query. These unsigned observations do not attest complete inputs or scanner configuration. |
| Historical review-v1 | Version the original source review's six-file inventory, exact revisions, completed review and empty findings list, with an explicit identity mapping. | The publishing account does not establish implementation authorship. Agent identity and independence remain unauthenticated. |
| Effective required-check policy | Record the existing connector endpoint refusal in the review provenance document. | The allowed connector endpoint set cannot supply this policy. Observed successful jobs cannot be substituted for the unknown required set. |

The event-range capture and retrospective review-range capture have separate
directories and exact base/head records. The retrospective step runs only after
the event scan and collection succeed. It scans the immutable range recorded in
the historical review-v1 document using the same verified binary, redaction and
default configuration lookup. It does not recover the original run's output and
does not scan the wider strategy PR's history or resolve its findings.

Before invoking the scanner, the collector requires both declared revisions to
be full nonzero commit identifiers that resolve to commit objects in the local
checkout. It records that outcome and stops without a fabricated scanner exit
when either object is missing. It does not require an ancestry relationship;
divergent pull request ranges remain valid. This preflight is necessary because
the pinned native scanner can return zero and write an empty report after a Git
range error. A zero native exit alone is insufficient evidence of a valid range.

Each collection uses a new directory. The helper refuses an existing directory
so an earlier report cannot be mistaken for a current scan. Native nonzero exits
remain nonzero with their original code, even when report collection fails. A
native zero exit with an absent, malformed, nonregular or oversized report, or a
failed receipt write, fails collection. Raw report bytes remain unchanged. A
native signal retains its negative process code and uses the normal shell
`128 + signal` exit. The helper never treats a collector success as authenticated
scanner or review acceptance.

The uploader runs even after failure and names only the two captures' four
files and the two historical review documents. It fails if none exist; failed
scan or collection steps remain failed. Partial failure artifacts can therefore
be retained without becoming a successful run. The artifact expires after seven
days. Scanner redaction is retained, but paths, commit metadata and author
metadata may still identify source material: the artifact is not anonymized.
Do not copy native reports or real credential findings into Git or public logs.
Only source-review observations are versioned in
[review-v1](../evidence/offline-governance-review-v1.json), with their
[provenance and capability limits](../evidence/offline-governance-review-provenance.md).

The historical review's `required_checks` and `checks` arrays intentionally
remain empty. That avoids inventing a policy or silently changing provider check
names to fit the version-1 token format. The unchanged inspector returns
`CONSISTENT`, `BLOCKED`, exit `2`, with authentication and required-check coverage
`UNVERIFIED`. The unchanged aggregate still has no authenticated acceptance path.
A trusted provider/verifier remains a separate implementation requirement.

## Alternatives and consequences

Putting the non-accepting adapter after every native scan would permanently fail
the existing scanner job. Ignoring its blocked result and presenting an evidence
acceptance success would hide missing inputs. Both designs are rejected. Keeping
the native scan and contract checks as separate jobs preserves their distinct
claims and provides a useful regression gate without relaxing either boundary.

Workflow changes still encounter the existing high-risk path gate. Only the
repository owner may apply `owner-approved`; an AI agent must keep a gated PR
as a draft and stop its advancement. No human code-review requirement is added.

## Rollback

For the retention addition, restore the original native scan command and remove
its retrospective capture, artifact upload, helper, retention tests and historical
review records. The original contract-test job and inspectors remain intact.
Reverting the earlier contract-wiring proposal is separate: remove that job and
the bounded adapter changes. No production dependency, trading, account,
risk-control, production configuration or deployment rollback is needed.
