# ADR: historical snapshot-field false positive

Status: Proposed.

## Context

Gitleaks 8.30.1 reports `generic-api-key` at
`application/joint_account_facts.py:75` in commit `8f0541e2322ce2b6e5df1be1716f37157b44da50`.
The immutable source blob is `76a0bd9affb7c6421d4bcf30c5366f4ec973953f`.
The statement enumerates fourteen literal snapshot-field names. It supplies
comparison keys for journal observations and contains no authentication value.
The detector spans adjacent literals on the same line.

The engineering workflow scans the added Git history with `base..head`.
Formatting a later version does not remove the finding in its introducing
commit. The source, its use and runtime behaviour remain unchanged here.

## Decision

Add only the exact native finding fingerprint to `.gitleaksignore`: the complete
introducing commit, repository-relative path, rule and original line number.
There is no rule, path, commit-wide or regular-expression exemption. Bundled
detection rules, scanner version, exit handling and the existing workflow are unchanged.
Any new occurrence has a different commit fingerprint and remains detectable.

This proposed exception applies to the identified field-name tuple only. A
separate finding requires its own source assessment and disposition. Unknown
credential semantics cannot be accepted by similarity to this case.

## Validation and limits

The pinned native binary reproduces one finding on the exact original source.
A one-field-per-line probe preserves the complete Python AST and produces no
directory-scan finding, but a native synthetic Git history still reports the
original occurrence after that formatting commit.

In the same native history probe, one exact fingerprint suppresses the reviewed
occurrence. Reintroducing the same line in a later commit still fails scanning;
a separately generated non-credential canary is also detected. Raw scanner
matches and values are not published as evidence.

Those controlled results establish the exception's matching scope. They do not
prove that the complete historical PR range has passed with this proposal.
Record the actual range and native scan result before treating that check as
resolved. A clean directory scan or this draft's narrower commit range is not
a substitute. Existing commitlint and owner-only approval gates remain separate.

## Rollback

Remove the exact fingerprint to restore the original scanner failure. There is
no runtime, dependency, account, risk-policy or production migration to reverse.
This draft authorizes no merge, deployment, live order or cloud write.
