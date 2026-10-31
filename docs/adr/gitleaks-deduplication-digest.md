# ADR: historical review digest false positive

Status: Proposed.

## Context

Gitleaks 8.30.1 reports `generic-api-key` for
`fixed_evidence.review_duplicate_key` at
`docs/d1_integration_manifest.json:57`. The finding was introduced by
`6169934e220f335f8aadae6a435f34a5242d4092`. The field and its containing file
remain byte-identical to that original addition. This occurrence predates the
strategy-reliability change range and is separate from the snapshot-field
finding.

The manifest records an offline synthetic, no-order integration. That label
alone is insufficient to classify a credential-like value. Classification here
depends on the exact derivation and the producer and consumer implementations:

- The [pinned producer](https://github.com/QuantStrategyLab/QuantAIService/blob/817ea07628d189df93bba5b1292b7b49d118cedc/service/development_research_review.py)
  constructs `duplicate_key` using unkeyed SHA-256 over canonical JSON of the
  review's economic identity. Its packaging entry is a symlink to this source.
- The [pinned consumer](https://github.com/QuantStrategyLab/QuantRuntimeSettings/blob/bf3672fe584e3984c7f473989b95bd4cbf92c05e/python/scripts/development_research_review_consumer.py)
  independently recomputes the same digest before using it to index advisory
  review receipts. A conflicting result for an existing identity is rejected.
- The bound fields are schema, message version, research and producer
  identities, the normalized input-index digest, policy identities and cost
  identity. No credential is an input to this derivation. The source files'
  SHA-256 values match the manifest's producer and consumer bindings.
- Recomputing the canonical preimage from those fixed bindings exactly matches
  the documented value. This uses the input-index digest anchor already fixed
  by the validators; it does not reconstruct or authenticate the raw research
  inputs.

The field is a content-derived deduplication digest, not an authentication
secret. It does not grant adoption, deployment or trading authority. The
matched value and private evidence identifier are not reproduced in this ADR.

## Decision

Add one exact historical finding fingerprint to `.gitleaksignore`: the complete
introducing commit, repository-relative path, native rule and original line.
Preserve the existing snapshot-field exception byte-for-byte. Do not rename the
manifest field, change its value or alter the producer, consumer or runtime.

There is no path, rule, commit-wide or regular-expression exemption. A later
occurrence has a different commit fingerprint and remains detectable. Unknown
values do not inherit this classification merely because they resemble a hash.

## Validation and limits

Native Gitleaks reproduces the finding against the immutable manifest bytes.
Isolated Git-history controls verify that an exact occurrence exception leaves
later occurrences and unrelated generated test values detectable. Incorrect
commit, path, rule and line components do not suppress the finding. The
production fingerprint does not suppress a synthetic occurrence.

The existing native scanner, its version and rules, exit handling and workflow
are unchanged. The complete historical strategy-reliability scan has not been
rerun with this proposal. A green child-PR scan covers that child's added range;
it does not prove that a different historical range is clear. A directory scan
has no commit identity and is not covered by this historical exception.

The reviewed source semantics and digest equality establish the classification
of this occurrence. They do not authenticate an operational evidence pack,
approve a research result or authorize runtime adoption.

## Rollback

Remove only this occurrence's fingerprint. The earlier snapshot-field
disposition and all native checks remain in place; the historical occurrence
becomes reportable again.
