# Historical offline-governance review provenance

## Purpose and authority

The adjacent [review-v1 document](offline-governance-review-v1.json) transcribes
the existing independent source-review record for
[CharlesSchwabPlatform #647](https://github.com/QuantStrategyLab/CharlesSchwabPlatform/pull/647). It is an unsigned
declaration for the historical revision range below. It is not a review of the
child PR that adds this file and does not assert authenticated acceptance.

The review record reports a completed separate-agent source review with no
findings. Its test receipt records 81 standard-library test methods passing in
three suites (26, 33 and 22), with no failures or skips. These are historical
observations from that review, not newly executed tests or a claim about current
required checks. The previous review described publication as a draft and left
remote CI and owner-only gates pending at that time.

## Exact reviewed source

| Field | Observation |
| --- | --- |
| Repository | `QuantStrategyLab/CharlesSchwabPlatform` |
| Base | `842acc62956b745df1725374913915dce25875a5` |
| Head | `3a4875b7494e687c8f9c703f45b4f4900be443d2` |
| Head tree | `63fee0629a380967ff7af20dd2d5649feca4c068` |
| Changed files | 6; the complete PR API inventory for this range |
| Scope digest | `427f7dafd4d4c00bed8f63232ad56d479871941027ab214342076a391a60ae45` |
| Original review receipt SHA-256 | `39089b54c9885379a4c6a03380b00609e89f2d1518d3ec61f805501fbb2c315c` |
| Original test receipt SHA-256 | `ab9b7dd1362e488c22cd73c15ef5e7821f73245a9a65383e1389c2b2bd66b68e` |

All six changed file blobs match the exact GitHub head tree, complete PR file
inventory and original review record. The head has the recorded base as its
sole parent. The review and test receipt hashes identify the retained process
records; hashes alone do not establish an issuer, signature or independent
execution. Those process records are not a trusted provider attestation.

| Reviewed path | Git blob |
| --- | --- |
| `.github/engineering.md` | `4b8721998fac828e5733ae4992ea7a1a2eaa1540` |
| `.github/workflows/engineering-governance.yml` | `c3ef4e6e37ae66c4c983af906ea404e5b11432bb` |
| `docs/adr/offline-governance-contracts.md` | `a3c2dabdbd861f26960d68c3f260352c1d5558dc` |
| `scripts/ai_review_evidence.py` | `1dc13b74455b40e4d8e1ce8aca481950d3dfa87c` |
| `scripts/governance_evidence_gate.py` | `21ddd09ac08eb05c20dc115adee6b059d85d0369` |
| `tests/test_governance_evidence_gate.py` | `526f660b21cfb03bfdfd5ccc23326b42ab8e75fb` |

The scope digest follows the unchanged v1 inspector: SHA-256 of compact,
sorted-key JSON containing `base_sha`, `head_sha`, `changed_files`,
`inventory_complete` and the file inventory sorted by path. It binds the
declaration to its stated scope. It does not authenticate inventory coverage.

## Identity representation

| JSON identifier | Observed source | Limit |
| --- | --- | --- |
| `github:20649888` | GitHub reports PR publisher `Pigbibi`, account ID `20649888`. | This is the shared publishing account, not verified implementation authorship or a distinct human identity. |
| `agent:root.phase1f_longbridge_review` | The original receipt names `/root/phase1f_longbridge_review`. | This explicit token alias represents the recorded local agent name; it is not a provider principal, signature or authentication result. |

The v1 token grammar excludes `/`. The reviewer token is an explicitly documented
alias, not a silently substituted or newly authenticated identity. Different
tokens satisfy only the format comparison in the inspector. They do not prove
that the reviewer and author were distinct execution principals. The original
record describes a separate review process, but the current implementation has
no trusted mechanism to verify its assignment, execution or author relationship.

## Required-check capability limit

`required_checks` and `checks` are intentionally empty. The effective required
policy for `codex/offline-governance-inspectors-20261031` was not observed. A previous read of the
canonical `/repos/QuantStrategyLab/CharlesSchwabPlatform/rules/branches/codex/offline-governance-inspectors-20261031`
endpoint was rejected by the GitHub connector as an unsupported allowed endpoint
(`INVALID_ARGUMENT`). This was a connector capability refusal before policy data
was returned, not a GitHub permission finding or evidence of an empty policy.
No alternate endpoint, identity, browser or CLI was used to bypass the refusal.

The empty required list means that required-check evidence is missing; it does
not mean no checks are required. Observed successful jobs have not been promoted
to the required set. Check display names containing spaces have not been silently
rewritten into v1 token names. An authenticated provider must supply the effective
policy and a stable, unambiguous mapping to exact-head check observations.

## Expected inspector result

Run the unchanged inspector from the repository root:

```bash
python scripts/ai_review_evidence.py docs/evidence/offline-governance-review-v1.json
```

The expected result is `BLOCKED`, exit `2`, with structural status `CONSISTENT`,
authentication and required-check coverage `UNVERIFIED`, risk tier `high`, and
operational authority `NONE`. The remaining reason codes are:

- `required_checks_missing`
- `required_check_policy_unverified`
- `trusted_provenance_unavailable`

The document resolves the missing review-v1 representation. It does not resolve
trusted provenance or required-policy evidence. No validator, acceptance rule,
review finding, branch policy or owner-only label is changed by this document.
A future provider/verifier implementation remains separate work.
