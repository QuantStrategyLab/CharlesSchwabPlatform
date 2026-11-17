English | [简体中文](notify-route-identity.zh-CN.md)

# Serving Telegram Read-Only Route Identity Diagnostic

Entrypoint: `notify-route-check.yml` -> `notify-route-check-protected.yml` -> `scripts/notify_route_check.py`. Accepts manual `workflow_dispatch` on `main` only; never modifies production settings, secrets, IAM, Cloud Run, notification routes, trading, or budgets.

## Read scope and provable claims

- Reads existing service configuration; passes private lists, target JSON, region, and GCP settings as same-value reusable secrets registered as masks before runner headers. Creates no production secrets and never prints `add-mask` commands or raw values. Permissions remain `contents: read` and `id-token: write`, reusing existing WIF / SA.
- Describes each Cloud Run revision carrying **positive traffic** in status for actual containers and environment variables; never treats `latestReady` or the service template as serving. Requires valid traffic rows summing to 100% and never guesses missing revisions. Multi-revision traffic splits still read each revision, while overall and per-row bot comparisons remain `unknown` / `null`; duplicate or incomplete traffic also stays `unknown`.
- Resolves `TELEGRAM_TOKEN` from a single container only. Plain env values remain in memory for `getMe`; duplicate, missing, or malformed references are never guessed. This selector never reads `TG_TOKEN` and never falls back to a secondary token when the primary token fails.
- `secretKeyRef` versions must be exact positive integers. Reads the bound project, secret, and fixed version; supports full resource paths and explicit `run.googleapis.com/secrets` alias-to-resource mappings. Malformed or duplicate aliases never fall back to a same-named secret in the same project. **`latest` and other version aliases cannot prove which immutable token the running process loaded, so they stay `unknown` without changing bindings.**
- Compares against the `getMe` bot identity of the latest version of **this project's** existing `quant-sentinel-telegram-bot-token`. This is not a centrally approved canonical reference, and identical secret names do not prove cross-project equivalence. Both `getMe` calls are GET-only and require `ok=true`, `is_bot=true`, and a positive integer `id`; missing data, read failures, or bad responses return `null` / `unknown` instead of `false`. Outputs `match` / `true` or `mismatch` / `false` only when both identities are known and sole-serving is unambiguous.
- All `gcloud` calls capture stdout/stderr with a 30-second timeout; `getMe` uses a 15-second timeout and a 64 KiB cap. Performs no updates, `sendMessage`, broker calls, live operations, or trading calls. Service/reference values and tokens remain in private memory.

This is a point-in-time comparison of Telegram **configuration bindings**; diagnostic `source_sha` cannot prove the runtime image, installed code, active channel, or actual delivery. If traffic is changing, re-observe a steady state rather than treating one snapshot as atomic release proof.

## Public output

Stdout and the GitHub summary emit `qsl.notify_route_identity.v1`: fixed status/reason enums, nullable booleans, `service#N` / `revision#N` ordinals, and a validated 40-char lowercase hex diagnostic `source_sha`. Never prints tokens, chat IDs, hashes, last4 digits, project/service/revision/SA/secret references or versions, raw bot IDs/names, free-form error text, or URLs. No public private-file artifact is uploaded.

`status=ok` / exit 0 means only that the diagnostic produced a result, **not that the route is ready**. Inspect per-service `bot_status=match|mismatch|unknown`, `same_bot_as_contract=true|false|null`, and fixed reasons. Scope remains `project_contract_only` with `canonical_route_status=unknown`.

`traffic_issue` / `traffic_detail` report fixed structural reasons. Per [Cloud Run REST v1 TrafficTarget](https://cloud.google.com/run/docs/reference/rest/v1/TrafficTarget) ("defaults to zero if unspecified"), rows completely omitting the `percent` key retain zero-skip and record `percent_default_zero` (explicit `null`, boolean, string, float, or out-of-range values still reject); defaulting missing `percent` to zero never proves positive traffic, approved identity, or readiness, and is not a general parser relaxation.

## Unverified items

`same_chat_as_canonical` and `same_topic_as_canonical` stay `null` and their statuses stay `unknown`: no approved central private canonical reference exists, and this diagnostic creates no new secrets, global routing rules, or topic values and reads no additional chat secrets. Existing runtime chat resolution prefers `QSL_GLOBAL` over `GLOBAL`; heartbeat/guard iterate across `GLOBAL`; central daily digest prefers `QSL_GLOBAL`/`GLOBAL`/plugin and takes the first ID. Each selector needs three-state evidence at a protected comparison boundary rather than rewriting selectors to make them match.

This diagnostic does not verify GHA heartbeat/guard injection, scanner/plugin/drill/VPS installations, or central Environments; inspected senders in this repository do not pass `message_thread_id`, which does not prove natural messages enter the same topic. Same bot does not imply same chat/topic, shared dedup, or exactly-once delivery. Existing alert/order reachability and quiet settings stay intact without switching dedup mode.

Central production `reader`/`sections_v2` adoption, approved producer/runtime/source identity, natural-cycle safe receipts, and final Telegram screenshots remain separate acceptance items that read-only success, synthetic fixtures, or old screenshots cannot substitute.

## Local verification

```bash
uv sync --frozen --extra test
PYTHONPATH=. PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 uv run --no-sync python -m pytest -q tests/test_notify_route_check.py
uv run --no-sync ruff check --exclude external .
PATH=/path/to/shellcheck:$PATH actionlint
```

Regression tests use fake `gcloud`/`urllib` and fixed synthetic routes; also run the full `CONTRIBUTING.md` suite and lock/pin/startup checks. After source verification, an operator runs the controlled read-only diagnostic on `main` and records results per read/`getMe` state without turning missing facts into matches.
