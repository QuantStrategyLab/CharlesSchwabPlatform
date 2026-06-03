# CharlesSchwabPlatform

[Chinese README](README.zh-CN.md)

> ⚠️ Investing involves risk. This project does not provide investment advice and is for educational and research purposes only.

## What this project does

CharlesSchwabPlatform is an **Execution platform** in the QuantStrategyLab ecosystem. It runs US equity strategy execution through Charles Schwab integrations, including Cloud Run deployment, token refresh integration, and external strategy loading.

## Who this is for

- Engineers and researchers who want to inspect, reproduce, or extend this part of the QuantStrategyLab stack.
- Operators who need a clear entry point before reading the deeper runbooks or workflow files.
- Reviewers who need to understand the repository purpose, safety boundary, and evidence requirements before enabling automation.

## Current status

Production-oriented platform code; run read-only/dry-run checks before enabling order submission.

## Repository layout

- `application/`, `entrypoints/`, `notifications/`, `strategy/`: Python package code.
- `tests/`: unit and contract tests.
- `.github/workflows/`: CI, scheduled jobs, and deployment workflows.
- `scripts/`: operator scripts and local helpers.

## Quick start

From a fresh clone:

```bash
python -m pip install -r requirements.txt
python -m pytest -q
```

If a command requires credentials, run it only after reading the relevant workflow or runbook and configuring secrets outside Git.

## Deployment and operation

Configure Schwab credentials, token storage, runtime settings, and strategy package. Validate token refresh and dry-run execution before enabling scheduled production workflows.

Prefer manual or dry-run execution first. Enable schedules or live execution only after logs, artifacts, permissions, and rollback steps are reviewed.

## Strategy performance and evidence

The platform does not evaluate alpha. Use UsEquityStrategies and UsEquitySnapshotPipelines for return, drawdown, benchmark, and live-enable evidence.

README files are intentionally not a source of dated performance promises. Re-run the relevant tests, backtests, or pipeline jobs before relying on any result.

## Safety notes

- Never commit API keys, broker credentials, OAuth tokens, cookies, or account identifiers.
- Run new strategies and platform changes in dry-run or paper mode before any live execution.
- Review generated orders, artifacts, and logs manually before enabling schedules.

## Contributing

Keep changes small, reproducible, and covered by the narrowest useful tests. For strategy-facing changes, include the evidence artifact or command used to validate behavior.

## License

See [LICENSE](LICENSE) if present in this repository.
