# CharlesSchwabPlatform

Charles Schwab execution platform for us_equity and quant_combo strategies. Cloud Run deployment with token refresh integration.

## Key Files

- `main.py` — Flask app with /run, /dry-run, /probe, /health, /monitor-dispatch
- `strategy_registry.py` — Imports US + Combo catalogs, capability matrix, enabled profiles
- `runtime_config_support.py` — PlatformRuntimeSettings, env var loading
- `runtime_execution_policy.py` — DCA/fractional share execution policy
- `.env.example` — All required environment variables

## Deployment

- `gcloud run deploy charles-schwab-quant-service --source . --region=us-central1 --project=charlesschwabquant --clear-base-image`
- Scheduler: `charles-schwab-quant-service-scheduler` (45 15 * * *, America/New_York)
- Secrets: charles-schwab-api-key, charles-schwab-app-secret, charles-schwab-telegram-token

## Config

- Single source: `RUNTIME_TARGET_JSON` (strategy_profile, platform_id, execution_mode)
- Auto-sync: plugin mounts + monitor targets auto-align on startup
- Deprecated: STRATEGY_PROFILE standalone env var (removed)
