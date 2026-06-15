# stac_test_otel

End-to-end availability monitoring for the EODC STAC service, with automatic remediation via GitLab CI.

## Workflows

| Workflow | Environment | STAC URL | Schedule |
|---|---|---|---|
| `e2e_monitor_prod` | prod | stac.eodc.eu | every 30 min |
| `e2e_monitor_dev` | dev | dev.stac.eodc.eu | every 30 min |
| `func_test` | — | — | on push |
| `perf_test` | — | — | on push |

## What is monitored

Each availability run checks:
- Root endpoint (`/`)
- Collections list (`/collections`)
- Collection detail per collection
- Items list per collection
- Search (POST `/search`)
- Asset reachability
- Browser frontend (services.eodc.eu/browser)

Results are pushed to Grafana via OTel (`eodc_e2e_probe_success`, `eodc_e2e_probe_duration_seconds`, ...) with labels `env`, `service`, `probe`, `collection`.

## Auto-Remediation

When an availability check fails, the monitoring workflow automatically triggers the remediation pipeline on GitLab (`git.eodc.eu/eodc/metadata/stac/stac-platform-deployment`) — but only once per outage, not on every failing run.

**How it works:**

1. On the first failure (state change: success → failure), `scripts/trigger_gitlab_pipeline.py` is called
2. The script finds the latest pipeline on `main` and plays the appropriate manual job (`ansible-prod` or `ansible-dev`)
3. Subsequent failing runs are skipped — the trigger fires again only after a recovery followed by a new failure

**State tracking:** The last test outcome is persisted between GitHub Actions runs via `actions/cache`. Once triggered, all further failing runs see `last=failure` and skip.

**Required GitHub secrets:**

| Secret | Description |
|---|---|
| `OTEL_API_KEY` | OTel ingest key |
| `GITLAB_URL` | `https://git.eodc.eu` |
| `GITLAB_PROJECT_ID` | Numeric project ID of `stac-platform-deployment` |
| `GITLAB_ACCESS_TOKEN` | Project access token (role: Developer, scope: `api`) |
| `GITLAB_REF` | Branch to look for pipelines on (e.g. `main`) |

**Manual test:**

```bash
export GITLAB_URL=https://git.eodc.eu
export GITLAB_PROJECT_ID=<id>
export GITLAB_ACCESS_TOKEN=<token>
export GITLAB_REF=main
export GITLAB_JOB_NAME=ansible-prod   # or ansible-dev
python scripts/trigger_gitlab_pipeline.py
```

## GitLab pipeline

The `stac-platform-deployment` pipeline has two manual jobs:
- `ansible-prod` — runs `make -f Makefile.prod all-no-packer` and restarts Docker containers
- `ansible-dev` — runs `make -f Makefile.dev all-no-packer` and restarts Docker containers

These can also be triggered manually via the GitLab Pipelines page.
