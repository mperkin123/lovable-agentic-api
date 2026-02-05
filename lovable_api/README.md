# Lovable Agentic API (POC)

FastAPI backend that ingests a seed CSV, runs enrichment/scoring, emits live events, and generates tasks.

## Run

```bash
cd /home/curl/.openclaw/workspace
python3 -m venv .venv && source .venv/bin/activate
pip install -r lovable_api/requirements.txt
uvicorn lovable_api.main:app --reload --port 8000
```

## Env

Create `.env` in workspace root:

```
API_TOKEN=dev-token
GOOGLE_MAPS_API_KEY=...
```

## Endpoints
- POST /v1/campaign-runs
- POST /v1/campaign-runs/{id}/import/seed-csv
- POST /v1/campaign-runs/{id}/run
- GET  /v1/campaign-runs/{id}
- GET  /v1/campaign-runs/{id}/events
- GET  /v1/campaign-runs/{id}/events/stream  (SSE)
- GET  /v1/campaign-runs/{id}/leads
- GET  /v1/leads/{leadId}
- GET  /v1/tasks
- PATCH /v1/tasks/{taskId}
- GET  /v1/metrics/overview
