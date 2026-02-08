# Testing (btc-collector)

## Setup

```bash
cd CODE/btc-collector
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install pytest
```

## Run Tests

`app.py` starts background threads on import for production. Tests must disable this behavior:

```bash
cd CODE/btc-collector
DISABLE_BACKGROUND_THREADS=1 .venv/bin/python -m pytest -q
```

## What We Test Today

- `/health` status and staleness semantics:
  - `ok` when threads are alive and trade polling is fresh
  - `stale` when trade polling is stale
  - `stale` when candle data is stale
  - `degraded` when collector thread is down

## Suggested Next Tests (Phase 2)

- Gap-event cooldown behavior (no Discord spam)
- “HL API down” scenario drives `/health` stale and emits a stale alert
- DB disconnect drives `/health` degraded
- `/trades` and `/candles` endpoints return newest-first and respect `since_ms`

