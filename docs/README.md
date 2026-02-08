# btc-collector Docs

`btc-collector` is a small Flask service that continuously polls public Hyperliquid `/info` endpoints for BTC market data, persists it to Render Postgres, and exposes simple read-only HTTP endpoints for downstream consumers (papertrade/backtests, dashboards, etc).

## Quick Links

- Code: `CODE/btc-collector/app.py`
- Tests: `CODE/btc-collector/tests/`
- Hardening log (why changes were made): `CODE/btc-collector/docs/phase1-hardening-log.md`
- Ops runbook: `CODE/btc-collector/docs/operations-runbook.md`
- Cost model: `CODE/btc-collector/docs/costs.md`

## Environment Variables

- `DATABASE_URL`: Postgres connection string. When set, Postgres is the primary store.
  - If unset/empty, the service falls back to SQLite at `/tmp/collector.db` (local dev only).
- `DISCORD_WEBHOOK_URL`: Optional. Enables Discord alerts (stale, crash loop, possible gaps).
- `TRADING_ENABLED`: `1` (default) or `0`. Kill switch for downstream systems.
- `RENDER_EXTERNAL_URL`: Optional. If set, enables periodic self-ping to `/health`.
- `DISABLE_BACKGROUND_THREADS`: Set to `1` to prevent collector threads from starting on import (used by tests).

## Endpoints

- `GET /health`
  - `status`: `ok|stale|degraded`
  - `trade_poll_stale`: True when no successful `recentTrades` poll for `TRADE_STALE_SECS`
  - `trade_ok_age_s`: Age since last successful trade poll
  - `candle_data_stale`: True when candle timestamps lag real time by `CANDLE_DATA_STALE_SECS`
  - `db_connected`: Last DB write succeeded (best-effort)
  - `db_last_ok_age_s`: Age since last successful DB write
  - `loop_heartbeat_age_s`: Age since the collector loop last completed an iteration (wedge detector)
- `GET /trades?since_ms=&limit=` (limit max 10,000)
- `GET /candles?since_ms=&limit=` (limit max 5,000)
- `GET /gaps?limit=` (limit max 1,000): audit events for possible data loss / window overflow

## Important Semantics (Read This)

- **Do not assume `recentTrades.tid` is monotonic or contiguous.**
  - We cannot infer exact missing counts by comparing `min(tid)` and `max(tid)` across polls.
- Gap events are recorded as `data_gaps` rows.
  - `estimated_missing = -1` means **unknown** (detected possible `recentTrades` window overflow).
- **Run Gunicorn with a single worker** (`--workers 1`).
  - Each worker is a separate process; each would start its own collector threads and duplicate polling.

## Local Dev

Create a venv and install deps:
```bash
cd CODE/btc-collector
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Run with SQLite fallback:
```bash
cd CODE/btc-collector
unset DATABASE_URL
python3 app.py
```

## Tests

```bash
cd CODE/btc-collector
DISABLE_BACKGROUND_THREADS=1 .venv/bin/python -m pytest -q
```

