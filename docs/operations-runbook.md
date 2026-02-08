# Operations Runbook (btc-collector)

This runbook is for keeping the `btc-collector` service healthy on Render and knowing what to do when it isn’t.

## 1. Fast Triage

1. Check health:
```bash
curl -s https://<your-service>.onrender.com/health | python -m json.tool
```

2. Check recent gaps:
```bash
curl -s "https://<your-service>.onrender.com/gaps?limit=20" | python -m json.tool
```

3. Check Render logs:
- Look for: `Collector wedged`, `Collector error`, `DB connection failed`, `HL ... poll failed`.

## 2. Health Semantics

`/health.status` meanings:
- `ok`: threads alive, recent trade polling is healthy, candle timestamps are fresh, DB last write succeeded.
- `stale`: collector is running but data freshness is bad (trade polling or candle data is behind).
- `degraded`: a core component is down (collector thread dead, watchdog dead, or DB marked disconnected).

Key fields:
- `trade_poll_stale`: True when no successful `recentTrades` poll in `TRADE_STALE_SECS`.
- `trade_ok_age_s`: Seconds since last successful trade poll.
- `loop_heartbeat_age_s`: Seconds since the loop completed an iteration.
  - If this climbs above `HEARTBEAT_STALE_SECS`, the watchdog will restart the process (fail-fast).
- `candle_data_stale`: True when `last_candle_ts_ms` lags wall clock by `CANDLE_DATA_STALE_SECS`.
- `db_connected`: best-effort “last DB write succeeded”. See also `db_last_ok_age_s`.

## 3. Gap Events (`/gaps`)

`data_gaps` rows are an operational signal, not perfect accounting.
- `estimated_missing = -1` means: **unknown** (detected possible `recentTrades` window overflow).
- `severity`:
  - `warning`: overflow detected while polling at 10s.
  - `critical`: overflow detected even while polling at 5s.

What to do if gaps are frequent:
- Ensure polling is running at 5s during heavy volume.
- Treat this as “completeness is not guaranteed from `recentTrades` alone”.
- Phase 2 should reconcile/backfill from HL S3 to guarantee completeness.

## 4. Alerts (Discord)

Expected alert types:
- **Stale Collector**: no successful trade poll for > `TRADE_STALE_SECS` (rate-limited).
- **Collector Wedged**: loop heartbeat missing for > `HEARTBEAT_STALE_SECS` -> process restart.
- **Possible Data Gap (recentTrades window overflow)**: anchor overlap missing (rate-limited unless critical).
- **Crash Loop Detected**: >2 restarts in 10 minutes.

## 5. Render Configuration Notes

- Run Gunicorn with **one worker** (`--workers 1`).
  - Multiple workers create multiple collectors (one per process).
- Set Render health check path to `/health` (or add a dedicated `/healthz` if you want a slimmer response).
- Put the web service and Postgres in the same region and use the internal DB URL from Render.

## 6. Manual DB Checks (Optional)

Example SQL queries (run from a trusted client):
- Latest trade timestamp:
  - `SELECT MAX(ts_ms) FROM trades WHERE coin='BTC';`
- Row counts:
  - `SELECT COUNT(*) FROM trades WHERE coin='BTC';`
  - `SELECT COUNT(*) FROM candles_5m WHERE coin='BTC' AND interval='5m';`
- Recent gaps:
  - `SELECT * FROM data_gaps WHERE coin='BTC' ORDER BY detected_at DESC LIMIT 20;`

