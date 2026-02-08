# Phase 1 Hardening Log (btc-collector)

Date: 2026-02-08

This doc is a durable record of what we changed in Phase 1 hardening and why. The goal is to prevent re-learning the same lessons during Phase 2.

## 1. Removed DB URL Logging (Credential Leak)

Problem:
- Logging any portion of `DATABASE_URL` risks leaking credentials into Render logs (and any log shipping).

Fix:
- `CODE/btc-collector/app.py` no longer logs `DATABASE_URL` (even partially). It logs only that Postgres is enabled.

Why:
- Logs are not a secret store. Treat them as public.

## 2. Corrected Gap Detection: `tid` Is Not Monotonic

Problem:
- The original gap detection assumed trade IDs (`tid`) were contiguous/monotonic, e.g. `min_tid > last_max_tid + 1`.
- Hyperliquid `recentTrades` returns `tid` values that are unique identifiers but not safe to treat as a strict sequence.

Fix:
- Replaced contiguity logic with **anchor-overlap overflow detection**:
  - Keep a prior `trade_anchor=(tid,time)` from the last poll.
  - If the anchor is missing from the next batch, the `recentTrades` window likely overflowed between polls.
  - Record a `data_gaps` row with `estimated_missing = -1` (unknown).

Why:
- This creates an operational audit trail without claiming we can measure exact missing counts from `recentTrades` alone.
- Full completeness requires reconciliation/backfill from HL S3 historical data (Phase 2).

## 3. Rate-Limited Gap Alerts

Problem:
- If the market is busy enough to overflow `recentTrades` frequently, sending a Discord alert every poll will spam and obscure real incidents.

Fix:
- Added a cooldown (`GAP_ALERT_COOLDOWN_SECS`) for gap alerts.
- `severity=critical` bypasses cooldown.

Why:
- Alerts should be actionable, not noisy.

## 4. Made `/health` Represent Reality (API Failures Must Surface)

Problem:
- Previously `hl_post()` returned `None` on failure, but `fetch_trades()` converted that into `[]`.
- The collector loop treated that as “success” and updated heartbeats, masking an API outage.

Fix:
- `fetch_trades()` / `fetch_candles()` now return `None` on failure.
- Collector tracks:
  - `last_heartbeat`: loop heartbeat (wedge detector)
  - `last_trade_success_s`: last successful `recentTrades` poll
  - `trade_poll_stale`: computed from `last_trade_success_s`

Why:
- A collector that can’t successfully poll trades should be `stale`, not `ok`.

## 5. Fixed Watchdog Wedge Behavior (Avoid Duplicate Collectors)

Problem:
- Restarting the collector thread when it is “wedged” but still alive can spawn multiple collectors in the same process.

Fix:
- On wedge detection (no loop heartbeat for `HEARTBEAT_STALE_SECS`), watchdog sends `SIGTERM` to the process and hard-exits as a fallback.

Why:
- Render/Gunicorn can restart the worker cleanly.
- This avoids multiple collectors competing to write to the same DB.

## 6. API Query Performance Improvements

Problem:
- Endpoint queries could scan unnecessarily if not filtering by `coin` and `ts_ms` together.

Fix:
- `/trades` query now uses `WHERE coin = ? AND ts_ms >= ?` (SQLite) and `WHERE coin = %s AND ts_ms >= %s` (Postgres) to use `(coin, ts_ms)` index.
- `/candles` query now filters `coin` + `interval` + `ts_ms` to use PK/index.

Why:
- Keeps endpoints fast as tables grow.

## 7. Testability Improvements + Minimal Pytest Suite

Problem:
- Importing `app.py` started background threads, which breaks unit tests and local tooling.

Fix:
- Added `DISABLE_BACKGROUND_THREADS=1` switch to prevent thread startup during tests.
- Added pytest tests for `/health` semantics: `CODE/btc-collector/tests/test_health.py`.

Why:
- Prevents regressions and makes Phase 2 changes safer.

## 8. Repo Hygiene

Fix:
- Added `CODE/btc-collector/.gitignore` for `__pycache__/`, `*.pyc`, `.pytest_cache`, `.venv/`, etc.

