# HLP + OI + Funding Collector Expansion Spec

**Owner**: Josh (virtualshinobi)
**Author**: Claude Code (Opus 4.6)
**Date**: 2026-02-09
**Status**: READY — reviewed + hardened for safe auto-deploy on Render
**Repo**: `btc-collector/`
**Deploy**: `https://lifeboard-qf8y.onrender.com` (Render Starter, `--workers 1`)

---

## Problem Statement

Our algo trading pipeline has three critical data gaps:

| Dataset | Current State | Problem |
|---------|--------------|---------|
| **HLP Vault Positions** | 1 snapshot (single point) | **Zero history.** Can't backtest HLP Sentiment strategy. Every minute we delay = data lost forever. |
| **HL Open Interest** | 8,640 rows from Binance (30 days) | Binance API hard-capped at 30 days. Data expires as new days arrive. No HL-native OI history. |
| **HL Funding Rate** | 4,320 rows (179 days from HL API) | Good coverage via `fundingHistory`, but no live streaming. Backfill only. |

All three datasets are available **free** from HL's public `/info` API with **zero authentication**.

The btc-collector already runs 24/7 on Render with proven uptime (26+ hours, 138K trades, 0 errors, 0 restarts). Adding data collection here means **always-on, zero-cost incremental data accumulation** — far superior to a local cron job that only runs when the laptop is open.

---

## Solution: Expand btc-collector

Add a new **derivatives data thread** to the existing btc-collector that collects HLP positions, OI, and funding rate every 5 minutes and stores them in the same PostgreSQL database.

### Non-Negotiable Safety Requirements (Render Auto-Deploy)

This repo auto-deploys to Render on every push to `main`. The expansion must **not** jeopardize existing trade/candle collection.

- Derivs collection runs in its own thread and never blocks or wedges the trade collector.
- Derivs failures must never trigger process SIGTERM.
- Derivs uses its **own DB connection** (no cross-thread psycopg2 connection sharing).
- A `DERIVS_ENABLED` env var kill switch exists and can disable derivs collection without code changes.

### Architecture Change

```
BEFORE (3 threads):
  collector (5-10s) ── trades + candles + price
  pinger   (600s)   ── self-ping
  watchdog (30s)    ── thread health

AFTER (4 threads):
  collector   (5-10s) ── trades + candles + price (UNCHANGED)
  derivs      (300s)  ── HLP positions + OI + funding  (NEW)
  pinger      (600s)  ── self-ping
  watchdog    (30s)   ── thread health (extended to watch derivs thread)
```

### Why a Separate Thread?

- **Different cadence**: Trades poll at 5-10s, derivs data only needs 5-min snapshots
- **Isolation**: If HLP API calls fail, trade collection is unaffected
- **Clean shutdown**: Each thread manages its own error recovery
- **Matches existing pattern**: Same `_safe_*` wrapper + watchdog restart approach

---

## HL API Calls (New)

### 1. HLP Vault Positions (`clearinghouseState`)

Fetches position state for an HL vault address. We need 2 calls (one per active strategy vault).

```json
POST https://api.hyperliquid.xyz/info
{"type": "clearinghouseState", "user": "0x010461c14e146ac35fe42271bdc1134ee31c703a"}
```

**Response** (relevant fields):
```json
{
  "marginSummary": {"accountValue": "14523456.78"},
  "assetPositions": [
    {
      "position": {
        "coin": "BTC",
        "szi": "16.8800",        // signed size (+ = long, - = short)
        "positionValue": "1195432.88",
        "entryPx": "70821.5",
        "unrealizedPnl": "1234.56"
      }
    }
  ]
}
```

**Vault Addresses** (discovered and verified):
```python
HLP_STRATEGY_A = "0x010461c14e146ac35fe42271bdc1134ee31c703a"  # Strategy A
HLP_STRATEGY_B = "0x31ca8395cf837de08b24da3f660e77761dfb974b"  # Strategy B
```

These are the only two child vaults that hold BTC positions. The parent vault (0xdfc24b...) and 5 liquidator vaults have no positions to collect.

**Weight**: `clearinghouseState` is a lightweight user-state query. ~20 weight per call.

### 2. Open Interest + Funding (`metaAndAssetCtxs`)

Single call returns OI, funding rate, mark price, and volume for ALL perp coins.

```json
POST https://api.hyperliquid.xyz/info
{"type": "metaAndAssetCtxs"}
```

**Response** (BTC entry, from the asset contexts array):
```json
{
  "funding": "0.00001234",      // current hourly funding rate
  "openInterest": "12345.6789", // total OI in base asset (BTC)
  "prevDayPx": "69500.0",
  "dayNtlVlm": "987654321.0",
  "premium": "0.00005678",
  "oraclePx": "70800.0",
  "markPx": "70805.5",
  "midPx": "70803.0",
  "impactPxs": ["70795.0", "70811.0"]
}
```

**Weight**: ~2-5 weight (returns all coins in one response).

### Total New API Load

| Call | Per Cycle | Cycle Interval | Calls/min | Est. Weight/min |
|------|-----------|----------------|-----------|-----------------|
| `clearinghouseState` (vault A) | 1 | 300s | 0.2 | ~4 |
| `clearinghouseState` (vault B) | 1 | 300s | 0.2 | ~4 |
| `metaAndAssetCtxs` | 1 | 300s | 0.2 | ~1 |
| **Total new** | **3** | **300s** | **0.6** | **~9** |

**Current load**: ~6 calls/min for trades + ~1/min for candles + 1/min for price = ~8 calls/min, ~200 weight/min.

**After expansion**: ~8.6 calls/min, ~209 weight/min. HL limit is 1200 weight/min. We're at **17% utilization**. Plenty of headroom.

---

## New Database Tables

### `hlp_snapshots`

Stores HLP vault BTC position snapshots every 5 minutes.

**PostgreSQL:**
```sql
CREATE TABLE IF NOT EXISTS hlp_snapshots (
    ts_ms           BIGINT NOT NULL,
    net_btc         NUMERIC NOT NULL,      -- net BTC across both vaults
    net_usd         NUMERIC NOT NULL,      -- net USD notional
    strat_a_btc     NUMERIC NOT NULL,      -- Strategy A BTC size
    strat_b_btc     NUMERIC NOT NULL,      -- Strategy B BTC size
    total_acv       NUMERIC NOT NULL,      -- combined account value
    PRIMARY KEY (ts_ms)
);
CREATE INDEX IF NOT EXISTS idx_hlp_ts ON hlp_snapshots(ts_ms);
```

**SQLite fallback:**
```sql
CREATE TABLE IF NOT EXISTS hlp_snapshots (
    ts_ms       INTEGER PRIMARY KEY,
    net_btc     REAL NOT NULL,
    net_usd     REAL NOT NULL,
    strat_a_btc REAL NOT NULL,
    strat_b_btc REAL NOT NULL,
    total_acv   REAL NOT NULL
);
```

**Row size estimate**: ~50 bytes/row. At 288 rows/day = ~14 KB/day = ~430 KB/month. Trivial.

### `oi_snapshots`

Stores open interest, funding rate, and mark price snapshots every 5 minutes.

**PostgreSQL:**
```sql
CREATE TABLE IF NOT EXISTS oi_snapshots (
    ts_ms           BIGINT NOT NULL,
    coin            TEXT NOT NULL,
    open_interest   NUMERIC NOT NULL,      -- total OI in base asset
    funding_rate    NUMERIC NOT NULL,      -- current hourly funding rate
    mark_px         NUMERIC NOT NULL,      -- mark price at snapshot time
    oracle_px       NUMERIC,               -- oracle price (nullable)
    day_ntl_vlm     NUMERIC,               -- 24h notional volume (nullable)
    premium         NUMERIC,               -- premium to oracle (nullable)
    PRIMARY KEY (ts_ms, coin)
);
CREATE INDEX IF NOT EXISTS idx_oi_ts ON oi_snapshots(ts_ms);
```

**SQLite fallback:**
```sql
CREATE TABLE IF NOT EXISTS oi_snapshots (
    ts_ms           INTEGER NOT NULL,
    coin            TEXT NOT NULL,
    open_interest   REAL NOT NULL,
    funding_rate    REAL NOT NULL,
    mark_px         REAL NOT NULL,
    oracle_px       REAL,
    day_ntl_vlm     REAL,
    premium         REAL,
    PRIMARY KEY (ts_ms, coin)
);
```

**Row size estimate**: ~80 bytes/row. At 288 rows/day (BTC only) = ~23 KB/day = ~690 KB/month. Trivial.

**Note**: We collect BTC only for now. The schema supports multi-coin expansion if needed later (e.g., ETH, SOL).

### Storage Budget

| Table | Rows/day | Bytes/row | MB/month | GB/year |
|-------|----------|-----------|----------|---------|
| `hlp_snapshots` | 288 | ~50 | 0.43 | 0.005 |
| `oi_snapshots` | 288 | ~80 | 0.69 | 0.008 |
| **Total new** | 576 | - | **1.12** | **0.013** |

Current trades table: ~0.2-0.5 GB/month. New tables add <1% overhead.

---

## New Thread: `derivs_collector_loop()`

### Pseudocode

```python
DERIVS_INTERVAL = 300  # 5 minutes
HLP_VAULT_A = "0x010461c14e146ac35fe42271bdc1134ee31c703a"
HLP_VAULT_B = "0x31ca8395cf837de08b24da3f660e77761dfb974b"
DERIVS_COIN = "BTC"

def derivs_collector_loop():
    """Collect HLP positions + OI + funding every 5 minutes."""
    conn = get_db()

    while True:
        ts_ms = int(time.time() * 1000)

        # --- HLP Positions ---
        net_btc = 0.0
        net_usd = 0.0
        total_acv = 0.0
        strat_a = 0.0
        strat_b = 0.0

        for vault_addr, vault_label in [(HLP_VAULT_A, "A"), (HLP_VAULT_B, "B")]:
            state = hl_post({"type": "clearinghouseState", "user": vault_addr})
            if state is None:
                continue

            acv = float(state.get("marginSummary", {}).get("accountValue", 0))
            total_acv += acv

            for p in state.get("assetPositions", []):
                pos = p.get("position", {})
                if pos.get("coin") == DERIVS_COIN:
                    szi = float(pos.get("szi", 0))
                    val = float(pos.get("positionValue", 0))
                    net_btc += szi
                    net_usd += val if szi > 0 else -val
                    if vault_label == "A":
                        strat_a = szi
                    else:
                        strat_b = szi
                    break

        store_hlp_snapshot(conn, ts_ms, net_btc, net_usd, strat_a, strat_b, total_acv)

        # --- OI + Funding ---
        data = hl_post({"type": "metaAndAssetCtxs"})
        if data and isinstance(data, list) and len(data) == 2:
            meta, ctxs = data
            universe = meta.get("universe", [])
            for i, u in enumerate(universe):
                if isinstance(u, dict) and str(u.get("name", "")).upper() == DERIVS_COIN:
                    if i < len(ctxs):
                        ctx = ctxs[i]
                        store_oi_snapshot(conn, ts_ms, DERIVS_COIN, ctx)
                    break

        # Update stats
        collector_stats["last_derivs_poll"] = datetime.now(timezone.utc).isoformat()
        collector_stats["last_derivs_success_s"] = time.time()
        collector_stats["derivs_polls"] += 1
        collector_stats["last_hlp_net_btc"] = net_btc

        time.sleep(DERIVS_INTERVAL)
```

### Error Handling

Same pattern as existing collector:
- Each API call failure is logged but doesn't crash the loop
- DB connection errors trigger reconnect
- Thread wrapped in `_safe_derivs()` for auto-restart
- Watchdog extended to monitor derivs thread heartbeat

**DB connection detail (important)**: The existing trade collector uses a persistent Postgres connection. psycopg2 connections should not be used concurrently across threads without explicit locking. Therefore, the derivs thread uses its own independent Postgres connection (or opens a fresh connection per cycle).

### Stats Additions

```python
# Add to collector_stats dict:
"derivs_alive": False,
"derivs_polls": 0,
"last_derivs_poll": None,
"last_derivs_success_s": None,
"last_hlp_net_btc": None,
"total_hlp_snapshots": 0,
"total_oi_snapshots": 0,
"last_derivs_heartbeat": None,
```

---

## New REST API Endpoints

### `GET /hlp`

Returns recent HLP vault position snapshots.

**Query params:**
- `since_ms` (int, default 0): Return snapshots after this timestamp
- `limit` (int, default 1000, max 10000): Max rows to return

**Response:**
```json
[
  {
    "ts_ms": 1770606300000,
    "net_btc": 0.04,
    "net_usd": 2832.2,
    "strat_a_btc": 16.88,
    "strat_b_btc": -16.84,
    "total_acv": 14523456.78
  }
]
```

### `GET /oi`

Returns recent OI + funding snapshots.

**Query params:**
- `since_ms` (int, default 0)
- `limit` (int, default 1000, max 10000)
- `coin` (str, default "BTC")

**Response:**
```json
[
  {
    "ts_ms": 1770606300000,
    "coin": "BTC",
    "open_interest": 12345.6789,
    "funding_rate": 0.00001234,
    "mark_px": 70805.5,
    "oracle_px": 70800.0,
    "day_ntl_vlm": 987654321.0,
    "premium": 0.00005678
  }
]
```

### `GET /health` (Updated)

Add to existing health response:
```json
{
  "derivs_alive": true,
  "derivs_polls": 42,
  "last_hlp_net_btc": 0.04,
  "total_hlp_snapshots": 42,
  "total_oi_snapshots": 42,
  "derivs_heartbeat_age_s": 3.2
}
```

---

## Watchdog Extension

The existing watchdog already monitors collector + pinger threads. Extend it to also monitor the derivs thread:

```python
# In watchdog_loop(), add after existing collector/pinger checks:

# Check derivs thread
if _derivs_thread is None or not _derivs_thread.is_alive():
    collector_stats["derivs_alive"] = False
    log.warning("Derivs thread dead — restarting")
    _derivs_thread = threading.Thread(
        target=_safe_derivs, daemon=True, name="derivs")
    _derivs_thread.start()

# Derivs heartbeat check (same wedge-detection pattern)
derivs_hb = collector_stats.get("last_derivs_heartbeat")
if derivs_hb is not None and time.time() - derivs_hb > DERIVS_INTERVAL + 120:
    # Derivs thread may be wedged. Log it but don't SIGTERM (it's not critical path).
    log.error("Derivs thread heartbeat stale (%ds)", int(time.time() - derivs_hb))
    send_discord_alert("**Derivs Thread Stale**: no heartbeat for %ds" % int(time.time() - derivs_hb))
```

**Key difference from trade collector**: A wedged derivs thread does NOT trigger process SIGTERM. Trade collection is the critical path. Derivs stalling should be alerted but not kill the service.

---

## Papertrade Consumption

The papertrade package on local can fetch from these new endpoints via `remote_data.py` or a new utility.

**Already compatible**: The local `data_feeds.py` stores HLP snapshots in the same schema. We can either:
1. Fetch from `/hlp` endpoint and store locally (for backtesting)
2. Connect directly to Render Postgres for heavier queries (research mode)

**Data flow for backtest**:
```
btc-collector (Render 24/7)
  └── /hlp, /oi endpoints
       └── papertrade/remote_data.py fetches + stores local Parquet
            └── backtest_runner.py reads Parquet tapes
```

---

## Deployment Plan

### Step 1: Schema Migration
Add new tables via `_init_db()`. The function already runs DDL idempotently with `CREATE TABLE IF NOT EXISTS`.

### Step 2: Code Changes to `app.py`

**New constants:**
```python
DERIVS_INTERVAL = 300  # 5 minutes
DERIVS_ENABLED = os.environ.get("DERIVS_ENABLED", "1") == "1"  # kill switch (default ON)
HLP_VAULT_A = "0x010461c14e146ac35fe42271bdc1134ee31c703a"
HLP_VAULT_B = "0x31ca8395cf837de08b24da3f660e77761dfb974b"
DERIVS_COIN = "BTC"
```

Recommended: allow vault addresses + coin to be overridden via env vars for zero-code remediation if HL changes structures.

**New functions:**
- `derivs_collector_loop()` — main derivs collection loop
- `_safe_derivs()` — crash-resilient wrapper
- `store_hlp_snapshot()` — insert into hlp_snapshots
- `store_oi_snapshot()` — insert into oi_snapshots
- `get_hlp_count()` / `get_oi_count()` — for stats

**Modified functions:**
- `_init_db()` — add 2 new CREATE TABLE statements
- `start_background_threads()` — launch derivs thread
- `watchdog_loop()` — monitor derivs thread
- `health()` — include derivs stats
- `collector_stats` dict — add derivs fields

**New routes:**
- `GET /hlp` — HLP snapshots
- `GET /oi` — OI snapshots

### Step 3: No New Dependencies
All API calls use `requests` (already in requirements.txt). No new packages needed.

### Step 4: Deploy
```bash
git add app.py
git commit -m "feat: add HLP position + OI + funding collection (derivs thread)"
git push  # triggers Render auto-deploy
```

### Step 5: Verify
```bash
# Wait 5 minutes, then check:
curl -s https://lifeboard-qf8y.onrender.com/health | jq '.derivs_alive, .total_hlp_snapshots'
curl -s https://lifeboard-qf8y.onrender.com/hlp?limit=3 | python3 -m json.tool
curl -s https://lifeboard-qf8y.onrender.com/oi?limit=3 | python3 -m json.tool
```

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| HLP vault addresses change | LOW | Data gaps | Alert on 0 BTC position for >1 hour |
| HL rate limit hit | VERY LOW (17% utilization) | Temporary API failures | Existing retry logic handles this |
| Derivs thread crashes | LOW | HLP/OI gaps | Watchdog restart + Discord alert |
| DB storage overflow | NEGLIGIBLE | ~1 MB/month new data | Years before this matters |
| Increased Render memory | LOW | Process restart | Thread sleeps 5 min; minimal memory |

### Vault Address Monitoring

HLP vault addresses are hardcoded. If Hyperliquid changes their vault structure:
- Strategy A/B positions will read as 0
- Discord alert fires when `net_btc == 0` for 3+ consecutive polls
- Manual intervention to update addresses

---

## Data Accumulation Timeline

| Milestone | When | HLP Snapshots | OI Snapshots | Unlocks |
|-----------|------|---------------|--------------|---------|
| Day 1 | Feb 9 | 288 | 288 | Live monitoring |
| Week 1 | Feb 16 | 2,016 | 2,016 | Basic Z-score (48+ samples) |
| Week 2 | Feb 23 | 4,032 | 4,032 | HLP Sentiment forward-test viable |
| Month 1 | Mar 9 | 8,640 | 8,640 | First real backtest window |
| Month 2 | Apr 9 | 17,280 | 17,280 | Robust backtest with regime variety |
| Month 6 | Aug 9 | 51,840 | 51,840 | Full strategy validation dataset |

**Critical insight**: This data does not exist anywhere else. There is no S3 backfill, no third-party source, no historical API. The ONLY way to get HLP position history is to collect it ourselves, starting now.

---

## Files to Modify

| File | Changes |
|------|---------|
| `app.py` | Add derivs thread, 2 tables, 2 endpoints, extend watchdog + health + stats |
| `requirements.txt` | No changes |

**Estimated diff**: ~200-250 lines added, ~20 lines modified. Zero changes to existing trade/candle collection logic.

---

## Questions for Codex Review

### Decisions (Applied)

1. **DB connection**: derivs uses its own Postgres connection (no cross-thread sharing).
2. **Scope**: collect **BTC only** for now; schema supports multi-coin later.
3. **Wedge behavior**: derivs staleness alerts only; never SIGTERM.
4. **Kill switch**: add `DERIVS_ENABLED` env var (default ON).
5. **Cadence**: 5 minutes (300s) is correct for the signal; low load and high value.

---

*Spec ready for Codex review. After Codex approval + refinements, implement and deploy.*
