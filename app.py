"""BTC trade + candle collector for Hyperliquid.

Lightweight Flask app that:
1. Polls recentTrades with adaptive interval (5-10s), candles every 60s
2. Stores in PostgreSQL (Render) with SQLite fallback for local dev
3. Exposes /health, /stats, /trades, /candles, /gaps endpoints
4. Self-pings every 10 min to prevent Render free-tier spin-down
5. Watchdog restarts dead threads; fail-fast restarts the process if the collector loop wedges
6. Best-effort gap detection via recentTrades window overflow (anchor overlap) + gap event logging
7. Discord webhook alerting (down, stale, crash loop, gaps)
8. TRADING_ENABLED kill switch for downstream consumers
"""

import os
import signal
import threading
import time
import logging
import requests
from datetime import datetime, timezone
from flask import Flask, jsonify, request

app = Flask(__name__)
log = logging.getLogger("collector")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HL_API = "https://api.hyperliquid.xyz/info"
COIN = "BTC"
CANDLE_INTERVAL = 60     # seconds
PRICE_INTERVAL = 60      # seconds
SELF_PING_INTERVAL = 600 # 10 min
WATCHDOG_INTERVAL = 30   # check thread health every 30s
MAX_RETRIES = 3
BASE_DELAY = 0.25

# Health/staleness thresholds
TRADE_STALE_SECS = 300          # 5 min without successful poll
CANDLE_DATA_STALE_SECS = 600    # 10 min behind latest candle timestamp
HEARTBEAT_STALE_SECS = 120      # collector wedged if no heartbeat for 2 min
GAP_ALERT_COOLDOWN_SECS = 300   # rate limit gap alerts to Discord
STARTUP_GRACE_SECS = 30         # allow boot time before flagging staleness
STALE_ALERT_COOLDOWN_SECS = 300 # rate limit stale alerts to Discord

# Database config
DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_POSTGRES = bool(DATABASE_URL)

# Alerting
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# Kill switch
TRADING_ENABLED = os.environ.get("TRADING_ENABLED", "1") == "1"

# Derivatives collector (HLP positions + open interest + funding)
DERIVS_ENABLED = os.environ.get("DERIVS_ENABLED", "1") == "1"
try:
    DERIVS_INTERVAL = int(os.environ.get("DERIVS_INTERVAL", "300"))  # seconds
except ValueError:
    DERIVS_INTERVAL = 300
DERIVS_COIN = os.environ.get("DERIVS_COIN", COIN)
HLP_VAULT_A = os.environ.get("HLP_VAULT_A", "0x010461c14e146ac35fe42271bdc1134ee31c703a").strip()
HLP_VAULT_B = os.environ.get("HLP_VAULT_B", "0x31ca8395cf837de08b24da3f660e77761dfb974b").strip()

# ---------------------------------------------------------------------------
# Database abstraction
# ---------------------------------------------------------------------------

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras
    # Never log DATABASE_URL (contains credentials).
    log.info("Using PostgreSQL (DATABASE_URL set)")
else:
    import sqlite3
    DB_PATH = "/tmp/collector.db"
    log.info("Using SQLite: %s (ephemeral)", DB_PATH)

_db_initialized = False
_db_init_lock = threading.Lock()

# Persistent connection for collector thread (Postgres only)
_pg_conn = None
_pg_conn_lock = threading.Lock()


def _get_pg_conn():
    """Get or create a persistent PostgreSQL connection."""
    global _pg_conn
    with _pg_conn_lock:
        if _pg_conn is None or _pg_conn.closed:
            _pg_conn = psycopg2.connect(DATABASE_URL)
            _pg_conn.autocommit = False
        return _pg_conn


def _reset_pg_conn():
    """Close and reset the persistent PostgreSQL connection."""
    global _pg_conn
    with _pg_conn_lock:
        if _pg_conn is not None:
            try:
                _pg_conn.close()
            except Exception:
                pass
            _pg_conn = None


def _init_db():
    """Run DDL once per process."""
    global _db_initialized
    if _db_initialized:
        return
    with _db_init_lock:
        if _db_initialized:
            return
        if USE_POSTGRES:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    coin    TEXT NOT NULL,
                    tid     BIGINT NOT NULL,
                    ts_ms   BIGINT NOT NULL,
                    side    TEXT NOT NULL,
                    px      NUMERIC NOT NULL,
                    sz      NUMERIC NOT NULL,
                    source  TEXT DEFAULT 'poll',
                    PRIMARY KEY (coin, tid)
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(coin, ts_ms);
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS candles_5m (
                    coin     TEXT NOT NULL,
                    interval TEXT NOT NULL DEFAULT '5m',
                    ts_ms    BIGINT NOT NULL,
                    open     NUMERIC NOT NULL,
                    high     NUMERIC NOT NULL,
                    low      NUMERIC NOT NULL,
                    close    NUMERIC NOT NULL,
                    volume   NUMERIC NOT NULL,
                    PRIMARY KEY (coin, interval, ts_ms)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS data_gaps (
                    id          SERIAL PRIMARY KEY,
                    coin        TEXT NOT NULL,
                    detected_at BIGINT NOT NULL,
                    last_tid    BIGINT NOT NULL,
                    next_tid    BIGINT NOT NULL,
                    estimated_missing INTEGER NOT NULL,
                    severity    TEXT NOT NULL
                );
            """)

            # Derivatives snapshots (idempotent DDL; safe on auto-deploy).
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hlp_snapshots (
                    ts_ms           BIGINT NOT NULL,
                    net_btc         NUMERIC NOT NULL,
                    net_usd         NUMERIC NOT NULL,
                    strat_a_btc     NUMERIC NOT NULL,
                    strat_b_btc     NUMERIC NOT NULL,
                    total_acv       NUMERIC NOT NULL,
                    PRIMARY KEY (ts_ms)
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS oi_snapshots (
                    ts_ms           BIGINT NOT NULL,
                    coin            TEXT NOT NULL,
                    open_interest   NUMERIC NOT NULL,
                    funding_rate    NUMERIC NOT NULL,
                    mark_px         NUMERIC NOT NULL,
                    oracle_px       NUMERIC,
                    day_ntl_vlm     NUMERIC,
                    premium         NUMERIC,
                    PRIMARY KEY (ts_ms, coin)
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_oi_ts ON oi_snapshots(ts_ms);
            """)
            cur.close()
            conn.close()
            log.info("PostgreSQL schema initialized")
        else:
            conn = sqlite3.connect(DB_PATH)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    tid     INTEGER PRIMARY KEY,
                    coin    TEXT NOT NULL,
                    side    TEXT NOT NULL,
                    px      REAL NOT NULL,
                    sz      REAL NOT NULL,
                    ts_ms   INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts_ms);

                CREATE TABLE IF NOT EXISTS candles_5m (
                    ts_ms   INTEGER PRIMARY KEY,
                    coin    TEXT NOT NULL,
                    open    REAL NOT NULL,
                    high    REAL NOT NULL,
                    low     REAL NOT NULL,
                    close   REAL NOT NULL,
                    volume  REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS data_gaps (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin        TEXT NOT NULL,
                    detected_at INTEGER NOT NULL,
                    last_tid    INTEGER NOT NULL,
                    next_tid    INTEGER NOT NULL,
                    estimated_missing INTEGER NOT NULL,
                    severity    TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS hlp_snapshots (
                    ts_ms       INTEGER PRIMARY KEY,
                    net_btc     REAL NOT NULL,
                    net_usd     REAL NOT NULL,
                    strat_a_btc REAL NOT NULL,
                    strat_b_btc REAL NOT NULL,
                    total_acv   REAL NOT NULL
                );

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
                CREATE INDEX IF NOT EXISTS idx_oi_ts ON oi_snapshots(ts_ms);
            """)
            conn.close()
            log.info("SQLite initialized at %s", DB_PATH)
        _db_initialized = True


def get_db():
    """Get a database connection (new connection for SQLite, persistent for Postgres)."""
    _init_db()
    if USE_POSTGRES:
        return _get_pg_conn()
    else:
        return sqlite3.connect(DB_PATH, timeout=10)


def _close_db(conn):
    """Close connection if SQLite (Postgres uses persistent connection)."""
    if not USE_POSTGRES:
        conn.close()


# ---------------------------------------------------------------------------
# Hyperliquid API (read-only)
# ---------------------------------------------------------------------------

def hl_post(payload: dict, timeout: float = 10.0):
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(HL_API, json=payload,
                                 headers={"Content-Type": "application/json"},
                                 timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            if attempt == MAX_RETRIES - 1:
                log.error("HL API failed: %s", exc)
                return None
            time.sleep(BASE_DELAY * (2 ** attempt))


def fetch_trades(coin=COIN):
    # Important: return None on failure so the collector can mark itself unhealthy.
    return hl_post({"type": "recentTrades", "coin": coin})


def fetch_candles(coin=COIN, start_ms=None, end_ms=None):
    now_ms = int(time.time() * 1000)
    req = {"coin": coin, "interval": "5m"}
    if start_ms:
        req["startTime"] = start_ms
    if end_ms:
        req["endTime"] = end_ms
    else:
        req["endTime"] = now_ms
    if not start_ms:
        req["startTime"] = now_ms - (6 * 3600 * 1000)
    # Important: return None on failure so the collector can mark itself unhealthy.
    return hl_post({"type": "candleSnapshot", "req": req})


def fetch_mid_price(coin=COIN):
    data = hl_post({"type": "allMids"})
    if data and coin in data:
        return float(data[coin])
    return 0.0

# ---------------------------------------------------------------------------
# Storage operations
# ---------------------------------------------------------------------------

def store_trades(trades, conn):
    if not trades:
        return 0
    if USE_POSTGRES:
        cur = conn.cursor()
        inserted = 0
        for t in trades:
            try:
                cur.execute(
                    "INSERT INTO trades (coin, tid, ts_ms, side, px, sz, source) "
                    "VALUES (%s, %s, %s, %s, %s, %s, 'poll') "
                    "ON CONFLICT (coin, tid) DO NOTHING",
                    (t.get("coin", COIN), int(t["tid"]), int(t["time"]),
                     t["side"], float(t["px"]), float(t["sz"]))
                )
                if cur.rowcount > 0:
                    inserted += 1
            except (KeyError, ValueError):
                pass
        conn.commit()
        cur.close()
        return inserted
    else:
        inserted = 0
        for t in trades:
            try:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO trades (tid,coin,side,px,sz,ts_ms) VALUES (?,?,?,?,?,?)",
                    (int(t["tid"]), t.get("coin", COIN), t["side"],
                     float(t["px"]), float(t["sz"]), int(t["time"]))
                )
                if cur.rowcount > 0:
                    inserted += 1
            except (KeyError, ValueError):
                pass
        conn.commit()
        return inserted


def store_candles(candles, conn):
    if not candles:
        return 0
    if USE_POSTGRES:
        cur = conn.cursor()
        inserted = 0
        for c in candles:
            try:
                cur.execute(
                    "INSERT INTO candles_5m (coin, interval, ts_ms, open, high, low, close, volume) "
                    "VALUES (%s, '5m', %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (coin, interval, ts_ms) DO NOTHING",
                    (COIN, int(c["t"]), float(c["o"]), float(c["h"]),
                     float(c["l"]), float(c["c"]), float(c["v"]))
                )
                if cur.rowcount > 0:
                    inserted += 1
            except (KeyError, ValueError):
                pass
        conn.commit()
        cur.close()
        return inserted
    else:
        inserted = 0
        for c in candles:
            try:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO candles_5m (ts_ms,coin,open,high,low,close,volume) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (int(c["t"]), COIN, float(c["o"]), float(c["h"]),
                     float(c["l"]), float(c["c"]), float(c["v"]))
                )
                if cur.rowcount > 0:
                    inserted += 1
            except (KeyError, ValueError):
                pass
        conn.commit()
        return inserted


def get_trade_count(conn):
    """Get total trade count efficiently."""
    if USE_POSTGRES:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM trades")
        count = cur.fetchone()[0]
        cur.close()
        return count
    else:
        return conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]


def get_candle_count(conn):
    """Get total candle count efficiently."""
    if USE_POSTGRES:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM candles_5m")
        count = cur.fetchone()[0]
        cur.close()
        return count
    else:
        return conn.execute("SELECT COUNT(*) FROM candles_5m").fetchone()[0]


def store_hlp_snapshot(conn, ts_ms: int, net_btc: float, net_usd: float,
                       strat_a_btc: float, strat_b_btc: float, total_acv: float) -> int:
    if USE_POSTGRES:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO hlp_snapshots (ts_ms, net_btc, net_usd, strat_a_btc, strat_b_btc, total_acv) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (ts_ms) DO NOTHING",
            (int(ts_ms), float(net_btc), float(net_usd), float(strat_a_btc), float(strat_b_btc), float(total_acv)),
        )
        inserted = int(cur.rowcount or 0)
        conn.commit()
        cur.close()
        return inserted
    else:
        cur = conn.execute(
            "INSERT OR IGNORE INTO hlp_snapshots (ts_ms, net_btc, net_usd, strat_a_btc, strat_b_btc, total_acv) "
            "VALUES (?,?,?,?,?,?)",
            (int(ts_ms), float(net_btc), float(net_usd), float(strat_a_btc), float(strat_b_btc), float(total_acv)),
        )
        conn.commit()
        return int(cur.rowcount or 0)


def store_oi_snapshot(conn, ts_ms: int, coin: str, open_interest: float, funding_rate: float,
                      mark_px: float, oracle_px=None, day_ntl_vlm=None, premium=None) -> int:
    coin = str(coin or "").upper()
    if USE_POSTGRES:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO oi_snapshots (ts_ms, coin, open_interest, funding_rate, mark_px, oracle_px, day_ntl_vlm, premium) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (ts_ms, coin) DO NOTHING",
            (
                int(ts_ms),
                coin,
                float(open_interest),
                float(funding_rate),
                float(mark_px),
                (None if oracle_px is None else float(oracle_px)),
                (None if day_ntl_vlm is None else float(day_ntl_vlm)),
                (None if premium is None else float(premium)),
            ),
        )
        inserted = int(cur.rowcount or 0)
        conn.commit()
        cur.close()
        return inserted
    else:
        cur = conn.execute(
            "INSERT OR IGNORE INTO oi_snapshots (ts_ms, coin, open_interest, funding_rate, mark_px, oracle_px, day_ntl_vlm, premium) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                int(ts_ms),
                coin,
                float(open_interest),
                float(funding_rate),
                float(mark_px),
                (None if oracle_px is None else float(oracle_px)),
                (None if day_ntl_vlm is None else float(day_ntl_vlm)),
                (None if premium is None else float(premium)),
            ),
        )
        conn.commit()
        return int(cur.rowcount or 0)


def get_hlp_count(conn) -> int:
    if USE_POSTGRES:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM hlp_snapshots")
        count = int(cur.fetchone()[0])
        cur.close()
        return count
    else:
        return int(conn.execute("SELECT COUNT(*) FROM hlp_snapshots").fetchone()[0])


def get_oi_count(conn) -> int:
    if USE_POSTGRES:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM oi_snapshots")
        count = int(cur.fetchone()[0])
        cur.close()
        return count
    else:
        return int(conn.execute("SELECT COUNT(*) FROM oi_snapshots").fetchone()[0])


def store_gap_event(conn, coin, last_tid, next_tid, estimated_missing, severity, *, send_alert: bool = True):
    """Record a detected data gap.

    Note: "estimated_missing" may be -1 to mean "unknown" (e.g., recentTrades window overflow).
    """
    now_ms = int(time.time() * 1000)
    if USE_POSTGRES:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO data_gaps (coin, detected_at, last_tid, next_tid, estimated_missing, severity) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (coin, now_ms, last_tid, next_tid, estimated_missing, severity)
        )
        conn.commit()
        cur.close()
    else:
        conn.execute(
            "INSERT INTO data_gaps (coin, detected_at, last_tid, next_tid, estimated_missing, severity) "
            "VALUES (?,?,?,?,?,?)",
            (coin, now_ms, last_tid, next_tid, estimated_missing, severity)
        )
        conn.commit()
    log.warning("GAP DETECTED: %s tids %d -> %d (est. %d missing, %s)",
                coin, last_tid, next_tid, estimated_missing, severity)
    if send_alert:
        title = "**Data Gap Detected**"
        if int(estimated_missing) == -1:
            title = "**Possible Data Gap (recentTrades window overflow)**"
        send_discord_alert(
            f"{title}\nCoin: {coin}\n"
            f"Last tid: {last_tid} -> Next tid: {next_tid}\n"
            f"Estimated missing: {estimated_missing}\nSeverity: {severity}"
        )

# ---------------------------------------------------------------------------
# Discord alerting
# ---------------------------------------------------------------------------

def send_discord_alert(message: str):
    """Send an alert via Discord webhook. Silent fail if not configured."""
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        requests.post(DISCORD_WEBHOOK_URL,
                      json={"content": f"[btc-collector] {message}"},
                      timeout=5)
    except Exception as exc:
        log.warning("Discord alert failed: %s", exc)

# ---------------------------------------------------------------------------
# Collector thread
# ---------------------------------------------------------------------------

collector_stats = {
    "started_at": None,
    "started_s": None,
    "total_trades": 0,
    "total_candles": 0,
    "total_gaps": 0,
    "last_trade_poll": None,
    "last_trade_success_s": None,
    "last_candle_poll": None,
    "last_candle_success_s": None,
    "last_candle_ts_ms": None,
    "last_price": 0.0,
    "polls": 0,
    "errors": 0,
    "restarts": 0,
    "collector_alive": False,
    "pinger_alive": False,
    "last_heartbeat": None,
    "last_stale_alert_s": None,
    "db_type": "postgres" if USE_POSTGRES else "sqlite",
    "db_connected": False,
    "db_last_ok_s": None,
    "trading_enabled": TRADING_ENABLED,
    "last_max_tid": None,
    "trade_anchor": None,  # {"tid": int, "time": int} for overflow detection
    "last_trade_batch_size": None,
    "last_gap_alert_s": None,
    "current_poll_interval": 10.0,

    # Derivs collector (non-critical path; should never break trade collection)
    "derivs_enabled": DERIVS_ENABLED,
    "derivs_alive": False,
    "derivs_restarts": 0,
    "derivs_polls": 0,
    "last_derivs_poll": None,
    "last_derivs_success_s": None,
    "last_derivs_heartbeat": None,
    "last_derivs_stale_alert_s": None,
    "last_hlp_net_btc": None,
    "total_hlp_snapshots": 0,
    "total_oi_snapshots": 0,
    "derivs_interval_s": DERIVS_INTERVAL,
    "derivs_coin": DERIVS_COIN,
}

_collector_thread = None
_pinger_thread = None
_watchdog_thread = None
_derivs_thread = None
_threads_lock = threading.Lock()


def collector_loop():
    log.info("Collector starting (db=%s)...", "postgres" if USE_POSTGRES else "sqlite")
    collector_stats["collector_alive"] = True

    try:
        conn = get_db()
        collector_stats["db_connected"] = True
        collector_stats["db_last_ok_s"] = time.time()
    except Exception as exc:
        log.error("DB connection failed: %s", exc)
        send_discord_alert(f"**DB Connection Failed**: {exc}")
        raise

    # Backfill last 24h of candles on startup
    try:
        now_ms = int(time.time() * 1000)
        backfill = fetch_candles(start_ms=now_ms - (24 * 3600 * 1000), end_ms=now_ms)
        bf_count = store_candles(backfill, conn)
        log.info("Backfilled %d candles (24h)", bf_count)
    except Exception as exc:
        log.error("Backfill failed: %s", exc)

    collector_stats["started_at"] = datetime.now(timezone.utc).isoformat()
    collector_stats["started_s"] = time.time()
    try:
        collector_stats["total_trades"] = get_trade_count(conn)
        collector_stats["total_candles"] = get_candle_count(conn)
    except Exception:
        pass

    # Fetch initial price
    try:
        price = fetch_mid_price()
        if price and price > 0:
            collector_stats["last_price"] = price
            log.info("Initial BTC price: $%.2f", price)
    except Exception as exc:
        log.error("Initial price fetch failed: %s", exc)

    # Initialize last_max_tid from database
    try:
        if USE_POSTGRES:
            cur = conn.cursor()
            cur.execute("SELECT MAX(tid) FROM trades WHERE coin = %s", (COIN,))
            row = cur.fetchone()
            cur.close()
        else:
            row = conn.execute("SELECT MAX(tid) FROM trades WHERE coin = ?", (COIN,)).fetchone()
        if row and row[0] is not None:
            collector_stats["last_max_tid"] = row[0]
            log.info("Resuming from tid=%d", row[0])
    except Exception:
        pass

    last_candle_poll = time.time()
    last_price_poll = time.time()
    poll_interval = 10.0  # adaptive: 5-10s

    while True:
        try:
            # Poll trades (treat HL API failures as unhealthy; do NOT mask as empty batch)
            trades = fetch_trades()
            collector_stats["last_trade_poll"] = datetime.now(timezone.utc).isoformat()
            collector_stats["polls"] += 1

            if trades is None:
                collector_stats["errors"] += 1
                log.warning("HL recentTrades poll failed; leaving last_trade_success_s unchanged")
                collector_stats["last_trade_batch_size"] = None
            else:
                collector_stats["last_trade_success_s"] = time.time()
                collector_stats["last_trade_batch_size"] = len(trades)

            overflow_detected = False
            if trades:
                # Gap/overflow detection for recentTrades:
                # - Hyperliquid trade IDs (tid) are unique identifiers but NOT guaranteed contiguous/monotonic.
                # - Detect potential missed trades by verifying overlap with the previous poll's "anchor" trade.
                #   If the previous anchor is not present in the current batch, the API window may have overflowed.
                current_ids = set()
                for t in trades:
                    if "tid" in t and "time" in t:
                        try:
                            current_ids.add((int(t["tid"]), int(t["time"])))
                        except (TypeError, ValueError):
                            continue

                prev_anchor = collector_stats.get("trade_anchor")
                if prev_anchor and current_ids:
                    prev_id = (int(prev_anchor["tid"]), int(prev_anchor["time"]))
                    if prev_id not in current_ids:
                        overflow_detected = True
                        # We can't know exact missing count without a monotonic sequence; use -1 = unknown.
                        severity = "critical" if poll_interval <= 5.0 else "warning"
                        newest = max(current_ids, key=lambda x: x[1])
                        now_s = time.time()
                        last_gap_alert_s = collector_stats.get("last_gap_alert_s")
                        # Rate-limit gap events (and Discord alerts) aggressively; otherwise
                        # `recentTrades` (fixed window=10) will overflow constantly during active periods.
                        #
                        # IMPORTANT: We still count overflows in `total_gaps`, but we only
                        # persist/log a gap event once per cooldown window to avoid noise.
                        should_alert = (
                            last_gap_alert_s is None
                            or (now_s - float(last_gap_alert_s)) >= GAP_ALERT_COOLDOWN_SECS
                        )
                        if should_alert:
                            store_gap_event(
                                conn,
                                COIN,
                                prev_id[0],
                                newest[0],
                                -1,
                                severity,
                                send_alert=True,
                            )
                        collector_stats["total_gaps"] += 1
                        if should_alert:
                            collector_stats["last_gap_alert_s"] = now_s
                        # Push polling faster immediately.
                        poll_interval = 5.0
                        collector_stats["current_poll_interval"] = poll_interval

                # Store trades
                new_trades = store_trades(trades, conn)
                collector_stats["total_trades"] += new_trades
                collector_stats["db_connected"] = True
                collector_stats["db_last_ok_s"] = time.time()

                # Update last_max_tid
                if current_ids:
                    max_incoming_tid = max(current_ids, key=lambda x: x[0])[0]
                    if (collector_stats["last_max_tid"] is None or
                            max_incoming_tid > collector_stats["last_max_tid"]):
                        collector_stats["last_max_tid"] = max_incoming_tid

                    # Update anchor to the most recent trade by timestamp.
                    latest_tid, latest_time = max(current_ids, key=lambda x: x[1])
                    collector_stats["trade_anchor"] = {"tid": latest_tid, "time": latest_time}

            # Adaptive polling (keep 5s if overflow was detected in this batch)
            if trades is not None:
                if overflow_detected or (trades and len(trades) > 50):
                    poll_interval = 5.0
                else:
                    poll_interval = 10.0
                collector_stats["current_poll_interval"] = poll_interval

            # Poll candles every CANDLE_INTERVAL
            now = time.time()
            if now - last_candle_poll >= CANDLE_INTERVAL:
                candles = fetch_candles()
                if candles is None:
                    collector_stats["errors"] += 1
                    log.warning("HL candleSnapshot poll failed; leaving candle freshness unchanged")
                else:
                    new_candles = store_candles(candles, conn)
                    collector_stats["total_candles"] += new_candles
                    collector_stats["last_candle_poll"] = datetime.now(timezone.utc).isoformat()
                    collector_stats["last_candle_success_s"] = time.time()
                    collector_stats["db_connected"] = True
                    collector_stats["db_last_ok_s"] = time.time()
                    try:
                        if candles:
                            collector_stats["last_candle_ts_ms"] = max(int(c["t"]) for c in candles if "t" in c)
                    except Exception:
                        pass
                    last_candle_poll = now

            # Price check every PRICE_INTERVAL
            if now - last_price_poll >= PRICE_INTERVAL:
                try:
                    price = fetch_mid_price()
                    if price and price > 0:
                        collector_stats["last_price"] = price
                except Exception as exc:
                    log.warning("Price fetch failed: %s", exc)
                last_price_poll = now

            if collector_stats["polls"] % 30 == 0:
                log.info("trades=%d candles=%d price=$%.2f polls=%d err=%d gaps=%d interval=%.0fs",
                         collector_stats["total_trades"],
                         collector_stats["total_candles"],
                         collector_stats["last_price"],
                         collector_stats["polls"],
                         collector_stats["errors"],
                         collector_stats["total_gaps"],
                         poll_interval)
            # Loop heartbeat: updated once per successful iteration through the loop body.
            collector_stats["last_heartbeat"] = time.time()

        except Exception as exc:
            collector_stats["errors"] += 1
            log.error("Collector error: %s", exc)
            collector_stats["db_connected"] = False
            # The loop is still alive; update heartbeat so watchdog doesn't treat repeated errors as a wedge.
            collector_stats["last_heartbeat"] = time.time()
            # Reconnect DB on error
            if USE_POSTGRES:
                _reset_pg_conn()
                try:
                    conn = get_db()
                except Exception:
                    pass
            else:
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    conn = get_db()
                except Exception:
                    pass

        time.sleep(poll_interval)


def self_ping_loop():
    """Ping our own health endpoint to prevent Render spin-down."""
    collector_stats["pinger_alive"] = True
    service_url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if not service_url:
        log.info("No RENDER_EXTERNAL_URL set, self-ping disabled")
        return

    while True:
        time.sleep(SELF_PING_INTERVAL)
        try:
            resp = requests.get(f"{service_url}/health", timeout=10)
            log.info("Self-ping OK (%d)", resp.status_code)
        except Exception as exc:
            log.warning("Self-ping failed: %s", exc)


def watchdog_loop():
    """Monitor collector and pinger threads, restart if dead or wedged."""
    global _collector_thread, _pinger_thread, _derivs_thread
    log.info("Watchdog started")
    recent_restarts = []  # timestamps of recent restarts

    while True:
        time.sleep(WATCHDOG_INTERVAL)
        with _threads_lock:
            # Check collector: dead thread OR wedged (no heartbeat for 2 min)
            collector_dead = (_collector_thread is None or
                             not _collector_thread.is_alive())
            heartbeat = collector_stats.get("last_heartbeat")
            collector_wedged = (heartbeat is not None and
                                time.time() - heartbeat > HEARTBEAT_STALE_SECS)

            if collector_wedged:
                # If the collector thread is alive but no longer updating its heartbeat, it is
                # likely wedged in a blocking call (network/DB). Starting another collector
                # thread would create duplicate collectors in the same process. Fail-fast so
                # Render/Gunicorn restarts the worker cleanly.
                reason = "wedged (no heartbeat)"
                send_discord_alert(
                    f"**Collector Wedged**: no heartbeat for {int(time.time() - heartbeat)}s. "
                    "Requesting process restart to avoid duplicate collectors."
                )
                log.error("Collector %s; sending SIGTERM to self (pid=%d)", reason, os.getpid())
                try:
                    os.kill(os.getpid(), signal.SIGTERM)
                finally:
                    # If SIGTERM is ignored for any reason, hard-exit.
                    time.sleep(2)
                    os._exit(1)

            if collector_dead:
                reason = "dead"
                collector_stats["collector_alive"] = False
                collector_stats["restarts"] += 1
                collector_stats["last_heartbeat"] = None
                log.warning("Collector %s — restarting (#%d)",
                            reason, collector_stats["restarts"])

                # Crash loop detection
                now = time.time()
                recent_restarts.append(now)
                recent_restarts = [t for t in recent_restarts if now - t < 600]
                if len(recent_restarts) > 2:
                    send_discord_alert(
                        f"**Crash Loop Detected**: {len(recent_restarts)} restarts "
                        f"in 10 minutes. Reason: {reason}"
                    )

                _collector_thread = threading.Thread(
                    target=_safe_collector, daemon=True, name="collector")
                _collector_thread.start()

            if _pinger_thread is None or not _pinger_thread.is_alive():
                collector_stats["pinger_alive"] = False
                log.warning("Pinger thread dead — restarting")
                _pinger_thread = threading.Thread(
                    target=_safe_pinger, daemon=True, name="pinger")
                _pinger_thread.start()

            if DERIVS_ENABLED:
                if _derivs_thread is None or not _derivs_thread.is_alive():
                    collector_stats["derivs_alive"] = False
                    collector_stats["derivs_restarts"] += 1
                    log.warning("Derivs thread dead — restarting (#%d)",
                                collector_stats["derivs_restarts"])
                    _derivs_thread = threading.Thread(
                        target=_safe_derivs, daemon=True, name="derivs")
                    _derivs_thread.start()

        # Derivs heartbeat staleness alert (non-critical; never SIGTERM).
        if DERIVS_ENABLED:
            derivs_hb = collector_stats.get("last_derivs_heartbeat")
            if derivs_hb is not None:
                try:
                    derivs_hb_age_s = time.time() - float(derivs_hb)
                except (TypeError, ValueError):
                    derivs_hb_age_s = None
                if derivs_hb_age_s is not None and derivs_hb_age_s > (DERIVS_INTERVAL + 120):
                    now_s = time.time()
                    last_alert_s = collector_stats.get("last_derivs_stale_alert_s")
                    should_alert = (
                        last_alert_s is None or
                        (now_s - float(last_alert_s)) >= STALE_ALERT_COOLDOWN_SECS
                    )
                    if should_alert:
                        collector_stats["last_derivs_stale_alert_s"] = now_s
                        send_discord_alert(
                            f"**Derivs Thread Stale**: No heartbeat for {int(derivs_hb_age_s)}s."
                        )
                    log.error("Derivs thread heartbeat stale (%ds)", int(derivs_hb_age_s))

        # Staleness alert: no successful trade poll within TRADE_STALE_SECS (rate-limited).
        trade_ok_s = collector_stats.get("last_trade_success_s")
        if trade_ok_s is None:
            started_s = collector_stats.get("started_s")
            started_age_s = None
            if started_s is not None:
                try:
                    started_age_s = time.time() - float(started_s)
                except (TypeError, ValueError):
                    started_age_s = None
            if started_age_s is not None and started_age_s > STARTUP_GRACE_SECS:
                now_s = time.time()
                last_alert_s = collector_stats.get("last_stale_alert_s")
                should_alert = (
                    last_alert_s is None or
                    (now_s - float(last_alert_s)) >= STALE_ALERT_COOLDOWN_SECS
                )
                if should_alert:
                    collector_stats["last_stale_alert_s"] = now_s
                    send_discord_alert("**Stale Collector**: No successful trade poll since start.")

        else:
            try:
                trade_ok_age_s = time.time() - float(trade_ok_s)
            except (TypeError, ValueError):
                trade_ok_age_s = None
            if trade_ok_age_s is not None and trade_ok_age_s > TRADE_STALE_SECS:
                now_s = time.time()
                last_alert_s = collector_stats.get("last_stale_alert_s")
                should_alert = (
                    last_alert_s is None or
                    (now_s - float(last_alert_s)) >= STALE_ALERT_COOLDOWN_SECS
                )
                if should_alert:
                    collector_stats["last_stale_alert_s"] = now_s
                    send_discord_alert(
                        f"**Stale Collector**: No successful trade poll in {int(trade_ok_age_s)}s."
                    )


def _safe_collector():
    """Wrapper that catches all exceptions so the thread never dies silently."""
    while True:
        try:
            collector_loop()
        except Exception as exc:
            collector_stats["errors"] += 1
            log.error("Collector crashed: %s — restarting in 5s", exc)
            send_discord_alert(f"**Collector Crashed**: {exc}")
            time.sleep(5)


def _safe_pinger():
    """Wrapper that catches all exceptions so the thread never dies silently."""
    while True:
        try:
            self_ping_loop()
        except Exception as exc:
            log.error("Pinger crashed: %s — restarting in 5s", exc)
        time.sleep(SELF_PING_INTERVAL)

# ---------------------------------------------------------------------------
# Derivatives collector thread
# ---------------------------------------------------------------------------


def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def fetch_clearinghouse_state(user: str):
    return hl_post({"type": "clearinghouseState", "user": str(user)})


def fetch_meta_and_asset_ctxs():
    return hl_post({"type": "metaAndAssetCtxs"})


def _extract_btc_from_state(state: dict, *, coin: str):
    """Return (signed_size_btc, signed_usd_notional, account_value_usd)."""
    if not isinstance(state, dict):
        return 0.0, 0.0, 0.0

    acv = _to_float((state.get("marginSummary") or {}).get("accountValue")) or 0.0
    szi = 0.0
    signed_usd = 0.0
    positions = state.get("assetPositions") or []
    for p in positions:
        pos = (p or {}).get("position") or {}
        if str(pos.get("coin", "")).upper() == str(coin).upper():
            szi = _to_float(pos.get("szi")) or 0.0
            pv = _to_float(pos.get("positionValue")) or 0.0
            signed_usd = pv if szi >= 0 else -pv
            break
    return float(szi), float(signed_usd), float(acv)


def derivs_collector_loop():
    """Collect HLP positions + open interest + funding every DERIVS_INTERVAL seconds.

    Non-critical path: failures must never disrupt trade/candle collection.
    """
    if not DERIVS_ENABLED:
        collector_stats["derivs_alive"] = False
        log.info("Derivs collector disabled (DERIVS_ENABLED=0)")
        return

    log.info("Derivs collector starting (coin=%s interval=%ss)", DERIVS_COIN, DERIVS_INTERVAL)
    collector_stats["derivs_alive"] = True

    _init_db()

    # Use an independent DB connection (never share the trade collector connection across threads).
    conn = None
    while True:
        ts_ms = int(time.time() * 1000)
        collector_stats["last_derivs_poll"] = datetime.now(timezone.utc).isoformat()
        collector_stats["derivs_polls"] += 1
        collector_stats["last_derivs_heartbeat"] = time.time()

        try:
            if conn is None:
                if USE_POSTGRES:
                    conn = psycopg2.connect(DATABASE_URL)
                    conn.autocommit = False
                else:
                    conn = sqlite3.connect(DB_PATH, timeout=10)

                # Load existing counts for visibility (small tables; cheap once per thread start).
                try:
                    collector_stats["total_hlp_snapshots"] = get_hlp_count(conn)
                    collector_stats["total_oi_snapshots"] = get_oi_count(conn)
                except Exception:
                    pass

            inserted_any = False

            # --- HLP positions (2 vaults) ---
            state_a = fetch_clearinghouse_state(HLP_VAULT_A)
            collector_stats["last_derivs_heartbeat"] = time.time()
            state_b = fetch_clearinghouse_state(HLP_VAULT_B)
            collector_stats["last_derivs_heartbeat"] = time.time()

            if state_a is not None and state_b is not None:
                strat_a_btc, strat_a_usd, acv_a = _extract_btc_from_state(state_a, coin=DERIVS_COIN)
                strat_b_btc, strat_b_usd, acv_b = _extract_btc_from_state(state_b, coin=DERIVS_COIN)
                net_btc = strat_a_btc + strat_b_btc
                net_usd = strat_a_usd + strat_b_usd
                total_acv = acv_a + acv_b
                inserted = store_hlp_snapshot(conn, ts_ms, net_btc, net_usd, strat_a_btc, strat_b_btc, total_acv)
                if inserted:
                    collector_stats["total_hlp_snapshots"] += inserted
                    inserted_any = True
                collector_stats["last_hlp_net_btc"] = net_btc
            else:
                log.warning("HLP snapshot skipped (vault state missing)")

            # --- OI + funding (metaAndAssetCtxs) ---
            data = fetch_meta_and_asset_ctxs()
            collector_stats["last_derivs_heartbeat"] = time.time()
            if data and isinstance(data, list) and len(data) == 2:
                meta, ctxs = data
                universe = (meta or {}).get("universe") if isinstance(meta, dict) else None
                if isinstance(universe, list) and isinstance(ctxs, list):
                    idx = None
                    for i, u in enumerate(universe):
                        if isinstance(u, dict) and str(u.get("name", "")).upper() == str(DERIVS_COIN).upper():
                            idx = i
                            break
                    if idx is not None and idx < len(ctxs):
                        ctx = ctxs[idx] if isinstance(ctxs[idx], dict) else {}
                        open_interest = _to_float(ctx.get("openInterest"))
                        funding_rate = _to_float(ctx.get("funding"))
                        mark_px = _to_float(ctx.get("markPx"))
                        oracle_px = _to_float(ctx.get("oraclePx"))
                        day_ntl_vlm = _to_float(ctx.get("dayNtlVlm"))
                        premium = _to_float(ctx.get("premium"))
                        if open_interest is not None and funding_rate is not None and mark_px is not None:
                            inserted = store_oi_snapshot(
                                conn,
                                ts_ms,
                                DERIVS_COIN,
                                open_interest,
                                funding_rate,
                                mark_px,
                                oracle_px,
                                day_ntl_vlm,
                                premium,
                            )
                            if inserted:
                                collector_stats["total_oi_snapshots"] += inserted
                                inserted_any = True
                        else:
                            log.warning("OI snapshot skipped (missing required fields for %s)", DERIVS_COIN)
                    else:
                        log.warning("OI snapshot skipped (coin not found: %s)", DERIVS_COIN)
            else:
                log.warning("OI snapshot skipped (metaAndAssetCtxs failed)")

            if inserted_any:
                collector_stats["last_derivs_success_s"] = time.time()

        except Exception as exc:
            log.error("Derivs collector error: %s", exc)
            try:
                if USE_POSTGRES and conn is not None:
                    conn.rollback()
            except Exception:
                pass
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None

        collector_stats["last_derivs_heartbeat"] = time.time()
        time.sleep(DERIVS_INTERVAL)


def _safe_derivs():
    """Wrapper that catches all exceptions so the thread never dies silently."""
    while True:
        try:
            derivs_collector_loop()
            return  # disabled
        except Exception as exc:
            collector_stats["errors"] += 1
            log.error("Derivs crashed: %s — restarting in 30s", exc)
            send_discord_alert(f"**Derivs Crashed**: {exc}")
            time.sleep(30)

# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    with _threads_lock:
        collector_alive = _collector_thread is not None and _collector_thread.is_alive()
        pinger_alive = _pinger_thread is not None and _pinger_thread.is_alive()
        watchdog_alive = _watchdog_thread is not None and _watchdog_thread.is_alive()
        derivs_alive = _derivs_thread is not None and _derivs_thread.is_alive()
    collector_stats["collector_alive"] = collector_alive
    collector_stats["pinger_alive"] = pinger_alive
    collector_stats["derivs_alive"] = derivs_alive

    # Staleness indicators
    hb = collector_stats.get("last_heartbeat")
    hb_age_s = None
    if hb is not None:
        try:
            hb_age_s = time.time() - float(hb)
        except (TypeError, ValueError):
            hb_age_s = None

    trade_ok_s = collector_stats.get("last_trade_success_s")
    trade_ok_age_s = None
    if trade_ok_s is not None:
        try:
            trade_ok_age_s = time.time() - float(trade_ok_s)
        except (TypeError, ValueError):
            trade_ok_age_s = None

    started_s = collector_stats.get("started_s")
    started_age_s = None
    if started_s is not None:
        try:
            started_age_s = time.time() - float(started_s)
        except (TypeError, ValueError):
            started_age_s = None

    trade_stale = False
    if trade_ok_s is None:
        if started_age_s is None or started_age_s > STARTUP_GRACE_SECS:
            trade_stale = True
    elif trade_ok_age_s is not None and trade_ok_age_s > TRADE_STALE_SECS:
        trade_stale = True
    now_ms = int(time.time() * 1000)
    last_candle_ts_ms = collector_stats.get("last_candle_ts_ms")
    candle_data_stale = (
        last_candle_ts_ms is not None and (now_ms - int(last_candle_ts_ms)) > (CANDLE_DATA_STALE_SECS * 1000)
    )

    db_last_ok_s = collector_stats.get("db_last_ok_s")
    db_last_ok_age_s = None
    if db_last_ok_s is not None:
        try:
            db_last_ok_age_s = time.time() - float(db_last_ok_s)
        except (TypeError, ValueError):
            db_last_ok_age_s = None
    db_connected = bool(collector_stats.get("db_connected"))

    derivs_hb = collector_stats.get("last_derivs_heartbeat")
    derivs_hb_age_s = None
    if derivs_hb is not None:
        try:
            derivs_hb_age_s = time.time() - float(derivs_hb)
        except (TypeError, ValueError):
            derivs_hb_age_s = None

    derivs_ok_s = collector_stats.get("last_derivs_success_s")
    derivs_ok_age_s = None
    if derivs_ok_s is not None:
        try:
            derivs_ok_age_s = time.time() - float(derivs_ok_s)
        except (TypeError, ValueError):
            derivs_ok_age_s = None

    status = "ok"
    if not collector_alive or not watchdog_alive:
        status = "degraded"
    elif not db_connected:
        status = "degraded"
    elif trade_stale:
        status = "stale"
    elif candle_data_stale:
        status = "stale"

    resp = dict(collector_stats)
    resp.update({
        "status": status,
        "coin": COIN,
        "watchdog_alive": watchdog_alive,
        "trade_poll_stale": trade_stale,
        "trade_ok_age_s": trade_ok_age_s,
        "candle_data_stale": candle_data_stale,
        "db_connected": db_connected,
        "db_last_ok_age_s": db_last_ok_age_s,
        "loop_heartbeat_age_s": hb_age_s,

        # Derivs health (non-critical path)
        "derivs_enabled": DERIVS_ENABLED,
        "derivs_alive": derivs_alive,
        "derivs_heartbeat_age_s": derivs_hb_age_s,
        "derivs_ok_age_s": derivs_ok_age_s,
    })
    return jsonify(resp)


@app.route("/stats")
def stats():
    return jsonify(collector_stats)


@app.route("/trades")
def get_trades():
    since = request.args.get("since_ms", 0, type=int)
    limit = request.args.get("limit", 1000, type=int)
    limit = min(limit, 10000)
    order = (request.args.get("order", "desc", type=str) or "desc").lower().strip()
    if order not in ("asc", "desc"):
        order = "desc"
    order_sql = "ASC" if order == "asc" else "DESC"

    _init_db()
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            f"SELECT tid, coin, side, px, sz, ts_ms FROM trades "
            f"WHERE coin = %s AND ts_ms >= %s ORDER BY ts_ms {order_sql} LIMIT %s",
            (COIN, since, limit),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    else:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            f"SELECT tid,coin,side,px,sz,ts_ms FROM trades "
            f"WHERE coin = ? AND ts_ms >= ? ORDER BY ts_ms {order_sql} LIMIT ?",
            (COIN, since, limit),
        ).fetchall()
        conn.close()

    return jsonify([
        {"tid": r[0], "coin": r[1], "side": r[2],
         "px": str(r[3]), "sz": str(r[4]), "time": r[5]}
        for r in rows
    ])


@app.route("/candles")
def get_candles():
    since = request.args.get("since_ms", 0, type=int)
    limit = request.args.get("limit", 5000, type=int)
    limit = min(limit, 5000)
    order = (request.args.get("order", "desc", type=str) or "desc").lower().strip()
    if order not in ("asc", "desc"):
        order = "desc"
    order_sql = "ASC" if order == "asc" else "DESC"

    _init_db()
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            f"SELECT ts_ms, coin, open, high, low, close, volume FROM candles_5m "
            f"WHERE coin = %s AND interval = '5m' AND ts_ms >= %s ORDER BY ts_ms {order_sql} LIMIT %s",
            (COIN, since, limit),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    else:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            f"SELECT ts_ms,coin,open,high,low,close,volume FROM candles_5m "
            f"WHERE coin = ? AND ts_ms >= ? ORDER BY ts_ms {order_sql} LIMIT ?",
            (COIN, since, limit),
        ).fetchall()
        conn.close()

    return jsonify([
        {"t": r[0], "coin": r[1], "o": float(r[2]), "h": float(r[3]),
         "l": float(r[4]), "c": float(r[5]), "v": float(r[6])}
        for r in rows
    ])


@app.route("/hlp")
def get_hlp():
    since = request.args.get("since_ms", 0, type=int)
    limit = request.args.get("limit", 1000, type=int)
    limit = min(limit, 10000)
    order = (request.args.get("order", "desc", type=str) or "desc").lower().strip()
    if order not in ("asc", "desc"):
        order = "desc"
    order_sql = "ASC" if order == "asc" else "DESC"

    _init_db()
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            f"SELECT ts_ms, net_btc, net_usd, strat_a_btc, strat_b_btc, total_acv "
            f"FROM hlp_snapshots WHERE ts_ms >= %s ORDER BY ts_ms {order_sql} LIMIT %s",
            (since, limit),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    else:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            f"SELECT ts_ms, net_btc, net_usd, strat_a_btc, strat_b_btc, total_acv "
            f"FROM hlp_snapshots WHERE ts_ms >= ? ORDER BY ts_ms {order_sql} LIMIT ?",
            (since, limit),
        ).fetchall()
        conn.close()

    return jsonify([
        {
            "ts_ms": r[0],
            "net_btc": str(r[1]),
            "net_usd": str(r[2]),
            "strat_a_btc": str(r[3]),
            "strat_b_btc": str(r[4]),
            "total_acv": str(r[5]),
        }
        for r in rows
    ])


@app.route("/oi")
def get_oi():
    since = request.args.get("since_ms", 0, type=int)
    limit = request.args.get("limit", 1000, type=int)
    limit = min(limit, 10000)
    coin = request.args.get("coin", "BTC", type=str)
    coin = (coin or "BTC").upper()
    order = (request.args.get("order", "desc", type=str) or "desc").lower().strip()
    if order not in ("asc", "desc"):
        order = "desc"
    order_sql = "ASC" if order == "asc" else "DESC"

    _init_db()
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            f"SELECT ts_ms, coin, open_interest, funding_rate, mark_px, oracle_px, day_ntl_vlm, premium "
            f"FROM oi_snapshots WHERE coin = %s AND ts_ms >= %s ORDER BY ts_ms {order_sql} LIMIT %s",
            (coin, since, limit),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    else:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            f"SELECT ts_ms, coin, open_interest, funding_rate, mark_px, oracle_px, day_ntl_vlm, premium "
            f"FROM oi_snapshots WHERE coin = ? AND ts_ms >= ? ORDER BY ts_ms {order_sql} LIMIT ?",
            (coin, since, limit),
        ).fetchall()
        conn.close()

    return jsonify([
        {
            "ts_ms": r[0],
            "coin": r[1],
            "open_interest": str(r[2]),
            "funding_rate": str(r[3]),
            "mark_px": str(r[4]),
            "oracle_px": (None if r[5] is None else str(r[5])),
            "day_ntl_vlm": (None if r[6] is None else str(r[6])),
            "premium": (None if r[7] is None else str(r[7])),
        }
        for r in rows
    ])


@app.route("/gaps")
def get_gaps():
    """Return detected data gaps for audit."""
    limit = request.args.get("limit", 100, type=int)
    limit = min(limit, 1000)

    _init_db()
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            "SELECT id, coin, detected_at, last_tid, next_tid, estimated_missing, severity "
            "FROM data_gaps ORDER BY detected_at DESC LIMIT %s",
            (limit,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    else:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            "SELECT id, coin, detected_at, last_tid, next_tid, estimated_missing, severity "
            "FROM data_gaps ORDER BY detected_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()

    return jsonify([
        {"id": r[0], "coin": r[1], "detected_at": r[2], "last_tid": r[3],
         "next_tid": r[4], "estimated_missing": r[5], "severity": r[6]}
        for r in rows
    ])


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

_started = False
_start_lock = threading.Lock()


def start_background_threads():
    """Start threads once per process. Safe to call multiple times."""
    global _collector_thread, _pinger_thread, _watchdog_thread, _derivs_thread, _started
    with _start_lock:
        if _started:
            return
        _started = True

    with _threads_lock:
        _collector_thread = threading.Thread(
            target=_safe_collector, daemon=True, name="collector")
        _collector_thread.start()

        if DERIVS_ENABLED:
            _derivs_thread = threading.Thread(
                target=_safe_derivs, daemon=True, name="derivs")
            _derivs_thread.start()
        else:
            log.info("Derivs collector disabled via DERIVS_ENABLED=0")

        _pinger_thread = threading.Thread(
            target=_safe_pinger, daemon=True, name="pinger")
        _pinger_thread.start()

        _watchdog_thread = threading.Thread(
            target=watchdog_loop, daemon=True, name="watchdog")
        _watchdog_thread.start()

    log.info("Background threads started (collector + derivs + pinger + watchdog) [pid=%d]",
             os.getpid())
    send_discord_alert("**Service Started**: Collector online, db=%s" %
                       ("postgres" if USE_POSTGRES else "sqlite"))


if os.environ.get("DISABLE_BACKGROUND_THREADS", "0") == "1":
    log.info("Background threads disabled via DISABLE_BACKGROUND_THREADS=1")
else:
    start_background_threads()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
