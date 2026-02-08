"""BTC trade + candle collector for Hyperliquid.

Lightweight Flask app that:
1. Polls recentTrades with adaptive interval (5-10s), candles every 60s
2. Stores in PostgreSQL (Render) with SQLite fallback for local dev
3. Exposes /health, /stats, /trades, /candles endpoints
4. Self-pings every 10 min to prevent Render free-tier spin-down
5. Watchdog restarts dead collector threads automatically
6. Gap detection via tid tracking + gap event logging
7. Discord webhook alerting (down, stale, crash loop, gaps)
8. TRADING_ENABLED kill switch for downstream consumers
"""

import os
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
HEARTBEAT_STALE_SECS = 120  # collector wedged if no heartbeat for 2 min

# Database config
DATABASE_URL = os.environ.get("DATABASE_URL", "")
USE_POSTGRES = bool(DATABASE_URL)

# Alerting
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# Kill switch
TRADING_ENABLED = os.environ.get("TRADING_ENABLED", "1") == "1"

# ---------------------------------------------------------------------------
# Database abstraction
# ---------------------------------------------------------------------------

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras
    log.info("Using PostgreSQL: %s...%s", DATABASE_URL[:30], DATABASE_URL[-15:])
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
    return hl_post({"type": "recentTrades", "coin": coin}) or []


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
    return hl_post({"type": "candleSnapshot", "req": req}) or []


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


def store_gap_event(conn, coin, last_tid, next_tid, estimated_missing, severity):
    """Record a detected data gap."""
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
    send_discord_alert(
        f"**Data Gap Detected**\nCoin: {coin}\n"
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
    "total_trades": 0,
    "total_candles": 0,
    "total_gaps": 0,
    "last_trade_poll": None,
    "last_candle_poll": None,
    "last_price": 0.0,
    "polls": 0,
    "errors": 0,
    "restarts": 0,
    "collector_alive": False,
    "pinger_alive": False,
    "last_heartbeat": None,
    "db_type": "postgres" if USE_POSTGRES else "sqlite",
    "trading_enabled": TRADING_ENABLED,
    "last_max_tid": None,
    "current_poll_interval": 10.0,
}

_collector_thread = None
_pinger_thread = None
_watchdog_thread = None
_threads_lock = threading.Lock()


def collector_loop():
    log.info("Collector starting (db=%s)...", "postgres" if USE_POSTGRES else "sqlite")
    collector_stats["collector_alive"] = True

    try:
        conn = get_db()
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
    try:
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
            # Poll trades
            trades = fetch_trades()
            if trades:
                # Gap detection: check if tids are contiguous
                incoming_tids = sorted(int(t["tid"]) for t in trades if "tid" in t)
                if incoming_tids and collector_stats["last_max_tid"] is not None:
                    min_incoming = incoming_tids[0]
                    last_max = collector_stats["last_max_tid"]
                    if min_incoming > last_max + 1:
                        gap_size = min_incoming - last_max - 1
                        severity = "warning" if gap_size < 100 else "critical"
                        store_gap_event(conn, COIN, last_max, min_incoming, gap_size, severity)
                        collector_stats["total_gaps"] += 1

                # Store trades
                new_trades = store_trades(trades, conn)
                collector_stats["total_trades"] += new_trades

                # Update last_max_tid
                if incoming_tids:
                    max_incoming = incoming_tids[-1]
                    if (collector_stats["last_max_tid"] is None or
                            max_incoming > collector_stats["last_max_tid"]):
                        collector_stats["last_max_tid"] = max_incoming

                # Adaptive polling
                if len(trades) > 50:
                    poll_interval = 5.0
                else:
                    poll_interval = 10.0
                collector_stats["current_poll_interval"] = poll_interval

            collector_stats["last_trade_poll"] = datetime.now(timezone.utc).isoformat()
            collector_stats["polls"] += 1
            collector_stats["last_heartbeat"] = time.time()

            # Poll candles every CANDLE_INTERVAL
            now = time.time()
            if now - last_candle_poll >= CANDLE_INTERVAL:
                candles = fetch_candles()
                new_candles = store_candles(candles, conn)
                collector_stats["total_candles"] += new_candles
                collector_stats["last_candle_poll"] = datetime.now(timezone.utc).isoformat()
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

        except Exception as exc:
            collector_stats["errors"] += 1
            log.error("Collector error: %s", exc)
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
    global _collector_thread, _pinger_thread
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

            if collector_dead or collector_wedged:
                reason = "dead" if collector_dead else "wedged (no heartbeat)"
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

        # Staleness alert: no successful poll within 5 min
        hb = collector_stats.get("last_heartbeat")
        if hb is not None and time.time() - hb > 300:
            send_discord_alert(
                f"**Stale Collector**: No successful poll in "
                f"{int(time.time() - hb)}s. Last heartbeat: {hb}"
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
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    with _threads_lock:
        collector_alive = _collector_thread is not None and _collector_thread.is_alive()
        pinger_alive = _pinger_thread is not None and _pinger_thread.is_alive()
        watchdog_alive = _watchdog_thread is not None and _watchdog_thread.is_alive()
    collector_stats["collector_alive"] = collector_alive
    collector_stats["pinger_alive"] = pinger_alive

    # Staleness indicators
    hb = collector_stats.get("last_heartbeat")
    trade_stale = hb is not None and time.time() - hb > 300
    candle_poll = collector_stats.get("last_candle_poll")

    status = "ok"
    if not collector_alive:
        status = "degraded"
    elif trade_stale:
        status = "stale"

    return jsonify({
        "status": status,
        "coin": COIN,
        "db_type": "postgres" if USE_POSTGRES else "sqlite",
        "db_connected": True,
        "trading_enabled": TRADING_ENABLED,
        "watchdog_alive": watchdog_alive,
        "trade_poll_stale": trade_stale,
        "last_trade_poll": collector_stats.get("last_trade_poll"),
        "last_candle_poll": candle_poll,
        "current_poll_interval": collector_stats.get("current_poll_interval", 10.0),
        **collector_stats
    })


@app.route("/stats")
def stats():
    return jsonify(collector_stats)


@app.route("/trades")
def get_trades():
    since = request.args.get("since_ms", 0, type=int)
    limit = request.args.get("limit", 1000, type=int)
    limit = min(limit, 10000)

    _init_db()
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            "SELECT tid, coin, side, px, sz, ts_ms FROM trades "
            "WHERE ts_ms >= %s ORDER BY ts_ms DESC LIMIT %s",
            (since, limit)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    else:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            "SELECT tid,coin,side,px,sz,ts_ms FROM trades "
            "WHERE ts_ms >= ? ORDER BY ts_ms DESC LIMIT ?",
            (since, limit)
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

    _init_db()
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            "SELECT ts_ms, coin, open, high, low, close, volume FROM candles_5m "
            "WHERE ts_ms >= %s ORDER BY ts_ms DESC LIMIT %s",
            (since, limit)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    else:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        rows = conn.execute(
            "SELECT ts_ms,coin,open,high,low,close,volume FROM candles_5m "
            "WHERE ts_ms >= ? ORDER BY ts_ms DESC LIMIT ?",
            (since, limit)
        ).fetchall()
        conn.close()

    return jsonify([
        {"t": r[0], "coin": r[1], "o": float(r[2]), "h": float(r[3]),
         "l": float(r[4]), "c": float(r[5]), "v": float(r[6])}
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
    global _collector_thread, _pinger_thread, _watchdog_thread, _started
    with _start_lock:
        if _started:
            return
        _started = True

    with _threads_lock:
        _collector_thread = threading.Thread(
            target=_safe_collector, daemon=True, name="collector")
        _collector_thread.start()

        _pinger_thread = threading.Thread(
            target=_safe_pinger, daemon=True, name="pinger")
        _pinger_thread.start()

        _watchdog_thread = threading.Thread(
            target=watchdog_loop, daemon=True, name="watchdog")
        _watchdog_thread.start()

    log.info("Background threads started (collector + pinger + watchdog) [pid=%d]",
             os.getpid())
    send_discord_alert("**Service Started**: Collector online, db=%s" %
                       ("postgres" if USE_POSTGRES else "sqlite"))


start_background_threads()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
