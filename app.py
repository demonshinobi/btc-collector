"""BTC trade + candle collector for Hyperliquid.

Lightweight Flask app that:
1. Polls recentTrades every 10s, candles every 60s
2. Stores in SQLite (ephemeral on Render, backfills candles on restart)
3. Exposes /health, /stats, /trades, /candles endpoints
4. Self-pings every 10 min to prevent Render free-tier spin-down
5. Watchdog restarts dead collector threads automatically
"""

import os
import sqlite3
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
TRADE_INTERVAL = 10      # seconds
CANDLE_INTERVAL = 60     # seconds
PRICE_INTERVAL = 60      # seconds (was every 10s, wasteful)
SELF_PING_INTERVAL = 600 # 10 min
WATCHDOG_INTERVAL = 30   # check thread health every 30s
DB_PATH = "/tmp/collector.db"
MAX_RETRIES = 3
BASE_DELAY = 0.25

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
        req["startTime"] = now_ms - (6 * 3600 * 1000)  # default 6h
    return hl_post({"type": "candleSnapshot", "req": req}) or []


def fetch_mid_price(coin=COIN):
    data = hl_post({"type": "allMids"})
    if data and coin in data:
        return float(data[coin])
    return 0.0

# ---------------------------------------------------------------------------
# SQLite storage
# ---------------------------------------------------------------------------

_db_initialized = False
_db_init_lock = threading.Lock()


def _init_db():
    """Run DDL once per process, not on every connection."""
    global _db_initialized
    if _db_initialized:
        return
    with _db_init_lock:
        if _db_initialized:
            return
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
        """)
        conn.close()
        _db_initialized = True
        log.info("Database initialized at %s", DB_PATH)


def get_db():
    _init_db()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    return conn


def store_trades(trades, conn):
    if not trades:
        return 0
    before = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    for t in trades:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO trades (tid,coin,side,px,sz,ts_ms) VALUES (?,?,?,?,?,?)",
                (int(t["tid"]), t.get("coin", COIN), t["side"],
                 float(t["px"]), float(t["sz"]), int(t["time"]))
            )
        except (KeyError, ValueError):
            pass
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    return after - before


def store_candles(candles, conn):
    if not candles:
        return 0
    before = conn.execute("SELECT COUNT(*) FROM candles_5m").fetchone()[0]
    for c in candles:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO candles_5m (ts_ms,coin,open,high,low,close,volume) "
                "VALUES (?,?,?,?,?,?,?)",
                (int(c["t"]), COIN, float(c["o"]), float(c["h"]),
                 float(c["l"]), float(c["c"]), float(c["v"]))
            )
        except (KeyError, ValueError):
            pass
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM candles_5m").fetchone()[0]
    return after - before

# ---------------------------------------------------------------------------
# Collector thread
# ---------------------------------------------------------------------------

collector_stats = {
    "started_at": None,
    "total_trades": 0,
    "total_candles": 0,
    "last_trade_poll": None,
    "last_candle_poll": None,
    "last_price": 0.0,
    "polls": 0,
    "errors": 0,
    "restarts": 0,
    "collector_alive": False,
    "pinger_alive": False,
    "last_heartbeat": None,
}

# Thread references for watchdog
_collector_thread = None
_pinger_thread = None
_watchdog_thread = None
_threads_lock = threading.Lock()


def collector_loop():
    log.info("Collector starting...")
    collector_stats["collector_alive"] = True
    conn = get_db()

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
        collector_stats["total_candles"] = conn.execute(
            "SELECT COUNT(*) FROM candles_5m").fetchone()[0]
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

    last_candle_poll = time.time()
    last_price_poll = time.time()

    while True:
        try:
            # Poll trades
            trades = fetch_trades()
            new_trades = store_trades(trades, conn)
            collector_stats["total_trades"] = conn.execute(
                "SELECT COUNT(*) FROM trades").fetchone()[0]
            collector_stats["last_trade_poll"] = datetime.now(timezone.utc).isoformat()
            collector_stats["polls"] += 1
            collector_stats["last_heartbeat"] = time.time()

            # Poll candles every CANDLE_INTERVAL
            now = time.time()
            if now - last_candle_poll >= CANDLE_INTERVAL:
                candles = fetch_candles()
                new_candles = store_candles(candles, conn)
                collector_stats["total_candles"] = conn.execute(
                    "SELECT COUNT(*) FROM candles_5m").fetchone()[0]
                collector_stats["last_candle_poll"] = datetime.now(timezone.utc).isoformat()
                last_candle_poll = now

            # Price check every PRICE_INTERVAL (not every trade poll)
            if now - last_price_poll >= PRICE_INTERVAL:
                try:
                    price = fetch_mid_price()
                    if price and price > 0:
                        collector_stats["last_price"] = price
                except Exception as exc:
                    log.warning("Price fetch failed: %s", exc)
                last_price_poll = now

            if collector_stats["polls"] % 30 == 0:  # log every 5 min
                log.info("trades=%d candles=%d price=$%.2f polls=%d err=%d",
                         collector_stats["total_trades"],
                         collector_stats["total_candles"],
                         collector_stats["last_price"],
                         collector_stats["polls"],
                         collector_stats["errors"])

        except Exception as exc:
            collector_stats["errors"] += 1
            log.error("Collector error: %s", exc)
            # Reconnect DB on error
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = get_db()
            except Exception:
                pass

        time.sleep(TRADE_INTERVAL)


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


HEARTBEAT_STALE_SECS = 120  # collector wedged if no heartbeat for 2 min


def watchdog_loop():
    """Monitor collector and pinger threads, restart if dead or wedged."""
    global _collector_thread, _pinger_thread
    log.info("Watchdog started")

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
                _collector_thread = threading.Thread(
                    target=_safe_collector, daemon=True, name="collector")
                _collector_thread.start()

            if _pinger_thread is None or not _pinger_thread.is_alive():
                collector_stats["pinger_alive"] = False
                log.warning("Pinger thread dead — restarting")
                _pinger_thread = threading.Thread(
                    target=_safe_pinger, daemon=True, name="pinger")
                _pinger_thread.start()


def _safe_collector():
    """Wrapper that catches all exceptions so the thread never dies silently."""
    while True:
        try:
            collector_loop()
        except Exception as exc:
            collector_stats["errors"] += 1
            log.error("Collector crashed: %s — restarting in 5s", exc)
            time.sleep(5)


def _safe_pinger():
    """Wrapper that catches all exceptions so the thread never dies silently."""
    while True:
        try:
            self_ping_loop()
        except Exception as exc:
            log.error("Pinger crashed: %s — restarting in 5s", exc)
        # Always sleep before retry — prevents CPU spin when RENDER_EXTERNAL_URL unset
        time.sleep(SELF_PING_INTERVAL)

# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    # Live thread status check
    with _threads_lock:
        collector_alive = _collector_thread is not None and _collector_thread.is_alive()
        pinger_alive = _pinger_thread is not None and _pinger_thread.is_alive()
        watchdog_alive = _watchdog_thread is not None and _watchdog_thread.is_alive()
    collector_stats["collector_alive"] = collector_alive
    collector_stats["pinger_alive"] = pinger_alive
    status = "ok" if collector_alive else "degraded"
    return jsonify({
        "status": status, "coin": COIN,
        "watchdog_alive": watchdog_alive,
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
    conn = get_db()
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
    conn = get_db()
    rows = conn.execute(
        "SELECT ts_ms,coin,open,high,low,close,volume FROM candles_5m "
        "WHERE ts_ms >= ? ORDER BY ts_ms DESC LIMIT ?",
        (since, limit)
    ).fetchall()
    conn.close()
    return jsonify([
        {"t": r[0], "coin": r[1], "o": r[2], "h": r[3],
         "l": r[4], "c": r[5], "v": r[6]}
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
        # Collector (wrapped in safe loop)
        _collector_thread = threading.Thread(
            target=_safe_collector, daemon=True, name="collector")
        _collector_thread.start()

        # Self-pinger (wrapped in safe loop)
        _pinger_thread = threading.Thread(
            target=_safe_pinger, daemon=True, name="pinger")
        _pinger_thread.start()

        # Watchdog (monitors and restarts dead threads)
        _watchdog_thread = threading.Thread(
            target=watchdog_loop, daemon=True, name="watchdog")
        _watchdog_thread.start()

    log.info("Background threads started (collector + pinger + watchdog) [pid=%d]",
             os.getpid())


# Start collector when module loads (gunicorn/flask)
# Safe: _started guard prevents duplicates if imported multiple times
start_background_threads()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
