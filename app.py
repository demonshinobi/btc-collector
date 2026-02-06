"""BTC trade + candle collector for Hyperliquid.

Lightweight Flask app that:
1. Polls recentTrades every 10s, candles every 60s
2. Stores in SQLite (ephemeral on Render, backfills candles on restart)
3. Exposes /health, /stats, /trades, /candles endpoints
4. Self-pings every 10 min to prevent Render free-tier spin-down
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
SELF_PING_INTERVAL = 600 # 10 min
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

def get_db():
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
}


def collector_loop():
    log.info("Collector starting...")
    conn = get_db()

    # Backfill last 24h of candles on startup
    now_ms = int(time.time() * 1000)
    backfill = fetch_candles(start_ms=now_ms - (24 * 3600 * 1000), end_ms=now_ms)
    bf_count = store_candles(backfill, conn)
    log.info("Backfilled %d candles (24h)", bf_count)

    collector_stats["started_at"] = datetime.now(timezone.utc).isoformat()
    collector_stats["total_candles"] = conn.execute(
        "SELECT COUNT(*) FROM candles_5m").fetchone()[0]

    last_candle_poll = time.time()

    while True:
        try:
            # Poll trades
            trades = fetch_trades()
            new_trades = store_trades(trades, conn)
            collector_stats["total_trades"] = conn.execute(
                "SELECT COUNT(*) FROM trades").fetchone()[0]
            collector_stats["last_trade_poll"] = datetime.now(timezone.utc).isoformat()
            collector_stats["polls"] += 1

            # Poll candles every CANDLE_INTERVAL
            now = time.time()
            if now - last_candle_poll >= CANDLE_INTERVAL:
                candles = fetch_candles()
                new_candles = store_candles(candles, conn)
                collector_stats["total_candles"] = conn.execute(
                    "SELECT COUNT(*) FROM candles_5m").fetchone()[0]
                collector_stats["last_candle_poll"] = datetime.now(timezone.utc).isoformat()
                last_candle_poll = now

            # Price check
            try:
                collector_stats["last_price"] = fetch_mid_price()
            except Exception:
                pass

            if collector_stats["polls"] % 30 == 0:  # log every 5 min
                log.info("trades=%d candles=%d price=$%.2f polls=%d",
                         collector_stats["total_trades"],
                         collector_stats["total_candles"],
                         collector_stats["last_price"],
                         collector_stats["polls"])

        except Exception as exc:
            collector_stats["errors"] += 1
            log.error("Collector error: %s", exc)

        time.sleep(TRADE_INTERVAL)


def self_ping_loop():
    """Ping our own health endpoint to prevent Render spin-down."""
    service_url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if not service_url:
        log.info("No RENDER_EXTERNAL_URL set, self-ping disabled")
        return

    while True:
        time.sleep(SELF_PING_INTERVAL)
        try:
            requests.get(f"{service_url}/health", timeout=5)
            log.debug("Self-ping OK")
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"status": "ok", "coin": COIN, **collector_stats})


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

def start_background_threads():
    t1 = threading.Thread(target=collector_loop, daemon=True)
    t1.start()
    t2 = threading.Thread(target=self_ping_loop, daemon=True)
    t2.start()
    log.info("Background threads started")


# Start collector when module loads (gunicorn/flask)
start_background_threads()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
