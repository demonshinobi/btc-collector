import sqlite3


def _setup_sqlite_db(tmp_path):
    import app as collector_app

    # Use an isolated sqlite file per test run (avoid /tmp collisions).
    collector_app.DB_PATH = str(tmp_path / "collector.db")
    collector_app._db_initialized = False
    collector_app._init_db()
    return collector_app


def test_candles_endpoint_order_param(tmp_path):
    collector_app = _setup_sqlite_db(tmp_path)

    conn = sqlite3.connect(collector_app.DB_PATH, timeout=10)
    collector_app.store_candles(
        [
            {"t": 100, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 10},
            {"t": 200, "o": 2, "h": 3, "l": 1.5, "c": 2.5, "v": 20},
            {"t": 300, "o": 3, "h": 4, "l": 2.5, "c": 3.5, "v": 30},
        ],
        conn,
    )
    conn.close()

    client = collector_app.app.test_client()

    # Default behavior: newest-first (DESC)
    resp = client.get("/candles?since_ms=0&limit=2")
    assert resp.status_code == 200
    rows = resp.get_json()
    assert [r["t"] for r in rows] == [300, 200]

    # Ascending order for safe forward paging
    resp = client.get("/candles?since_ms=0&limit=2&order=asc")
    assert resp.status_code == 200
    rows = resp.get_json()
    assert [r["t"] for r in rows] == [100, 200]

    # Asc + since_ms should page forward
    resp = client.get("/candles?since_ms=150&limit=2&order=asc")
    assert resp.status_code == 200
    rows = resp.get_json()
    assert [r["t"] for r in rows] == [200, 300]

