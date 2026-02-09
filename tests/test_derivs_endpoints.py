import sqlite3


def _setup_sqlite_db(tmp_path):
    import app as collector_app

    # Use an isolated sqlite file per test run (avoid /tmp collisions).
    collector_app.DB_PATH = str(tmp_path / "collector.db")
    collector_app._db_initialized = False
    collector_app._init_db()
    return collector_app


def test_hlp_endpoint_returns_rows(tmp_path):
    collector_app = _setup_sqlite_db(tmp_path)

    conn = sqlite3.connect(collector_app.DB_PATH, timeout=10)
    collector_app.store_hlp_snapshot(
        conn,
        ts_ms=123,
        net_btc=0.04,
        net_usd=2832.2,
        strat_a_btc=16.88,
        strat_b_btc=-16.84,
        total_acv=14523456.78,
    )
    conn.close()

    client = collector_app.app.test_client()
    resp = client.get("/hlp?limit=10")
    assert resp.status_code == 200
    rows = resp.get_json()
    assert rows and rows[0]["ts_ms"] == 123
    assert rows[0]["net_btc"] == "0.04"
    assert rows[0]["strat_a_btc"] == "16.88"


def test_oi_endpoint_coin_filter(tmp_path):
    collector_app = _setup_sqlite_db(tmp_path)

    conn = sqlite3.connect(collector_app.DB_PATH, timeout=10)
    collector_app.store_oi_snapshot(
        conn,
        ts_ms=100,
        coin="BTC",
        open_interest=12345.6789,
        funding_rate=0.00001234,
        mark_px=70805.5,
        oracle_px=70800.0,
        day_ntl_vlm=987654321.0,
        premium=0.00005678,
    )
    collector_app.store_oi_snapshot(
        conn,
        ts_ms=101,
        coin="ETH",
        open_interest=1.0,
        funding_rate=2.0,
        mark_px=3.0,
    )
    conn.close()

    client = collector_app.app.test_client()
    resp = client.get("/oi?coin=BTC&limit=10")
    assert resp.status_code == 200
    rows = resp.get_json()
    assert len(rows) == 1
    assert rows[0]["coin"] == "BTC"
    assert rows[0]["open_interest"] == "12345.6789"

