import time


class _DummyThread:
    def __init__(self, alive: bool):
        self._alive = alive

    def is_alive(self) -> bool:
        return self._alive


def _set_threads(mod, *, collector=True, pinger=True, watchdog=True):
    mod._collector_thread = _DummyThread(collector)
    mod._pinger_thread = _DummyThread(pinger)
    mod._watchdog_thread = _DummyThread(watchdog)


def test_health_ok():
    import app as collector_app

    _set_threads(collector_app, collector=True, pinger=True, watchdog=True)
    collector_app.collector_stats["started_s"] = time.time() - 60
    collector_app.collector_stats["last_heartbeat"] = time.time()
    collector_app.collector_stats["last_trade_success_s"] = time.time()
    collector_app.collector_stats["last_candle_ts_ms"] = int(time.time() * 1000)
    collector_app.collector_stats["db_connected"] = True
    collector_app.collector_stats["db_last_ok_s"] = time.time()

    client = collector_app.app.test_client()
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "ok"
    assert data["watchdog_alive"] is True
    assert data["trade_poll_stale"] is False
    assert data["candle_data_stale"] is False
    assert data["db_connected"] is True
    assert data["db_last_ok_age_s"] is not None


def test_health_trade_stale():
    import app as collector_app

    _set_threads(collector_app, collector=True, pinger=True, watchdog=True)
    collector_app.collector_stats["started_s"] = time.time() - 3600
    collector_app.collector_stats["last_heartbeat"] = time.time()
    collector_app.collector_stats["last_trade_success_s"] = time.time() - (collector_app.TRADE_STALE_SECS + 1)
    collector_app.collector_stats["last_candle_ts_ms"] = int(time.time() * 1000)
    collector_app.collector_stats["db_connected"] = True

    client = collector_app.app.test_client()
    resp = client.get("/health")
    data = resp.get_json()
    assert data["trade_poll_stale"] is True
    assert data["status"] == "stale"


def test_health_candle_data_stale():
    import app as collector_app

    _set_threads(collector_app, collector=True, pinger=True, watchdog=True)
    collector_app.collector_stats["started_s"] = time.time() - 3600
    collector_app.collector_stats["last_heartbeat"] = time.time()
    collector_app.collector_stats["last_trade_success_s"] = time.time()
    collector_app.collector_stats["last_candle_ts_ms"] = int(time.time() * 1000) - (
        (collector_app.CANDLE_DATA_STALE_SECS * 1000) + 1
    )
    collector_app.collector_stats["db_connected"] = True

    client = collector_app.app.test_client()
    resp = client.get("/health")
    data = resp.get_json()
    assert data["candle_data_stale"] is True
    assert data["status"] == "stale"


def test_health_degraded_when_threads_down():
    import app as collector_app

    _set_threads(collector_app, collector=False, pinger=True, watchdog=True)
    collector_app.collector_stats["started_s"] = time.time() - 3600
    collector_app.collector_stats["last_heartbeat"] = time.time()
    collector_app.collector_stats["last_trade_success_s"] = time.time()
    collector_app.collector_stats["db_connected"] = True

    client = collector_app.app.test_client()
    resp = client.get("/health")
    data = resp.get_json()
    assert data["status"] == "degraded"
