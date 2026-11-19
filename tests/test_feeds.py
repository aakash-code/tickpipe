"""Feed plugins: how they are found, and the OpenAlgo plugin against a stand-in for its websocket."""

from __future__ import annotations

import json
import threading
import time

import pytest

from helpers import MemoryStore, free_port
from tickpipe import feeds
from tickpipe.feeds import Feed
from tickpipe.feeds.openalgo import OpenAlgoFeed, event_ns
from tickpipe.recorder import Recorder, RecordingError

PLUGIN = """
from tickpipe.feeds import Feed

class FileFeed(Feed):
    def start(self, out):
        out.trade(self.config["ticker"], 101.5, 3)

    def stop(self):
        pass

class NotAFeed:
    pass
"""


def test_built_in_feeds_are_listed_and_load_by_name():
    assert {"openalgo", "synthetic"} <= set(feeds.available())
    assert feeds.resolve("openalgo") is OpenAlgoFeed


def test_a_plugin_loads_from_a_file_beside_the_config(tmp_path):
    (tmp_path / "plugins").mkdir()
    (tmp_path / "plugins" / "my_broker.py").write_text(PLUGIN, encoding="utf-8")
    feed = feeds.load({"kind": "plugins/my_broker.py:FileFeed", "ticker": "ABC"}, tmp_path)
    store = MemoryStore()
    Recorder(store, flush_seconds=0.02, spill_dir=tmp_path).run(feed, seconds=0.1)
    assert store.stored("trades").select("ticker", "price", "qty").rows() == [("ABC", 101.5, 3)]
    with pytest.raises(TypeError, match="not a tickpipe Feed"):
        feeds.resolve("plugins/my_broker.py:NotAFeed", tmp_path)


def test_a_plugin_loads_from_an_importable_module():
    assert feeds.resolve("tickpipe.feeds.synthetic:SyntheticFeed").__name__ == "SyntheticFeed"


def test_a_plugin_registered_by_an_installed_package_loads_by_name(monkeypatch):
    class Entry:
        name, value = "mybroker", "tickpipe.feeds.synthetic:SyntheticFeed"

    monkeypatch.setattr(feeds.metadata, "entry_points", lambda group: [Entry] if group == "tickpipe.feeds" else [])
    assert "mybroker" in feeds.available()
    assert feeds.resolve("mybroker").__name__ == "SyntheticFeed"


def test_unknown_feed_kind_says_how_to_name_one():
    with pytest.raises(ValueError, match=r"path/to/file\.py:ClassName"):
        feeds.resolve("nosuchbroker")
    with pytest.raises(KeyError, match="needs a kind"):
        feeds.load({})


def test_synthetic_feed_plays_and_stops(tmp_path):
    store = MemoryStore()
    feed = feeds.load({"kind": "synthetic", "minutes": 1, "speed": 600, "contracts": ["A", "B"]})
    Recorder(store, flush_seconds=0.02, spill_dir=tmp_path).run(feed, seconds=1.0)
    quotes, trades = store.stored("quotes"), store.stored("trades")
    assert quotes.height > 500 and trades.height > 50
    assert set(quotes["ticker"].unique()) == {"A", "B"} and quotes["exch_ts"].is_sorted()


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (1756376445, 1756376445 * 10**9),  # seconds
        (1756376445123, 1756376445123 * 10**6),  # milliseconds
        ("1756376445123", 1756376445123 * 10**6),  # digits as text
        (1756376445123456, 1756376445123456 * 10**3),  # microseconds
        (1756376445123456789, 1756376445123456789),  # nanoseconds
        (0, None), (None, None), ("", None), ("09:15:00", None), (12, None), (True, None), (-5, None),
    ],
)  # fmt: skip
def test_broker_timestamps_are_read_by_their_size(value, expected):
    assert event_ns(value) == expected


# ---- the OpenAlgo plugin against a stand-in server --------------------------------------


class FakeOpenAlgo:
    """Speaks the OpenAlgo websocket protocol: checks the key, records requests,
    then sends whatever frames the test queued. One script per connection."""

    def __init__(self, scripts: list[list[dict]], key: str = "good-key") -> None:
        from websockets.sync.server import serve

        self.scripts, self.key = scripts, key
        self.requests: list[dict] = []
        self.connections = 0
        self.port = free_port()
        self.server = serve(self._handle, "127.0.0.1", self.port)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    def _handle(self, connection) -> None:
        index = self.connections
        self.connections += 1
        hello = json.loads(connection.recv())
        self.requests.append(hello)
        if hello.get("api_key") != self.key:
            connection.send(json.dumps({"type": "error", "status": "error", "message": "Invalid API key"}))
            return
        connection.send(json.dumps({"type": "auth", "status": "success", "broker": "test"}))
        expected = 0
        while expected < 2:  # the quote and depth subscriptions
            self.requests.append(json.loads(connection.recv()))
            expected += 1
            connection.send(json.dumps({"type": "subscribe", "status": "success", "subscriptions": []}))
        connection.send("this is not JSON")
        for frame in self.scripts[index] if index < len(self.scripts) else []:
            connection.send(json.dumps(frame))
        if index + 1 < len(self.scripts):
            return  # drop this connection so the client has to reconnect
        time.sleep(1.0)

    def close(self) -> None:
        self.server.shutdown()


def quote_frame(symbol: str, ltp: float, volume: int | None, ltq: int = 0, stamp: int = 1756376445123) -> dict:
    data = {"ltp": ltp, "last_trade_quantity": ltq, "timestamp": stamp}
    if volume is not None:
        data["volume"] = volume
    return {"type": "market_data", "symbol": symbol, "exchange": "NSE", "mode": 2, "data": data}


def depth_frame(symbol: str, bid: float, ask: float, bid_qty: int = 10, ask_qty: int = 20) -> dict:
    depth = {"buy": [{"price": bid, "quantity": bid_qty, "orders": 1}, {"price": bid - 1, "quantity": 5, "orders": 1}],
             "sell": [{"price": ask, "quantity": ask_qty, "orders": 2}]}  # fmt: skip
    return {"type": "market_data", "symbol": symbol, "exchange": "NSE", "mode": 3,
            "data": {"ltp": bid, "depth": depth, "timestamp": 1756376445123}}  # fmt: skip


def run_openalgo(server: FakeOpenAlgo, tmp_path, seconds: float = 1.2, **config) -> MemoryStore:
    store = MemoryStore()
    feed = feeds.load(
        {"kind": "openalgo", "ws_url": server.url, "api_key": "good-key", "symbols": ["NSE:SBIN", "NSE:INFY"],
         "reconnect_seconds": 1, **config}
    )  # fmt: skip
    try:
        Recorder(store, flush_seconds=0.05, spill_dir=tmp_path).run(feed, seconds=seconds)
    finally:
        server.close()
    return store


def test_openalgo_feed_authenticates_subscribes_and_reads_ticks(tmp_path):
    pytest.importorskip("websocket")
    server = FakeOpenAlgo(
        [[
            quote_frame("SBIN", 812.4, 1000),  # first sight: price known, size not
            quote_frame("SBIN", 812.4, 1000),  # nothing traded: skipped
            quote_frame("SBIN", 812.6, 1250, ltq=50),  # 250 traded since the last snapshot
            quote_frame("SBIN", 812.7, 1250),  # price moved, volume did not
            quote_frame("SBIN", 812.9, 40, ltq=40),  # the count restarted
            quote_frame("INFY", 1500.0, None, ltq=7),  # a broker that sends no volume
            quote_frame("INFY", 1500.0, None, ltq=7),  # unchanged: skipped
            quote_frame("INFY", 1500.5, None, ltq=9, stamp=1756376446000),
            quote_frame("INFY", 0, 500),  # no price: not a tick
            depth_frame("SBIN", 812.3, 812.5),
            depth_frame("SBIN", 812.3, 812.5),  # identical: skipped
            depth_frame("SBIN", 812.3, 812.6, ask_qty=25),
            depth_frame("INFY", 0, 1500.6),  # no bid on this side of the book
        ]]
    )  # fmt: skip
    store = run_openalgo(server, tmp_path)

    assert server.requests[0] == {"action": "authenticate", "api_key": "good-key"}
    symbols = [{"exchange": "NSE", "symbol": "SBIN"}, {"exchange": "NSE", "symbol": "INFY"}]
    assert server.requests[1] == {"action": "subscribe", "symbols": symbols, "mode": "Quote"}
    assert server.requests[2] == {"action": "subscribe", "symbols": symbols, "mode": "Depth", "depth": 5}

    stamp = 1756376445123 * 10**6
    assert store.stored("trades").select("ticker", "price", "qty", "volume", "exch_ts").rows() == [
        ("NSE:SBIN", 812.4, 0, 1000, stamp),
        ("NSE:SBIN", 812.6, 250, 1250, stamp),
        ("NSE:SBIN", 812.7, 0, 1250, stamp),
        ("NSE:SBIN", 812.9, 40, 40, stamp),
        ("NSE:INFY", 1500.0, 0, None, stamp),
        ("NSE:INFY", 1500.5, 9, None, 1756376446000 * 10**6),
    ]
    assert store.stored("quotes").select("ticker", "bid", "ask", "bid_size", "ask_size").rows() == [
        ("NSE:SBIN", 812.3, 812.5, 10, 20),
        ("NSE:SBIN", 812.3, 812.6, 10, 25),
        ("NSE:INFY", None, 1500.6, None, 20),
    ]


def test_openalgo_feed_reconnects_and_starts_a_fresh_baseline(tmp_path):
    pytest.importorskip("websocket")
    server = FakeOpenAlgo(
        [
            [quote_frame("SBIN", 812.4, 1000), quote_frame("SBIN", 812.5, 1100)],
            [quote_frame("SBIN", 815.0, 9000), quote_frame("SBIN", 815.1, 9010)],
        ]
    )
    store = run_openalgo(server, tmp_path, seconds=3.5)
    assert server.connections >= 2
    assert [request["action"] for request in server.requests[:6]] == ["authenticate", "subscribe", "subscribe"] * 2
    # 7,900 contracts traded while the connection was down. They are not booked as one trade.
    assert store.stored("trades").select("price", "qty").rows() == [(812.4, 0), (812.5, 100), (815.0, 0), (815.1, 10)]


def test_openalgo_feed_stops_the_recording_when_the_key_is_refused(tmp_path):
    pytest.importorskip("websocket")
    server = FakeOpenAlgo([[]], key="another-key")
    with pytest.raises(RecordingError, match="Invalid API key"):
        run_openalgo(server, tmp_path, seconds=5.0)


def test_openalgo_feed_checks_its_config():
    with pytest.raises(KeyError, match="api_key"):
        OpenAlgoFeed({"symbols": ["NSE:SBIN"]})
    with pytest.raises(ValueError, match="EXCHANGE:SYMBOL"):
        OpenAlgoFeed({"api_key": "k", "symbols": ["SBIN"]})
    with pytest.raises(ValueError, match="modes"):
        OpenAlgoFeed({"api_key": "k", "symbols": ["NSE:SBIN"], "modes": ["level3"]})
    assert OpenAlgoFeed({"api_key": "k", "symbols": ["NSE:SBIN"], "modes": ["depth"]}).trade_mode == 3
    assert issubclass(OpenAlgoFeed, Feed)
