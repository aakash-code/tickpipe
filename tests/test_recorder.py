"""The recorder loses nothing, orders everything, and does not grow while it runs."""

from __future__ import annotations

import threading
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor

import polars as pl
import pytest

from helpers import CONTRACTS, MemoryStore, make_settings, scripted_feed
from tickpipe import historical, live, signal_recorder
from tickpipe.__main__ import main
from tickpipe.feeds import Feed
from tickpipe.recorder import Recorder, RecordingError
from tickpipe.stores import open_store


def recorded(store: MemoryStore) -> tuple[pl.DataFrame, pl.DataFrame]:
    return store.stored("quotes"), store.stored("trades")


def test_every_tick_is_stored_once_and_in_order(tmp_path):
    store = MemoryStore()
    stats = Recorder(store, flush_seconds=0.02, capacity=4096, spill_dir=tmp_path).run(
        scripted_feed(9_000), seconds=1.0
    )
    quotes, trades = recorded(store)
    assert (quotes.height, trades.height) == (12_000, 6_000) and stats.total == 18_000
    assert stats.flushes > 5 and store.closed
    stamps = pl.concat([quotes["ts"], trades["ts"]]).sort()
    assert (stamps.diff().drop_nulls() > 0).all()  # one clock, strictly increasing, across both streams
    for frame in (quotes, trades):
        assert frame["seq"].to_list() == list(range(frame.height))
        for ticker in ("SYM0", "SYM1"):  # each thread's ticks keep the order it sent them in
            own = frame.filter(pl.col("ticker") == ticker)
            assert own["exch_ts"].drop_nulls().is_sorted() and own["ts"].is_sorted()
    assert quotes["bid"].null_count() == quotes["bid_size"].null_count() > 0
    assert quotes["exch_ts"].null_count() > 0 and trades["volume"].null_count() > 0
    assert not quotes["ask"].is_nan().any() and trades["qty"].null_count() == 0


def test_a_slow_store_slows_the_feed_instead_of_losing_ticks(tmp_path):
    store = MemoryStore(delay=0.02)
    recorder = Recorder(store, flush_seconds=0.01, capacity=256, spill_dir=tmp_path)
    stats = recorder.run(scripted_feed(3_000), seconds=3.0)
    quotes, trades = recorded(store)
    assert stats.waits > 0  # the feed threads did have to wait for room
    assert quotes.height + trades.height == 6_000
    assert max(frame.height for frames in store.frames.values() for frame in frames) <= 256


def test_memory_does_not_grow_with_the_length_of_a_recording(tmp_path):
    class Counting(MemoryStore):
        def write(self, stream, frame):
            self.rows = getattr(self, "rows", 0) + frame.height

    recorder = Recorder(Counting(), capacity=4096, verify=False, spill_dir=tmp_path)
    buffers = {id(b) for group in (recorder._active, recorder._spare) for b in group.values()}

    def cycle() -> None:
        for i in range(1_500):
            recorder.quote(f"SYM{i % 40}", 99.5, 100.5, 10, 12, exch_ts=i)
            recorder.trade(f"SYM{i % 40}", 100.0, 5, volume=i)
        recorder._flush()

    for _ in range(40):
        cycle()
    tracemalloc.start()
    before, _ = tracemalloc.get_traced_memory()
    for _ in range(400):
        cycle()
    after, _ = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    assert recorder.store.rows == 440 * 3_000
    assert after - before < 64_000, f"{after - before} bytes kept after 1.2 million more ticks"
    assert {id(b) for group in (recorder._active, recorder._spare) for b in group.values()} == buffers


def test_ticks_go_to_the_spill_folder_while_the_store_is_down_and_can_be_loaded_later(tmp_path):
    class Flaky(MemoryStore):
        """Refuses every write until `up` is set."""

        up = False

        def write(self, stream, frame):
            if not self.up:
                raise ConnectionError("store unavailable")
            super().write(stream, frame)

    store = Flaky()
    recorder = Recorder(store, flush_seconds=0.02, verify=False, spill_dir=tmp_path / "spill")
    for i in range(200):
        recorder.trade("SYM0", 100.0 + i, 1)
    recorder._flush()  # retried, then spilled
    assert recorder.stats.spilled_rows == 200 and recorder.stats.store_errors == 4
    for i in range(50):
        recorder.trade("SYM0", 300.0 + i, 1)
    recorder._flush()  # the store is known to be down: one try, then spilled
    assert recorder.stats.spilled_rows == 250 and recorder.stats.store_errors == 5
    store.up = True
    for i in range(30):
        recorder.trade("SYM0", 400.0 + i, 1)
    recorder._flush(final=True)
    recorder._close()
    assert store.stored("trades").height == 30 and recorder.stats.spilled_rows == 250

    # Loading the spilled ticks into a store afterwards restores the whole recording.
    target = {"kind": "sqlite", "database": "ticks.sqlite"}
    config = tmp_path / "config.toml"
    config.write_text(
        '[stores.ticks]\nkind = "sqlite"\ndatabase = "ticks.sqlite"\n[run]\nout_dir = "."\n', encoding="utf-8"
    )
    assert main(["--config", str(config), "copy", "--from", "spill", "--to", "ticks"]) == 0
    restored = open_store(target, tmp_path).read_back("trades")
    assert restored["seq"].to_list() == list(range(250)) and restored["ts"].is_sorted()


def test_a_store_that_changes_ticks_is_caught_on_the_first_batch(tmp_path):
    class Lossy(MemoryStore):
        def write(self, stream, frame):
            super().write(stream, frame.with_columns(pl.col("ts") // 1_000 * 1_000))  # keeps microseconds only

    recorder = Recorder(Lossy(), flush_seconds=0.02, spill_dir=tmp_path)
    with pytest.raises(RecordingError, match="did not return the first"):
        recorder.run(scripted_feed(500), seconds=1.0)
    assert recorder.store.closed


def test_a_feed_that_fails_stops_the_recording_and_keeps_what_arrived(tmp_path):
    class Dying(Feed):
        def start(self, out):
            for i in range(100):
                out.trade("SYM0", 100.0 + i, 1)
            threading.Timer(0.05, out.fail, [ConnectionError("broker closed the session")]).start()

        def stop(self):
            pass

    store = MemoryStore()
    with pytest.raises(RecordingError, match="broker closed the session"):
        Recorder(store, flush_seconds=0.02, spill_dir=tmp_path).run(Dying({}))
    assert store.stored("trades").height == 100 and store.closed


def test_a_feed_that_cannot_start_still_closes_the_store(tmp_path):
    class Broken(Feed):
        def start(self, out):
            raise PermissionError("bad API key")

        def stop(self):
            raise RuntimeError("was never started")

    store = MemoryStore()
    with pytest.raises(PermissionError, match="bad API key"):
        Recorder(store, spill_dir=tmp_path).run(Broken({}))
    assert store.closed


def test_stop_ends_a_recording_cleanly(tmp_path):
    store = MemoryStore()
    recorder = Recorder(store, flush_seconds=0.02, spill_dir=tmp_path)
    threading.Timer(0.3, recorder.stop).start()
    started = time.monotonic()
    recorder.run(scripted_feed(2_000))
    assert time.monotonic() - started < 5 and store.closed
    assert sum(frame.height for frame in recorded(store)) == 4_000


def test_ticks_recorded_while_publishing_give_the_same_signal_when_replayed_from_the_store(ticks, tmp_path):
    """Record a feed and run the live signal on it at the same time. A historical
    run over the stored recording afterwards must produce exactly that signal."""
    quotes, trades = ticks

    class Play(Feed):
        """Sends the sample quotes and trades in their original time order."""

        def start(self, out):
            q = quotes.select("ts", "ticker", "bid", "ask").with_columns(pl.lit(0).alias("kind"))
            t = trades.select(
                "ts", "ticker", pl.col("price").alias("bid"), pl.col("qty").cast(pl.Float64).alias("ask")
            ).with_columns(pl.lit(1).alias("kind"))
            for ts, ticker, a, b, kind in pl.concat([q, t]).sort("ts", maintain_order=True).iter_rows():
                if kind == 0:
                    out.quote(ticker, a, b, exch_ts=ts)
                else:
                    out.trade(ticker, a, int(b), exch_ts=ts)

        def stop(self):
            pass

    cfg = make_settings(
        tmp_path,
        {"quotes": {"store": "ticks"}, "trades": {"store": "ticks"}},
        stores={"ticks": {"kind": "parquet", "path": "ticks"}},
        out_dir=tmp_path,
        window_ns=3_000_000,  # 3 ms of recording time, so ticks also leave the window
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        signal_file = tmp_path / "live.parquet"
        listener = pool.submit(signal_recorder.run, cfg, signal_file, idle_timeout=60)
        pipeline = pool.submit(live.run, cfg, subscribers=1, idle_timeout=60, quiet=True)
        recorder = Recorder(
            cfg.store("ticks"),
            flush_seconds=0.01,
            spill_dir=tmp_path / "spill",
            publish={"quotes": cfg.quotes_url, "trades": cfg.trades_url},
            subscribers=1,
        )
        stats = recorder.run(Play({}), seconds=0.5)
        published, written = pipeline.result(timeout=60), listener.result(timeout=60)

    assert stats.rows == {"quotes": quotes.height, "trades": trades.height}
    from_store = historical.run(cfg)
    live_signal = pl.read_parquet(signal_file)
    assert published == written == from_store.height == trades.filter(pl.col("ticker").is_in(CONTRACTS)).height
    assert live_signal.equals(from_store)
    assert from_store["level_flow"].n_unique() > 100  # a real signal, not a constant
