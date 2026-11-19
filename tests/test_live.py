"""The live path over real sockets gives exactly the historical result."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import polars as pl
import pytest

from helpers import free_port, make_settings
from tickpipe import END, Pipeline, compare, historical, live, replay, signal_recorder, wire
from tickpipe.pipeline import FeedError


def parquet_sources() -> dict:
    return {
        "quotes": {"kind": "parquet", "path": "quotes.parquet"},
        "trades": {"kind": "parquet", "path": "trades.parquet"},
    }


@pytest.mark.parametrize("speed", [0.0, 40.0])
def test_replay_through_sockets_equals_historical(parquet_store, tmp_path, speed):
    cfg = make_settings(parquet_store, parquet_sources(), out_dir=tmp_path)
    recorded = tmp_path / "live.parquet"
    with ThreadPoolExecutor(max_workers=3) as pool:
        recorder = pool.submit(signal_recorder.run, cfg, recorded, idle_timeout=60)
        pipeline = pool.submit(live.run, cfg, subscribers=1, idle_timeout=60, quiet=True)
        feed = pool.submit(replay.run, cfg, speed=speed, subscribers=1)
        sent = feed.result(timeout=120)
        published = pipeline.result(timeout=120)
        written = recorder.result(timeout=120)

    expected = historical.run(cfg)
    got = pl.read_parquet(recorded)
    assert sent.rows > 10_000
    assert published == written == expected.height > 1_000
    assert got.equals(expected)
    parity, _ = compare.compare(expected, got)
    assert parity.identical and parity.matched == expected.height


def test_compare_reports_differences(ticks):
    from helpers import run_batch

    expected = run_batch(*ticks)
    tampered = expected.with_columns(
        pl.when(pl.int_range(pl.len()) == 10).then(pl.col("slope_flow") + 1e-9).otherwise(pl.col("slope_flow"))
    ).slice(1)
    parity, _ = compare.compare(expected, tampered)
    assert not parity.identical
    assert parity.only_historical == 1 and parity.only_live == 0
    assert parity.different == {"level_flow": 0, "slope_flow": 1}
    assert 0 < parity.max_abs_diff["slope_flow"] < 1e-6


def test_a_lost_message_stops_the_live_run():
    url = f"tcp://127.0.0.1:{free_port()}"
    schema = {"ts": pl.Int64, "x": pl.Int64}

    def consume() -> None:
        with Pipeline() as p:
            p.source("feed", schema).collect()
            p.run_live({"feed": url}, idle_timeout=30)

    with ThreadPoolExecutor(max_workers=1) as pool:
        consumer = pool.submit(consume)
        socket = wire.publisher(url, subscribers=1, timeout=30)
        row = pl.DataFrame({"ts": [1], "x": [1]})
        socket.send_multipart(wire.pack("feed", 0, row, 0))
        socket.send_multipart(wire.pack("feed", 2, row.clear(), END))  # message 1 never arrives
        with pytest.raises(FeedError, match="expected message 1, got 2"):
            consumer.result(timeout=60)
        socket.close()
