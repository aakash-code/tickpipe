"""Shared pieces for the tests."""

from __future__ import annotations

import socket
from pathlib import Path

import numpy as np
import polars as pl

from tickpipe import END, Pipeline
from tickpipe.settings import Settings
from tickpipe.signal import build

CONTRACTS = tuple(f"FUT{i:02d}" for i in range(1, 6))
WINDOW = 20 * 10**9  # short enough that ticks also leave the window within the sample


def run_batch(quotes: pl.DataFrame, trades: pl.DataFrame) -> pl.DataFrame:
    with Pipeline() as p:
        out = build(p, CONTRACTS, WINDOW).collect()
        p.run_batch({"quotes": quotes, "trades": trades})
        return out.frame()


def split(frame: pl.DataFrame, rng: np.random.Generator, max_rows: int) -> list[pl.DataFrame]:
    parts, at = [], 0
    while at < frame.height:
        size = int(rng.integers(1, max_rows + 1))
        parts.append(frame.slice(at, size))
        at += size
    return parts


def run_chunked(quotes: pl.DataFrame, trades: pl.DataFrame, rng: np.random.Generator, max_rows: int) -> pl.DataFrame:
    """Cut both streams into random pieces and deliver them in a random order."""
    parts = {"quotes": split(quotes, rng, max_rows), "trades": split(trades, rng, max_rows)}
    order = [name for name, chunks in parts.items() for _ in chunks]
    rng.shuffle(order)
    feeds = {name: iter(chunks) for name, chunks in parts.items()}
    with Pipeline() as p:
        out = build(p, CONTRACTS, WINDOW).collect()
        for name in order:
            p.feed(name, next(feeds[name]))
        for name in parts:
            p.feed(name, None, END)
        return out.frame()


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def make_settings(root: Path, sources: dict, **overrides) -> Settings:
    values = {
        "root": root,
        "contracts": CONTRACTS,
        "window_ns": WINDOW,
        "sources": sources,
        "quotes_url": f"tcp://127.0.0.1:{free_port()}",
        "trades_url": f"tcp://127.0.0.1:{free_port()}",
        "signal_url": f"tcp://127.0.0.1:{free_port()}",
        "out_dir": root / "out",
        "batch_rows": 0,
    }
    return Settings(**{**values, **overrides})


# ---- recording --------------------------------------------------------------------


def tick_batch(stream: str, first_seq: int, rows: int, first_ts: int, step: int = 1_000) -> pl.DataFrame:
    """A batch of recorded ticks with every kind of missing value in it."""
    from tickpipe import ticks

    seq = np.arange(first_seq, first_seq + rows)
    columns = {
        "ts": first_ts + (seq - first_seq) * step,
        "exch_ts": [None if i % 4 == 0 else first_ts - 5_000_000 + int(i) for i in seq],
        "ticker": [f"SYM{i % 3}" for i in seq],
        "seq": seq,
    }
    if stream == "quotes":
        columns |= {
            "bid": [None if i % 11 == 0 else 99.5 + i * 0.05 for i in seq],
            "ask": [100.5 + i * 0.05 for i in seq],
            "bid_size": [None if i % 11 == 0 else int(i) for i in seq],
            "ask_size": [None if i % 7 == 0 else int(i) + 1 for i in seq],
        }
    else:
        columns |= {
            "price": [100.0 + i * 0.05 for i in seq],
            "qty": [int(i) % 9 for i in seq],
            "volume": [None if i % 5 == 0 else int(i) * 10 for i in seq],
        }
    return pl.DataFrame(columns, schema=ticks.STREAMS[stream])


class MemoryStore:
    """A store that keeps batches in a list. `delay` makes every write slow."""

    kind = "memory"

    def __init__(self, delay: float = 0.0) -> None:
        self.delay = delay
        self.frames: dict[str, list[pl.DataFrame]] = {"quotes": [], "trades": []}
        self.closed = False

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        import time

        if self.delay:
            time.sleep(self.delay)
        self.frames[stream].append(frame.clone())

    def stored(self, stream: str) -> pl.DataFrame:
        from tickpipe import ticks

        return pl.concat(self.frames[stream]) if self.frames[stream] else ticks.empty(stream)

    def read_back(self, stream: str, start_ns=None, end_ns=None) -> pl.DataFrame:
        frame = self.stored(stream)
        if start_ns is not None:
            frame = frame.filter(pl.col("ts") >= start_ns)
        if end_ns is not None:
            frame = frame.filter(pl.col("ts") < end_ns)
        return frame

    def reset(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


def scripted_feed(rows: int, threads: int = 2):
    """A feed whose threads push `rows` known ticks each, with missing values among them."""
    import threading

    from tickpipe.feeds import Feed

    class Scripted(Feed):
        def start(self, out) -> None:
            self.workers = [threading.Thread(target=self.push, args=(out, k)) for k in range(threads)]
            for worker in self.workers:
                worker.start()

        def push(self, out, k: int) -> None:
            for i in range(rows):
                if i % 3 == 0:
                    out.trade(
                        f"SYM{k}",
                        100.0 + i * 0.05,
                        qty=i % 7,
                        volume=None if i % 5 == 0 else i * 10,
                        exch_ts=10**18 + i,
                    )
                else:
                    missing = i % 11 == 0
                    out.quote(
                        f"SYM{k}",
                        None if missing else 99.5 + i * 0.01,
                        100.5 + i * 0.01,
                        bid_size=None if missing else i,
                        ask_size=i + 1,
                        exch_ts=None if i % 4 == 0 else 10**18 + i,
                    )

        def stop(self) -> None:
            for worker in self.workers:
                worker.join()

    return Scripted({})
