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
