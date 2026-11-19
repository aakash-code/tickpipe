"""Replay stored ticks onto the feed sockets, as a stand-in for a live market.

Ticks are published in time slices. Each slice of each stream carries a
watermark, so a quiet stream still tells the pipeline how far it has got and
never holds the other one back.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import numpy as np

from tickpipe import sources, wire
from tickpipe.pipeline import END
from tickpipe.settings import Settings
from tickpipe.signal import QUOTES, TRADES

SCHEMAS = {"quotes": QUOTES, "trades": TRADES}


@dataclass(frozen=True)
class Replayed:
    rows: int
    messages: int
    seconds: float


def run(cfg: Settings, *, speed: float = 30.0, subscribers: int = 1, step_ms: float = 20.0) -> Replayed:
    """Publish the configured ticks at `speed` times real time (0 = as fast as possible).

    Publishing starts once `subscribers` consumers have joined each stream, so
    no tick is sent into the void.
    """
    urls = {"quotes": cfg.quotes_url, "trades": cfg.trades_url}
    frames = {name: sources.read_all(cfg.source(name), root=cfg.root).select(list(SCHEMAS[name])) for name in urls}
    stamps = {name: frame["ts"].to_numpy() for name, frame in frames.items()}
    busy = [name for name in urls if frames[name].height]

    sockets = {}
    for url in dict.fromkeys(urls.values()):
        sharing = sum(1 for other in urls.values() if other == url)
        sockets[url] = wire.publisher(url, subscribers=subscribers * sharing)

    # Without pacing, slices cover a quarter second of market time each.
    step_ns = int(step_ms * 1e6 * speed) if speed > 0 else 250_000_000
    first = min((int(stamps[name][0]) for name in busy), default=0)
    last = max((int(stamps[name][-1]) for name in busy), default=-1)
    sent = dict.fromkeys(urls, 0)
    index = dict.fromkeys(urls, 0)
    started = time.perf_counter()
    edge = first
    try:
        while edge <= last:
            edge += step_ns
            for name, url in urls.items():
                upto = int(np.searchsorted(stamps[name], edge, side="left"))
                chunk = frames[name].slice(sent[name], upto - sent[name])
                sockets[url].send_multipart(wire.pack(name, index[name], chunk, edge - 1))
                sent[name], index[name] = upto, index[name] + 1
            if speed > 0:
                pause = started + (edge - first) / 1e9 / speed - time.perf_counter()
                if pause > 0:
                    time.sleep(pause)
        for name, url in urls.items():
            sockets[url].send_multipart(wire.pack(name, index[name], frames[name].clear(), END))
            index[name] += 1
    finally:
        for socket in sockets.values():
            socket.close()
    return Replayed(sum(sent.values()), sum(index.values()), time.perf_counter() - started)
