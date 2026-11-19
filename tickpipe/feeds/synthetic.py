"""A feed that plays synthetic ticks in real time, for trying the recorder with no account.

Config:

    [feeds.demo]
    kind = "synthetic"
    contracts = ["FUT01", "FUT02", "FUT03"]   # default: three contracts
    minutes = 15                              # how much synthetic market to play
    speed = 60                                # multiple of real time
    loop = false                              # start again when it ends
"""

from __future__ import annotations

import threading
import time

import numpy as np
import polars as pl

from tickpipe import synth
from tickpipe.feeds import Feed, TickSink


class SyntheticFeed(Feed):
    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.contracts = list(config.get("contracts", ["FUT01", "FUT02", "FUT03"]))
        self.minutes = int(config.get("minutes", 15))
        self.speed = float(config.get("speed", 60))
        self.loop = bool(config.get("loop", False))
        self.seed = int(config.get("seed", 7))
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self, out: TickSink) -> None:
        self._thread = threading.Thread(target=self._play, args=(out,), name="tickpipe-synthetic", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _events(self) -> list[tuple]:
        """Quotes and trades merged into one list in time order."""
        quotes, trades = synth.generate(
            self.contracts, seed=self.seed, minutes=self.minutes, shift_after_minutes=max(1, self.minutes // 3)
        )
        quotes = quotes.with_columns(pl.col("ts").dt.epoch("ns"), pl.lit(0).alias("kind"))
        trades = trades.with_columns(pl.col("ts").dt.epoch("ns"), pl.lit(1).alias("kind"))
        q = quotes.select("ts", "kind", "ticker", "bid", "ask", "bid_size", "ask_size").rows()
        t = trades.select("ts", "kind", "ticker", "price", "qty").rows()
        order = np.argsort(np.array([row[0] for row in q] + [row[0] for row in t]), kind="stable")
        merged = q + t
        return [merged[i] for i in order]

    def _play(self, out: TickSink) -> None:
        try:
            events = self._events()
            while not self._stop.is_set():
                origin, started = events[0][0], time.monotonic()
                for event in events:
                    due = started + (event[0] - origin) / 1e9 / self.speed if self.speed > 0 else 0.0
                    pause = due - time.monotonic()
                    if pause > 0 and self._stop.wait(pause):
                        return
                    if self._stop.is_set():
                        return
                    if event[1] == 0:
                        out.quote(event[2], event[3], event[4], event[5], event[6], exch_ts=event[0])
                    else:
                        out.trade(event[2], event[3], event[4], exch_ts=event[0])
                if not self.loop:
                    return
        except Exception as error:  # noqa: BLE001 - report it instead of dying silently
            out.fail(error)
