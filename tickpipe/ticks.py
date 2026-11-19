"""The shape of a recorded tick.

Every feed plugin delivers ticks in this shape and every store keeps them in it,
so a recording made from one broker or vendor replays the same way as any other.

Two streams:

* quotes: the best bid and ask (level 1) with their sizes
* trades: the price and quantity of a trade, and the day's cumulative volume
  where the feed reports it

Times are Int64 nanoseconds since the Unix epoch, UTC.

* `ts` is when the recorder received the tick. It strictly increases across both
  streams, so it orders a recording completely and is the clock a replay runs on.
* `exch_ts` is the time the feed itself attached (exchange or vendor time), or
  null when it gave none. It is kept for latency analysis and is never used to
  order ticks: feeds deliver it late, coarse, or out of order.
* `seq` counts the ticks of one stream within one recording, from 0.
"""

from __future__ import annotations

import polars as pl

QUOTES = {
    "ts": pl.Int64,
    "exch_ts": pl.Int64,
    "ticker": pl.String,
    "seq": pl.Int64,
    "bid": pl.Float64,
    "ask": pl.Float64,
    "bid_size": pl.Int64,
    "ask_size": pl.Int64,
}
TRADES = {
    "ts": pl.Int64,
    "exch_ts": pl.Int64,
    "ticker": pl.String,
    "seq": pl.Int64,
    "price": pl.Float64,
    "qty": pl.Int64,
    "volume": pl.Int64,
}
STREAMS: dict[str, dict] = {"quotes": QUOTES, "trades": TRADES}

# Columns that are always present; the others may be null.
REQUIRED = {"ts", "ticker", "seq", "price", "qty"}


def empty(stream: str) -> pl.DataFrame:
    return pl.DataFrame(schema=STREAMS[stream])
