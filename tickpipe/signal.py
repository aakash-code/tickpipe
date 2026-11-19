"""The signal, written once.

Trade flow imbalance across a futures strip: every trade is signed by the side
that crossed the spread, signed contracts are summed per contract over a moving
window, and the per-contract flows are combined into a level and a slope.

`build` is the only place the signal exists. historical.py and live.py both call
it, on the same Pipeline class, and differ only in where ticks come from and
where results go.
"""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

from tickpipe.pipeline import Loading, Pipeline, Stream

QUOTES = {"ts": pl.Int64, "ticker": pl.String, "bid": pl.Float64, "ask": pl.Float64}
TRADES = {"ts": pl.Int64, "ticker": pl.String, "seq": pl.Int64, "price": pl.Float64, "qty": pl.Int64}

KEYS = ("ts", "ticker", "seq")  # identify one output tick
VALUES = ("level_flow", "slope_flow")


def features() -> list[pl.Expr]:
    """Per-trade columns. A trade at or above the ask was a buyer crossing the
    spread, at or below the bid a seller. Anything else, or a trade with no
    quote yet, counts as unsigned."""
    side = (
        pl.when(pl.col("price") >= pl.col("ask"))
        .then(1)
        .when(pl.col("price") <= pl.col("bid"))
        .then(-1)
        .otherwise(0)
        .cast(pl.Int64)
        .alias("side")
    )
    signed_qty = (pl.col("side") * pl.col("qty")).alias("signed_qty")
    return [side, signed_qty]


def loadings(n: int) -> list[Loading]:
    """Level adds the flows of all contracts. Slope weights them linearly from
    -1 at the front contract to +1 at the back one."""
    if n < 2:
        raise ValueError("a strip needs at least two contracts")
    return [
        Loading("level_flow", (1,) * n),
        Loading("slope_flow", tuple(2 * i - (n - 1) for i in range(n)), n - 1),
    ]


def flow_signal(
    p: Pipeline, quotes: Stream, trades: Stream, contracts: Sequence[str], window_ns: int
) -> Stream:
    skinny = p.asof(trades, quotes, by="ticker", take=["bid", "ask"])
    rows = p.enrich(skinny, *features())
    return p.aggregate(
        rows,
        loadings(len(contracts)),
        value="signed_qty",
        by="ticker",
        keys=contracts,
        window_ns=window_ns,
        keep=[*KEYS, "signed_qty"],
    )


def build(p: Pipeline, contracts: Sequence[str], window_ns: int) -> Stream:
    quotes = p.source("quotes", QUOTES)
    trades = p.source("trades", TRADES)
    return flow_signal(p, quotes, trades, contracts, window_ns)


def output_schema(contracts: Sequence[str], window_ns: int) -> dict:
    return dict(build(Pipeline(), contracts, window_ns).schema)
