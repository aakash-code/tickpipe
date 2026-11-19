"""The engine computes the right numbers, and batching never changes them."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from helpers import CONTRACTS, WINDOW, run_batch, run_chunked, split
from tickpipe import Pipeline
from tickpipe.pipeline import FeedError
from tickpipe.signal import build


def reference(quotes: pl.DataFrame, trades: pl.DataFrame) -> list[tuple]:
    """The signal spelled out one tick at a time, with no engine involved."""
    n = len(CONTRACTS)
    quote_rows = quotes.rows(named=True)
    latest, next_quote, seen, out = {}, 0, [], []
    for trade in trades.rows(named=True):
        while next_quote < len(quote_rows) and quote_rows[next_quote]["ts"] <= trade["ts"]:
            latest[quote_rows[next_quote]["ticker"]] = quote_rows[next_quote]
            next_quote += 1
        quote = latest.get(trade["ticker"])
        side = 0
        if quote is not None:
            side = 1 if trade["price"] >= quote["ask"] else -1 if trade["price"] <= quote["bid"] else 0
        if trade["ticker"] not in CONTRACTS:
            continue
        signed = side * trade["qty"]
        seen.append((trade["ts"], CONTRACTS.index(trade["ticker"]), signed))
        flows = [0] * n
        for ts, contract, quantity in seen:
            if ts > trade["ts"] - WINDOW:
                flows[contract] += quantity
        level = float(sum(flows))
        slope = sum((2 * i - (n - 1)) * flow for i, flow in enumerate(flows)) / (n - 1)
        out.append((trade["ts"], trade["ticker"], trade["seq"], signed, level, slope))
    return out


def test_sample_exercises_the_awkward_cases(ticks):
    quotes, trades = ticks
    result = run_batch(quotes, trades)
    assert trades.filter(pl.col("ticker") == "OTHER").height > 0  # tickers outside the strip
    assert trades.group_by("ts").len()["len"].max() > 1  # trades sharing a timestamp
    assert trades["ts"].is_in(quotes["ts"].implode()).any()  # a trade and a quote in the same instant
    assert (result["signed_qty"] == 0).any()  # unsigned trades
    assert result["ts"][-1] - result["ts"][0] > 3 * WINDOW  # ticks leave the window


def test_matches_tick_by_tick_reference(ticks):
    quotes, trades = ticks
    assert run_batch(quotes, trades).rows() == reference(quotes, trades)


@pytest.mark.parametrize("max_rows", [1, 3, 40, 700, 100_000])
@pytest.mark.parametrize("seed", [0, 1])
def test_batching_and_arrival_order_do_not_change_the_output(ticks, max_rows, seed):
    quotes, trades = ticks
    expected = run_batch(quotes, trades)
    result = run_chunked(quotes, trades, np.random.default_rng(seed), max_rows)
    assert result.equals(expected)


def test_streamed_historical_run_equals_single_batch(ticks):
    quotes, trades = ticks
    rng = np.random.default_rng(5)
    with Pipeline() as p:
        out = build(p, CONTRACTS, WINDOW).collect()
        p.run_batch({"quotes": iter(split(quotes, rng, 900)), "trades": iter(split(trades, rng, 60))})
    assert out.frame().equals(run_batch(quotes, trades))


def test_rows_out_of_time_order_are_rejected(ticks):
    _, trades = ticks
    with Pipeline() as p:
        build(p, CONTRACTS, WINDOW)
        with pytest.raises(FeedError, match="time order"):
            p.feed("trades", trades.reverse())


def test_rows_behind_the_watermark_are_rejected(ticks):
    _, trades = ticks
    with Pipeline() as p:
        build(p, CONTRACTS, WINDOW)
        p.feed("trades", trades.slice(100, 50))
        with pytest.raises(FeedError, match="time order"):
            p.feed("trades", trades.slice(0, 50))


def test_timestamps_must_be_nanosecond_integers(stored):
    with Pipeline() as p:
        build(p, CONTRACTS, WINDOW)
        with pytest.raises(FeedError, match="Int64 nanoseconds"):
            p.feed("trades", stored[1], 0)
