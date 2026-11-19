"""Synthetic ticks for a futures strip, so the pipeline can be tried without market data.

Fifteen minutes of quotes and trades around a regime shift: before it, buyers
lift offers mostly in the front contracts; after it, activity jumps and sellers
hit bids along the strip. The numbers mean nothing beyond giving the signal
something to do.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import polars as pl

TICK = 0.005


def _arrivals(rng: np.random.Generator, span: float, shift: float, base: float, burst: float, decay: float):
    """Event times in [0, span) with rate `base`, plus a burst after `shift` that decays."""
    peak = base + burst
    candidates = np.sort(rng.uniform(0.0, span, rng.poisson(peak * span)))
    rate = base + np.where(candidates >= shift, burst * np.exp(-(candidates - shift) / decay), 0.0)
    return candidates[rng.uniform(0.0, peak, candidates.size) < rate]


def generate(
    contracts: Sequence[str],
    *,
    seed: int = 7,
    start: datetime = datetime(2026, 4, 29, 13, 55),
    minutes: int = 15,
    shift_after_minutes: int = 5,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (quotes, trades), each sorted by time with a per-stream sequence number."""
    rng = np.random.default_rng(seed)
    span, shift = minutes * 60.0, shift_after_minutes * 60.0
    origin = int((start.replace(tzinfo=UTC) - datetime(1970, 1, 1, tzinfo=UTC)) / timedelta(microseconds=1)) * 1000
    quotes, trades = [], []

    for i, ticker in enumerate(contracts):
        depth = i / max(len(contracts) - 1, 1)  # 0 at the front contract, 1 at the back
        activity = 1.3 - 0.6 * depth

        q_time = _arrivals(rng, span, shift, base=7.0 * activity, burst=30.0 * activity, decay=100.0)
        after = q_time >= shift
        fresh = after & (q_time < shift + 120.0)  # prices drift down right after the shift
        step = rng.choice([-1, 0, 1], size=q_time.size, p=[0.1, 0.8, 0.1])
        step = np.where(fresh & (rng.uniform(size=q_time.size) < 0.12), -1, step)
        bid_ticks = int(round((96.30 - 0.05 * i) / TICK)) + np.cumsum(step)
        spread = np.where(rng.uniform(size=q_time.size) < 0.12, 2, 1)
        bid = bid_ticks * TICK
        ask = (bid_ticks + spread) * TICK
        quotes.append(
            pl.DataFrame(
                {
                    "ms": np.floor(q_time * 1000).astype(np.int64),
                    "ticker": ticker,
                    "bid": bid,
                    "ask": ask,
                    "bid_size": rng.integers(50, 4000, q_time.size),
                    "ask_size": rng.integers(50, 4000, q_time.size),
                }
            )
        )

        t_time = _arrivals(rng, span, shift, base=1.1 * activity, burst=3.0 * activity, decay=140.0)
        quote = np.clip(np.searchsorted(q_time, t_time, side="right") - 1, 0, None)
        calm = t_time < shift
        buy_bias = np.where(calm, 0.10 * (1.0 - depth), -0.11 * (1.0 - 0.6 * depth))
        buy = rng.uniform(size=t_time.size) < 0.5 + buy_bias
        price = np.where(buy, ask[quote], bid[quote])
        inside = rng.uniform(size=t_time.size) < 0.04  # some trades print inside the spread
        price = np.where(inside, (bid[quote] + ask[quote]) / 2.0, price)
        size = np.minimum(rng.geometric(0.035, t_time.size), 2000)
        trades.append(
            pl.DataFrame(
                {
                    "ms": np.floor(t_time * 1000).astype(np.int64),
                    "ticker": ticker,
                    "price": price,
                    "qty": size.astype(np.int64),
                }
            )
        )

    def finish(parts: list[pl.DataFrame]) -> pl.DataFrame:
        frame = pl.concat(parts).sort("ms", maintain_order=True)
        stamp = (pl.lit(origin, dtype=pl.Int64) + pl.col("ms") * 1_000_000).cast(pl.Datetime("ns"))
        return frame.select(
            stamp.alias("ts"),
            "ticker",
            pl.int_range(pl.len(), dtype=pl.Int64).alias("seq"),
            pl.exclude("ms", "ticker"),
        )

    return finish(quotes), finish(trades)


def write(contracts: Sequence[str], folder: str | Path, **options) -> tuple[Path, Path]:
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    quotes, trades = generate(contracts, **options)
    quotes_path, trades_path = folder / "quotes.parquet", folder / "trades.parquet"
    quotes.write_parquet(quotes_path)
    trades.write_parquet(trades_path)
    return quotes_path, trades_path
