from __future__ import annotations

import polars as pl
import pytest

from helpers import CONTRACTS
from tickpipe import synth


@pytest.fixture(scope="session")
def stored():
    """Quotes and trades as they sit in a store: Datetime timestamps, extra
    columns, and one ticker that is not part of the strip."""
    return synth.generate([*CONTRACTS, "OTHER"], seed=3, minutes=2, shift_after_minutes=1)


@pytest.fixture(scope="session")
def ticks(stored):
    """The same ticks as the engine sees them: ts in Int64 nanoseconds."""
    quotes, trades = (frame.with_columns(pl.col("ts").dt.epoch("ns")) for frame in stored)
    return quotes, trades


@pytest.fixture(scope="session")
def parquet_store(stored, tmp_path_factory):
    folder = tmp_path_factory.mktemp("store")
    stored[0].write_parquet(folder / "quotes.parquet")
    stored[1].write_parquet(folder / "trades.parquet", row_group_size=500)
    return folder
