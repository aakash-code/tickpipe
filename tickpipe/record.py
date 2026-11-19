"""Record what the live pipeline actually published, for comparison with a backtest."""

from __future__ import annotations

from pathlib import Path

from tickpipe.pipeline import Pipeline
from tickpipe.settings import Settings
from tickpipe.signal import output_schema


def run(cfg: Settings, path: str | Path, *, idle_timeout: float | None = None) -> int:
    """Subscribe to the signal socket and write every row to Parquet when the stream ends."""
    with Pipeline() as p:
        signal = p.source("signal", output_schema(cfg.contracts, cfg.window_ns))
        signal.to_parquet(path)
        p.run_live({"signal": cfg.signal_url}, idle_timeout=idle_timeout)
        return p.sources["signal"].rows
