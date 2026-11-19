"""Historical pipeline: the signal over stored ticks.

With batch_rows = 0 this is one batch call over the whole selection. With a
positive batch_rows the same call streams the data, so it can be larger than
memory. Either way the result is the same.
"""

from __future__ import annotations

import polars as pl

from tickpipe import sources
from tickpipe.pipeline import Pipeline
from tickpipe.settings import Settings
from tickpipe.signal import build


def run(cfg: Settings) -> pl.DataFrame:
    with Pipeline() as p:
        out = build(p, cfg.contracts, cfg.window_ns).collect()
        p.run_batch(
            {
                name: sources.read(cfg.sources[name], root=cfg.root, batch_rows=cfg.batch_rows)
                for name in ("quotes", "trades")
            }
        )
        return out.frame()
