"""Tick-by-tick comparison of a live run against the historical run.

Ticks are matched on time, contract and sequence number. A tick present on one
side only counts as a difference, and values must be equal bit for bit.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from tickpipe.signal import KEYS, VALUES
from tickpipe.timefmt import clock


@dataclass(frozen=True)
class Parity:
    historical_rows: int
    live_rows: int
    matched: int
    only_historical: int
    only_live: int
    different: dict[str, int]
    max_abs_diff: dict[str, float]

    @property
    def identical(self) -> bool:
        return not (self.only_historical or self.only_live or any(self.different.values()))


def compare(historical: pl.DataFrame, live: pl.DataFrame) -> tuple[Parity, pl.DataFrame]:
    """Return the verdict and the joined ticks (historical values beside live ones)."""
    for name, frame in (("historical", historical), ("live", live)):
        if frame.select(KEYS).is_duplicated().any():
            raise ValueError(f"{name}: ticks are not unique on {KEYS}")
    left = historical.select(*KEYS, *VALUES, pl.lit(True).alias("in_historical"))
    right = live.select(*KEYS, *(pl.col(v).alias(f"{v}_live") for v in VALUES), pl.lit(True).alias("in_live"))
    joined = left.join(right, on=list(KEYS), how="full", coalesce=True).sort(KEYS)
    both = joined.filter(pl.col("in_historical") & pl.col("in_live"))
    different, max_abs = {}, {}
    for value in VALUES:
        gap = (both[value] - both[f"{value}_live"]).abs()
        different[value] = int((gap != 0).sum())
        max_abs[value] = float(gap.max() or 0.0)
    parity = Parity(
        historical_rows=historical.height,
        live_rows=live.height,
        matched=both.height,
        only_historical=joined["in_live"].is_null().sum(),
        only_live=joined["in_historical"].is_null().sum(),
        different=different,
        max_abs_diff=max_abs,
    )
    return parity, joined


def sample(joined: pl.DataFrame, rows: int = 8, at_ns: int | None = None) -> pl.DataFrame:
    """A few consecutive ticks, starting at `at_ns` or in the middle of the run."""
    start = joined.height // 2 if at_ns is None else int(joined["ts"].search_sorted(at_ns))
    return joined.slice(max(0, min(start, joined.height - rows)), rows)


def _same(row: dict) -> bool:
    return all(row[v] is not None and row[v] == row[f"{v}_live"] for v in VALUES)


def text(parity: Parity, joined: pl.DataFrame) -> str:
    lines = [
        f"historical ticks   {parity.historical_rows:>10,}",
        f"live ticks         {parity.live_rows:>10,}",
        f"matched            {parity.matched:>10,}",
        f"only historical    {parity.only_historical:>10,}",
        f"only live          {parity.only_live:>10,}",
    ]
    lines.extend(
        f"{value + ' differs':<18} {parity.different[value]:>10,}   max abs diff {parity.max_abs_diff[value]:g}"
        for value in VALUES
    )
    lines.append("")
    lines.append(
        f"{'time':<13}{'contract':<10}{'level live':>12}{'historical':>12}{'slope live':>14}{'historical':>14}  same"
    )
    for row in sample(joined).iter_rows(named=True):

        def cell(value: float | None, digits: int, width: int) -> str:
            return f"{'missing' if value is None else format(value, f',.{digits}f'):>{width}}"

        lines.append(
            f"{clock(row['ts']):<13}{row['ticker']:<10}"
            f"{cell(row['level_flow_live'], 0, 12)}{cell(row['level_flow'], 0, 12)}"
            f"{cell(row['slope_flow_live'], 2, 14)}{cell(row['slope_flow'], 2, 14)}"
            f"  {'yes' if _same(row) else 'NO'}"
        )
    lines.append("")
    lines.append(
        "RESULT: live and historical are identical, tick for tick."
        if parity.identical
        else "RESULT: live and historical DIFFER."
    )
    return "\n".join(lines)
