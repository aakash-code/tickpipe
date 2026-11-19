"""Stores: where recorded ticks are kept.

A store writes the two tick streams and can describe how to read them back, so
a recording goes straight into `historical`, `replay` or another store without
any export step.

    parquet     a folder of Parquet files, local or on an object store (s3://, gs://, az://)
    duckdb      a DuckDB file
    sqlite      a SQLite file
    postgres    PostgreSQL
    questdb     QuestDB
    clickhouse  ClickHouse
    sqlserver   SQL Server
    sql         anything with a SQLAlchemy URL
    snowflake   Snowflake
    kdb         kdb+

Times are kept as whole nanoseconds everywhere, so a tick reads back exactly as
it was written whatever the store: a nanosecond timestamp type in Parquet, a
64-bit integer in the databases. QuestDB and ClickHouse also get a native
timestamp column (`tick_time`) derived from `ts` for their own time functions.
"""

from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from tickpipe import ticks

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*")


def identifier(name: str) -> str:
    """A table name that is safe to put into SQL as it is."""
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"{name!r} is not usable as a table name: use letters, digits and underscores")
    return name


def columns_sql(stream: str, types: dict[str, str], *, not_null: str = " NOT NULL", nullable: str = "") -> str:
    """Column definitions of a stream's table. `types` maps int, float and str to the store's type names."""
    parts = []
    for name, dtype in ticks.STREAMS[stream].items():
        kind = "str" if dtype == pl.String else "float" if dtype == pl.Float64 else "int"
        parts.append(f"{name} {types[kind]}{not_null if name in ticks.REQUIRED else nullable}")
    return ", ".join(parts)


def select_sql(table: str, stream: str, start_ns: int | None, end_ns: int | None) -> str:
    """The query that reads a stream back in recording order."""
    bounds = []
    if start_ns is not None:
        bounds.append(f"ts >= {int(start_ns)}")
    if end_ns is not None:
        bounds.append(f"ts < {int(end_ns)}")
    where = f" where {' and '.join(bounds)}" if bounds else ""
    return f"select {', '.join(ticks.STREAMS[stream])} from {table}{where} order by ts, seq"


class Store:
    """Base class of a store. Connections are opened on first use, not on construction."""

    kind = ""

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        self.spec = dict(spec)
        self.root = Path(root)
        names = self.spec.get("tables", {})
        unknown = set(names) - set(ticks.STREAMS)
        if unknown:
            raise ValueError(f"tables names unknown streams {sorted(unknown)}; the streams are {sorted(ticks.STREAMS)}")
        self.tables = {stream: identifier(names.get(stream, f"tickpipe_{stream}")) for stream in ticks.STREAMS}

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        """Append one batch of a stream. `frame` has exactly the stream's columns."""
        raise NotImplementedError

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        """A `tickpipe.sources` spec that reads the stream back, optionally between two times."""
        raise NotImplementedError

    def read_back(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> pl.DataFrame:
        """The stored ticks of a stream as one frame, in recording order."""
        from tickpipe import sources

        frames = list(sources.read(self.source_spec(stream, start_ns, end_ns), root=self.root))
        frame = pl.concat(frames) if frames else ticks.empty(stream)
        return frame.select(list(ticks.STREAMS[stream])).cast(ticks.STREAMS[stream])

    def reset(self) -> None:
        """Drop any open connection, so the next write starts from a fresh one."""

    def close(self) -> None:
        """Finish cleanly at the end of a recording."""


def kinds() -> dict[str, type[Store]]:
    from tickpipe.stores.embedded import DuckDBStore, SQLiteStore
    from tickpipe.stores.parquet import ParquetStore
    from tickpipe.stores.servers import (
        ClickHouseStore,
        KdbStore,
        PostgresStore,
        QuestDBStore,
        SnowflakeStore,
        SqlAlchemyStore,
        SqlServerStore,
    )

    return {
        "parquet": ParquetStore,
        "duckdb": DuckDBStore,
        "sqlite": SQLiteStore,
        "postgres": PostgresStore,
        "questdb": QuestDBStore,
        "clickhouse": ClickHouseStore,
        "sqlserver": SqlServerStore,
        "sql": SqlAlchemyStore,
        "snowflake": SnowflakeStore,
        "kdb": KdbStore,
    }


def open_store(spec: dict, root: str | Path = ".") -> Store:
    """Build the store a config table describes."""
    from tickpipe.settings import expand_env

    spec = expand_env(spec)
    available = kinds()
    if spec.get("kind") not in available:
        raise ValueError(f"unknown store kind {spec.get('kind')!r}; choose one of {sorted(available)}")
    return available[spec["kind"]](spec, root)
