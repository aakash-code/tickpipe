"""Where stored ticks come from.

Each stream in the config names a `kind` and the pipeline reads it in place:

    parquet     local files, globs, and object stores (s3://, gs://, az://, https://)
    duckdb      a DuckDB file, or DuckDB in memory as a query layer over other data
    sqlite      a SQLite file
    sql         anything with a SQLAlchemy URL
    postgres    PostgreSQL
    questdb     QuestDB, over its PostgreSQL wire port
    clickhouse  ClickHouse, Arrow over HTTP
    snowflake   Snowflake, Arrow result batches
    sqlserver   SQL Server, through ODBC
    odbc        any other ODBC data source
    kdb         kdb+, over q IPC

Whatever the kind, `read` yields frames with the same shape: `ts` as Int64
nanoseconds, in time order, with the pipeline's column names. Database kinds
take a `query`, and that query must return rows ordered by time.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from tickpipe.settings import expand_env

CANONICAL = ("ts", "ticker", "seq", "bid", "ask", "price", "qty")
_TS_FACTOR = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

Reader = Callable[[dict, Path, int], Iterator[pl.DataFrame]]


def read(spec: dict, *, root: str | Path = ".", batch_rows: int = 0) -> Iterator[pl.DataFrame]:
    """Yield the ticks of one configured stream.

    batch_rows = 0 returns everything as one frame. A positive value streams
    the data in frames of about that many rows, for data larger than memory.
    """
    spec = expand_env(spec)
    kind = spec.get("kind")
    if kind not in READERS:
        raise ValueError(f"unknown source kind {kind!r}; choose one of {sorted(READERS)}")
    rows = 0
    for frame in READERS[kind](spec, Path(root), batch_rows):
        if "seq" not in frame.columns:  # no sequence in the store: number the rows
            frame = frame.with_columns(pl.int_range(rows, rows + frame.height, dtype=pl.Int64).alias("seq"))
        rows += frame.height
        yield frame


def read_all(spec: dict, *, root: str | Path = ".") -> pl.DataFrame:
    frames = list(read(spec, root=root))
    return pl.concat(frames) if len(frames) > 1 else frames[0]


# ---- shaping -----------------------------------------------------------------


def _time_ns(value: str | datetime) -> int:
    moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    delta = moment - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _shape(lf: pl.LazyFrame, spec: dict) -> pl.LazyFrame:
    """Rename columns to the pipeline's names, turn the timestamp into Int64
    nanoseconds, and apply the optional start/end bounds.

    Column names match case-insensitively (Snowflake reports TS for ts). A
    timestamp without a time zone is taken at face value; one with a zone is
    converted to UTC. Use the same convention for every stream.
    """
    schema = lf.collect_schema()
    names = schema.names()
    by_lower = {name.lower(): name for name in names}
    mapping = spec.get("columns", {})
    unknown = set(mapping) - set(CANONICAL)
    if unknown:
        raise ValueError(f"columns maps unknown names {sorted(unknown)}; valid names are {CANONICAL}")

    rename = {}
    for canonical in CANONICAL:
        wanted = mapping.get(canonical, canonical)
        actual = wanted if wanted in schema else by_lower.get(wanted.lower())
        if actual is None:
            if canonical in mapping:
                raise KeyError(f"column {wanted!r} (mapped to {canonical}) not found in {names}")
        elif actual != canonical:
            rename[actual] = canonical
    shadowed = [target for target in rename.values() if target in names]
    if shadowed:
        lf = lf.drop(shadowed)
    if rename:
        lf = lf.rename(rename)

    source_ts = next((old for old, new in rename.items() if new == "ts"), "ts")
    if source_ts not in schema:
        raise KeyError(f"no timestamp column: expected {mapping.get('ts', 'ts')!r} in {names}")
    dtype = schema[source_ts]
    ts = pl.col("ts")
    if isinstance(dtype, pl.Datetime):
        ts = ts.dt.epoch("ns")
    elif dtype.is_integer():
        ts = ts.cast(pl.Int64) * _TS_FACTOR[spec.get("ts_unit", "ns")]
    elif dtype == pl.String:
        ts = ts.str.to_datetime(time_unit="ns").dt.epoch("ns")
    else:
        raise TypeError(
            f"timestamp column {source_ts!r} has type {dtype}. Supply a full timestamp, "
            "or integer epochs together with ts_unit."
        )
    lf = lf.with_columns(ts.cast(pl.Int64).alias("ts"))
    if spec.get("start") is not None:
        lf = lf.filter(pl.col("ts") >= _time_ns(spec["start"]))
    if spec.get("end") is not None:
        lf = lf.filter(pl.col("ts") < _time_ns(spec["end"]))
    return lf


def _shaped(frames: Iterator[pl.DataFrame], spec: dict) -> Iterator[pl.DataFrame]:
    for frame in frames:
        yield _shape(frame.lazy(), spec).collect()


def _local(path: str, root: Path) -> str:
    return path if "://" in path else str((root / path).resolve())


def _query(spec: dict) -> str:
    if not spec.get("query"):
        raise KeyError(f"a {spec['kind']} source needs a query that returns rows ordered by time")
    return spec["query"]


def _run_query(query: str, connection: object, batch_rows: int) -> Iterator[pl.DataFrame]:
    """Polars picks the fastest fetch the driver offers: Arrow batches where the
    driver has them (DuckDB, Snowflake, ODBC), plain cursor rows otherwise."""
    if batch_rows:
        yield from pl.read_database(query, connection, iter_batches=True, batch_size=batch_rows)
    else:
        yield pl.read_database(query, connection)


# ---- readers -----------------------------------------------------------------


def _parquet(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
    path = spec["path"]
    path = [_local(p, root) for p in path] if isinstance(path, list) else _local(path, root)
    options = {key: str(value) for key, value in spec.get("storage_options", {}).items()}
    lf = _shape(pl.scan_parquet(path, storage_options=options or None), spec)
    if spec.get("sort"):
        lf = lf.sort("ts", maintain_order=True)
    if batch_rows:
        yield from lf.collect_batches(chunk_size=batch_rows, maintain_order=True)
    else:
        yield lf.collect()


def _duckdb(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
    import duckdb

    database = spec.get("database", ":memory:")
    in_memory = database == ":memory:"
    connection = duckdb.connect(
        database if in_memory else _local(database, root),
        read_only=spec.get("read_only", not in_memory),
    )
    try:
        yield from _shaped(_run_query(_query(spec), connection, batch_rows), spec)
    finally:
        connection.close()


def _sqlite(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
    import sqlite3

    connection = sqlite3.connect(_local(spec["database"], root))
    try:
        yield from _shaped(_run_query(_query(spec), connection, batch_rows), spec)
    finally:
        connection.close()


def _sql(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
    import sqlalchemy

    engine = sqlalchemy.create_engine(spec["url"])
    try:
        with engine.connect() as connection:
            yield from _shaped(_run_query(_query(spec), connection, batch_rows), spec)
    finally:
        engine.dispose()


def _postgres_like(defaults: dict) -> Reader:
    def reader(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
        import psycopg

        params = {**defaults, **spec.get("connect", {})}
        with psycopg.connect(autocommit=True, **params) as connection:
            yield from _shaped(_run_query(_query(spec), connection, batch_rows), spec)

    return reader


def _clickhouse(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
    import clickhouse_connect

    client = clickhouse_connect.get_client(**spec.get("connect", {}))
    query = _query(spec)
    try:
        if batch_rows:  # the server decides the block size
            with client.query_arrow_stream(query, use_strings=True) as blocks:
                yield from _shaped((pl.from_arrow(block) for block in blocks), spec)
        else:
            yield from _shaped(iter([pl.from_arrow(client.query_arrow(query, use_strings=True))]), spec)
    finally:
        client.close()


def _snowflake(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
    import snowflake.connector

    connection = snowflake.connector.connect(**spec.get("connect", {}))
    try:
        yield from _shaped(_run_query(_query(spec), connection, batch_rows), spec)
    finally:
        connection.close()


def _odbc_value(value: object) -> str:
    text = str(value)
    if any(ch in text for ch in ";{}") or text != text.strip():
        return "{" + text.replace("}", "}}") + "}"
    return text


def sqlserver_connection_string(connect: dict) -> str:
    """Build an ODBC connection string. Without a user it asks for Windows authentication."""
    options = dict(connect)
    driver = options.pop("driver", "ODBC Driver 18 for SQL Server")
    host = options.pop("host", "localhost")
    port = options.pop("port", None)
    parts = [f"Driver={{{driver}}}", f"Server={host},{port}" if port else f"Server={host}"]
    if "database" in options:
        parts.append(f"Database={_odbc_value(options.pop('database'))}")
    if "user" in options:
        parts.append(f"UID={_odbc_value(options.pop('user'))}")
        parts.append(f"PWD={_odbc_value(options.pop('password', ''))}")
    else:
        parts.append("Trusted_Connection=yes")
    parts.extend(f"{key}={_odbc_value(value)}" for key, value in options.items())
    return ";".join(parts)


def _sqlserver(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
    connection = sqlserver_connection_string(spec.get("connect", {}))
    yield from _shaped(_run_query(_query(spec), connection, batch_rows), spec)


def _odbc(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
    yield from _shaped(_run_query(_query(spec), spec["connection_string"], batch_rows), spec)


def _kdb(spec: dict, root: Path, batch_rows: int) -> Iterator[pl.DataFrame]:
    import os

    if spec.get("unlicensed", True):  # q IPC needs no local q licence
        os.environ.setdefault("PYKX_UNLICENSED", "true")
    import pykx

    params = {"host": "localhost", "port": 5000, **spec.get("connect", {})}
    with pykx.SyncQConnection(**params) as q:
        table = q(_query(spec))
    yield from _shaped(iter([pl.from_arrow(table.pa())]), spec)


READERS: dict[str, Reader] = {
    "parquet": _parquet,
    "duckdb": _duckdb,
    "sqlite": _sqlite,
    "sql": _sql,
    "postgres": _postgres_like({"host": "localhost", "port": 5432}),
    "questdb": _postgres_like(
        {"host": "localhost", "port": 8812, "user": "admin", "password": "quest", "dbname": "qdb"}
    ),
    "clickhouse": _clickhouse,
    "snowflake": _snowflake,
    "sqlserver": _sqlserver,
    "odbc": _odbc,
    "kdb": _kdb,
}
