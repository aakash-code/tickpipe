"""Stores that are database servers.

Each one creates its two tables on first use if they are missing, appends
batches with the fastest bulk path its driver offers, and keeps one connection
open for the life of a recording. After a failed write the recorder calls
`reset`, and the next write reconnects.
"""

from __future__ import annotations

import contextlib
import urllib.error
import urllib.request
from pathlib import Path
from typing import ClassVar

import polars as pl

from tickpipe import ticks
from tickpipe.stores import Store, columns_sql, select_sql

_INT64_MIN = -(2**63)


def _index_name(table: str) -> str:
    return table.replace(".", "_") + "_ts"


class PostgresStore(Store):
    """PostgreSQL. Batches are appended with COPY; a block range index on `ts`
    keeps time-range reads fast without slowing the appends."""

    kind = "postgres"
    _TYPES: ClassVar[dict] = {"int": "bigint", "float": "double precision", "str": "text"}
    _DEFAULTS: ClassVar[dict] = {"host": "localhost", "port": 5432}

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        self.params = {**self._DEFAULTS, **self.spec.get("connect", {})}
        self._connection = None
        self._copy = {
            stream: f"COPY {table} ({', '.join(ticks.STREAMS[stream])}) FROM STDIN"
            for stream, table in self.tables.items()
        }

    def _connect(self):
        if self._connection is None:
            import psycopg

            connection = psycopg.connect(autocommit=True, **self.params)
            for stream, table in self.tables.items():
                connection.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql(stream, self._TYPES)})")
                connection.execute(f"CREATE INDEX IF NOT EXISTS {_index_name(table)} ON {table} USING brin (ts)")
            self._connection = connection
        return self._connection

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        with self._connect().cursor() as cursor, cursor.copy(self._copy[stream]) as copy:
            for row in frame.iter_rows():
                copy.write_row(row)

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        return {
            "kind": "postgres",
            "connect": self.spec.get("connect", {}),
            "query": select_sql(self.tables[stream], stream, start_ns, end_ns),
        }

    def reset(self) -> None:
        if self._connection is not None:
            with contextlib.suppress(Exception):  # it is being discarded anyway
                self._connection.close()
            self._connection = None

    close = reset


def _tag(value: str) -> str:
    """Escape a tag value for the line protocol."""
    return value.replace("\\", "\\\\").replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


class QuestDBStore(Store):
    """QuestDB. Tables are created over its PostgreSQL port; ticks are appended
    over HTTP with the line protocol, which is the fast path for ingestion.
    `tick_time` is the designated timestamp, so the tables partition by day."""

    kind = "questdb"
    _TYPES: ClassVar[dict] = {"int": "LONG", "float": "DOUBLE", "str": "SYMBOL"}
    _DEFAULTS: ClassVar[dict] = {
        "host": "localhost",
        "port": 8812,
        "user": "admin",
        "password": "quest",
        "dbname": "qdb",
    }

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        self.params = {**self._DEFAULTS, **self.spec.get("connect", {})}
        self.http_url = str(self.spec.get("http_url", f"http://{self.params['host']}:9000")).rstrip("/")
        self.headers = dict(self.spec.get("http_headers", {}))
        self._ready = False

    def _prepare(self) -> None:
        import psycopg

        with psycopg.connect(autocommit=True, **self.params) as connection:
            for stream, table in self.tables.items():
                columns = columns_sql(stream, self._TYPES, not_null="")
                connection.execute(
                    f"CREATE TABLE IF NOT EXISTS {table} (tick_time TIMESTAMP, {columns}) "
                    "TIMESTAMP(tick_time) PARTITION BY DAY WAL"
                )
        self._ready = True

    def _lines(self, stream: str, frame: pl.DataFrame) -> str:
        table = self.tables[stream]
        names = [name for name in ticks.STREAMS[stream] if name != "ticker"]
        ticker_at = list(ticks.STREAMS[stream]).index("ticker")
        lines = []
        for row in frame.iter_rows():
            values = row[:ticker_at] + row[ticker_at + 1 :]
            fields = ",".join(
                f"{name}={value!r}" if isinstance(value, float) else f"{name}={value}i"
                for name, value in zip(names, values, strict=True)
                if value is not None
            )
            lines.append(f"{table},ticker={_tag(row[ticker_at])} {fields} {row[0]}")
        return "\n".join(lines) + "\n"

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        if not self._ready:
            self._prepare()
        request = urllib.request.Request(
            f"{self.http_url}/write?precision=n",
            data=self._lines(stream, frame).encode("utf-8"),
            headers=self.headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                response.read()
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", "replace")[:300]
            raise OSError(f"QuestDB refused a batch of {stream}: HTTP {error.code} {detail}") from error

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        return {
            "kind": "questdb",
            "connect": self.spec.get("connect", {}),
            "query": select_sql(self.tables[stream], stream, start_ns, end_ns),
        }

    def reset(self) -> None:
        self._ready = False


class ClickHouseStore(Store):
    """ClickHouse. Batches go in as Arrow tables into a MergeTree ordered by
    recording time; `tick_time` is computed by the server from `ts`."""

    kind = "clickhouse"
    _TYPES: ClassVar[dict] = {"int": "Int64", "float": "Float64", "str": "LowCardinality(String)"}

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        self._client = None

    def _columns(self, stream: str) -> str:
        parts = []
        for name, dtype in ticks.STREAMS[stream].items():
            kind = "str" if dtype == pl.String else "float" if dtype == pl.Float64 else "int"
            base = self._TYPES[kind]
            parts.append(f"{name} {base if name in ticks.REQUIRED else f'Nullable({base})'}")
        return ", ".join(parts)

    def _connect(self):
        if self._client is None:
            import clickhouse_connect

            client = clickhouse_connect.get_client(**self.spec.get("connect", {}))
            for stream, table in self.tables.items():
                client.command(
                    f"CREATE TABLE IF NOT EXISTS {table} ({self._columns(stream)}, "
                    "tick_time DateTime64(9) MATERIALIZED fromUnixTimestamp64Nano(ts)) "
                    "ENGINE = MergeTree ORDER BY (ts, seq)"
                )
            self._client = client
        return self._client

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        import pyarrow as pa

        table = frame.to_arrow()
        text = table.schema.get_field_index("ticker")
        table = table.set_column(text, "ticker", table.column("ticker").cast(pa.string()))
        self._connect().insert_arrow(self.tables[stream], table)

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        return {
            "kind": "clickhouse",
            "connect": self.spec.get("connect", {}),
            "query": select_sql(self.tables[stream], stream, start_ns, end_ns),
        }

    def reset(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):  # it is being discarded anyway
                self._client.close()
            self._client = None

    close = reset


class SqlServerStore(Store):
    """SQL Server, through ODBC bulk inserts. The tables are clustered on recording time."""

    kind = "sqlserver"
    _TYPES: ClassVar[dict] = {"int": "BIGINT", "float": "FLOAT", "str": "VARCHAR(64)"}

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        from tickpipe.sources import sqlserver_connection_string

        self.connection_string = sqlserver_connection_string(self.spec.get("connect", {}))
        self._ready = False

    def _prepare(self) -> None:
        import arrow_odbc

        for stream, table in self.tables.items():
            columns = columns_sql(stream, self._TYPES, nullable=" NULL")
            arrow_odbc.read_arrow_batches_from_odbc(
                query=(
                    f"IF OBJECT_ID(N'{table}', N'U') IS NULL BEGIN "
                    f"CREATE TABLE {table} ({columns}); "
                    f"CREATE CLUSTERED INDEX {_index_name(table)} ON {table} (ts, seq); END"
                ),
                connection_string=self.connection_string,
            )
        self._ready = True

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        import arrow_odbc

        if not self._ready:
            self._prepare()
        arrow_odbc.insert_into_table(
            reader=frame.to_arrow().to_reader(),
            chunk_size=min(max(frame.height, 1), 5000),
            table=self.tables[stream],
            connection_string=self.connection_string,
        )

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        return {
            "kind": "sqlserver",
            "connect": self.spec.get("connect", {}),
            "query": select_sql(self.tables[stream], stream, start_ns, end_ns),
        }

    def reset(self) -> None:
        self._ready = False


class SqlAlchemyStore(Store):
    """Any database with a SQLAlchemy URL. The general path: correct everywhere,
    and slower than a store written for one database."""

    kind = "sql"

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        self._engine = None
        self._tables: dict = {}

    def _connect(self):
        if self._engine is None:
            import sqlalchemy as sa

            engine = sa.create_engine(self.spec["url"])
            catalogue = sa.MetaData()
            for stream, table in self.tables.items():
                schema, _, name = table.rpartition(".")
                columns = [
                    sa.Column(
                        column,
                        sa.String(64) if dtype == pl.String else sa.Float(53) if dtype == pl.Float64 else sa.BigInteger,
                        nullable=column not in ticks.REQUIRED,
                        index=column == "ts",
                    )
                    for column, dtype in ticks.STREAMS[stream].items()
                ]
                self._tables[stream] = sa.Table(name, catalogue, *columns, schema=schema or None)
            catalogue.create_all(engine)
            self._engine = engine
        return self._engine

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        engine = self._connect()
        with engine.begin() as connection:
            connection.execute(self._tables[stream].insert(), frame.to_dicts())

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        return {
            "kind": "sql",
            "url": self.spec["url"],
            "query": select_sql(self.tables[stream], stream, start_ns, end_ns),
        }

    def reset(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._tables = {}

    close = reset


class SnowflakeStore(Store):
    """Snowflake. Not verified against a real account: see the README."""

    kind = "snowflake"
    _TYPES: ClassVar[dict] = {"int": "NUMBER(19,0)", "float": "FLOAT", "str": "VARCHAR(64)"}

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        self._connection = None
        self._insert = {
            stream: (
                f"INSERT INTO {table} ({', '.join(ticks.STREAMS[stream])}) "
                f"VALUES ({', '.join(['%s'] * len(ticks.STREAMS[stream]))})"
            )
            for stream, table in self.tables.items()
        }

    def _connect(self):
        if self._connection is None:
            import snowflake.connector

            connection = snowflake.connector.connect(**self.spec.get("connect", {}))
            cursor = connection.cursor()
            for stream, table in self.tables.items():
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql(stream, self._TYPES)})")
            self._connection = connection
        return self._connection

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        self._connect().cursor().executemany(self._insert[stream], frame.rows())

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        return {
            "kind": "snowflake",
            "connect": self.spec.get("connect", {}),
            "query": select_sql(self.tables[stream], stream, start_ns, end_ns),
        }

    def reset(self) -> None:
        if self._connection is not None:
            with contextlib.suppress(Exception):  # it is being discarded anyway
                self._connection.close()
            self._connection = None

    close = reset


def q_table(frame: pl.DataFrame, stream: str):
    """A tick batch as a pandas frame that converts to a q table with q's own
    nulls: 0N for a missing long, 0n for a missing float, symbols for tickers."""
    fills = [
        pl.col(name).fill_null(float("nan") if dtype == pl.Float64 else _INT64_MIN)
        for name, dtype in ticks.STREAMS[stream].items()
        if name not in ticks.REQUIRED
    ]
    return frame.with_columns(fills).to_pandas()


class KdbStore(Store):
    """kdb+ over q IPC. Not verified against a real q process: see the README."""

    kind = "kdb"
    _APPEND = "{[name; rows] $[name in tables[]; name insert rows; name set rows]; count rows}"

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        self.params = {"host": "localhost", "port": 5000, **self.spec.get("connect", {})}
        self._q = None

    def _connect(self):
        if self._q is None:
            import os

            if self.spec.get("unlicensed", True):
                os.environ.setdefault("PYKX_UNLICENSED", "true")
            import pykx

            self._q = pykx.SyncQConnection(**self.params)
        return self._q

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        connection = self._connect()
        import pykx

        connection(self._APPEND, pykx.SymbolAtom(self.tables[stream]), pykx.toq(q_table(frame, stream)))

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        bounds = []
        if start_ns is not None:
            bounds.append(f"ts>={int(start_ns)}")
        if end_ns is not None:
            bounds.append(f"ts<{int(end_ns)}")
        where = f" where {', '.join(bounds)}" if bounds else ""
        columns = ",".join(ticks.STREAMS[stream])
        return {
            "kind": "kdb",
            "connect": self.spec.get("connect", {}),
            "query": f"`ts`seq xasc select {columns} from {self.tables[stream]}{where}",
        }

    def reset(self) -> None:
        if self._q is not None:
            with contextlib.suppress(Exception):  # it is being discarded anyway
                self._q.close()
            self._q = None

    close = reset
