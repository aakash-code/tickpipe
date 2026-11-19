"""Stores that are a single file on this machine: DuckDB and SQLite."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

import polars as pl

from tickpipe import ticks
from tickpipe.stores import Store, columns_sql, select_sql


class DuckDBStore(Store):
    """A DuckDB file. DuckDB lets one process write at a time, so replay from the
    file after the recording has stopped, not while it runs."""

    kind = "duckdb"
    _TYPES: ClassVar[dict] = {"int": "BIGINT", "float": "DOUBLE", "str": "VARCHAR"}

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        self.path = str((self.root / self.spec["database"]).resolve())
        self._connection = None

    def _connect(self):
        if self._connection is None:
            import duckdb

            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            self._connection = duckdb.connect(self.path)
            for stream, table in self.tables.items():
                self._connection.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql(stream, self._TYPES)})")
        return self._connection

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        connection = self._connect()
        connection.register("tickpipe_batch", frame.to_arrow())
        try:
            connection.execute(f"INSERT INTO {self.tables[stream]} SELECT * FROM tickpipe_batch")
        finally:
            connection.unregister("tickpipe_batch")

    def read_back(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> pl.DataFrame:
        if self._connection is None:
            return super().read_back(stream, start_ns, end_ns)
        # The file is open for writing here, so read through the same connection.
        frame = pl.read_database(select_sql(self.tables[stream], stream, start_ns, end_ns), self._connection)
        return frame.cast(ticks.STREAMS[stream])

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        return {
            "kind": "duckdb",
            "database": self.path,
            "query": select_sql(self.tables[stream], stream, start_ns, end_ns),
        }

    def reset(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    close = reset


class SQLiteStore(Store):
    """A SQLite file in write-ahead mode, so it can be read while it is being recorded to."""

    kind = "sqlite"
    _TYPES: ClassVar[dict] = {"int": "INTEGER", "float": "REAL", "str": "TEXT"}

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        self.path = str((self.root / self.spec["database"]).resolve())
        self._connection = None
        self._insert = {
            stream: f"INSERT INTO {table} VALUES ({', '.join('?' * len(ticks.STREAMS[stream]))})"
            for stream, table in self.tables.items()
        }

    def _connect(self):
        if self._connection is None:
            import sqlite3

            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            for stream, table in self.tables.items():
                connection.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql(stream, self._TYPES)})")
                connection.execute(f"CREATE INDEX IF NOT EXISTS {table}_ts ON {table} (ts)")
            self._connection = connection
        return self._connection

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        connection = self._connect()
        connection.execute("BEGIN")
        try:
            connection.executemany(self._insert[stream], frame.iter_rows())
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        return {
            "kind": "sqlite",
            "database": self.path,
            "query": select_sql(self.tables[stream], stream, start_ns, end_ns),
        }

    def reset(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            finally:
                self._connection = None

    close = reset
