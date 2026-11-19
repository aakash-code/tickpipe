"""Every store that can run on this machine yields the same ticks, and the same signal."""

from __future__ import annotations

import sqlite3
from datetime import datetime

import polars as pl
import pytest

from helpers import make_settings, run_batch
from tickpipe import historical, sources

TRADE_COLUMNS = ["ts", "ticker", "seq", "price", "qty"]
QUOTE_COLUMNS = ["ts", "ticker", "seq", "bid", "ask"]


def all_rows(spec: dict, root, batch_rows: int = 0) -> pl.DataFrame:
    return pl.concat(list(sources.read(spec, root=root, batch_rows=batch_rows)))


# ---- parquet -------------------------------------------------------------------


def test_parquet_single_batch(parquet_store, ticks):
    got = all_rows({"kind": "parquet", "path": "trades.parquet"}, parquet_store)
    assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))


def test_parquet_streamed_in_batches(parquet_store, ticks):
    frames = list(sources.read({"kind": "parquet", "path": "trades.parquet"}, root=parquet_store, batch_rows=400))
    assert len(frames) > 3
    assert pl.concat(frames).select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))


def test_parquet_glob_over_several_files(stored, ticks, tmp_path):
    trades = stored[1]
    half = trades.height // 2
    trades.slice(0, half).write_parquet(tmp_path / "part-0.parquet")
    trades.slice(half).write_parquet(tmp_path / "part-1.parquet")
    got = all_rows({"kind": "parquet", "path": "part-*.parquet"}, tmp_path)
    assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))


def test_foreign_column_names_and_millisecond_epochs(stored, ticks, tmp_path):
    foreign = stored[1].select(
        (pl.col("ts").dt.epoch("ms")).alias("EXCH_TIME"),
        pl.col("ticker").alias("SYM"),
        pl.col("price").alias("PX"),
        pl.col("qty").alias("SIZE"),
    )
    foreign.write_parquet(tmp_path / "foreign.parquet")
    spec = {
        "kind": "parquet",
        "path": "foreign.parquet",
        "ts_unit": "ms",
        "columns": {"ts": "exch_time", "ticker": "sym", "price": "px", "qty": "size"},
    }
    got = all_rows(spec, tmp_path, batch_rows=700)
    assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))  # seq was numbered for us


def test_time_zone_aware_and_string_timestamps(stored, ticks, tmp_path):
    trades = stored[1]
    trades.with_columns(
        pl.col("ts").dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Kolkata").dt.cast_time_unit("us")
    ).write_parquet(tmp_path / "zoned.parquet")
    trades.with_columns(pl.col("ts").dt.strftime("%Y-%m-%d %H:%M:%S%.3f")).write_parquet(tmp_path / "text.parquet")
    for name in ("zoned.parquet", "text.parquet"):
        got = all_rows({"kind": "parquet", "path": name}, tmp_path)
        assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS)), name


def test_start_and_end_bounds(parquet_store, ticks):
    spec = {
        "kind": "parquet",
        "path": "trades.parquet",
        "start": "2026-04-29 13:55:30",
        "end": datetime(2026, 4, 29, 13, 56, 0),
    }
    got = all_rows(spec, parquet_store)
    low, high = sources._time_ns("2026-04-29 13:55:30"), sources._time_ns("2026-04-29 13:56:00")
    expected = ticks[1].filter((pl.col("ts") >= low) & (pl.col("ts") < high))
    assert 0 < expected.height < ticks[1].height
    assert got.select(TRADE_COLUMNS).equals(expected.select(TRADE_COLUMNS))


def test_sort_option_orders_an_unsorted_file(stored, ticks, tmp_path):
    stored[1].sample(fraction=1.0, shuffle=True, seed=1).write_parquet(tmp_path / "shuffled.parquet")
    got = all_rows({"kind": "parquet", "path": "shuffled.parquet", "sort": True}, tmp_path)
    assert got["ts"].is_sorted()
    assert got.sort("seq").select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))


def test_s3_object_store(stored, ticks):
    """Parquet on S3, served by a local emulation of the S3 API."""
    boto3 = pytest.importorskip("boto3")
    server_module = pytest.importorskip("moto.server")
    import io

    from helpers import free_port

    port = free_port()
    server = server_module.ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    try:
        endpoint = f"http://127.0.0.1:{port}"
        client = boto3.client(
            "s3", endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
        )
        client.create_bucket(Bucket="ticks")
        half = stored[1].height // 2
        for index, part in enumerate((stored[1].slice(0, half), stored[1].slice(half))):
            body = io.BytesIO()
            part.write_parquet(body)
            client.put_object(Bucket="ticks", Key=f"trades/date=2026-04-29/part-{index}.parquet", Body=body.getvalue())
        spec = {
            "kind": "parquet",
            "path": "s3://ticks/trades/date=2026-04-29/*.parquet",
            "storage_options": {
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test",
                "aws_region": "us-east-1",
                "aws_endpoint_url": endpoint,
                "aws_allow_http": "true",
            },
        }
        for batch_rows in (0, 500):
            got = all_rows(spec, ".", batch_rows=batch_rows)
            assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS)), batch_rows
    finally:
        server.stop()


# ---- databases -----------------------------------------------------------------


@pytest.fixture(scope="module")
def duckdb_file(stored, tmp_path_factory):
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path_factory.mktemp("duck") / "ticks.duckdb"
    with duckdb.connect(str(path)) as connection:
        for name, frame in zip(("quotes", "trades"), stored, strict=True):
            connection.register("incoming", frame.to_arrow())
            connection.execute(f"create table {name} as select * from incoming")
            connection.unregister("incoming")
    return path


@pytest.fixture(scope="module")
def sqlite_file(ticks, tmp_path_factory):
    path = tmp_path_factory.mktemp("sqlite") / "ticks.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("create table trades (ts integer, ticker text, seq integer, price real, qty integer)")
        connection.executemany("insert into trades values (?, ?, ?, ?, ?)", ticks[1].select(TRADE_COLUMNS).rows())
        connection.execute("create table quotes (ts integer, ticker text, seq integer, bid real, ask real)")
        connection.executemany("insert into quotes values (?, ?, ?, ?, ?)", ticks[0].select(QUOTE_COLUMNS).rows())
    connection.close()
    return path


@pytest.mark.parametrize("batch_rows", [0, 600])
def test_duckdb(duckdb_file, ticks, batch_rows):
    spec = {"kind": "duckdb", "database": duckdb_file.name, "query": "select * from trades order by ts, seq"}
    got = all_rows(spec, duckdb_file.parent, batch_rows=batch_rows)
    assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))


def test_duckdb_in_memory_as_a_query_layer_over_parquet(parquet_store, ticks):
    path = (parquet_store / "trades.parquet").as_posix()
    spec = {"kind": "duckdb", "query": f"select * from read_parquet('{path}') order by ts, seq"}
    got = all_rows(spec, parquet_store, batch_rows=900)
    assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))


@pytest.mark.parametrize("batch_rows", [0, 600])
def test_sqlite_through_a_plain_database_cursor(sqlite_file, ticks, batch_rows):
    spec = {"kind": "sqlite", "database": sqlite_file.name, "query": "select * from trades order by ts, seq"}
    got = all_rows(spec, sqlite_file.parent, batch_rows=batch_rows)
    assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))


@pytest.mark.parametrize("batch_rows", [0, 600])
def test_sqlalchemy_url(sqlite_file, ticks, batch_rows):
    pytest.importorskip("sqlalchemy")
    spec = {
        "kind": "sql",
        "url": f"sqlite:///{sqlite_file.as_posix()}",
        "query": "select ts, ticker, price, qty from trades order by ts, seq",
    }
    got = all_rows(spec, ".", batch_rows=batch_rows)
    assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))  # seq numbered for us


def test_signal_is_the_same_whichever_store_holds_the_ticks(parquet_store, duckdb_file, sqlite_file, ticks):
    expected = run_batch(*ticks)
    stores = {
        "parquet": {
            "quotes": {"kind": "parquet", "path": str(parquet_store / "quotes.parquet")},
            "trades": {"kind": "parquet", "path": str(parquet_store / "trades.parquet")},
        },
        "duckdb": {
            name: {"kind": "duckdb", "database": str(duckdb_file), "query": f"select * from {name} order by ts, seq"}
            for name in ("quotes", "trades")
        },
        "sqlite": {
            name: {"kind": "sqlite", "database": str(sqlite_file), "query": f"select * from {name} order by ts, seq"}
            for name in ("quotes", "trades")
        },
        "mixed": {
            "quotes": {
                "kind": "duckdb",
                "database": str(duckdb_file),
                "query": "select * from quotes order by ts, seq",
            },
            "trades": {"kind": "parquet", "path": str(parquet_store / "trades.parquet")},
        },
    }
    for name, spec in stores.items():
        for batch_rows in (0, 350):
            cfg = make_settings(parquet_store, spec, batch_rows=batch_rows)
            assert historical.run(cfg).equals(expected), (name, batch_rows)


# ---- configuration -------------------------------------------------------------


def test_unknown_kind_lists_the_valid_ones():
    with pytest.raises(ValueError, match="snowflake"):
        list(sources.read({"kind": "oracle9"}))


def test_database_kinds_need_a_query(tmp_path):
    with pytest.raises(KeyError, match="ordered by time"):
        list(sources.read({"kind": "duckdb"}, root=tmp_path))


def test_secrets_come_from_the_environment(monkeypatch, parquet_store, ticks):
    monkeypatch.setenv("TICK_FILE", "trades.parquet")
    got = all_rows({"kind": "parquet", "path": "${TICK_FILE}"}, parquet_store)
    assert got.height == ticks[1].height
    monkeypatch.delenv("TICK_FILE")
    with pytest.raises(KeyError, match="TICK_FILE"):
        all_rows({"kind": "parquet", "path": "${TICK_FILE}"}, parquet_store)


def test_sql_server_connection_string():
    text = sources.sqlserver_connection_string(
        {"host": "db01", "port": 1433, "database": "ticks", "user": "reader", "password": "p;w}d", "Encrypt": "yes"}
    )
    assert text == (
        "Driver={ODBC Driver 18 for SQL Server};Server=db01,1433;Database=ticks;UID=reader;PWD={p;w}}d};Encrypt=yes"
    )
    assert "Trusted_Connection=yes" in sources.sqlserver_connection_string({"host": "db01"})


def test_kdb_tables_convert(stored, ticks, monkeypatch):
    """No q process here, so this checks the part that can be checked: a q table,
    as PyKX hands it back from a query, becomes the frame the pipeline expects."""
    monkeypatch.setenv("PYKX_UNLICENSED", "true")
    pykx = pytest.importorskip("pykx")
    table = pykx.toq(stored[1].rename({"ticker": "sym"}).to_pandas())
    got = sources._shape(pl.from_arrow(table.pa()).lazy(), {"columns": {"ticker": "sym"}}).collect()
    assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))


# ---- servers ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def postgres_server(stored, postgres_cluster):
    """The stored ticks loaded into the throwaway PostgreSQL cluster."""
    import psycopg

    quotes, trades = stored
    with psycopg.connect(autocommit=True, **postgres_cluster) as connection:
        for suffix, number in (("", "double precision"), ("_decimal", "numeric(12,4)")):
            connection.execute(
                f"create table trades{suffix} (ts timestamp, ticker text, seq bigint, price {number}, qty bigint)"
            )
            connection.execute(
                f"create table quotes{suffix} (ts timestamp, ticker text, seq bigint, bid {number}, ask {number})"
            )
            with connection.cursor().copy(f"copy trades{suffix} from stdin") as copy:
                for row in trades.select(TRADE_COLUMNS).iter_rows():
                    copy.write_row(row)
            with connection.cursor().copy(f"copy quotes{suffix} from stdin") as copy:
                for row in quotes.select(QUOTE_COLUMNS).iter_rows():
                    copy.write_row(row)
    return postgres_cluster


@pytest.mark.parametrize("batch_rows", [0, 600])
def test_postgres(postgres_server, ticks, batch_rows):
    spec = {"kind": "postgres", "connect": postgres_server, "query": "select * from trades order by ts, seq"}
    got = all_rows(spec, ".", batch_rows=batch_rows)
    assert got.select(TRADE_COLUMNS).equals(ticks[1].select(TRADE_COLUMNS))


def test_signal_from_postgres_with_decimal_prices(postgres_server, ticks, tmp_path):
    spec = {
        name: {
            "kind": "postgres",
            "connect": postgres_server,
            "query": f"select * from {name}_decimal order by ts, seq",
        }
        for name in ("quotes", "trades")
    }
    assert historical.run(make_settings(tmp_path, spec)).equals(run_batch(*ticks))


UNREACHABLE = {
    "postgres": ("psycopg", {"host": "127.0.0.1", "port": 9, "user": "u", "dbname": "d", "connect_timeout": 2}),
    "questdb": ("psycopg", {"host": "127.0.0.1", "port": 9, "connect_timeout": 2}),
    "clickhouse": ("clickhouse_connect", {"host": "127.0.0.1", "port": 9, "connect_timeout": 2}),
    "snowflake": (
        "snowflake.connector",
        {
            "account": "none",
            "user": "u",
            "password": "p",
            "host": "127.0.0.1",
            "port": 9,
            "protocol": "http",
            "login_timeout": 2,
        },
    ),
    "sqlserver": ("arrow_odbc", {"host": "127.0.0.1", "port": 9, "user": "u", "password": "p", "LoginTimeout": 2}),
    "kdb": ("pykx", {"host": "127.0.0.1", "port": 9, "timeout": 2.0}),
}


@pytest.mark.parametrize("kind", sorted(UNREACHABLE))
@pytest.mark.filterwarnings("ignore")
def test_remote_kinds_reach_their_driver(kind, monkeypatch):
    """No server for these here. This only proves the wiring: the driver loads,
    accepts our parameters, and fails on the connection, not before it."""
    monkeypatch.setenv("PYKX_UNLICENSED", "true")
    module, connect = UNREACHABLE[kind]
    pytest.importorskip(module)
    with pytest.raises(Exception) as caught:
        list(sources.read({"kind": kind, "query": "select 1", "connect": connect}))
    assert type(caught.value).__module__.split(".")[0] == module.split(".")[0], repr(caught.value)


def test_a_query_with_no_rows_gives_an_empty_stream(sqlite_file):
    spec = {"kind": "sqlite", "database": sqlite_file.name, "query": "select * from trades where ts < 0 order by ts"}
    frames = list(sources.read(spec, root=sqlite_file.parent))
    assert sum(frame.height for frame in frames) == 0 and "ts" in frames[0].columns
