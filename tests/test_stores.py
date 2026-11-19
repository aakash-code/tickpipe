"""Every store that can run on this machine keeps recorded ticks exactly, and gives them back in order."""

from __future__ import annotations

import os
import uuid

import polars as pl
import pytest

from helpers import CONTRACTS, WINDOW, free_port, make_settings, run_batch, tick_batch
from tickpipe import historical, sources, ticks
from tickpipe.stores import identifier, open_store
from tickpipe.stores.parquet import ParquetStore

DAY_NS = 86_400_000_000_000
T0 = 20_776 * DAY_NS + 5 * 3_600_000_000_000  # 2026-11-19 05:00 UTC, as nanoseconds
LOCAL_KINDS = ["parquet", "duckdb", "sqlite", "sql", "postgres"]


@pytest.fixture(params=LOCAL_KINDS)
def spec(request, tmp_path):
    kind = request.param
    if kind == "parquet":
        return {"kind": "parquet", "path": "ticks"}
    if kind == "duckdb":
        return {"kind": "duckdb", "database": "ticks.duckdb"}
    if kind == "sqlite":
        return {"kind": "sqlite", "database": "ticks.sqlite"}
    if kind == "sql":
        pytest.importorskip("sqlalchemy")
        return {"kind": "sql", "url": f"sqlite:///{(tmp_path / 'alchemy.sqlite').as_posix()}"}
    cluster = request.getfixturevalue("postgres_cluster")
    tag = uuid.uuid4().hex[:10]
    return {"kind": "postgres", "connect": cluster, "tables": {"quotes": f"q_{tag}", "trades": f"t_{tag}"}}


def write_batches(store, batches: int = 3, rows: int = 400) -> dict[str, pl.DataFrame]:
    written = {}
    for stream in ticks.STREAMS:
        parts = [
            tick_batch(stream, i * rows, rows, T0 + i * rows * 1_000 + (stream == "trades") * 500)
            for i in range(batches)
        ]
        for part in parts:
            store.write(stream, part)
        written[stream] = pl.concat(parts)
    return written


def test_ticks_read_back_exactly_as_written(spec, tmp_path):
    store = open_store(spec, tmp_path)
    written = write_batches(store)
    store.close()
    again = open_store(spec, tmp_path)
    for stream, frame in written.items():
        assert frame.null_count().sum_horizontal().item() > 0  # the sample has missing values
        assert again.read_back(stream).equals(frame), stream
    again.close()


def test_time_bounds_select_part_of_a_recording(spec, tmp_path):
    store = open_store(spec, tmp_path)
    written = write_batches(store)
    store.close()
    low, high = T0 + 250_000, T0 + 700_000
    for stream, frame in written.items():
        expected = frame.filter((pl.col("ts") >= low) & (pl.col("ts") < high))
        assert 0 < expected.height < frame.height
        assert open_store(spec, tmp_path).read_back(stream, low, high).equals(expected), stream


def test_a_second_recording_appends_after_the_first(spec, tmp_path):
    first = open_store(spec, tmp_path)
    first.write("trades", tick_batch("trades", 0, 300, T0))
    first.close()
    second = open_store(spec, tmp_path)
    second.write("trades", tick_batch("trades", 0, 200, T0 + 10**9))  # a new recording counts from 0 again
    second.close()
    stored = open_store(spec, tmp_path).read_back("trades")
    assert stored.height == 500 and stored["ts"].is_sorted()
    assert stored["seq"].to_list() == [*range(300), *range(200)]


def test_reading_a_store_in_batches_gives_the_same_ticks(spec, tmp_path):
    store = open_store(spec, tmp_path)
    written = write_batches(store)
    store.close()
    frames = list(sources.read(open_store(spec, tmp_path).source_spec("quotes"), root=tmp_path, batch_rows=250))
    assert len(frames) > 1
    assert pl.concat(frames).select(list(ticks.QUOTES)).cast(ticks.QUOTES).equals(written["quotes"])


def test_a_recorded_store_feeds_the_historical_run(spec, tmp_path, ticks_as_recorded):
    quotes, trades = ticks_as_recorded
    store = open_store(spec, tmp_path)
    for stream, frame in (("quotes", quotes), ("trades", trades)):
        for start in range(0, frame.height, 2_000):
            store.write(stream, frame.slice(start, 2_000))
    store.close()
    cfg = make_settings(tmp_path, {"quotes": {"store": "ticks"}, "trades": {"store": "ticks"}}, stores={"ticks": spec})
    assert historical.run(cfg).equals(run_batch(quotes, trades))


@pytest.fixture(scope="module")
def ticks_as_recorded(ticks):
    """The sample ticks in the shape the recorder stores them."""
    quotes, trades = ticks
    quotes = quotes.with_columns(pl.lit(None, dtype=pl.Int64).alias("exch_ts")).select(list(ticks_schema("quotes")))
    trades = trades.with_columns(
        pl.lit(None, dtype=pl.Int64).alias("exch_ts"), pl.lit(None, dtype=pl.Int64).alias("volume")
    ).select(list(ticks_schema("trades")))
    return quotes.cast(ticks_schema("quotes")), trades.cast(ticks_schema("trades"))


def ticks_schema(stream: str) -> dict:
    return ticks.STREAMS[stream]


# ---- parquet ---------------------------------------------------------------------------


def parts(folder) -> list[str]:
    return sorted(path.name for path in folder.glob("part-*.parquet"))


def test_parquet_writes_one_file_per_batch_and_merges_them_on_close(tmp_path):
    store = open_store({"kind": "parquet", "path": "ticks"}, tmp_path)
    written = write_batches(store, batches=5)
    day = tmp_path / "ticks" / "trades" / "date=2026-11-19"
    assert len(parts(day)) == 5 and not list(day.glob("*.tmp"))
    store.close()
    assert len(parts(day)) == 1
    assert not [path.name for path in day.parent.iterdir() if path.name.startswith("_")]
    assert open_store({"kind": "parquet", "path": "ticks"}, tmp_path).read_back("trades").equals(written["trades"])


def test_parquet_splits_a_batch_at_midnight_and_merges_the_finished_day(tmp_path):
    store = open_store({"kind": "parquet", "path": "ticks"}, tmp_path)
    midnight = 20_777 * DAY_NS
    first = tick_batch("trades", 0, 100, midnight - 3_000_000, step=10_000)
    second = tick_batch("trades", 100, 100, midnight - 2_000_000, step=40_000)  # runs past midnight
    root = tmp_path / "ticks" / "trades"
    store.write("trades", first)
    store.write("trades", tick_batch("trades", 1_000, 10, midnight - 2_005_000, step=1))
    assert len(parts(root / "date=2026-11-19")) == 2
    store.write("trades", second)
    # The batch was split at midnight, and the finished day was merged as the recording moved on.
    assert len(parts(root / "date=2026-11-19")) == 1 and len(parts(root / "date=2026-11-20")) == 1
    before_midnight = pl.read_parquet(root / "date=2026-11-19" / parts(root / "date=2026-11-19")[0])
    assert before_midnight.height == 100 + 10 + 50
    store.write("trades", tick_batch("trades", 200, 50, midnight + 10**9))
    assert len(parts(root / "date=2026-11-20")) == 2
    store.close()
    assert len(parts(root / "date=2026-11-20")) == 1
    stored = open_store({"kind": "parquet", "path": "ticks"}, tmp_path).read_back("trades")
    assert stored.height == 260 and stored["ts"].is_sorted()
    assert stored.filter(pl.col("seq") < 1_000)["seq"].to_list() == list(range(250))


def test_parquet_without_merging_keeps_its_files(tmp_path):
    store = open_store({"kind": "parquet", "path": "ticks", "compact": False}, tmp_path)
    write_batches(store, batches=4)
    store.close()
    assert len(parts(tmp_path / "ticks" / "quotes" / "date=2026-11-19")) == 4


def test_parquet_recovers_from_an_interrupted_merge(tmp_path):
    store = open_store({"kind": "parquet", "path": "ticks", "compact": False}, tmp_path)
    written = write_batches(store, batches=3)
    store.close()
    root = tmp_path / "ticks" / "trades"
    os.rename(root / "date=2026-11-19", root / "_old_2026-11-19")  # stopped between the two renames
    (root / "_compact_2026-11-19").mkdir()
    (root / "_compact_2026-11-19" / "part-half.parquet").write_bytes(b"not a parquet file")
    reopened = open_store({"kind": "parquet", "path": "ticks"}, tmp_path)
    assert sorted(path.name for path in root.iterdir()) == ["date=2026-11-19"]
    assert reopened.read_back("trades").equals(written["trades"])


def test_parquet_says_so_when_nothing_was_recorded(tmp_path):
    with pytest.raises(FileNotFoundError, match="no recorded quotes"):
        open_store({"kind": "parquet", "path": "ticks"}, tmp_path).source_spec("quotes")


def test_parquet_on_an_object_store(tmp_path):
    """Recording to S3, served by a local emulation of the S3 API."""
    boto3 = pytest.importorskip("boto3")
    server_module = pytest.importorskip("moto.server")
    port = free_port()
    server = server_module.ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    try:
        endpoint = f"http://127.0.0.1:{port}"
        keys = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}
        boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1", **keys).create_bucket(Bucket="ticks")
        spec = {
            "kind": "parquet",
            "path": "s3://ticks/recorded",
            "storage_options": {
                **keys,
                "aws_region": "us-east-1",
                "aws_endpoint_url": endpoint,
                "aws_allow_http": "true",
            },
        }
        store = open_store(spec, tmp_path)
        assert isinstance(store, ParquetStore) and store.remote and not store.merge
        written = write_batches(store)
        store.close()
        for stream, frame in written.items():
            assert open_store(spec, tmp_path).read_back(stream).equals(frame), stream
    finally:
        server.stop()


# ---- pieces that can be checked without a server -----------------------------------------


def test_table_names_are_checked_before_they_reach_sql():
    assert identifier("ticks.trades_2026") == "ticks.trades_2026"
    for bad in ("trades; drop table x", "1trades", "tra des", ""):
        with pytest.raises(ValueError, match="not usable as a table name"):
            identifier(bad)
    with pytest.raises(ValueError, match="unknown streams"):
        open_store({"kind": "sqlite", "database": "x.sqlite", "tables": {"bars": "b"}})


def test_unknown_store_kind_lists_the_valid_ones():
    with pytest.raises(ValueError, match="questdb"):
        open_store({"kind": "tape"})


def test_questdb_line_protocol_rows():
    from tickpipe.stores.servers import QuestDBStore

    store = QuestDBStore({"kind": "questdb"})
    frame = pl.DataFrame(
        {"ts": [1700000000000000001, 1700000000000000002], "exch_ts": [None, 1699999999000000000], "ticker": ["NSE:A B", "X=Y,Z"],
         "seq": [0, 1], "bid": [None, 99.5], "ask": [100.25, 1e-05], "bid_size": [None, 7], "ask_size": [3, None]},
        schema=ticks.QUOTES,
    )  # fmt: skip
    assert store._lines("quotes", frame).splitlines() == [
        "tickpipe_quotes,ticker=NSE:A\\ B ts=1700000000000000001i,seq=0i,ask=100.25,ask_size=3i 1700000000000000001",
        "tickpipe_quotes,ticker=X\\=Y\\,Z ts=1700000000000000002i,exch_ts=1699999999000000000i,seq=1i,"
        "bid=99.5,ask=1e-05,bid_size=7i 1700000000000000002",
    ]


def test_kdb_batches_become_q_tables_with_q_nulls(monkeypatch):
    monkeypatch.setenv("PYKX_UNLICENSED", "true")
    pykx = pytest.importorskip("pykx")
    from tickpipe.stores.servers import KdbStore, q_table

    frame = tick_batch("quotes", 0, 30, T0)
    table = pykx.toq(q_table(frame, "quotes"))
    assert isinstance(table, pykx.Table)
    back = pl.from_arrow(table.pa())
    assert back.schema["ticker"] == pl.String and back.schema["ts"] == pl.Int64 and back.schema["bid"] == pl.Float64
    assert back["ts"].to_list() == frame["ts"].to_list() and back["ticker"].to_list() == frame["ticker"].to_list()
    assert back["bid"].fill_nan(None).to_list() == frame["bid"].to_list()
    query = KdbStore({"kind": "kdb"}).source_spec("trades", 5, 9)["query"]
    assert query == "`ts`seq xasc select ts,exch_ts,ticker,seq,price,qty,volume from tickpipe_trades where ts>=5, ts<9"


UNREACHABLE = {
    "questdb": ("psycopg", {"connect": {"host": "127.0.0.1", "port": 9, "connect_timeout": 2}}),
    "clickhouse": ("clickhouse_connect", {"connect": {"host": "127.0.0.1", "port": 9, "connect_timeout": 2}}),
    "snowflake": (
        "snowflake.connector",
        {
            "connect": {
                "account": "none",
                "user": "u",
                "password": "p",
                "host": "127.0.0.1",
                "port": 9,
                "protocol": "http",
                "login_timeout": 2,
            }
        },
    ),
    "sqlserver": (
        "arrow_odbc",
        {"connect": {"host": "127.0.0.1", "port": 9, "user": "u", "password": "p", "LoginTimeout": 2}},
    ),
    "kdb": ("pykx", {"connect": {"host": "127.0.0.1", "port": 9, "timeout": 2.0}}),
}


@pytest.mark.parametrize("kind", sorted(UNREACHABLE))
@pytest.mark.filterwarnings("ignore")
def test_server_stores_reach_their_driver(kind, monkeypatch):
    """No server for these in the suite. This proves the wiring only: the driver
    loads, takes our parameters, and fails on the connection, not before it."""
    monkeypatch.setenv("PYKX_UNLICENSED", "true")
    module, extra = UNREACHABLE[kind]
    pytest.importorskip(module)
    store = open_store({"kind": kind, **extra})
    with pytest.raises(Exception) as caught:
        store.write("trades", tick_batch("trades", 0, 5, T0))
    assert type(caught.value).__module__.split(".")[0] == module.split(".")[0], repr(caught.value)


def test_store_backed_sources_are_resolved_when_used(tmp_path):
    cfg = make_settings(
        tmp_path,
        {
            "quotes": {"store": "ticks", "start": "2026-11-19 05:00:00"},
            "plain": {"kind": "parquet", "path": "x.parquet"},
        },
        stores={"ticks": {"kind": "sqlite", "database": "ticks.sqlite"}},
    )
    assert cfg.source("plain") == {"kind": "parquet", "path": "x.parquet"}
    resolved = cfg.source("quotes")
    assert (
        resolved["kind"] == "sqlite" and f"ts >= {T0}" in resolved["query"] and "tickpipe_quotes" in resolved["query"]
    )
    with pytest.raises(KeyError, match=r"stores\.missing"):
        make_settings(tmp_path, {"quotes": {"store": "missing"}}).source("quotes")
    assert len(CONTRACTS) and WINDOW  # the shared sample settings are in use


def test_parquet_merges_in_bounded_groups_and_keeps_recording_order(tmp_path):
    spec = {"kind": "parquet", "path": "ticks", "merge_rows": 1_000}
    store = open_store(spec, tmp_path)
    written = write_batches(store, batches=9, rows=400)  # 3,600 ticks per stream in 9 files
    day = tmp_path / "ticks" / "trades" / "date=2026-11-19"
    store.close()
    sizes = [pl.read_parquet(day / name).height for name in parts(day)]
    assert sizes == [1_200, 1_200, 1_200]  # groups close once they reach merge_rows
    assert open_store(spec, tmp_path).read_back("trades").equals(written["trades"])
    reopened = open_store(spec, tmp_path)
    assert reopened.compact("trades", "2026-11-19") is False  # nothing left to merge
    assert [pl.read_parquet(day / name).height for name in parts(day)] == sizes
