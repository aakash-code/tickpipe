from __future__ import annotations

import shutil
import subprocess

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


@pytest.fixture(scope="session")
def postgres_cluster(tmp_path_factory):
    """Connection parameters of a throwaway PostgreSQL cluster on a free local port.
    Skipped when the PostgreSQL binaries are not installed."""
    from helpers import free_port

    pytest.importorskip("psycopg")
    initdb, pg_ctl = shutil.which("initdb"), shutil.which("pg_ctl")
    if not (initdb and pg_ctl):
        pytest.skip("PostgreSQL binaries are not on PATH")
    folder = tmp_path_factory.mktemp("postgres")
    cluster, port = folder / "cluster", free_port()
    quiet = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    subprocess.run(
        [initdb, "-D", str(cluster), "-U", "postgres", "-A", "trust", "-E", "UTF8", "--no-sync"], check=True, **quiet
    )
    options = f"-p {port} -c listen_addresses=127.0.0.1"
    subprocess.run(
        [pg_ctl, "-D", str(cluster), "-l", str(folder / "server.log"), "-o", options, "-w", "start"],
        check=True,
        **quiet,
    )
    try:
        yield {"host": "127.0.0.1", "port": port, "user": "postgres", "dbname": "postgres"}
    finally:
        subprocess.run([pg_ctl, "-D", str(cluster), "-m", "immediate", "stop"], **quiet)
