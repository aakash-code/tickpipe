"""Soak test and throughput benchmark for the recorder.

Feeds the recorder a steady stream of ticks for a set time, writes them to a real
store, and watches the process: how many ticks per second it took, how long the
flushes were, whether a feed thread ever had to wait, and whether memory grew.

    uv run python benchmarks/soak.py --store parquet --rate 20000 --seconds 600
    uv run python benchmarks/soak.py --store sqlite  --rate 0 --seconds 20      # 0 = as fast as possible
    uv run python benchmarks/soak.py --store null    --rate 0 --seconds 10      # the recorder alone

Memory is the resident size of the process, sampled every second. The verdict
compares the second half of the run with itself (a line fitted through the
samples), so start-up allocations do not count as growth. Run it for an hour or
a day before trusting a new store or plugin in production:

    uv run python benchmarks/soak.py --store parquet --rate 5000 --seconds 3600

The stored ticks go to --path (default out/soak) and are deleted afterwards
unless --keep is given.
"""

from __future__ import annotations

import argparse
import shutil
import statistics
import sys
import threading
import time
from pathlib import Path

import psutil

from tickpipe import runtime

runtime.lean("record")  # as `tickpipe record` does, before NumPy and Polars are imported

from tickpipe.feeds import Feed, TickSink  # noqa: E402
from tickpipe.recorder import Recorder, RecordingError  # noqa: E402
from tickpipe.stores import Store, open_store  # noqa: E402

TICKERS = [f"SYM{i:03d}" for i in range(200)]


class NullStore(Store):
    """Takes every batch and keeps nothing: measures the recorder without a store."""

    kind = "null"

    def __init__(self) -> None:
        self.rows = 0

    def write(self, stream, frame) -> None:
        self.rows += frame.height

    def read_back(self, stream, start_ns=None, end_ns=None):
        raise NotImplementedError


class Firehose(Feed):
    """Sends ticks at a steady rate from several threads. Rate 0 means no pacing."""

    def start(self, out: TickSink) -> None:
        self.stop_event = threading.Event()
        self.sent = [0] * self.config["threads"]
        self.workers = [
            threading.Thread(target=self._send, args=(out, k), daemon=True) for k in range(self.config["threads"])
        ]
        for worker in self.workers:
            worker.start()

    def _send(self, out: TickSink, k: int) -> None:
        rate = self.config["rate"] / self.config["threads"]
        chunk = max(1, int(rate / 200)) if rate else 500  # pace in slices of 5 ms
        started, sent = time.perf_counter(), 0
        price = 100.0
        while not self.stop_event.is_set():
            try:
                for i in range(chunk):
                    ticker = TICKERS[(sent + i + k * 7) % len(TICKERS)]
                    stamp = 1_800_000_000_000_000_000 + sent + i
                    if (sent + i) % 3:
                        out.quote(ticker, price - 0.05, price + 0.05, 100 + i, 120 + i, exch_ts=stamp)
                    else:
                        out.trade(ticker, price, 1 + i % 50, volume=sent + i, exch_ts=stamp)
            except RecordingError:
                self.sent[k] = sent + i  # the recorder stopped: tick i of this slice did not go in
                return
            sent += chunk
            self.sent[k] = sent
            price += 0.05 if sent % 2 else -0.05
            if rate:
                pause = started + sent / rate - time.perf_counter()
                if pause > 0:
                    self.stop_event.wait(pause)

    def stop(self) -> None:
        self.stop_event.set()
        for worker in self.workers:
            worker.join(timeout=5)


def slope_mb_per_hour(samples: list[tuple[float, float]]) -> float:
    """Least-squares slope of memory over time."""
    if len(samples) < 3:
        return 0.0
    times, sizes = [s[0] for s in samples], [s[1] for s in samples]
    mean_t, mean_s = statistics.fmean(times), statistics.fmean(sizes)
    spread = sum((t - mean_t) ** 2 for t in times)
    return (
        0.0
        if spread == 0
        else sum((t - mean_t) * (s - mean_s) for t, s in zip(times, sizes, strict=True)) / spread * 3600
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--store", default="parquet", choices=["null", "parquet", "sqlite", "duckdb"])
    parser.add_argument("--rate", type=float, default=20_000, help="ticks per second, 0 = as fast as possible")
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--threads", type=int, default=2, help="feed threads")
    parser.add_argument("--flush", type=float, default=1.0, help="seconds between flushes")
    parser.add_argument("--capacity", type=int, default=65_536)
    parser.add_argument("--path", default="out/soak")
    parser.add_argument("--keep", action="store_true", help="keep the stored ticks")
    args = parser.parse_args()

    folder = Path(args.path).resolve()
    if args.store != "null":
        shutil.rmtree(folder, ignore_errors=True)
        folder.mkdir(parents=True)
    specs = {
        "parquet": {"kind": "parquet", "path": "ticks"},
        "sqlite": {"kind": "sqlite", "database": "ticks.sqlite"},
        "duckdb": {"kind": "duckdb", "database": "ticks.duckdb"},
    }
    store = NullStore() if args.store == "null" else open_store(specs[args.store], folder)
    recorder = Recorder(
        store,
        flush_seconds=args.flush,
        capacity=args.capacity,
        verify=args.store != "null",
        spill_dir=folder / "spill",
    )
    feed = Firehose({"rate": args.rate, "threads": args.threads})

    process = psutil.Process()
    samples: list[tuple[float, float]] = []
    committed: list[float] = []
    done = threading.Event()

    def watch() -> None:
        started = time.monotonic()
        while not done.wait(1.0):
            info = process.memory_info()
            samples.append((time.monotonic() - started, info.rss / 1e6))
            committed.append(getattr(info, "private", info.vms) / 1e6)

    watcher = threading.Thread(target=watch, daemon=True)
    watcher.start()
    started = time.perf_counter()
    try:
        stats = recorder.run(feed, seconds=args.seconds, status_every=10**9)
    finally:
        done.set()
        watcher.join()
    elapsed = time.perf_counter() - started

    peak = max((size for _, size in samples), default=0.0)
    samples = [sample for sample in samples if sample[0] <= args.seconds]  # the recording, not the closing
    half = samples[len(samples) // 2 :]
    growth = slope_mb_per_hour(half)
    quarter = max(1, len(samples) // 4)
    marks = [samples[min(len(samples) - 1, i * quarter)][1] for i in range(1, 4)] + [samples[-1][1]] if samples else []
    print(f"store            {args.store}")
    print(f"ticks recorded   {stats.total:,} in {elapsed:.1f} s ({stats.total / elapsed:,.0f} ticks per second)")
    print(f"ticks sent       {sum(feed.sent):,}")
    print(
        f"flushes          {stats.flushes:,}, average {stats.flush_seconds_total / max(stats.flushes, 1) * 1000:.1f} ms, slowest {stats.flush_seconds_max * 1000:.0f} ms"
    )
    print(f"feed waits       {stats.waits:,} (times a feed thread waited for buffer room)")
    print(f"store errors     {stats.store_errors}, rows spilled {stats.spilled_rows:,}")
    if marks:
        print("memory (MB)      " + ", ".join(f"{m:.0f}" for m in marks) + " at 25, 50, 75 and 100 percent of the run")
        if args.seconds >= 300:
            print(f"memory trend     {growth:+.1f} MB per hour over the second half of the recording")
        else:
            print("memory trend     run for at least 300 seconds to get a trend")
        print(f"memory peak      {peak:.0f} MB, including the merge of files when the store closes")
        half_way = committed[len(committed) // 2]
        print(f"committed (MB)   {half_way:.0f} half way, {committed[-1]:.0f} at the end")
    lost = sum(feed.sent) - stats.total
    print(f"ticks lost       {lost:,}")
    if args.store != "null" and not args.keep:
        shutil.rmtree(folder, ignore_errors=True)
    return 0 if lost == 0 and stats.store_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
