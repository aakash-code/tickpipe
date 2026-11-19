"""The tick recorder: a feed plugin in, a store out.

Built to run for a whole session, or for days, without growing:

* Ticks land in fixed-size column buffers, never in lists that grow. Memory use
  is set by `capacity` and does not depend on how long the recorder has run.
* Two buffers per stream take turns. A flush swaps them in one step under the
  lock, so a feed thread is never held up by a slow write to the store.
* If the store falls behind until both buffers are full, the feed thread waits.
  Slowing the intake is the safe failure: nothing is dropped and nothing grows.
* A store that keeps failing does not lose ticks either. Batches go to local
  Parquet files in the spill folder until the store answers again, and
  `tickpipe copy` loads them into the store afterwards.
* The first batch of each stream is read back from the store and compared with
  what was written, so a store that mangles ticks is caught in the first
  seconds, not after the session.

Receive times are stamped here, strictly increasing across both streams. That
one clock orders a recording completely and is what a replay runs on.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import polars as pl

from tickpipe import ticks
from tickpipe.feeds import Feed

log = logging.getLogger("tickpipe.recorder")

NULL_INT = int(np.iinfo(np.int64).min)  # marks a missing integer inside a buffer
_NAN = float("nan")


class RecordingError(RuntimeError):
    """The recording had to stop."""


class _Buffer:
    """Fixed-capacity columns for one stream. Reused for the life of the recorder."""

    def __init__(self, schema: dict, capacity: int) -> None:
        self.n = 0
        self.columns = {
            name: np.empty(
                capacity,
                dtype=np.int32 if name == "ticker" else (np.float64 if dtype == pl.Float64 else np.int64),
            )
            for name, dtype in schema.items()
        }


@dataclass
class Stats:
    """Running totals of one recording."""

    started: float = field(default_factory=time.monotonic)
    rows: dict[str, int] = field(default_factory=lambda: {"quotes": 0, "trades": 0})
    flushes: int = 0
    flush_seconds_max: float = 0.0
    flush_seconds_total: float = 0.0
    waits: int = 0  # times a feed thread had to wait for buffer room
    spilled_rows: int = 0
    store_errors: int = 0

    @property
    def total(self) -> int:
        return sum(self.rows.values())

    def rate(self) -> float:
        elapsed = time.monotonic() - self.started
        return self.total / elapsed if elapsed > 0 else 0.0


class Recorder:
    """Receives ticks from a feed (it is the feed's `TickSink`) and writes them to a store."""

    def __init__(
        self,
        store,
        *,
        flush_seconds: float = 1.0,
        capacity: int = 65_536,
        verify: bool = True,
        spill_dir: str | Path = "spill",
        publish: dict[str, str] | None = None,
        subscribers: int = 0,
        clock=time.time_ns,
    ) -> None:
        if capacity < 2:
            raise ValueError("capacity must be at least 2")
        self.store = store
        self.flush_seconds = float(flush_seconds)
        self.capacity = int(capacity)
        self.verify = verify
        self.spill_dir = Path(spill_dir)
        self.stats = Stats()
        self._clock = clock
        self._lock = threading.Lock()
        self._room = threading.Condition(self._lock)
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._failure: BaseException | None = None
        self._last = 0
        self._seq = {"quotes": 0, "trades": 0}
        self._active = {name: _Buffer(schema, self.capacity) for name, schema in ticks.STREAMS.items()}
        self._spare = {name: _Buffer(schema, self.capacity) for name, schema in ticks.STREAMS.items()}
        self._codes: dict[str, int] = {}
        self._names: list[str] = []
        self._verified: set[str] = set()
        self._spilling = False
        self._spill = None
        self._wake_at = max(1, self.capacity // 2)
        self._sockets: dict[str, object] = {}
        self._message = {"quotes": 0, "trades": 0}
        if publish:
            from tickpipe import wire

            by_url: dict[str, object] = {}
            for name in ticks.STREAMS:
                url = publish[name]
                if url not in by_url:
                    sharing = sum(1 for other in publish.values() if other == url)
                    by_url[url] = wire.publisher(url, subscribers=subscribers * sharing)
                self._sockets[name] = by_url[url]

    # ---- the feed's side: called on the feed's threads -------------------------------

    def _slot(self, stream: str) -> tuple[_Buffer, int, int]:
        """Reserve the next row of a stream. Caller holds the lock."""
        buffer = self._active[stream]
        while buffer.n >= self.capacity:
            self.stats.waits += 1
            self._wake.set()
            self._room.wait(timeout=1.0)
            if self._stop.is_set() and self._active[stream].n >= self.capacity:
                raise RecordingError("the recorder stopped while a tick was waiting for room")
            buffer = self._active[stream]
        now = self._clock()
        if now <= self._last:
            now = self._last + 1
        self._last = now
        return buffer, buffer.n, now

    def _code(self, ticker: str) -> int:
        code = self._codes.get(ticker)
        if code is None:
            code = self._codes[ticker] = len(self._names)
            self._names.append(ticker)
        return code

    def quote(self, ticker, bid, ask, bid_size=None, ask_size=None, exch_ts=None) -> None:
        with self._lock:
            buffer, i, now = self._slot("quotes")
            c = buffer.columns
            c["ts"][i] = now
            c["exch_ts"][i] = NULL_INT if exch_ts is None else exch_ts
            c["ticker"][i] = self._code(ticker)
            c["seq"][i] = self._seq["quotes"]
            c["bid"][i] = _NAN if bid is None else bid
            c["ask"][i] = _NAN if ask is None else ask
            c["bid_size"][i] = NULL_INT if bid_size is None else bid_size
            c["ask_size"][i] = NULL_INT if ask_size is None else ask_size
            self._seq["quotes"] += 1
            buffer.n = i + 1
            if buffer.n >= self._wake_at:
                self._wake.set()

    def trade(self, ticker, price, qty=0, volume=None, exch_ts=None) -> None:
        with self._lock:
            buffer, i, now = self._slot("trades")
            c = buffer.columns
            c["ts"][i] = now
            c["exch_ts"][i] = NULL_INT if exch_ts is None else exch_ts
            c["ticker"][i] = self._code(ticker)
            c["seq"][i] = self._seq["trades"]
            c["price"][i] = price
            c["qty"][i] = qty
            c["volume"][i] = NULL_INT if volume is None else volume
            self._seq["trades"] += 1
            buffer.n = i + 1
            if buffer.n >= self._wake_at:
                self._wake.set()

    def fail(self, error: BaseException) -> None:
        self._failure = error
        self._stop.set()
        self._wake.set()

    # ---- the recorder's side: one thread ---------------------------------------------

    def stop(self) -> None:
        """Ask `run` to finish: flush what is buffered and close the store."""
        self._stop.set()
        self._wake.set()

    def run(self, feed: Feed, *, seconds: float | None = None, status_every: float = 30.0) -> Stats:
        """Record until `stop()` is called, the feed fails, or `seconds` have passed."""
        deadline = None if seconds is None else time.monotonic() + seconds
        next_status = time.monotonic() + status_every
        try:
            feed.start(self)
            while not self._stop.is_set():
                wait = self.flush_seconds
                if deadline is not None:
                    wait = min(wait, max(0.0, deadline - time.monotonic()))
                self._wake.wait(timeout=wait)
                self._wake.clear()
                self._flush()
                if deadline is not None and time.monotonic() >= deadline:
                    break
                if time.monotonic() >= next_status:
                    self._log_status()
                    next_status = time.monotonic() + status_every
        finally:
            self._stop.set()
            try:
                feed.stop()
            except Exception:
                log.exception("the feed did not stop cleanly")
            try:
                self._flush(final=True)
            finally:
                self._close()
        if self._failure is not None:
            raise RecordingError(f"the feed stopped: {self._failure}") from self._failure
        return self.stats

    def _swap(self) -> tuple[dict[str, _Buffer], int, list[str]]:
        with self._lock:
            full = self._active
            self._active = self._spare
            self._spare = full
            mark = self._last
            names = list(self._names)
            self._room.notify_all()
        return full, mark, names

    def _frame(self, stream: str, buffer: _Buffer, names: list[str]) -> pl.DataFrame:
        n = buffer.n
        schema = ticks.STREAMS[stream]
        data = {}
        for name, dtype in schema.items():
            # Copy out of the buffer: it is reused for the next ticks, and a frame
            # built on a view of it would change under whoever still holds it.
            values = buffer.columns[name][:n].copy()
            if name == "ticker":
                data[name] = pl.Series(names, dtype=pl.String).gather(pl.Series(values))
            else:
                data[name] = pl.Series(values, dtype=dtype)
        frame = pl.DataFrame(data)
        fixes = []
        for name, dtype in schema.items():
            if name in ticks.REQUIRED:
                continue
            if dtype == pl.Float64:
                fixes.append(pl.col(name).fill_nan(None))
            else:
                fixes.append(pl.when(pl.col(name) == NULL_INT).then(None).otherwise(pl.col(name)).alias(name))
        return frame.with_columns(fixes) if fixes else frame

    def _flush(self, final: bool = False) -> None:
        started = time.perf_counter()
        full, mark, names = self._swap()
        try:
            for stream in ticks.STREAMS:
                buffer = full[stream]
                frame = self._frame(stream, buffer, names) if buffer.n else None
                if frame is not None:
                    self._write(stream, frame)
                    self.stats.rows[stream] += frame.height
                if self._sockets:
                    self._publish(stream, frame, mark, final)
        finally:
            for buffer in full.values():
                buffer.n = 0
        elapsed = time.perf_counter() - started
        self.stats.flushes += 1
        self.stats.flush_seconds_total += elapsed
        self.stats.flush_seconds_max = max(self.stats.flush_seconds_max, elapsed)

    def _write(self, stream: str, frame: pl.DataFrame) -> None:
        delay = 0.25
        for attempt in range(4):
            if self._spilling and attempt:
                break  # the store is known to be down: one try per flush is enough
            try:
                self.store.write(stream, frame)
            except Exception:
                self.stats.store_errors += 1
                log.exception(
                    "writing %s rows of %s to the store failed (attempt %d)", frame.height, stream, attempt + 1
                )
                self.store.reset()
                if attempt < 3 and not self._spilling:
                    time.sleep(delay)
                    delay *= 2
                continue
            if self._spilling:
                log.warning(
                    "the store is answering again; new ticks go to it. Spilled ticks stay in %s", self.spill_dir
                )
                self._spilling = False
            if self.verify and stream not in self._verified:
                self._verified.add(stream)
                self._check(stream, frame)
            return
        self._spill_frame(stream, frame)

    def _spill_frame(self, stream: str, frame: pl.DataFrame) -> None:
        from tickpipe.stores.parquet import ParquetStore

        if self._spill is None:
            self._spill = ParquetStore({"kind": "parquet", "path": str(self.spill_dir), "compact": False}, Path("."))
        if not self._spilling:
            log.error("the store is not accepting ticks. Writing them to %s until it does", self.spill_dir)
            self._spilling = True
        self._spill.write(stream, frame)
        self.stats.spilled_rows += frame.height

    def _check(self, stream: str, frame: pl.DataFrame) -> None:
        """Read the first batch back and compare it with what was written."""
        low, high = int(frame["ts"][0]), int(frame["ts"][-1]) + 1
        deadline = time.monotonic() + 20.0
        found, problem = "nothing", None
        while True:
            try:
                stored = self.store.read_back(stream, low, high).select(frame.columns)
            except Exception as error:  # noqa: BLE001 - a store may not be readable for a moment after a write
                problem, complete = error, False
            else:
                if stored.equals(frame):
                    return
                found, problem, complete = f"{stored.height} rows", None, stored.height >= frame.height
            if complete or time.monotonic() >= deadline:
                break
            time.sleep(0.5)  # some stores apply writes a moment later
        raise RecordingError(
            f"the {self.store.kind} store did not return the first {stream} batch as written "
            f"({frame.height} rows written, {found} read back"
            + (f", last error: {problem}" if problem else "")
            + "). Recording stopped so that a whole session is not lost to it."
        ) from problem

    def _publish(self, stream: str, frame: pl.DataFrame | None, mark: int, final: bool) -> None:
        from tickpipe import wire
        from tickpipe.pipeline import END

        payload = frame if frame is not None else ticks.empty(stream)
        self._sockets[stream].send_multipart(wire.pack(stream, self._message[stream], payload, END if final else mark))
        self._message[stream] += 1

    def _log_status(self) -> None:
        s = self.stats
        fill = max(buffer.n for buffer in self._active.values()) / self.capacity
        log.info(
            "recorded %s quotes, %s trades (%.0f ticks/s), %d flushes, slowest %.0f ms, buffer %.0f%% full, waits %d",
            f"{s.rows['quotes']:,}", f"{s.rows['trades']:,}", s.rate(), s.flushes,
            s.flush_seconds_max * 1000, fill * 100, s.waits,
        )  # fmt: skip

    def _close(self) -> None:
        for socket in set(self._sockets.values()):
            socket.close()
        if self._spill is not None:
            self._spill.close()
        self.store.close()
