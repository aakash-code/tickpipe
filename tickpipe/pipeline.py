"""The engine.

A pipeline is a small push-based dataflow graph. Every operator is written so
that its output does not depend on how the input was cut into batches. That one
property is what makes a backtest equal a live run: the historical driver pushes
a day of stored ticks in one batch, the live driver pushes whatever arrived on
the sockets since the last poll, and both go through the same operator code.

Two conventions hold everywhere:

* ``ts`` is Int64 nanoseconds and never decreases within a stream.
* Every push carries a watermark W, a promise that all rows with ``ts <= W``
  on that stream have now been delivered. Operators that combine streams wait
  for the watermark instead of guessing, so arrival order across sockets
  cannot change a result.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

START = int(np.iinfo(np.int64).min)  # watermark before anything was promised
END = int(np.iinfo(np.int64).max)  # watermark of a finished stream


class FeedError(RuntimeError):
    """The input broke a promise the engine relies on (time order, a lost message)."""


@dataclass(frozen=True)
class Loading:
    """A cross-sectional combination of per-key window sums.

    The output is ``sum(weights[i] * window_sum[i]) / divisor``. Weights are
    integers on purpose: window sums of integer quantities are exact, an exact
    integer numerator followed by a single division is bit-reproducible, and so
    the value is identical however the ticks were batched.
    """

    name: str
    weights: tuple[int, ...]
    divisor: int = 1


def _pick(frame: pl.DataFrame, names: Sequence[str]) -> pl.DataFrame:
    """Select columns without a trip through the query engine (this runs per tick)."""
    return pl.DataFrame([frame.get_column(name) for name in names])


class Node:
    schema: pl.Schema

    def __init__(self) -> None:
        self.children: list[tuple[Node, int]] = []

    def push(self, port: int, frame: pl.DataFrame, watermark: int) -> tuple[pl.DataFrame, int]:
        raise NotImplementedError

    def close(self) -> None:
        pass


class Source(Node):
    """Entry point of one input stream. Enforces the schema and time order."""

    def __init__(self, name: str, schema: dict) -> None:
        super().__init__()
        self.name = name
        self.schema = pl.Schema(schema)
        self.watermark = START
        self.last_ts = START
        self.rows = 0
        self._empty = pl.DataFrame(schema=self.schema)

    def push(self, port: int, frame: pl.DataFrame | None, watermark: int) -> tuple[pl.DataFrame, int]:
        if frame is None or frame.height == 0:
            frame = self._empty
        else:
            missing = [c for c in self.schema.names() if c not in frame.columns]
            if missing:
                raise FeedError(f"{self.name}: missing columns {missing}, found {frame.columns}")
            if frame.schema["ts"] != pl.Int64:
                raise FeedError(
                    f"{self.name}: ts must be Int64 nanoseconds, got {frame.schema['ts']} "
                    "(tickpipe.sources converts timestamps for you)"
                )
            if frame.columns != self.schema.names():
                frame = _pick(frame, self.schema.names())
            if frame.schema != self.schema:
                frame = frame.cast(dict(self.schema))
            ts = frame["ts"]
            if ts[0] <= self.watermark or ts[0] < self.last_ts or not ts.is_sorted():
                raise FeedError(
                    f"{self.name}: rows are not in time order. Sort the source by its "
                    "timestamp (ORDER BY in a query, sort = true for Parquet)."
                )
            self.last_ts = ts[-1]
            self.rows += frame.height
        self.watermark = max(self.watermark, watermark)
        return frame, self.watermark


class AsofJoin(Node):
    """Attach to each left row the latest right row at or before its timestamp.

    Left rows are held until the right stream's watermark has passed them, so a
    trade is never joined before every quote that could precede it has arrived.
    When several right rows share one key and timestamp, the last one received
    wins.
    """

    def __init__(self, left: pl.Schema, right: pl.Schema, on: str, by: str, take: Sequence[str]) -> None:
        super().__init__()
        self.on, self.by = on, by
        self.right_cols = [on, by, *take]
        self.schema = pl.Schema({**left, **{c: right[c] for c in take}})
        self.left: list[pl.DataFrame] = []  # rows waiting for the right watermark
        self.left_first = END  # timestamp of the oldest waiting row
        self.right: list[pl.DataFrame] = []  # right rows that can still be matched
        self.right_rows = 0
        self.marks = [START, START]
        self._empty = pl.DataFrame(schema=self.schema)
        self._prune_at = 4096

    def _right(self) -> pl.DataFrame:
        if len(self.right) != 1:
            self.right = [pl.concat(self.right, rechunk=True)]
        return self.right[0]

    def push(self, port: int, frame: pl.DataFrame, watermark: int) -> tuple[pl.DataFrame, int]:
        on = self.on
        if frame.height:
            if port == 0:
                if not self.left:
                    self.left_first = frame[on][0]
                self.left.append(frame)
            else:
                self.right.append(_pick(frame, self.right_cols))
                self.right_rows += frame.height
        self.marks[port] = max(self.marks[port], watermark)
        safe = self.marks[1]
        done = min(self.marks)

        out = self._empty
        if self.left and self.left_first <= safe:
            waiting = self.left[0] if len(self.left) == 1 else pl.concat(self.left, rechunk=True)
            if waiting[on][-1] <= safe:
                ready, self.left, self.left_first = waiting, [], END
            else:
                ready = waiting.filter(pl.col(on) <= safe)
                rest = waiting.filter(pl.col(on) > safe)
                self.left, self.left_first = [rest], rest[on][0]
            if self.right:
                out = ready.join_asof(
                    self._right(), on=on, by=self.by, strategy="backward", check_sortedness=False
                )
            else:  # no right row has arrived yet: every match is null
                out = pl.concat([self._empty, ready], how="diagonal")

        # Every later left row has ts > done, so of the right rows at or before
        # `done` only the newest per key can still be matched. Drop the rest.
        if self.right_rows > self._prune_at:
            right = self._right()
            newest = right.filter(pl.col(on) <= done).unique(subset=[self.by], keep="last", maintain_order=True)
            self.right = [pl.concat([newest, right.filter(pl.col(on) > done)], rechunk=True)]
            self.right_rows = self.right[0].height
            self._prune_at = max(4096, 2 * self.right_rows)
        return out, done


class Enrich(Node):
    """Add row-wise columns. Features are applied in order, so one may use another."""

    def __init__(self, schema: pl.Schema, features: Sequence[pl.Expr]) -> None:
        super().__init__()
        self.features = list(features)
        self._empty = self._apply(pl.DataFrame(schema=schema))
        self.schema = self._empty.schema

    def _apply(self, frame: pl.DataFrame) -> pl.DataFrame:
        query = frame.lazy()
        for feature in self.features:
            query = query.with_columns(feature)
        return query.collect()

    def push(self, port: int, frame: pl.DataFrame, watermark: int) -> tuple[pl.DataFrame, int]:
        return (self._apply(frame) if frame.height else self._empty), watermark


class WindowAggregate(Node):
    """Time-window sums per key, combined across keys by integer loadings.

    For a row at time t the window sum of key i covers that key's rows with
    ``t - window < ts <= t``, up to and including the row itself in stream
    order. Sums are kept as running Int64 totals, so the window value is a
    difference of two exact integers and never drifts.
    """

    def __init__(
        self,
        schema: pl.Schema,
        loadings: Sequence[Loading],
        *,
        value: str,
        by: str,
        keys: Sequence[str],
        window_ns: int,
        on: str,
        keep: Sequence[str] | None,
    ) -> None:
        super().__init__()
        if schema[value] != pl.Int64:
            raise TypeError(f"aggregate needs an Int64 value column, {value!r} is {schema[value]}")
        if window_ns <= 0:
            raise ValueError("window must be positive")
        for loading in loadings:
            if len(loading.weights) != len(keys) or loading.divisor == 0:
                raise ValueError(f"loading {loading.name!r} does not fit {len(keys)} keys")
        self.loadings = list(loadings)
        self.value, self.by, self.on = value, by, on
        self.keys = list(keys)
        self.key_type = pl.Enum(self.keys)
        self.window = int(window_ns)
        self.keep = list(keep) if keep is not None else schema.names()
        self.schema = pl.Schema(
            {**{c: schema[c] for c in self.keep}, **{ld.name: pl.Float64 for ld in self.loadings}}
        )
        self._empty = pl.DataFrame(schema=self.schema)
        # Per key: running total, and the (ts, running total) of rows still inside
        # a future window. Slot 0 is a sentinel holding the total before them.
        self.total = [0] * len(self.keys)
        self.hist_ts = [np.array([START], dtype=np.int64) for _ in self.keys]
        self.hist_sum = [np.zeros(1, dtype=np.int64) for _ in self.keys]

    def push(self, port: int, frame: pl.DataFrame, watermark: int) -> tuple[pl.DataFrame, int]:
        if frame.height == 0:
            return self._empty, watermark
        codes = frame[self.by].cast(self.key_type, strict=False)
        if codes.null_count():  # rows for keys outside the configured set
            frame = frame.filter(codes.is_not_null())
            codes = codes.drop_nulls()
        n = frame.height
        if n == 0:
            return self._empty, watermark

        ts = frame[self.on].to_numpy()
        key = codes.to_physical().to_numpy()
        val = frame[self.value].fill_null(0).to_numpy()
        opens = ts - self.window  # rows at or before this time have left the window
        cutoff = ts[-1] - self.window
        numerators = np.zeros((len(self.loadings), n), dtype=np.int64)

        for i in range(len(self.keys)):
            mine = key == i
            running = self.total[i] + np.cumsum(np.where(mine, val, 0))
            hist_ts, hist_sum = self.hist_ts[i], self.hist_sum[i]
            if mine.any():
                hist_ts = np.concatenate([hist_ts, ts[mine]])
                hist_sum = np.concatenate([hist_sum, running[mine]])
            flow = running - hist_sum[np.searchsorted(hist_ts, opens, side="right") - 1]
            for j, loading in enumerate(self.loadings):
                if loading.weights[i]:
                    numerators[j] += loading.weights[i] * flow
            # Rows at or before the cutoff are outside every later window: fold
            # them into the sentinel.
            first = np.searchsorted(hist_ts, cutoff, side="right") - 1
            hist_ts = hist_ts[first:].copy()
            hist_ts[0] = START
            self.hist_ts[i], self.hist_sum[i] = hist_ts, hist_sum[first:].copy()
            self.total[i] = int(running[-1])

        columns = [frame[name] for name in self.keep]
        columns += [
            pl.Series(loading.name, numerators[j] / loading.divisor, dtype=pl.Float64)
            for j, loading in enumerate(self.loadings)
        ]
        return pl.DataFrame(columns), watermark


class Collect(Node):
    """Keep every output row in memory."""

    def __init__(self, schema: pl.Schema) -> None:
        super().__init__()
        self.schema = schema
        self.frames: list[pl.DataFrame] = []
        self.rows = 0
        self.watermark = START

    def push(self, port: int, frame: pl.DataFrame, watermark: int) -> tuple[pl.DataFrame, int]:
        if frame.height:
            self.frames.append(frame)
            self.rows += frame.height
        self.watermark = watermark
        return frame, watermark

    def frame(self) -> pl.DataFrame:
        return pl.concat(self.frames) if self.frames else pl.DataFrame(schema=self.schema)


class ParquetSink(Collect):
    """Collect output rows and write them to one Parquet file when the pipeline closes."""

    def __init__(self, schema: pl.Schema, path: str | Path) -> None:
        super().__init__(schema)
        self.path = Path(path)

    def close(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.frame().write_parquet(self.path)


class ZmqSink(Node):
    """Publish output rows on a ZeroMQ socket as they are produced."""

    def __init__(self, schema: pl.Schema, url: str, topic: str, subscribers: int) -> None:
        super().__init__()
        from tickpipe import wire

        self.schema = schema
        self.topic = topic
        self.rows = 0
        self.index = 0
        self._wire = wire
        self.socket = wire.publisher(url, subscribers=subscribers)

    def push(self, port: int, frame: pl.DataFrame, watermark: int) -> tuple[pl.DataFrame, int]:
        if frame.height or watermark == END:
            self.socket.send_multipart(self._wire.pack(self.topic, self.index, frame, watermark))
            self.index += 1
            self.rows += frame.height
        return frame, watermark

    def close(self) -> None:
        self.socket.close()


class Stream:
    """Handle to the output of one node, used to wire up the graph."""

    def __init__(self, pipeline: Pipeline, node: Node) -> None:
        self.pipeline = pipeline
        self.node = node

    @property
    def schema(self) -> pl.Schema:
        return self.node.schema

    def collect(self) -> Collect:
        node = Collect(self.schema)
        self.pipeline._attach(node, self)
        return node

    def to_parquet(self, path: str | Path) -> Stream:
        self.pipeline._attach(ParquetSink(self.schema, path), self)
        return self

    def to_zmq(self, url: str, *, topic: str, subscribers: int = 0) -> Stream:
        """Publish on `url`. With `subscribers` > 0, wait for that many before the first send."""
        self.pipeline._attach(ZmqSink(self.schema, url, topic, subscribers), self)
        return self


class Pipeline:
    def __init__(self) -> None:
        self.sources: dict[str, Source] = {}
        self.nodes: list[Node] = []

    # ---- building the graph -------------------------------------------------

    def _attach(self, node: Node, *inputs: Stream) -> Stream:
        for port, stream in enumerate(inputs):
            stream.node.children.append((node, port))
        self.nodes.append(node)
        return Stream(self, node)

    def source(self, name: str, schema: dict) -> Stream:
        node = Source(name, schema)
        self.sources[name] = node
        return self._attach(node)

    def asof(self, left: Stream, right: Stream, *, by: str, take: Sequence[str], on: str = "ts") -> Stream:
        return self._attach(AsofJoin(left.schema, right.schema, on, by, take), left, right)

    def enrich(self, rows: Stream, *features: pl.Expr) -> Stream:
        """Add row-wise columns. A feature must depend only on its own row: anything
        cumulative or shifted would see batch boundaries and break parity."""
        return self._attach(Enrich(rows.schema, features), rows)

    def aggregate(
        self,
        rows: Stream,
        loadings: Sequence[Loading],
        *,
        value: str,
        by: str,
        keys: Sequence[str],
        window_ns: int,
        on: str = "ts",
        keep: Sequence[str] | None = None,
    ) -> Stream:
        node = WindowAggregate(
            rows.schema, loadings, value=value, by=by, keys=keys, window_ns=window_ns, on=on, keep=keep
        )
        return self._attach(node, rows)

    # ---- running it ---------------------------------------------------------

    def feed(self, name: str, frame: pl.DataFrame | None, watermark: int | None = None) -> None:
        """Push rows into a source. Without an explicit watermark the rows only
        promise that nothing earlier than their last timestamp is still to come."""
        if watermark is None:
            if frame is None or frame.height == 0:
                return
            watermark = int(frame["ts"][-1]) - 1
        stack: list[tuple[Node, int, pl.DataFrame | None, int]] = [(self.sources[name], 0, frame, watermark)]
        while stack:
            node, port, data, mark = stack.pop()
            out, out_mark = node.push(port, data, mark)
            for child, child_port in reversed(node.children):
                stack.append((child, child_port, out, out_mark))

    def run_batch(self, inputs: dict[str, pl.DataFrame | Iterable[pl.DataFrame]]) -> None:
        """Run over stored ticks: one frame per stream, or an iterator of frames
        when the data is larger than memory. The stream that is furthest behind
        is always read next, which keeps the join buffers small."""
        readers = {
            name: iter([data] if isinstance(data, pl.DataFrame) else data) for name, data in inputs.items()
        }
        unknown = set(readers) ^ set(self.sources)
        if unknown:
            raise ValueError(f"inputs and sources differ: {sorted(unknown)}")
        open_streams = sorted(readers)
        while open_streams:
            name = min(open_streams, key=lambda n: self.sources[n].watermark)
            frame = next(readers[name], None)
            if frame is None:
                self.feed(name, None, END)
                open_streams.remove(name)
            else:
                self.feed(name, frame)

    def run_live(
        self,
        urls: dict[str, str],
        *,
        idle_timeout: float | None = None,
        on_progress: Callable[[Pipeline], None] | None = None,
        progress_every: float = 2.0,
    ) -> None:
        """Run over live sockets until every stream reports its end.

        When the feed outruns the pipeline, pending messages of a stream are
        merged into one push. Because operators ignore batch boundaries this
        changes throughput and nothing else.
        """
        import zmq

        from tickpipe import wire

        poller = zmq.Poller()
        streams: dict[zmq.Socket, str] = {}
        for name, url in urls.items():
            socket = wire.subscriber(url, name)
            poller.register(socket, zmq.POLLIN)
            streams[socket] = name
        expected = dict.fromkeys(urls, 0)
        open_streams = set(urls)
        wait_ms = None if idle_timeout is None else int(idle_timeout * 1000)
        next_report = time.monotonic() + progress_every
        try:
            while open_streams:
                ready = dict(poller.poll(wait_ms))
                if not ready:
                    raise TimeoutError(f"no message on {sorted(open_streams)} for {idle_timeout} s")
                for socket in ready:
                    name = streams[socket]
                    frames, mark = [], START
                    for _ in range(512):
                        try:
                            parts = socket.recv_multipart(zmq.NOBLOCK)
                        except zmq.Again:
                            break
                        _, index, frame, message_mark = wire.unpack(parts)
                        if index != expected[name]:
                            raise FeedError(
                                f"{name}: expected message {expected[name]}, got {index}. "
                                "Messages were lost, so window state can no longer be trusted."
                            )
                        expected[name] += 1
                        if frame is not None:
                            frames.append(frame)
                        mark = max(mark, message_mark)
                    self.feed(name, pl.concat(frames) if frames else None, mark)
                    if mark == END:
                        open_streams.discard(name)
                if on_progress and time.monotonic() >= next_report:
                    on_progress(self)
                    next_report = time.monotonic() + progress_every
        finally:
            for socket in streams:
                socket.close(linger=0)

    def close(self) -> None:
        for node in self.nodes:
            node.close()

    def __enter__(self) -> Pipeline:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
