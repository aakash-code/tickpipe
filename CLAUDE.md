# CLAUDE.md

Guidance for Claude Code working in this repository. This file carries what is
not discoverable by reading the code: what the product is for, the invariants
that must not break, and the conventions. Structure, commands and configuration
are in the code and in `README.md`; read them there.

## What tickpipe is

A Python package (`pip install tickpipe`, repository `marketcalls/tickpipe`) with
four jobs, in the order data flows:

1. **Record** live ticks through a feed plugin (one per broker or data vendor).
2. **Store** them in whatever the user already runs (Parquet, object stores, and
   the databases listed in the README).
3. **Replay** a recording through sockets, as a market simulator after hours.
4. **Verify** that one signal definition gives identical output over stored
   ticks and over the live stream.

It is not a backtester and not an execution engine. There are no orders, fills
or P&L, and none should be added: strategies and trading belong to the consumer
(the user's own code, OpenAlgo, or another trading platform).

## Invariants: do not break these

### The engine ignores batch boundaries

Every operator in `tickpipe/pipeline.py` must give the same output however its
input is cut into batches. That one property is why a backtest equals a live
run. A new operator, or a change to one, is not done until
`test_batching_and_arrival_order_do_not_change_the_output` passes with it, and
the row by row reference in `tests/test_engine.py` still matches.

- Streams are combined by watermark, never by arrival order.
- Window sums are integer totals and loadings are integer weights over one
  divisor. Do not introduce a floating point running sum: it drifts with the
  batching and the comparison stops being exact.
- Features passed to `enrich` must be row-wise.

### The recorder never grows and never drops

`tickpipe/recorder.py` is built to run for days.

- Ticks go into fixed-size buffers that are reused. Do not add a list, dict or
  cache that grows with the number of ticks. `test_memory_does_not_grow_with_the_length_of_a_recording`
  guards this.
- Frames handed to a store must own their data. `_frame` copies out of the
  buffer on purpose: a frame built on a view of a reused buffer changes under
  whoever still holds it. This was a real defect, caught by the test suite.
- A full buffer makes the feed thread wait. It must never drop a tick and never
  allocate more.
- A failing store must not lose ticks: retries, then the spill folder.
- The first batch of each stream is read back and compared. Keep that check on
  by default; it is what protects a session from a store that alters ticks.

### One clock orders a recording

The recorder stamps `ts` on arrival, strictly increasing across both streams.
Replay and the signal run on `ts`. `exch_ts` (the feed's own time) is kept for
analysis and must never be used to order ticks: feeds deliver it late, coarse
and out of order.

### Times are whole nanoseconds in every store

A nanosecond timestamp type in Parquet, a 64-bit integer in every database. Do
not switch a store to a native timestamp type with less precision; add a derived
column instead, as QuestDB and ClickHouse do with `tick_time`.

### `import tickpipe` loads nothing heavy

`tickpipe/__init__.py` resolves the engine names lazily, so that
`runtime.lean` can set thread limits before NumPy and Polars are first imported.
Do not add a top-level import of NumPy, Polars or a module that imports them to
`tickpipe/__init__.py`, `tickpipe/__main__.py`, `tickpipe/settings.py` or
`tickpipe/runtime.py`. `test_importing_tickpipe_loads_nothing_heavy` guards this.
The saving is about 500 MB of committed memory per process on a 16 core machine.

### Say what was verified, and only that

The README and changelog state for each store whether it was verified against a
running instance. Snowflake and kdb+ were not: no account or q process was
available. Do not upgrade a store to verified without running the four checks
against a real instance (copy in and read back identical, signal from the store
equals the Parquet one, a 60,000 tick burst through the recorder, a live feed
recorded straight into it).

## Adding things

- **A feed plugin:** a `Feed` subclass in `tickpipe/feeds/`, registered in
  `BUILT_IN`. It only delivers ticks; timestamps, ordering, batching and storage
  stay in the recorder. Test it against a stand-in server, as
  `tests/test_feeds.py` does for OpenAlgo, never against a real account in the
  suite.
- **A store:** a `Store` subclass in `tickpipe/stores/` with `write` and
  `source_spec`, registered in `kinds()`, with a block in `config.examples.toml`
  (a test checks every kind has one). Add it to the parametrised fixtures in
  `tests/test_stores.py` if it can run locally.
- **A command:** a handler function in `tickpipe/__main__.py`, an entry in
  `COMMANDS`, and a line in the README command list (a test checks both).

## Conventions

- **Always use uv.** `uv sync --all-extras`, `uv run pytest`, `uv run ruff check .`,
  `uv run ruff format .`. CI runs exactly these on every push.
- **Plain text everywhere.** No emoji and no icons, and no em or en dashes, in
  code, comments, log messages, documentation, commit messages and release
  notes. A test checks the README is plain ASCII.
- **Errors are never swallowed.** A recording that cannot continue stops with a
  message that says why and what to do. Library code does not print; the
  command line does.
- **Secrets come from the environment.** Config values use `${NAME}`. Never put
  a key in a tracked file, a test or a log line.
- **Every release has a changelog entry**, written for someone deciding whether
  to upgrade: what changed, what it breaks, and what is still not verified.
- **Third-party logos:** the diagram uses logos only from sets that are openly
  licensed for that use. SQL Server and kdb+ have a neutral symbol because no
  such logo was available. Do not add a logo without a clear right to use it.

## Releasing

The version lives in `pyproject.toml` only; `tickpipe.__version__` reads it from
the installed metadata. In this order:

1. Update `CHANGELOG.md` and the version. Run lint, format and the full suite.
2. Commit and push. Wait for CI to pass.
3. `uv build`, then `uvx twine check dist/*`.
4. Publish to PyPI with the token from the environment
   (`UV_PUBLISH_TOKEN=... uv publish dist/*`). A version number can never be
   reused, so this step comes only after CI is green.
5. Tag `vX.Y.Z`, push the tag, and create the GitHub release from it with notes
   taken from the changelog.

## Testing notes

- The suite needs no account and no network. PostgreSQL tests start a throwaway
  cluster when `initdb` and `pg_ctl` are on PATH and skip otherwise. S3 is a
  local emulation. The OpenAlgo plugin is tested against `FakeOpenAlgo`.
- QuestDB, ClickHouse and SQL Server were verified by hand against temporary
  local servers; they are not in the suite. To repeat it: QuestDB needs the JVM
  flag `--add-exports=java.base/jdk.internal.vm=io.questdb` when started with
  its bundled Java directly; ClickHouse has no Windows build and runs in WSL;
  SQL Server can use a temporary LocalDB instance.
- `benchmarks/soak.py` is the long-run check. Run it for an hour before trusting
  a new store or plugin in production, and keep the README benchmark table to
  figures that were actually measured.
