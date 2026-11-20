# Changelog

Every release is listed here, newest first, with what changed for someone using
tickpipe and anything that needs action when upgrading.

## Unreleased

### Added

- **OpenAlgo option chain feed** (`kind = "openalgo-chain"`). Records the index,
  the calls and puts within `strikes` of the money for the nearest expiry, and the
  nearest `futures`, choosing them with OpenAlgo's own `/api/v1/expiry` and
  `/api/v1/optionchain`. It chooses again every `refresh_seconds` and moves the
  subscriptions as the money moves and as contracts expire. A failed refresh keeps
  the current symbols; only the first selection must succeed. Verified against a
  live OpenAlgo instance (Upstox, NIFTY and BANKNIFTY, 26 symbols).

### Note on the `openalgo` feed

- With the default `modes = ["quote", "depth"]`, the OpenAlgo proxy sends one
  frame per tick tagged with the higher mode, so quote-mode frames do not arrive
  and no trades are recorded (seen on a live instance: 1,460 quotes, 0 trades).
  `modes = ["depth"]` records both. The option chain feed defaults to depth for
  this reason; the plain `openalgo` feed is unchanged.

## 0.2.0 - 2026-11-19

Recording. tickpipe can now capture live ticks itself, keep them in any store it
supports, and replay them, so a recorded session can be simulated after market hours.

### Added

- **Tick recorder** (`tickpipe record`). Fixed-size buffers, so memory use does not
  depend on how long it runs. A slow store slows the feed instead of dropping
  ticks. The first batch of each stream is read back and compared before the
  recording is trusted. While a store is down, ticks go to local Parquet files in
  the spill folder and can be loaded afterwards.
- **Feed plugins**, one per broker or data vendor. A plugin is a small class with
  `start` and `stop`; it can live in a file beside the config, in any importable
  module, or in an installed package that registers it.
- **OpenAlgo feed plugin**, for the broker OpenAlgo is logged in to: level 1
  quotes from depth mode and trades from the change in cumulative volume.
- **Synthetic feed plugin**, for trying the recorder with no account.
- **Stores that can be written to**: Parquet (local and object stores), DuckDB,
  SQLite, PostgreSQL, QuestDB, ClickHouse, SQL Server, anything with a SQLAlchemy
  URL, Snowflake and kdb+. Times are kept as whole nanoseconds in every store.
- **Sources that name a store** (`store = "ticks"`), so `historical` and `replay`
  read a recording with no query or path to write.
- `tickpipe copy` moves a recording from one store to another, `tickpipe compact`
  merges a day of Parquet files, `tickpipe feeds` lists the available plugins.
- `record --publish` also sends the ticks to the live pipeline, so a signal can
  run on a real feed while it is being recorded.
- `benchmarks/soak.py`, a soak test and throughput benchmark for the recorder.

### Changed

- **`tickpipe record` now records ticks.** The command that records the published
  signal, which had that name in 0.1.0, is now `tickpipe record-signal`.
- A config no longer needs `[signal]` or `[sources.*]` when it is only used for
  recording.
- A database query that returns no rows now gives an empty stream instead of an error.

### Not verified

- The Snowflake and kdb+ stores and sources are written against the documented
  interfaces of their drivers and tested as far as that goes without a server:
  the driver loads, takes the parameters, and fails at the connection, and q table
  conversion is checked. They have not read or written real rows. Every other
  store was verified against a running instance with recorded OpenAlgo ticks.

## 0.1.0 - 2026-11-19

First version.

- Engine whose operators give the same output however the input is batched:
  as-of join by watermark, row-wise features, exact integer window sums.
- Example signal: trade flow imbalance (level and slope) across a futures strip.
- `historical`, `live`, `replay`, `record`, `compare` and `demo` commands, and an
  HTML report of a live run against the historical run.
- Sources: Parquet (local and object stores), DuckDB, SQLite, SQLAlchemy,
  PostgreSQL, QuestDB, ClickHouse, SQL Server and ODBC, Snowflake, kdb+.
