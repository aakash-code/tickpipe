# tickpipe

Record live ticks from any broker or data vendor, keep them in the store you
already use, and replay them whenever you like, as a market simulator. Then write
a tick signal once and prove that it gives the same numbers on a live feed as it
does in a backtest.

![Live feeds, one plugin per broker or vendor, go through the recorder into stored ticks in Parquet, Amazon S3, PostgreSQL, QuestDB, ClickHouse, SQL Server, Snowflake, kdb+, DuckDB or SQLite. Stored ticks feed a historical batch run and, through a socket replay, a live streaming run. Both pass through the same signal code (as-of join, features, window sums), and their outputs are compared tick by tick.](https://raw.githubusercontent.com/marketcalls/tickpipe/main/docs/architecture.png)

    pip install tickpipe

## What it does

| | |
|---|---|
| **Record** | Capture live ticks through a feed plugin. One plugin per broker or vendor; OpenAlgo is included. |
| **Store** | Write them to Parquet, S3, DuckDB, SQLite, PostgreSQL, QuestDB, ClickHouse, SQL Server and more. Exact to the nanosecond in every store. |
| **Replay** | Play a recorded session back through sockets at any speed, after market hours, as if the market were open. |
| **Verify** | Run one signal definition over stored ticks and over the live stream, and compare the two tick by tick. |

## The problems it solves

**There is nothing to test against when the market is closed.** A strategy that
works on ticks can only be exercised while ticks are arriving, and every broker
and vendor delivers them in its own shape. tickpipe records what your feed
actually sent, in one shape for every source, and replays it on demand: the same
ticks, in the same order, as fast or as slow as you want.

**A tick signal usually gets written twice.** First as research code over stored
ticks, then again as streaming code for production, because the research version
cannot run on a live feed. The second version is a rewrite, and rewrites drift:

* a window keeps or drops the tick that sits exactly on its edge;
* a trade is matched to a quote that, live, had not arrived yet;
* a running sum collects rounding error that a batch sum does not;
* a message is lost on the wire and nothing notices.

Each of these moves the live signal a little away from the backtest. tickpipe
removes the rewrite and checks the result:

| | The usual way | With tickpipe |
|---|---|---|
| Ticks for testing | Only while the market is open | Any recorded session, any time |
| Broker and vendor formats | One integration per consumer | One plugin, one stored shape |
| Implementations of the signal | Two: research and production | One, used by both |
| Does live match the backtest? | Assumed, or compared by eye | Compared on every tick, bit for bit |
| Getting at the data | Export and convert first | Read in place from files or databases |

## What it does not do

* It is not a backtester and not an execution engine. There are no orders, fills
  or P&L. It supplies the ticks and the signal; your strategy, or a platform such
  as OpenAlgo or NautilusTrader, does the trading.
* It cannot invent information. A feed that sends periodic snapshots instead of
  every trade is recorded as snapshots, and trade sizes derived from it are an
  approximation (see the OpenAlgo plugin below).

## Quick start

From a checkout, with no account and no market data:

    uv sync
    uv run python -m tickpipe demo

This writes synthetic ticks for a 10 contract futures strip, runs the signal over
them in one batch, replays the same ticks through sockets into the live pipeline
(three separate processes), compares the two results tick by tick, and writes
`out/report.html`.

![Report showing 13,772 of 13,772 ticks identical, with level flow and slope flow charts where the live line lies on the historical band.](https://raw.githubusercontent.com/marketcalls/tickpipe/main/docs/report.png)

## Record live ticks

Describe a feed, a store and the pairing in `config.toml`:

    [feeds.live]
    kind = "openalgo"
    api_key = "${OPENALGO_API_KEY}"          # read from the environment
    symbols = ["NFO:NIFTY23NOV26FUT", "NSE:RELIANCE", "NSE:SBIN"]

    [stores.ticks]
    kind = "parquet"
    path = "ticks"

    [record]
    feed = "live"
    store = "ticks"

Then:

    tickpipe record                 # until Ctrl+C, which stops it cleanly
    tickpipe record --seconds 3600

The recorder is built to run for a whole session, or for days:

* **Bounded memory.** Ticks land in fixed-size buffers that are reused, never in
  lists that grow. Memory does not depend on how long it has been running.
* **Nothing dropped.** A flush swaps buffers in one step, so a slow write never
  holds up the feed. If the store falls so far behind that the buffers fill, the
  feed waits instead of losing ticks.
* **A store outage loses nothing.** Writes are retried, then batches go to local
  Parquet files in `out/spill` until the store answers again.
  `tickpipe copy --from out/spill --to ticks` loads them afterwards.
* **A bad store is caught at once.** The first batch of each stream is read back
  and compared with what was written. A store that alters ticks stops the
  recording in its first seconds, not after the session.
* **One clock.** The recorder stamps every tick on arrival, strictly increasing
  across both streams. That clock orders a recording completely and is what a
  replay runs on.

### What is stored

Two streams, the same shape whatever the source:

| Stream | Columns |
|---|---|
| quotes | `ts`, `exch_ts`, `ticker`, `seq`, `bid`, `ask`, `bid_size`, `ask_size` |
| trades | `ts`, `exch_ts`, `ticker`, `seq`, `price`, `qty`, `volume` |

`ts` is the receive time and `exch_ts` the time the feed itself attached, both in
nanoseconds since the epoch, UTC. `seq` counts the ticks of a stream within one
recording. `volume` is the cumulative volume of the day where the feed reports it.

## Feed plugins

A feed plugin connects to one source of live ticks and hands each tick to the
recorder. Timestamps, ordering, batching, storage and replay are tickpipe's job
and are the same for every feed, so a plugin is small.

    tickpipe feeds                  # list the plugins usable by name

### OpenAlgo

Records from the broker that OpenAlgo is logged in to, over its websocket.
Install it with `pip install "tickpipe[openalgo]"`.

    [feeds.live]
    kind = "openalgo"
    api_key = "${OPENALGO_API_KEY}"
    symbols = ["NFO:NIFTY23NOV26FUT", "NSE:RELIANCE"]   # EXCHANGE:SYMBOL
    ws_url = "ws://127.0.0.1:8765"                      # default
    modes = ["quote", "depth"]                          # default

OpenAlgo streams snapshots, not every exchange event, so the plugin reads them
like this: quotes are the best bid and ask of depth mode; a trade row is written
when the cumulative volume rises, with the last traded price and the rise in
volume as its quantity. Several trades between two snapshots arrive as one row.
The first snapshot of a symbol, and the first after a reconnect, has quantity 0.

### Writing your own

Subclass `Feed`, implement `start` and `stop`, and call `out.quote` and
`out.trade` as ticks arrive:

    from tickpipe.feeds import Feed

    class MyBrokerFeed(Feed):
        def start(self, out):
            self.client = connect(self.config["api_key"])
            self.client.on_trade = lambda t: out.trade(t.symbol, t.price, t.size)
            self.client.on_quote = lambda q: out.quote(q.symbol, q.bid, q.ask, q.bid_size, q.ask_size)
            self.client.subscribe(self.config["symbols"])

        def stop(self):
            self.client.close()

`start` returns once delivery has begun; the `out` methods are safe to call from
any thread. If the feed cannot continue, call `out.fail(error)` and the recording
stops with that error. Name the plugin in the config:

    kind = "plugins/my_broker.py:MyBrokerFeed"    # a file beside the config
    kind = "my_package.feeds:MyBrokerFeed"        # an importable module
    kind = "mybroker"                             # registered by an installed package
                                                  # under the entry point group "tickpipe.feeds"

Everything else in the feed's table reaches the plugin as `self.config`.

## Stores

The same recording can live in any of these, and `tickpipe copy` moves it from one
to another. Tables are created on first use.

| kind | Store | Extra to install | Verified |
|---|---|---|---|
| parquet | Local folder | none | yes |
| parquet | S3, GCS, Azure | none | yes, against a local emulation of the S3 API |
| duckdb | DuckDB file | duckdb | yes |
| sqlite | SQLite file | none | yes |
| postgres | PostgreSQL | postgres | yes, PostgreSQL 18 |
| questdb | QuestDB | questdb | yes, QuestDB 10.2 |
| clickhouse | ClickHouse | clickhouse | yes, ClickHouse 26.9 |
| sqlserver | SQL Server | sqlserver | yes, SQL Server 2019 LocalDB |
| sql | Any SQLAlchemy URL | sql | yes, with SQLite |
| snowflake | Snowflake | snowflake | no: wiring only |
| kdb | kdb+ | kdb | no: wiring and table conversion only |

"Verified" means all four of these passed against a running instance, using ticks
recorded live from OpenAlgo: the recording was copied in and read back identical;
the signal computed from the store equalled the one computed from Parquet; a burst
of 60,000 ticks went through the recorder with nothing lost; and OpenAlgo was
recorded straight into the store. S3 was checked for exact round trips only.

"Wiring only" means the driver loads, takes the parameters and fails at the
connection, and nothing more: no Snowflake account or q process was available.
Treat those two as untested until you have pointed them at your own instance.
The first-batch check will stop a recording into a store that does not return
what it was given.

Times are whole nanoseconds everywhere: a nanosecond timestamp type in Parquet,
a 64-bit integer in the databases. QuestDB and ClickHouse also get a native
timestamp column, `tick_time`, for their own time functions.

## Replay and simulate

A recording is read back by naming its store as a source:

    [sources.quotes]
    store = "ticks"

    [sources.trades]
    store = "ticks"
    start = "2026-11-19 03:45:00"     # optional bounds, UTC
    end = "2026-11-19 10:00:00"

    tickpipe replay --speed 10        # 10 times real time; 0 means no pacing

The replayer publishes the ticks on the feed sockets in recording order. A
consumer is a few lines:

    from tickpipe import settings, wire
    from tickpipe.pipeline import END

    cfg = settings.load("config.toml")
    socket = wire.subscriber(cfg.trades_url, "trades")
    while True:
        _, _, frame, watermark = wire.unpack(socket.recv_multipart())
        if frame is not None:
            print(frame)              # a Polars DataFrame with the next trades
        if watermark == END:          # the replay has finished
            break

To run the whole check on a recording (historical run, replay into the live
pipeline, comparison and report), list the tickers under `[signal]` and run
`tickpipe demo`. On a five minute recording of the NIFTY futures strip and six
stocks from OpenAlgo, replayed at 10 times real time, the live signal matched
the historical one on all 437 signal ticks.

`tickpipe record --publish` sends the ticks to the live pipeline while they are
being recorded, so the signal can run on the real feed and be checked against a
replay of the same session afterwards.

## One signal, live and historical

The example signal is trade flow imbalance across a futures strip: each trade is
signed by the side that crossed the spread, signed contracts are summed per
contract over a moving window, and the flows are combined into a level (their
sum) and a slope (weighted from -1 at the front contract to +1 at the back).

It lives in one function, `flow_signal` in `tickpipe/signal.py`:

    skinny = p.asof(trades, quotes, by="ticker", take=["bid", "ask"])
    rows = p.enrich(skinny, *features())
    return p.aggregate(rows, loadings(len(contracts)), value="signed_qty", ...)

`historical.py` and `live.py` both call it on the same `Pipeline` class. They
differ only in where ticks come from and where results go. Sharing the code is
necessary but not sufficient; four rules in the engine make the results identical
instead of merely close:

1. **Operators ignore batch boundaries.** One batch of a million ticks and a
   million batches of one tick go through the same operator code and give the
   same output.
2. **Streams are joined by watermark, not by arrival.** A trade is joined to its
   quote only once the quote stream has promised that nothing earlier is still
   to come.
3. **Window sums are exact.** They are running integer totals, and loadings are
   integer weights over one divisor, so a value is a single correctly rounded
   division of an exact integer.
4. **Lost messages stop the run.** Every message carries a counter per stream; a
   gap raises an error instead of quietly corrupting the window.

To write your own signal, wire these operators on a pipeline and return a stream:

* `p.source(name, schema)` declares an input stream.
* `p.asof(left, right, by=..., take=[...])` attaches the latest right row at or
  before each left row.
* `p.enrich(rows, *expressions)` adds columns with Polars expressions. They must
  be row-wise: a cumulative, shifted or rolling expression would see batch
  boundaries, and the batching test will catch it.
* `p.aggregate(rows, loadings, value=..., by=..., keys=[...], window_ns=...)`
  sums an Int64 column per key over a time window and combines the sums.

To check a new signal, reuse `run_chunked` from `tests/helpers.py`: it cuts the
input into random pieces, delivers them in random order, and the output must
equal the single batch run exactly.

## Architecture

| Module | Job |
|---|---|
| `tickpipe/feeds/` | Feed plugins and how they are found. `openalgo.py`, `synthetic.py`. |
| `tickpipe/recorder.py` | The capture loop: buffers, the receive clock, flushing, retries, spill, the first-batch check. |
| `tickpipe/ticks.py` | The shape of a recorded tick. The one definition every feed and store shares. |
| `tickpipe/stores/` | Writing ticks and describing how to read them back. One class per store. |
| `tickpipe/sources.py` | Reading ticks from any store or from your own files and tables. |
| `tickpipe/replay.py` | Publishing stored ticks on sockets at a chosen speed. |
| `tickpipe/wire.py` | The socket format: message counters and watermarks. |
| `tickpipe/pipeline.py` | The engine: sources, as-of join, features, window sums, sinks. |
| `tickpipe/signal.py` | The example signal, written once. |
| `tickpipe/historical.py`, `live.py` | The two ways of running a signal. |
| `tickpipe/compare.py`, `report.py` | Tick-by-tick comparison and the HTML report. |
| `tickpipe/settings.py`, `runtime.py` | The configuration file, and how the commands set up their process. |

Data flows one way: feed plugin, recorder, store, source, then either the
historical run or the replayer and the live run.

## Benchmarks

Measured on a 16 core desktop running Windows 11 and Python 3.14. The recorder
figures come from `benchmarks/soak.py`, which is there so you can get your own.

**Recorder throughput**, two feed threads sending as fast as they can for eight
seconds:

| Store | Ticks per second | Ticks lost |
|---|---|---|
| none (the recorder alone) | about 680,000 | 0 |
| Parquet | about 510,000 | 0 |
| DuckDB | about 500,000 | 0 |
| SQLite | about 310,000 | 0 |

**Recorder soak**, 20,000 ticks per second into Parquet for ten minutes:

| | |
|---|---|
| Ticks recorded | 12,000,100 |
| Ticks lost | 0 |
| Flush time | 6.8 ms on average, 37 ms at the slowest |
| Times the feed had to wait | 0 |
| Memory while recording | 83 to 86 MB resident, about 125 MB committed |
| Memory trend | flat: -0.9 MB per hour over the second half |
| Peak while merging 593 files at close | 189 MB |

At 100,000 ticks per second for a minute the picture was the same: nothing
lost, flushes of about 9 ms, and memory flat at about 95 MB.

A feed of a few hundred symbols from one broker is usually well under 5,000
ticks per second, so there is about two orders of magnitude of headroom.

**Memory.** A recorder process holds about 85 MB, and that does not grow.
tickpipe's commands start with only the threads they can use: the linear algebra
backend of NumPy is kept to one thread, because it reserves about 30 MB per CPU
core at import and tickpipe never calls it, and the recording and live commands
run Polars on two threads. Without those two settings the same recorder committed
about 600 MB before its first tick. Set `OPENBLAS_NUM_THREADS` or
`POLARS_MAX_THREADS` yourself to override either.

**Signal engine:**

| Path | Speed |
|---|---|
| Historical, one batch | about 27 million ticks per second |
| Live, one tick per push | about 160 microseconds per tick, roughly 6,000 ticks per second |

When the feed outruns the live pipeline, pending messages are merged into one
push, which raises throughput at the cost of a little latency.

Run the soak for an hour or a day before trusting a new store or plugin:

    uv run python benchmarks/soak.py --store parquet --rate 5000 --seconds 3600

## Commands

    tickpipe feeds            list the feed plugins usable by name
    tickpipe record           record live ticks from a feed into a store
    tickpipe copy             copy recorded ticks from one store to another
    tickpipe compact          merge the small Parquet files of each day
    tickpipe replay           publish stored ticks on the feed sockets
    tickpipe historical       run the signal over stored ticks
    tickpipe live             run the signal on the feed sockets and publish it
    tickpipe record-signal    record the published signal to out/live.parquet
    tickpipe compare          compare live with historical, write out/report.html
    tickpipe demo             historical, replay into live, compare, report
    tickpipe synth            write synthetic quotes and trades as Parquet

Every command takes `--config path/to/config.toml` before the command name.
`python -m tickpipe` is the same program.

## Reading ticks you already have

A source does not have to be a tickpipe recording. Point it at your own files or
tables and map the column names:

    [sources.trades]
    kind = "parquet"
    path = "s3://my-bucket/ticks/trades/date=2026-04-29/*.parquet"
    columns = { ts = "exch_time", ticker = "sym", price = "px", qty = "size" }

`config.examples.toml` has a ready block for every kind of source, store and feed.
Things every source shares:

* Column names are matched ignoring case.
* Timestamps may be datetimes of any precision, with or without a time zone,
  text, or integer epochs with `ts_unit`. They become Int64 nanoseconds.
* Rows must come back in time order. Queries need an `order by`.
* `${NAME}` in any value is read from the environment, which keeps passwords out
  of the file.
* `batch_rows` in `[run]` streams the historical run in frames of that size, so
  the data does not have to fit in memory.

## Limits worth knowing

* The demo data is synthetic. It exercises the pipeline and says nothing about markets.
* Recorded quotes are level 1: the best bid and ask. Deeper book levels are not stored.
* Window sums need integer quantities (contracts, shares, lots). Scale fractional
  sizes to integers first.
* Quotes are matched at or before the trade timestamp, inclusive.
* The signal engine runs in one process on one core.
* DuckDB lets one process write at a time: replay from a DuckDB store after the
  recording has stopped, not while it runs.
* On an object store, Parquet files are not merged. Use a longer `flush_seconds`
  there so that a day is not thousands of small objects.
* PostgreSQL and QuestDB results are fetched by the driver in full before they
  are cut into batches. For history larger than memory use Parquet, DuckDB or
  ClickHouse.
* Times are shown in UTC.

## Development

    uv sync --all-extras
    uv run ruff check .
    uv run ruff format --check .
    uv run pytest

The test suite starts a throwaway PostgreSQL cluster when the PostgreSQL binaries
are installed and skips those tests otherwise. `CHANGELOG.md` lists what changed
in each release.

## License

Copyright 2026 the tickpipe authors.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full text. The software is provided as is, without warranties of any kind. It
is a research tool and not investment advice.

Product names and logos shown in the diagram are trademarks of their respective
owners. They appear only to indicate which data stores tickpipe can read.
