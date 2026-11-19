"""Command line: tickpipe <command>, or python -m tickpipe <command>.

Each command is one small function below, listed in `COMMANDS`. They import what
they need when they run, so that `runtime.lean` can set up the process first and
so that a command never pays for a driver it does not use.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from pathlib import Path

from tickpipe import runtime, settings
from tickpipe.settings import Settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tickpipe", description="Record, store, replay and verify market ticks.")
    parser.add_argument("--config", default="config.toml", help="path to the TOML configuration")
    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("feeds", help="list the feed plugins that can be used by name")

    record = commands.add_parser("record", help="record live ticks from a feed plugin into a store")
    record.add_argument("--feed", help="name of a [feeds.<name>] block (default: [record] feed)")
    record.add_argument("--store", help="name of a [stores.<name>] block (default: [record] store)")
    record.add_argument("--seconds", type=float, default=None, help="stop after this long (default: until Ctrl+C)")
    record.add_argument("--publish", action="store_true", help="also publish the ticks on the feed sockets")
    record.add_argument("--subscribers", type=int, default=0, help="with --publish: consumers to wait for per stream")

    copy = commands.add_parser("copy", help="copy recorded ticks from one store to another")
    copy.add_argument("--from", dest="source", required=True, help="a store name, or a folder of recorded Parquet")
    copy.add_argument("--to", dest="target", required=True, help="a store name")
    copy.add_argument("--start", help="first time to copy, for example '2026-11-19 09:15:00' (UTC)")
    copy.add_argument("--end", help="time to stop before")

    compact = commands.add_parser("compact", help="merge the small Parquet files of each day in a store")
    compact.add_argument("--store", required=True)

    replay = commands.add_parser("replay", help="publish stored ticks on the feed sockets")
    replay.add_argument("--speed", type=float, default=30.0, help="multiple of real time, 0 = no pacing")
    replay.add_argument("--subscribers", type=int, default=1, help="consumers to wait for per stream")

    commands.add_parser("historical", help="run the signal over stored ticks")

    live = commands.add_parser("live", help="run the signal on the feed sockets and publish it")
    live.add_argument("--subscribers", type=int, default=0, help="signal consumers to wait for")
    live.add_argument("--idle-timeout", type=float, default=None, help="stop if the feed is silent this long")

    record_signal = commands.add_parser("record-signal", help="record the published signal to Parquet")
    record_signal.add_argument("--idle-timeout", type=float, default=None)

    commands.add_parser("compare", help="compare the recorded live run with the historical run")

    demo = commands.add_parser("demo", help="historical, replay into live, compare, report")
    demo.add_argument("--speed", type=float, default=30.0, help="replay speed, 0 = no pacing")

    synth = commands.add_parser("synth", help="write synthetic quotes and trades as Parquet")
    synth.add_argument("--out", default="data")
    synth.add_argument("--seed", type=int, default=7)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runtime.lean(args.command)  # before a command imports NumPy or Polars
    return COMMANDS[args.command](settings.load(args.config), args)


# ---- recording ----------------------------------------------------------------------


def _feeds(cfg: Settings, args: argparse.Namespace) -> int:
    from tickpipe import feeds

    for name, target in sorted(feeds.available().items()):
        print(f"{name:14s} {target}")
    return 0


def _record(cfg: Settings, args: argparse.Namespace) -> int:
    from tickpipe import feeds
    from tickpipe.recorder import Recorder, RecordingError

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    feed_name = args.feed or cfg.record.get("feed")
    store_name = args.store or cfg.record.get("store")
    if not feed_name or not store_name:
        raise SystemExit("record needs a feed and a store: set them under [record] or pass --feed and --store")
    if feed_name not in cfg.feeds:
        raise SystemExit(f"no [feeds.{feed_name}] in the config; the feeds are {sorted(cfg.feeds)}")
    feed = feeds.load(settings.expand_env(cfg.feeds[feed_name]), cfg.root)
    recorder = Recorder(
        cfg.store(store_name),
        flush_seconds=float(cfg.record.get("flush_seconds", 1.0)),
        capacity=int(cfg.record.get("capacity", 65_536)),
        verify=bool(cfg.record.get("verify", True)),
        spill_dir=cfg.out_dir / "spill",
        publish={"quotes": cfg.quotes_url, "trades": cfg.trades_url} if args.publish else None,
        subscribers=args.subscribers,
    )
    print(f"record: {feed_name} into {store_name}. Ctrl+C stops it cleanly.", flush=True)
    try:
        recorder.run(feed, seconds=args.seconds)
    except KeyboardInterrupt:
        pass
    except RecordingError as error:
        print(f"record: stopped: {error}")
        return 1
    finally:
        stats = recorder.stats
        spilled = f", {stats.spilled_rows:,} rows spilled to {cfg.out_dir / 'spill'}" if stats.spilled_rows else ""
        print(
            f"record: {stats.rows['quotes']:,} quotes and {stats.rows['trades']:,} trades "
            f"in {stats.flushes:,} flushes, slowest flush {stats.flush_seconds_max * 1000:.0f} ms{spilled}"
        )
    return 0


def _named_store(cfg: Settings, name: str):
    """A store from the config, or a folder of recorded Parquet such as the spill folder."""
    if name in cfg.stores:
        return cfg.store(name)
    folder = (cfg.root / name).resolve()
    if folder.is_dir():
        from tickpipe.stores import open_store

        return open_store({"kind": "parquet", "path": str(folder), "compact": False}, cfg.root)
    raise SystemExit(f"{name!r} is neither a store in the config nor a folder")


def _copy(cfg: Settings, args: argparse.Namespace) -> int:
    from tickpipe import sources, ticks
    from tickpipe.sources import _time_ns

    source, target = _named_store(cfg, args.source), _named_store(cfg, args.target)
    start = _time_ns(args.start) if args.start else None
    end = _time_ns(args.end) if args.end else None
    try:
        for stream, schema in ticks.STREAMS.items():
            try:
                spec = source.source_spec(stream, start, end)
            except FileNotFoundError:
                print(f"copy: no {stream} to copy")
                continue
            rows = 0
            for frame in sources.read(spec, root=cfg.root, batch_rows=250_000):
                if frame.height:
                    target.write(stream, frame.select(list(schema)).cast(schema))
                    rows += frame.height
            print(f"copy: {rows:,} {stream}")
    finally:
        source.close()
        target.close()
    return 0


def _compact(cfg: Settings, args: argparse.Namespace) -> int:
    store = cfg.store(args.store)
    if store.kind != "parquet" or store.remote:
        print("compact applies to a local Parquet store")
        return 2
    merged = 0
    for stream_folder in sorted(path for path in Path(store.base).iterdir() if path.is_dir()):
        for day_folder in sorted(stream_folder.glob("date=*")):
            merged += store.compact(stream_folder.name, day_folder.name[5:])
    print(f"compact: merged {merged} day folders")
    return 0


# ---- replaying and the signal ---------------------------------------------------------


def _contracts(cfg: Settings) -> tuple[str, ...]:
    if len(cfg.contracts) < 2:
        raise SystemExit("this command needs [signal] contracts with at least two tickers in the config")
    return cfg.contracts


def _replay(cfg: Settings, args: argparse.Namespace) -> int:
    from tickpipe import replay

    done = replay.run(cfg, speed=args.speed, subscribers=args.subscribers)
    print(f"replay: {done.rows:,} ticks in {done.messages:,} messages, {done.seconds:.1f} s")
    return 0


def _historical(cfg: Settings, args: argparse.Namespace) -> int:
    from tickpipe import historical

    _contracts(cfg)
    frame = historical.run(cfg)
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(cfg.out_dir / "historical.parquet")
    print(f"historical: {frame.height:,} signal ticks written to {cfg.out_dir / 'historical.parquet'}")
    return 0


def _live(cfg: Settings, args: argparse.Namespace) -> int:
    from tickpipe import live

    _contracts(cfg)
    rows = live.run(cfg, subscribers=args.subscribers, idle_timeout=args.idle_timeout)
    print(f"live: published {rows:,} signal ticks")
    return 0


def _record_signal(cfg: Settings, args: argparse.Namespace) -> int:
    from tickpipe import signal_recorder

    _contracts(cfg)
    rows = signal_recorder.run(cfg, cfg.out_dir / "live.parquet", idle_timeout=args.idle_timeout)
    print(f"record-signal: {rows:,} signal ticks written to {cfg.out_dir / 'live.parquet'}")
    return 0


def _compare(cfg: Settings, args: argparse.Namespace | None = None) -> int:
    import polars as pl

    from tickpipe import compare, report

    historical = pl.read_parquet(cfg.out_dir / "historical.parquet")
    live = pl.read_parquet(cfg.out_dir / "live.parquet")
    parity, joined = compare.compare(historical, live)
    print(compare.text(parity, joined))
    path = report.write(cfg.out_dir / "report.html", parity, joined, window_ns=cfg.window_ns)
    print(f"report: {path}")
    return 0 if parity.identical else 1


def _demo(cfg: Settings, args: argparse.Namespace) -> int:
    from tickpipe import demo

    _contracts(cfg)
    return demo.run(cfg, Path(args.config), speed=args.speed)


def _synth(cfg: Settings, args: argparse.Namespace) -> int:
    from tickpipe import synth

    quotes, trades = synth.write(_contracts(cfg), (cfg.root / args.out).resolve(), seed=args.seed)
    print(f"wrote {quotes} and {trades}")
    return 0


COMMANDS: dict[str, Callable[[Settings, argparse.Namespace], int]] = {
    "feeds": _feeds,
    "record": _record,
    "copy": _copy,
    "compact": _compact,
    "replay": _replay,
    "historical": _historical,
    "live": _live,
    "record-signal": _record_signal,
    "compare": _compare,
    "demo": _demo,
    "synth": _synth,
}


if __name__ == "__main__":
    sys.exit(main())
