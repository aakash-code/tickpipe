"""Command line: python -m tickpipe <command>."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

from tickpipe import settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="tickpipe", description=__doc__)
    parser.add_argument("--config", default="config.toml", help="path to the TOML configuration")
    commands = parser.add_subparsers(dest="command", required=True)

    synth = commands.add_parser("synth", help="write synthetic quotes and trades as Parquet")
    synth.add_argument("--out", default="data")
    synth.add_argument("--seed", type=int, default=7)

    commands.add_parser("historical", help="run the signal over stored ticks")

    replay = commands.add_parser("replay", help="publish stored ticks on the feed sockets")
    replay.add_argument("--speed", type=float, default=30.0, help="multiple of real time, 0 = no pacing")
    replay.add_argument("--subscribers", type=int, default=1, help="consumers to wait for per stream")

    live = commands.add_parser("live", help="run the signal on the feed sockets and publish it")
    live.add_argument("--subscribers", type=int, default=0, help="signal consumers to wait for")
    live.add_argument("--idle-timeout", type=float, default=None, help="stop if the feed is silent this long")

    record = commands.add_parser("record", help="record the published signal to Parquet")
    record.add_argument("--idle-timeout", type=float, default=None)

    commands.add_parser("compare", help="compare the recorded live run with the historical run")

    demo = commands.add_parser("demo", help="synth, historical, replay into live, compare, report")
    demo.add_argument("--speed", type=float, default=30.0, help="replay speed, 0 = no pacing")

    args = parser.parse_args(argv)
    cfg = settings.load(args.config)
    out = cfg.out_dir

    if args.command == "synth":
        from tickpipe import synth as synth_module

        folder = (cfg.root / args.out).resolve()
        quotes, trades = synth_module.write(cfg.contracts, folder, seed=args.seed)
        print(f"wrote {quotes} and {trades}")

    elif args.command == "historical":
        from tickpipe import historical

        frame = historical.run(cfg)
        out.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(out / "historical.parquet")
        print(f"historical: {frame.height:,} signal ticks written to {out / 'historical.parquet'}")

    elif args.command == "replay":
        from tickpipe import replay as replay_module

        done = replay_module.run(cfg, speed=args.speed, subscribers=args.subscribers)
        print(f"replay: {done.rows:,} ticks in {done.messages:,} messages, {done.seconds:.1f} s")

    elif args.command == "live":
        from tickpipe import live as live_module

        rows = live_module.run(cfg, subscribers=args.subscribers, idle_timeout=args.idle_timeout)
        print(f"live: published {rows:,} signal ticks")

    elif args.command == "record":
        from tickpipe import record as record_module

        rows = record_module.run(cfg, out / "live.parquet", idle_timeout=args.idle_timeout)
        print(f"record: {rows:,} signal ticks written to {out / 'live.parquet'}")

    elif args.command == "compare":
        return _compare(cfg)

    elif args.command == "demo":
        from tickpipe import demo as demo_module

        return demo_module.run(cfg, Path(args.config), speed=args.speed)

    return 0


def _compare(cfg: settings.Settings) -> int:
    from tickpipe import compare, report

    historical = pl.read_parquet(cfg.out_dir / "historical.parquet")
    live = pl.read_parquet(cfg.out_dir / "live.parquet")
    parity, joined = compare.compare(historical, live)
    print(compare.text(parity, joined))
    path = report.write(cfg.out_dir / "report.html", parity, joined, window_ns=cfg.window_ns)
    print(f"report: {path}")
    return 0 if parity.identical else 1


if __name__ == "__main__":
    sys.exit(main())
