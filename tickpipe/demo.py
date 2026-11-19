"""End to end: stored ticks through the historical pipeline, the same ticks
replayed through sockets into the live pipeline, then a tick-by-tick comparison.

The three live parts run as separate processes, as they would in production:

    replay  ->  quotes socket, trades socket  ->  live  ->  signal socket  ->  record
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from tickpipe import historical, synth
from tickpipe.settings import Settings


def _local_parquet(cfg: Settings, name: str) -> Path | None:
    spec = cfg.sources[name]
    path = spec.get("path")
    if spec.get("kind") != "parquet" or not isinstance(path, str) or "://" in path or "*" in path:
        return None
    return (cfg.root / path).resolve()


def run(cfg: Settings, config_path: Path, *, speed: float = 30.0) -> int:
    from tickpipe.__main__ import _compare

    paths = [_local_parquet(cfg, name) for name in ("quotes", "trades")]
    if all(paths) and not any(path.exists() for path in paths):
        if paths[0].parent != paths[1].parent:
            raise SystemExit("demo: quotes and trades point to different folders, run synth yourself")
        synth.write(cfg.contracts, paths[0].parent)
        print(f"synth: wrote synthetic ticks to {paths[0].parent}", flush=True)

    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    frame = historical.run(cfg)
    frame.write_parquet(cfg.out_dir / "historical.parquet")
    print(f"historical: {frame.height:,} signal ticks from one batch run", flush=True)

    base = [sys.executable, "-m", "tickpipe", "--config", str(config_path.resolve())]
    commands = {
        "record": [*base, "record", "--idle-timeout", "120"],
        "live": [*base, "live", "--subscribers", "1", "--idle-timeout", "120"],
        "replay": [*base, "replay", "--speed", str(speed), "--subscribers", "1"],
    }
    pace = f"{speed:g}x real time" if speed > 0 else "no pacing"
    print(f"live: replaying the same ticks through sockets at {pace}", flush=True)
    processes = {name: subprocess.Popen(command, cwd=cfg.root) for name, command in commands.items()}
    failed = []
    try:
        for name in ("replay", "live", "record"):
            if processes[name].wait(timeout=3600) != 0:
                failed.append(name)
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.kill()
    if failed:
        print(f"demo: {', '.join(failed)} failed")
        return 2
    return _compare(cfg)
