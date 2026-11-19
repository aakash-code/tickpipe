"""Configuration: one TOML file says where ticks live and how the signal is set up."""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_DURATION = {
    "ns": 1,
    "us": 1_000,
    "ms": 1_000_000,
    "s": 1_000_000_000,
    "m": 60_000_000_000,
    "h": 3_600_000_000_000,
}
_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def duration_ns(text: str) -> int:
    """'5m', '300s', '250ms' and so on, as nanoseconds."""
    match = re.fullmatch(r"\s*(\d+)\s*(ns|us|ms|s|m|h)\s*", str(text))
    if not match:
        raise ValueError(f"cannot read duration {text!r}; use forms like '5m', '30s', '250ms'")
    return int(match.group(1)) * _DURATION[match.group(2)]


def expand_env(value: Any) -> Any:
    """Replace ${NAME} with the environment variable, so secrets stay out of the file."""
    if isinstance(value, str):

        def lookup(match: re.Match) -> str:
            name = match.group(1)
            if name not in os.environ:
                raise KeyError(f"environment variable {name} is referenced in the config but not set")
            return os.environ[name]

        return _ENV.sub(lookup, value)
    if isinstance(value, dict):
        return {key: expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand_env(item) for item in value]
    return value


@dataclass(frozen=True)
class Settings:
    root: Path  # folder of the config file; relative paths resolve against it
    contracts: tuple[str, ...]
    window_ns: int
    sources: dict[str, dict]
    quotes_url: str
    trades_url: str
    signal_url: str
    out_dir: Path
    batch_rows: int


def load(path: str | Path = "config.toml") -> Settings:
    path = Path(path).resolve()
    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    root = path.parent
    signal = raw["signal"]
    feed = raw.get("feed", {})
    run = raw.get("run", {})
    sources = raw.get("sources", {})
    for name in ("quotes", "trades"):
        if name not in sources:
            raise KeyError(f"{path.name}: [sources.{name}] is missing")
    return Settings(
        root=root,
        contracts=tuple(signal["contracts"]),
        window_ns=duration_ns(signal.get("window", "5m")),
        sources=sources,
        quotes_url=feed.get("quotes_url", "tcp://127.0.0.1:47101"),
        trades_url=feed.get("trades_url", "tcp://127.0.0.1:47102"),
        signal_url=feed.get("signal_url", "tcp://127.0.0.1:47103"),
        out_dir=(root / run.get("out_dir", "out")).resolve(),
        batch_rows=int(run.get("batch_rows", 0)),
    )
