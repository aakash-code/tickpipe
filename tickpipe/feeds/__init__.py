"""Feed plugins: one per broker or data vendor.

A feed connects to one source of live ticks and hands each tick to the recorder.
That is all it does: timestamps, ordering, batching, storage and replay belong to
tickpipe and are the same for every feed.

Writing a plugin
----------------

Subclass `Feed`, implement `start` and `stop`, and call `out.quote` and
`out.trade` as ticks arrive:

    from tickpipe.feeds import Feed

    class MyBrokerFeed(Feed):
        def start(self, out):
            self.client = connect(self.config["api_key"])
            self.client.on_tick = lambda t: out.trade(t.symbol, t.price, t.size)
            self.client.subscribe(self.config["symbols"])

        def stop(self):
            self.client.close()

`start` must return once delivery has begun (run the connection on its own
thread). The `out` methods are safe to call from any thread. If the feed fails
in a way it cannot recover from, call `out.fail(error)` and the recording stops
with that error instead of running on with no data.

Using a plugin
--------------

Name it in the config as `kind`:

    kind = "openalgo"                       a feed that ships with tickpipe
    kind = "plugins/my_broker.py:MyBrokerFeed"   a file next to the config
    kind = "my_package.feeds:MyBrokerFeed"       an importable module
    kind = "mybroker"                       a name an installed package registers
                                            under the entry point group
                                            "tickpipe.feeds"
"""

from __future__ import annotations

import importlib
import importlib.util
from importlib import metadata
from pathlib import Path
from typing import Protocol

BUILT_IN = {
    "openalgo": "tickpipe.feeds.openalgo:OpenAlgoFeed",
    "synthetic": "tickpipe.feeds.synthetic:SyntheticFeed",
}
ENTRY_POINT_GROUP = "tickpipe.feeds"


class TickSink(Protocol):
    """What a feed delivers ticks to."""

    def quote(
        self,
        ticker: str,
        bid: float | None,
        ask: float | None,
        bid_size: int | None = None,
        ask_size: int | None = None,
        exch_ts: int | None = None,
    ) -> None:
        """One level 1 quote. `exch_ts` is the feed's own time in nanoseconds, if it has one."""

    def trade(
        self,
        ticker: str,
        price: float,
        qty: int = 0,
        volume: int | None = None,
        exch_ts: int | None = None,
    ) -> None:
        """One trade. `qty` is 0 when the feed gives no size; `volume` is the day's running total."""

    def fail(self, error: BaseException) -> None:
        """Stop the recording because the feed cannot continue."""


class Feed:
    """Base class of a feed plugin. `config` is the feed's table from the config file."""

    def __init__(self, config: dict) -> None:
        self.config = config

    def start(self, out: TickSink) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError


def available() -> dict[str, str]:
    """Feed kinds usable by name: the built-in ones and those installed packages register."""
    kinds = dict(BUILT_IN)
    for entry in metadata.entry_points(group=ENTRY_POINT_GROUP):
        kinds.setdefault(entry.name, entry.value)
    return kinds


def _from_file(path: Path, name: str) -> type:
    spec = importlib.util.spec_from_file_location(f"tickpipe_plugin_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load a feed plugin from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, name)


def resolve(kind: str, root: str | Path = ".") -> type:
    """The plugin class a `kind` names. Relative plugin files are looked up from `root`."""
    target = available().get(kind, kind)
    if ":" not in target:
        raise ValueError(
            f"unknown feed kind {kind!r}. Use one of {sorted(available())}, "
            "or 'path/to/file.py:ClassName', or 'package.module:ClassName'."
        )
    location, _, name = target.rpartition(":")
    if location.endswith(".py"):
        path = Path(location)
        cls = _from_file(path if path.is_absolute() else Path(root) / path, name)
    else:
        cls = getattr(importlib.import_module(location), name)
    if not (isinstance(cls, type) and issubclass(cls, Feed)):
        raise TypeError(f"{target} is not a tickpipe Feed subclass")
    return cls


def load(spec: dict, root: str | Path = ".") -> Feed:
    """Build the feed a config table describes."""
    if "kind" not in spec:
        raise KeyError("a feed needs a kind")
    return resolve(spec["kind"], root)(spec)
