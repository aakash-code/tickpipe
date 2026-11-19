"""tickpipe: record live ticks, store them anywhere, replay them, and run one signal over both.

The engine's names (`Pipeline`, `Stream`, `Loading`, `END`) are loaded the first
time they are used, so that importing the package stays cheap and tickpipe's own
commands can set up the process before NumPy and Polars are loaded
(see `tickpipe.runtime`).
"""

from __future__ import annotations

import importlib
from importlib import metadata
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tickpipe.pipeline import END, Loading, Pipeline, Stream

try:
    __version__ = metadata.version("tickpipe")
except metadata.PackageNotFoundError:  # running from a source tree that was never installed
    __version__ = "0+unknown"

__all__ = ["END", "Loading", "Pipeline", "Stream", "__version__"]

_ENGINE = {"END", "Loading", "Pipeline", "Stream"}


def __getattr__(name: str):
    if name in _ENGINE:
        value = getattr(importlib.import_module("tickpipe.pipeline"), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module 'tickpipe' has no attribute {name!r}")
