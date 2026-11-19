"""Process settings for tickpipe's own programs.

Both settings are defaults: a value already in the environment wins. Neither is
applied when tickpipe is imported as a library, because the program that imports
it may want the threads for its own work. They must run before NumPy and Polars
are first imported, which is why `import tickpipe` itself loads nothing heavy.
"""

from __future__ import annotations

import os

# Commands that handle ticks a few at a time. A large thread pool gives them
# nothing and costs memory: measured at 100,000 ticks per second into Parquet,
# 16 threads committed about 520 MB and 2 threads about 140 MB, with the same
# flush times.
SMALL_POOL_COMMANDS = frozenset({"record", "live", "replay", "record-signal", "feeds"})


def lean(command: str | None = None) -> None:
    """Start a tickpipe command with only the threads it can use.

    * The linear algebra backend of NumPy is kept to one thread. It reserves
      about 30 MB per CPU core at import, about 500 MB on a 16 core machine, and
      tickpipe never calls it: the engine and the recorder use plain array
      operations.
    * The commands in `SMALL_POOL_COMMANDS` run Polars on two threads. The
      historical run and the other batch commands keep the full pool.
    """
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    if command in SMALL_POOL_COMMANDS:
        os.environ.setdefault("POLARS_MAX_THREADS", "2")
