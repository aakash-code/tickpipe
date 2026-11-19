"""Live pipeline: the same signal, fed from sockets, published as it is computed."""

from __future__ import annotations

from tickpipe.pipeline import START, Pipeline
from tickpipe.settings import Settings
from tickpipe.signal import build
from tickpipe.timefmt import clock


def run(cfg: Settings, *, subscribers: int = 0, idle_timeout: float | None = None, quiet: bool = False) -> int:
    """Run until the feed ends. Returns the number of signal rows published.

    With `subscribers` > 0 the first signal row is held until that many
    consumers have joined, and only then does the pipeline subscribe to the feed.
    """
    with Pipeline() as p:
        signal = build(p, cfg.contracts, cfg.window_ns)
        signal.to_zmq(cfg.signal_url, topic="signal", subscribers=subscribers)
        sink = p.nodes[-1]  # the socket sink that was just attached

        def report(pipeline: Pipeline) -> None:
            trades, quotes = pipeline.sources["trades"], pipeline.sources["quotes"]
            at = clock(trades.last_ts) if trades.last_ts != START else "waiting"
            print(
                f"live  {at}  quotes {quotes.rows:>9,}  trades {trades.rows:>8,}  signal {sink.rows:>8,}",
                flush=True,
            )

        p.run_live(
            {"quotes": cfg.quotes_url, "trades": cfg.trades_url},
            idle_timeout=idle_timeout,
            on_progress=None if quiet else report,
        )
        if not quiet:
            report(p)
        return sink.rows
