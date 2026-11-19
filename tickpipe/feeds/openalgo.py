"""Feed plugin for OpenAlgo: ticks from the OpenAlgo websocket of whichever broker it is logged in to.

Config:

    [feeds.live]
    kind = "openalgo"
    api_key = "${OPENALGO_API_KEY}"
    symbols = ["NSE:RELIANCE", "NFO:NIFTY24NOV26FUT"]   # EXCHANGE:SYMBOL, as OpenAlgo names them
    ws_url = "ws://127.0.0.1:8765"                      # default
    modes = ["quote", "depth"]                          # default; any of ltp, quote, depth
    depth = 5                                           # levels to ask for in depth mode
    dedupe = true                                       # skip a quote identical to the last one

A recorded ticker is "EXCHANGE:SYMBOL".

What the feed gives, and how it is read
---------------------------------------

OpenAlgo streams snapshots, not every exchange event, so:

* Quotes come from depth mode: the best bid and ask with their sizes. A level
  with price 0 is a missing side.
* Trades come from quote mode (or, when that is not subscribed, from depth or
  LTP mode). A trade row is written when the day's cumulative volume rises: its
  price is the last traded price and its quantity is the rise in volume. When
  several trades happened between two snapshots they arrive as one row at the
  last price. The first snapshot of a symbol, and the first after a reconnect,
  is written with quantity 0: it gives the price, and the size is not known.
* Where a broker sends no volume, a row is written when the last traded price
  or its time changes, with the last traded quantity if the broker sends one.

`exch_ts` is the time in the frame, when the broker's adapter sends one that can
be read as an instant.
"""

from __future__ import annotations

import json
import logging
import threading

from tickpipe.feeds import Feed, TickSink

log = logging.getLogger("tickpipe.feeds.openalgo")

_MODES = {"ltp": 1, "quote": 2, "depth": 3}
_MODE_NAMES = {1: "LTP", 2: "Quote", 3: "Depth"}
_YEAR_2000_NS = 946_684_800_000_000_000
_YEAR_2100_NS = 4_102_444_800_000_000_000


def event_ns(value) -> int | None:
    """A broker timestamp as nanoseconds, read by its size: seconds, milli, micro or nano.
    Anything that is not a plausible instant between 2000 and 2100 gives None."""
    if isinstance(value, str):
        if not value.strip().isdigit():
            return None
        value = int(value)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        return None
    for limit, scale in ((1e17, 1), (1e14, 1_000), (1e11, 1_000_000), (1e8, 1_000_000_000)):
        if value >= limit:
            stamp = int(value * scale)
            return stamp if _YEAR_2000_NS <= stamp < _YEAR_2100_NS else None
    return None


def _positive(value) -> float | None:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0 else None


def _count(value) -> int | None:
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0 else None


class OpenAlgoFeed(Feed):
    def __init__(self, config: dict) -> None:
        super().__init__(config)
        if not config.get("api_key"):
            raise KeyError("the openalgo feed needs api_key (use ${OPENALGO_API_KEY} to keep it out of the file)")
        if not config.get("symbols"):
            raise KeyError("the openalgo feed needs symbols, for example ['NSE:RELIANCE']")
        self.url = config.get("ws_url", "ws://127.0.0.1:8765")
        self.symbols = []
        for text in config["symbols"]:
            exchange, _, symbol = str(text).partition(":")
            if not exchange or not symbol:
                raise ValueError(f"symbol {text!r} must be written EXCHANGE:SYMBOL, for example NSE:RELIANCE")
            self.symbols.append({"exchange": exchange, "symbol": symbol})
        modes = [str(mode).lower() for mode in config.get("modes", ["quote", "depth"])]
        unknown = set(modes) - set(_MODES)
        if unknown or not modes:
            raise ValueError(f"modes must be some of {sorted(_MODES)}, got {modes}")
        self.modes = sorted({_MODES[mode] for mode in modes})
        self.trade_mode = 2 if 2 in self.modes else max(self.modes)  # where trades are read from
        self.depth_levels = int(config.get("depth", 5))
        self.dedupe = bool(config.get("dedupe", True))
        self.reconnect_seconds = int(config.get("reconnect_seconds", 5))
        self._out: TickSink | None = None
        self._app = None
        self._thread: threading.Thread | None = None
        self._stopping = False
        self._authenticated = False
        self._quotes: dict[str, tuple] = {}
        self._trades: dict[str, tuple] = {}

    # ---- connection ------------------------------------------------------------------

    def start(self, out: TickSink) -> None:
        try:
            import websocket
        except ImportError as error:
            raise ImportError(
                "the openalgo feed needs the websocket-client package: install tickpipe with the openalgo extra"
            ) from error

        self._out = out
        self._app = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        self._thread = threading.Thread(target=self._run, name="tickpipe-openalgo", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        try:
            self._app.run_forever(ping_interval=20, ping_timeout=10, reconnect=self.reconnect_seconds)
        except Exception as error:  # noqa: BLE001 - anything here ends the feed
            if not self._stopping:
                self._out.fail(error)

    def stop(self) -> None:
        self._stopping = True
        if self._app is not None:
            self._app.close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _on_open(self, app) -> None:
        # A new connection: the first snapshot of each symbol sets a fresh baseline.
        self._authenticated = False
        self._quotes.clear()
        self._trades.clear()
        app.send(json.dumps({"action": "authenticate", "api_key": self.config["api_key"]}))

    def _subscribe(self, app) -> None:
        for mode in self.modes:
            request = {"action": "subscribe", "symbols": self.symbols, "mode": _MODE_NAMES[mode]}
            if mode == 3:
                request["depth"] = self.depth_levels
            app.send(json.dumps(request))
        log.info("subscribed to %d symbols on %s", len(self.symbols), self.url)

    def _on_error(self, app, error) -> None:
        if not self._stopping:
            log.warning("OpenAlgo feed error: %s (reconnecting in %d s)", error, self.reconnect_seconds)

    def _on_close(self, app, status, reason) -> None:
        if not self._stopping:
            log.warning("OpenAlgo feed closed (%s %s)", status, reason)

    # ---- messages --------------------------------------------------------------------

    def _on_message(self, app, raw) -> None:
        try:
            message = json.loads(raw)
        except ValueError:
            return
        kind = message.get("type")
        if kind == "market_data":
            self._market(message)
        elif kind == "auth":
            if message.get("status") == "success":
                self._authenticated = True
                self._subscribe(app)
            else:
                self._refused(message)
        elif kind == "error" and not self._authenticated:
            self._refused(message)
        elif kind == "subscribe":
            for item in message.get("subscriptions") or []:
                if item.get("status") == "error":
                    log.warning(
                        "OpenAlgo did not subscribe %s:%s: %s",
                        item.get("exchange"), item.get("symbol"), item.get("message"),
                    )  # fmt: skip

    def _refused(self, message: dict) -> None:
        self._stopping = True
        self._out.fail(PermissionError(f"OpenAlgo refused the connection: {message.get('message', message)}"))
        self._app.close()

    def _market(self, message: dict) -> None:
        data = message.get("data")
        if not isinstance(data, dict):
            return
        mode = message.get("mode")
        if isinstance(mode, str):
            mode = _MODES.get(mode.lower())
        ticker = f"{message.get('exchange')}:{message.get('symbol')}"
        if mode == 3:
            self._quote(ticker, data)
        if mode == self.trade_mode:
            self._trade(ticker, data)

    def _quote(self, ticker: str, data: dict) -> None:
        depth = data.get("depth")
        if not isinstance(depth, dict):
            return
        buy = (depth.get("buy") or [{}])[0] or {}
        sell = (depth.get("sell") or [{}])[0] or {}
        bid, ask = _positive(buy.get("price")), _positive(sell.get("price"))
        if bid is None and ask is None:
            return
        state = (
            bid,
            ask,
            _count(buy.get("quantity")) if bid is not None else None,
            _count(sell.get("quantity")) if ask is not None else None,
        )
        if self.dedupe and self._quotes.get(ticker) == state:
            return
        self._quotes[ticker] = state
        self._out.quote(ticker, *state, exch_ts=event_ns(data.get("timestamp")))

    def _trade(self, ticker: str, data: dict) -> None:
        price = _positive(data.get("ltp"))
        if price is None:
            return
        volume = _count(data.get("volume")) or None  # 0 is how a missing volume arrives
        stamp = data.get("ltt") or data.get("timestamp")
        last = self._trades.get(ticker)
        self._trades[ticker] = (price, volume, stamp)
        if last is None:
            quantity = 0  # first sight of this symbol: the price is known, the size is not
        elif volume is not None and last[1] is not None:
            if volume > last[1]:
                quantity = volume - last[1]
            elif volume < last[1]:
                quantity = _count(data.get("last_trade_quantity")) or 0  # the count restarted
            elif price != last[0]:
                quantity = 0
            else:
                return  # nothing traded since the last snapshot
        else:
            if self.dedupe and price == last[0] and stamp == last[2]:
                return
            quantity = _count(data.get("last_trade_quantity")) or 0
        self._out.trade(ticker, price, quantity, volume=volume, exch_ts=event_ns(stamp))
