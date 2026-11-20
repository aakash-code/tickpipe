"""Feed plugin for OpenAlgo that chooses its own symbols: an option chain around the money.

The plain `openalgo` feed records a fixed list of symbols. An index option chain
does not stay fixed: the strikes near the money move with the index during the
day, and the contracts roll off at every expiry. This feed asks OpenAlgo which
contracts to record, records them through the `openalgo` feed, and asks again
every `refresh_seconds`, subscribing what has come into the window and dropping
what has left it.

Config:

    [feeds.chain]
    kind = "openalgo-chain"
    api_key = "${OPENALGO_API_KEY}"
    underlyings = ["NIFTY:NSE_INDEX", "BANKNIFTY:NSE_INDEX"]   # UNDERLYING:QUOTE_EXCHANGE
    strikes = 15                     # strikes each side of the money, calls and puts
    futures = 2                      # nearest futures per underlying; 0 for none
    spot = true                      # also record the index itself
    symbols = ["NSE:RELIANCE"]       # optional fixed extras, as for the openalgo feed
    rest_url = "http://127.0.0.1:5000"     # OpenAlgo's REST API
    refresh_seconds = 60
    ws_url = "ws://127.0.0.1:8765"         # default
    modes = ["depth"]                      # default: quotes and trades both come from depth

The choice is made with OpenAlgo's own REST endpoints (`/api/v1/expiry` and
`/api/v1/optionchain`), so the strikes, the strike step and the expiry calendar
come from the broker's symbol master, never from a rule written here. The ticks
themselves are read exactly as the `openalgo` feed reads them.

A failed refresh keeps the current symbols and is retried on the next one, so a
slow or unreachable REST API never stops a recording. Only the first selection
must succeed: a feed that cannot say what to record stops at once with a reason.
"""

from __future__ import annotations

import json
import logging
import threading
import urllib.request
from collections.abc import Callable

from tickpipe.feeds import TickSink
from tickpipe.feeds.openalgo import _MODE_NAMES, OpenAlgoFeed

log = logging.getLogger("tickpipe.feeds.openalgo_chain")

# Where each quote exchange lists its options and futures.
_DERIVATIVES = {"NSE_INDEX": "NFO", "NSE": "NFO", "BSE_INDEX": "BFO", "BSE": "BFO"}

Fetch = Callable[[str, dict], dict]


def http_fetch(rest_url: str, timeout: float = 20.0) -> Fetch:
    """A fetch that POSTs JSON to OpenAlgo's REST API and returns the decoded reply."""
    base = rest_url.rstrip("/")

    def fetch(path: str, body: dict) -> dict:
        request = urllib.request.Request(
            f"{base}/api/v1/{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout) as reply:
            return json.loads(reply.read().decode())

    return fetch


def _expiries(fetch: Fetch, key: str, underlying: str, exchange: str, kind: str) -> list[str]:
    """Live expiries, nearest first, as DDMMMYY (the form OpenAlgo symbols carry)."""
    reply = fetch("expiry", {"apikey": key, "symbol": underlying, "exchange": exchange, "instrumenttype": kind})
    if reply.get("status") != "success":
        raise RuntimeError(f"OpenAlgo gave no {kind} expiries for {underlying}: {reply.get('message', reply)}")
    return [str(day).replace("-", "").upper() for day in reply.get("data") or []]


def select(fetch: Fetch, key: str, underlyings: list[str], strikes: int, futures: int, spot: bool) -> list[str]:
    """The tickers to record now, as EXCHANGE:SYMBOL, in a stable order."""
    chosen: list[str] = []
    for text in underlyings:
        underlying, _, quote_exchange = str(text).partition(":")
        quote_exchange = quote_exchange or "NSE_INDEX"
        derivatives = _DERIVATIVES.get(quote_exchange, quote_exchange)
        if spot:
            chosen.append(f"{quote_exchange}:{underlying}")
        if strikes > 0:
            expiry = _expiries(fetch, key, underlying, derivatives, "options")[0]
            reply = fetch(
                "optionchain",
                {"apikey": key, "underlying": underlying, "exchange": quote_exchange,
                 "expiry_date": expiry, "strike_count": strikes},
            )  # fmt: skip
            if reply.get("status") != "success":
                raise RuntimeError(f"OpenAlgo gave no option chain for {underlying}: {reply.get('message', reply)}")
            for row in reply.get("chain") or []:
                for side in ("ce", "pe"):
                    symbol = (row.get(side) or {}).get("symbol")
                    if symbol:
                        chosen.append(f"{derivatives}:{symbol}")
        if futures > 0:
            nearest = _expiries(fetch, key, underlying, derivatives, "futures")[:futures]
            chosen.extend(f"{derivatives}:{underlying}{expiry}FUT" for expiry in nearest)
    return list(dict.fromkeys(chosen))  # drop repeats, keep the order


class OpenAlgoChainFeed(OpenAlgoFeed):
    def __init__(self, config: dict, fetch: Fetch | None = None) -> None:
        if not config.get("api_key"):
            raise KeyError("the openalgo-chain feed needs api_key (use ${OPENALGO_API_KEY} to keep it out of the file)")
        if not config.get("underlyings"):
            raise KeyError("the openalgo-chain feed needs underlyings, for example ['NIFTY:NSE_INDEX']")
        self._key = str(config["api_key"])
        self.underlyings = [str(text) for text in config["underlyings"]]
        self.strikes = int(config.get("strikes", 15))
        self.futures = int(config.get("futures", 2))
        self.spot = bool(config.get("spot", True))
        if self.strikes < 0 or self.futures < 0:
            raise ValueError("strikes and futures must be 0 or more")
        self.extras = [str(text) for text in config.get("symbols") or []]
        self.refresh_seconds = float(config.get("refresh_seconds", 60))
        self.fetch = fetch or http_fetch(config.get("rest_url", "http://127.0.0.1:5000"))
        # Depth frames carry both the best bid and ask and the last trade. Asking for quote
        # and depth together, the OpenAlgo proxy sends one frame per tick tagged with the
        # higher mode, so the quote-mode trades never arrive and none are recorded.
        config = {"modes": ["depth"], **config}
        try:
            first = self._choose()
        except Exception as error:
            raise RuntimeError(f"the openalgo-chain feed could not choose its symbols: {error}") from error
        if not first:
            raise RuntimeError("the openalgo-chain feed found nothing to record for " + ", ".join(self.underlyings))
        super().__init__({**config, "symbols": first})
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._refresher: threading.Thread | None = None

    def _choose(self) -> list[str]:
        chosen = select(self.fetch, self._key, self.underlyings, self.strikes, self.futures, self.spot)
        return list(dict.fromkeys(chosen + self.extras))

    def start(self, out: TickSink) -> None:
        super().start(out)
        self._refresher = threading.Thread(target=self._refresh_loop, name="tickpipe-openalgo-chain", daemon=True)
        self._refresher.start()

    def stop(self) -> None:
        self._stop.set()
        if self._refresher is not None:
            self._refresher.join(timeout=5)
        super().stop()

    def _subscribe(self, app) -> None:
        with self._lock:
            super()._subscribe(app)

    def _refresh_loop(self) -> None:
        while not self._stop.wait(self.refresh_seconds):
            self.refresh()

    def refresh(self) -> tuple[list[str], list[str]]:
        """Choose again and move the subscriptions to match. Returns (added, dropped)."""
        try:
            wanted = self._choose()
        except Exception as error:  # noqa: BLE001 - keep recording what we have
            log.warning("could not refresh the option chain, keeping the current symbols: %s", error)
            return [], []
        if not wanted:
            return [], []
        with self._lock:
            current = [f"{item['exchange']}:{item['symbol']}" for item in self.symbols]
            added = [ticker for ticker in wanted if ticker not in current]
            dropped = [ticker for ticker in current if ticker not in wanted]
            if not added and not dropped:
                return [], []
            self.symbols = [self._split(ticker) for ticker in wanted]
            if self._authenticated and self._app is not None:
                self._send("unsubscribe", dropped)
                self._send("subscribe", added)
        log.info("option chain moved: %d added, %d dropped, %d recorded", len(added), len(dropped), len(wanted))
        return added, dropped

    @staticmethod
    def _split(ticker: str) -> dict:
        exchange, _, symbol = ticker.partition(":")
        return {"exchange": exchange, "symbol": symbol}

    def _send(self, action: str, tickers: list[str]) -> None:
        if not tickers:
            return
        symbols = [self._split(ticker) for ticker in tickers]
        for mode in self.modes:
            request = {"action": action, "symbols": symbols, "mode": _MODE_NAMES[mode]}
            if action == "subscribe" and mode == 3:
                request["depth"] = self.depth_levels
            try:
                self._app.send(json.dumps(request))
            except Exception as error:  # noqa: BLE001 - the reconnect resubscribes from self.symbols
                log.warning("could not %s %d symbols: %s", action, len(symbols), error)
                return
