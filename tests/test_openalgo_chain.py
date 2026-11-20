"""The openalgo-chain feed: choosing an option chain from OpenAlgo's REST API, moving with it."""

from __future__ import annotations

import json

import pytest

from tickpipe import feeds
from tickpipe.feeds import Feed
from tickpipe.feeds.openalgo import OpenAlgoFeed
from tickpipe.feeds.openalgo_chain import OpenAlgoChainFeed, select


class FakeRest:
    """Answers /expiry and /optionchain the way OpenAlgo does. `atm` moves the chain."""

    def __init__(self, atm: int = 24000, step: int = 50, fail: bool = False) -> None:
        self.atm, self.step, self.fail = atm, step, fail
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, path: str, body: dict) -> dict:
        self.calls.append((path, body))
        if self.fail:
            raise OSError("connection refused")
        if path == "expiry":
            if body["instrumenttype"] == "options":
                return {"status": "success", "data": ["25-NOV-26", "02-DEC-26"]}
            return {"status": "success", "data": ["25-NOV-26", "30-DEC-26", "27-JAN-27"]}
        if path == "optionchain":
            name, count = body["underlying"], body["strike_count"]
            strikes = [self.atm + self.step * offset for offset in range(-count, count + 1)]
            chain = [{"strike": s, "ce": {"symbol": f"{name}25NOV26{s}CE"}, "pe": {"symbol": f"{name}25NOV26{s}PE"}}
                     for s in strikes]  # fmt: skip
            return {"status": "success", "underlying_ltp": self.atm, "chain": chain}
        return {"status": "error", "message": "unknown"}


class FakeApp:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))


def chain_feed(rest: FakeRest, **config) -> OpenAlgoChainFeed:
    return OpenAlgoChainFeed({"api_key": "k", "underlyings": ["NIFTY:NSE_INDEX"], "strikes": 1, **config}, fetch=rest)


def test_select_picks_spot_options_around_the_money_and_futures():
    chosen = select(FakeRest(), "k", ["NIFTY:NSE_INDEX"], strikes=1, futures=2, spot=True)
    assert chosen == [
        "NSE_INDEX:NIFTY",
        "NFO:NIFTY25NOV2623950CE", "NFO:NIFTY25NOV2623950PE",
        "NFO:NIFTY25NOV2624000CE", "NFO:NIFTY25NOV2624000PE",
        "NFO:NIFTY25NOV2624050CE", "NFO:NIFTY25NOV2624050PE",
        "NFO:NIFTY25NOV26FUT", "NFO:NIFTY30DEC26FUT",
    ]  # fmt: skip


def test_select_asks_openalgo_for_the_nearest_expiry_and_the_window():
    rest = FakeRest()
    select(rest, "k", ["BANKNIFTY:NSE_INDEX"], strikes=3, futures=0, spot=False)
    assert rest.calls[0] == (
        "expiry", {"apikey": "k", "symbol": "BANKNIFTY", "exchange": "NFO", "instrumenttype": "options"}
    )  # fmt: skip
    assert rest.calls[1][1] == {
        "apikey": "k", "underlying": "BANKNIFTY", "exchange": "NSE_INDEX", "expiry_date": "25NOV26", "strike_count": 3,
    }  # fmt: skip
    assert len(rest.calls) == 2  # no futures asked for


def test_bse_underlyings_trade_on_bfo():
    chosen = select(FakeRest(), "k", ["SENSEX:BSE_INDEX"], strikes=0, futures=1, spot=True)
    assert chosen == ["BSE_INDEX:SENSEX", "BFO:SENSEX25NOV26FUT"]


def test_fixed_extras_are_recorded_alongside_the_chain():
    feed = chain_feed(FakeRest(), strikes=0, futures=0, symbols=["NSE:RELIANCE"])
    assert [f"{s['exchange']}:{s['symbol']}" for s in feed.symbols] == ["NSE_INDEX:NIFTY", "NSE:RELIANCE"]


def test_refresh_moves_the_subscriptions_with_the_chain():
    rest = FakeRest(atm=24000)
    feed = chain_feed(rest, futures=0, spot=False, modes=["quote", "depth"])
    feed._app, feed._authenticated = FakeApp(), True

    assert feed.refresh() == ([], [])  # nothing moved
    assert feed._app.sent == []

    rest.atm = 24050  # the index rose one strike
    added, dropped = feed.refresh()
    assert added == ["NFO:NIFTY25NOV2624100CE", "NFO:NIFTY25NOV2624100PE"]
    assert dropped == ["NFO:NIFTY25NOV2623950CE", "NFO:NIFTY25NOV2623950PE"]
    gone = [{"exchange": "NFO", "symbol": "NIFTY25NOV2623950CE"}, {"exchange": "NFO", "symbol": "NIFTY25NOV2623950PE"}]
    new = [{"exchange": "NFO", "symbol": "NIFTY25NOV2624100CE"}, {"exchange": "NFO", "symbol": "NIFTY25NOV2624100PE"}]
    assert feed._app.sent == [
        {"action": "unsubscribe", "symbols": gone, "mode": "Quote"},
        {"action": "unsubscribe", "symbols": gone, "mode": "Depth"},
        {"action": "subscribe", "symbols": new, "mode": "Quote"},
        {"action": "subscribe", "symbols": new, "mode": "Depth", "depth": 5},
    ]
    # A reconnect resubscribes the moved set, not the one the feed started with.
    assert {"exchange": "NFO", "symbol": "NIFTY25NOV2624100CE"} in feed.symbols


def test_depth_is_the_default_mode_so_trades_are_recorded():
    # Quote plus depth makes the OpenAlgo proxy send only depth frames, and the
    # quote-mode trades never arrive. Depth alone gives quotes and trades.
    feed = chain_feed(FakeRest(), futures=0)
    assert feed.modes == [3] and feed.trade_mode == 3
    assert chain_feed(FakeRest(), futures=0, modes=["quote", "depth"]).modes == [2, 3]  # still configurable


def test_a_failed_refresh_keeps_recording_what_it_has():
    rest = FakeRest()
    feed = chain_feed(rest, futures=0)
    before = list(feed.symbols)
    rest.fail = True
    assert feed.refresh() == ([], [])
    assert feed.symbols == before


def test_the_first_selection_must_succeed():
    with pytest.raises(RuntimeError, match="could not choose its symbols"):
        chain_feed(FakeRest(fail=True))


def test_openalgo_chain_feed_checks_its_config():
    with pytest.raises(KeyError):
        OpenAlgoChainFeed({"underlyings": ["NIFTY:NSE_INDEX"]}, fetch=FakeRest())
    with pytest.raises(KeyError):
        OpenAlgoChainFeed({"api_key": "k"}, fetch=FakeRest())
    with pytest.raises(ValueError):
        OpenAlgoChainFeed({"api_key": "k", "underlyings": ["NIFTY"], "strikes": -1}, fetch=FakeRest())
    assert issubclass(OpenAlgoChainFeed, OpenAlgoFeed) and issubclass(OpenAlgoChainFeed, Feed)
    assert feeds.resolve("openalgo-chain") is OpenAlgoChainFeed
