"""Formatting of nanosecond timestamps for console and report output."""

from __future__ import annotations

from datetime import UTC, datetime


def moment(ts_ns: int) -> datetime:
    return datetime.fromtimestamp(ts_ns // 1_000_000_000, UTC)


def clock(ts_ns: int, *, millis: bool = True) -> str:
    """Time of day, for example 14:00:20.334."""
    text = moment(ts_ns).strftime("%H:%M:%S")
    return f"{text}.{ts_ns % 1_000_000_000 // 1_000_000:03d}" if millis else text
