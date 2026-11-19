"""ZeroMQ wire format shared by the replayer, the live pipeline and the recorder.

One message is three frames: topic, a small JSON header, an Arrow IPC batch.
The header carries a per-stream message counter (so a lost message is detected,
not silently absorbed) and the stream's watermark (see tickpipe.pipeline).
"""

from __future__ import annotations

import io
import json
import time

import polars as pl
import zmq


def pack(topic: str, index: int, frame: pl.DataFrame, watermark: int) -> list[bytes]:
    head = json.dumps({"i": index, "wm": watermark, "n": frame.height}).encode()
    body = frame.write_ipc_stream(None).getvalue() if frame.height else b""
    return [topic.encode(), head, body]


def unpack(parts: list[bytes]) -> tuple[str, int, pl.DataFrame | None, int]:
    topic, head, body = parts
    header = json.loads(head)
    frame = pl.read_ipc_stream(io.BytesIO(body)) if header["n"] else None
    return topic.decode(), header["i"], frame, header["wm"]


def publisher(url: str, *, subscribers: int = 0, timeout: float = 120.0) -> zmq.Socket:
    """Bind a publishing socket and optionally wait for subscribers.

    A plain PUB socket drops whatever is sent before a subscriber has joined.
    XPUB reports each subscription, so the caller can hold the first message
    until its consumers are really connected.
    """
    socket = zmq.Context.instance().socket(zmq.XPUB)
    socket.setsockopt(zmq.SNDHWM, 0)  # never drop on a full queue
    socket.setsockopt(zmq.XPUB_VERBOSE, 1)  # report every subscription, not just the first
    socket.setsockopt(zmq.LINGER, 10_000)
    socket.bind(url)
    deadline = time.monotonic() + timeout
    seen = 0
    while seen < subscribers:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not socket.poll(int(remaining * 1000)):
            socket.close(linger=0)
            raise TimeoutError(f"{url}: {seen} of {subscribers} subscribers after {timeout} s")
        if socket.recv()[:1] == b"\x01":
            seen += 1
    return socket


def subscriber(url: str, topic: str) -> zmq.Socket:
    socket = zmq.Context.instance().socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 0)
    socket.setsockopt(zmq.SUBSCRIBE, topic.encode())
    socket.connect(url)
    return socket
