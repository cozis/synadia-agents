"""Best-effort edge delivery: acks, retries, eviction, drain-on-close.

Mirrors ``client-sdk/typescript/test/unit/edge-publisher.test.ts``. The
fake connection records every request and can be told to fail a given
number of times, so the retry schedule is observable without sleeping
real backoff intervals (delays are set to ~0 in these tests).
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import TYPE_CHECKING, cast

from synadia_ai.agents import MSG_ID_HEADER, EdgePublisher, EdgePublisherOptions

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from nats.aio.client import Client as NATSClient


def _sign(record_id: str) -> Callable[[bytes], Awaitable[dict[str, str]]]:
    """Stand-in for the identity signing step; the publisher only awaits it."""

    async def sign(_payload: bytes) -> dict[str, str]:
        return {
            MSG_ID_HEADER: record_id,
            "Agent-Sender": '{"v":1,"account":"$G","user":"U"}',
        }

    return sign


FAST = EdgePublisherOptions(
    ack_timeout_s=0.05,
    initial_retry_delay_s=0.001,
    max_retry_delay_s=0.004,
    close_drain_s=1.0,
)


class FakeConnection:
    """Records requests; fails the first ``fail_times`` of them."""

    def __init__(self, fail_times: int = 0) -> None:
        self.requests: list[tuple[str, bytes, dict[str, str]]] = []
        self._fail_times = fail_times

    async def request(
        self,
        subject: str,
        payload: bytes,
        timeout: float = 0.0,
        headers: dict[str, str] | None = None,
    ) -> object:
        self.requests.append((subject, payload, dict(headers or {})))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise TimeoutError("no ack")
        return object()


def _publisher(nc: FakeConnection, options: EdgePublisherOptions = FAST) -> EdgePublisher:
    return EdgePublisher(cast("NATSClient", nc), options)


async def _drain(publisher: EdgePublisher, timeout: float = 2.0) -> None:
    """Wait until the queue empties (or fail the test)."""
    deadline = asyncio.get_running_loop().time() + timeout
    while publisher.queued > 0:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"queue did not drain: {publisher.queued} left")
        await asyncio.sleep(0.005)


async def test_publishes_request_style_with_the_record_id_as_msg_id() -> None:
    nc = FakeConnection()
    publisher = _publisher(nc)
    publisher.enqueue("TRACE.edges", b'{"x":1}', "abc123", _sign("abc123"))
    await _drain(publisher)
    assert len(nc.requests) == 1
    subject, payload, headers = nc.requests[0]
    assert subject == "TRACE.edges"
    assert payload == b'{"x":1}'
    assert headers["Nats-Msg-Id"] == "abc123"
    assert "Agent-Sender" in headers
    assert publisher.dropped == 0


async def test_retries_the_same_bytes_until_acked() -> None:
    nc = FakeConnection(fail_times=2)
    publisher = _publisher(nc)
    publisher.enqueue("TRACE.edges", b'{"x":1}', "abc123", _sign("abc123"))
    await _drain(publisher)
    # Three attempts, byte-identical: the retry is idempotent at the
    # stream via Nats-Msg-Id.
    assert len(nc.requests) == 3
    assert {r[1] for r in nc.requests} == {b'{"x":1}'}
    assert {r[2]["Nats-Msg-Id"] for r in nc.requests} == {"abc123"}
    assert publisher.dropped == 0


async def test_preserves_fifo_order_across_records() -> None:
    nc = FakeConnection()
    publisher = _publisher(nc)
    for i in range(5):
        publisher.enqueue("TRACE.edges", str(i).encode(), f"rec{i}", _sign(f"rec{i}"))
    await _drain(publisher)
    assert [r[1] for r in nc.requests] == [b"0", b"1", b"2", b"3", b"4"]


async def test_full_ring_evicts_the_oldest_and_counts_it() -> None:
    # A connection that never acks, so the queue fills instead of draining.
    nc = FakeConnection(fail_times=10_000)
    publisher = _publisher(nc, replace(FAST, queue_capacity=3))
    for i in range(5):
        publisher.enqueue("TRACE.edges", str(i).encode(), f"rec{i}", _sign(f"rec{i}"))
    assert publisher.queued == 3
    assert publisher.dropped == 2
    await publisher.close()


async def test_enqueue_after_close_is_ignored() -> None:
    nc = FakeConnection()
    publisher = _publisher(nc)
    await publisher.close()
    publisher.enqueue("TRACE.edges", b"x", "rec", _sign("rec"))
    await asyncio.sleep(0.02)
    assert nc.requests == []
    assert publisher.queued == 0


async def test_close_flushes_what_is_queued() -> None:
    nc = FakeConnection()
    publisher = _publisher(nc)
    for i in range(3):
        publisher.enqueue("TRACE.edges", str(i).encode(), f"rec{i}", _sign(f"rec{i}"))
    await publisher.close()
    assert len(nc.requests) == 3
    assert publisher.queued == 0
