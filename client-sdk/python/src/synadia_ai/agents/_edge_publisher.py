"""Best-effort edge-record delivery.

The wire contract is best-effort (observability.md, Delivery
Guarantees); this publisher is how far *this* SDK takes it.
``prompt()`` only enqueues — every wait happens in a background drain
task nothing awaits, so nothing in the observability layer can slow an
agent.

One bounded FIFO ring per connection. The drain publishes request-style
and awaits the stream's PubAck, which is relayed back across the account
import; on failure it backs off exponentially and retries the same bytes
(idempotent within the stream's duplicate window via ``Nats-Msg-Id``).
Records are lost only to ring eviction, which is counted.

Mirrors the TS SDK's ``trace/publisher.ts``.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections import deque
from dataclasses import dataclass
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary

from ._logging import get_logger

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient

log = get_logger(__name__)

#: Records held before the oldest is evicted to make room.
DEFAULT_EDGE_QUEUE_CAPACITY = 100
#: How long one publish waits for its PubAck, in seconds.
DEFAULT_EDGE_ACK_TIMEOUT_S = 2.0
#: First retry delay after a failure; also the value reset to on success.
DEFAULT_EDGE_INITIAL_RETRY_DELAY_S = 1.0
#: Ceiling on the exponential backoff.
DEFAULT_EDGE_MAX_RETRY_DELAY_S = 60.0
#: Multiplier applied to the delay after each failed round.
DEFAULT_EDGE_RETRY_DELAY_FACTOR = 2.0
#: How long :meth:`EdgePublisher.close` gives the drain to flush.
DEFAULT_EDGE_CLOSE_DRAIN_S = 2.0

# Backoff jitter, ±15%, so a fleet does not retry in lockstep after an outage.
_JITTER = 0.15

_MSG_ID_HEADER = "Nats-Msg-Id"


@dataclass(frozen=True, slots=True)
class EdgePublisherOptions:
    """Delivery tuning; the defaults suit every deployment we know of."""

    queue_capacity: int = DEFAULT_EDGE_QUEUE_CAPACITY
    ack_timeout_s: float = DEFAULT_EDGE_ACK_TIMEOUT_S
    initial_retry_delay_s: float = DEFAULT_EDGE_INITIAL_RETRY_DELAY_S
    max_retry_delay_s: float = DEFAULT_EDGE_MAX_RETRY_DELAY_S
    retry_delay_factor: float = DEFAULT_EDGE_RETRY_DELAY_FACTOR
    close_drain_s: float = DEFAULT_EDGE_CLOSE_DRAIN_S


@dataclass(frozen=True, slots=True)
class _QueuedEdge:
    subject: str
    payload: bytes
    record_id: str


class EdgePublisher:
    """A bounded ring of edge records drained by one background task."""

    def __init__(self, nc: NATSClient, options: EdgePublisherOptions | None = None) -> None:
        self._nc = nc
        self._options = options if options is not None else EdgePublisherOptions()
        self._queue: deque[_QueuedEdge] = deque()
        self._drain_task: asyncio.Task[None] | None = None
        self._delay_s = self._options.initial_retry_delay_s
        self._dropped = 0
        self._closed = False

    @property
    def dropped(self) -> int:
        """Records dropped so far — always ring evictions, never silent losses."""
        return self._dropped

    @property
    def queued(self) -> int:
        """Records waiting to be delivered."""
        return len(self._queue)

    def enqueue(self, subject: str, payload: bytes, record_id: str) -> None:
        """Hand one record to the publisher.

        Never blocks, never raises: a full ring evicts its oldest entry
        (counted) to make room for the newest.
        """
        if self._closed:
            return
        if len(self._queue) >= self._options.queue_capacity:
            self._queue.popleft()
            self._dropped += 1
        self._queue.append(_QueuedEdge(subject=subject, payload=payload, record_id=record_id))
        if self._drain_task is None or self._drain_task.done():
            self._drain_task = asyncio.create_task(self._drain(), name="synadia-edge-drain")

    async def close(self) -> None:
        """Flush what is queued, then stop accepting records."""
        if self._closed:
            return
        task = self._drain_task
        if task is not None and not task.done():
            with contextlib.suppress(TimeoutError, asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(task), timeout=self._options.close_drain_s)
        self._closed = True
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _drain(self) -> None:
        while self._queue and not self._closed:
            head = self._queue[0]
            try:
                # Request-style: the reply is the stream's PubAck, relayed
                # back through the account import. A duplicate (a retry
                # after a lost ack) is absorbed by the stream's
                # `Nats-Msg-Id` window.
                await self._nc.request(
                    head.subject,
                    head.payload,
                    timeout=self._options.ack_timeout_s,
                    headers={_MSG_ID_HEADER: head.record_id},
                )
            except Exception:
                # Ambiguous by construction: the record may be stored with
                # the ack lost. Retrying the same bytes is safe, so back
                # off and keep the record queued — only eviction discards.
                await asyncio.sleep(_with_jitter(self._delay_s))
                self._delay_s = min(
                    self._delay_s * self._options.retry_delay_factor,
                    self._options.max_retry_delay_s,
                )
            else:
                self._queue.popleft()
                self._delay_s = self._options.initial_retry_delay_s


def _with_jitter(delay_s: float) -> float:
    return delay_s * (1 + random.uniform(-_JITTER, _JITTER))


# --- per-connection registry ------------------------------------------
#
# One publisher per NATS connection, so every client on that connection
# shares one queue and one drain. The first enabler's configuration wins;
# a differing later configuration warns rather than silently applying.

_registry: WeakKeyDictionary[NATSClient, EdgePublisher] = WeakKeyDictionary()


def edge_publisher_for(
    nc: NATSClient, options: EdgePublisherOptions | None = None
) -> EdgePublisher:
    """The connection's edge publisher, created on first use."""
    existing = _registry.get(nc)
    if existing is not None:
        if options is not None and options != existing._options:
            log.warning(
                "edge publisher for this connection already configured; "
                "ignoring the differing delivery options"
            )
        return existing
    publisher = EdgePublisher(nc, options)
    _registry[nc] = publisher
    return publisher


async def close_edge_publisher_for(nc: NATSClient) -> None:
    """Flush and drop the connection's publisher, if it has one."""
    publisher = _registry.pop(nc, None)
    if publisher is not None:
        await publisher.close()


__all__ = [
    "DEFAULT_EDGE_ACK_TIMEOUT_S",
    "DEFAULT_EDGE_CLOSE_DRAIN_S",
    "DEFAULT_EDGE_INITIAL_RETRY_DELAY_S",
    "DEFAULT_EDGE_MAX_RETRY_DELAY_S",
    "DEFAULT_EDGE_QUEUE_CAPACITY",
    "DEFAULT_EDGE_RETRY_DELAY_FACTOR",
    "EdgePublisher",
    "EdgePublisherOptions",
    "close_edge_publisher_for",
    "edge_publisher_for",
]
