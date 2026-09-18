"""Heartbeat publisher half — agent-side per protocol §8.

Periodically emits a §8.3 :class:`~synadia_ai.agents.HeartbeatPayload`
on the agent's heartbeat subject. The wire shape, the
``HeartbeatTracker`` (caller-side), and the ``now_iso`` helper live in
:mod:`synadia_ai.agents`; this module owns only the *publishing*
side and the ``build_heartbeat_payload`` helper that the
:class:`~synadia_ai.agent_service.AgentService` status handler reuses
to ensure heartbeat and status responses share the exact same payload
construction path.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING

from synadia_ai.agents import HeartbeatPayload
from synadia_ai.agents.heartbeat import now_iso

from ._logging import get_logger

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient
    from synadia_ai.agents import AgentSubject

log = get_logger(__name__)

#: Reads the extra heartbeat fields when a beat is built, so a value that
#: moves between beats — a counter — is current on every one.
ExtrasProvider = Callable[[], Mapping[str, object]]

# The §8.3 field names. An extra under one of these would either shadow
# the real field or, as a duplicate keyword, blow up construction with a
# TypeError; refused up front with a message that names the key instead.
_RESERVED_KEYS = frozenset(HeartbeatPayload.model_fields)


def build_heartbeat_payload(
    subject: AgentSubject,
    interval_s: int,
    instance_id: str,
    extras: Mapping[str, object] | None = None,
) -> HeartbeatPayload:
    """Construct a §8.3 heartbeat payload for ``subject``.

    Pure helper shared between the heartbeat publisher and the v0.3
    ``status`` request/response endpoint — both emit the same payload
    shape, and richer agent metadata added in future PRs lands here in
    one place.

    ``extras`` are forward-compat fields merged into the wire payload
    alongside the §8.3 ones (the TypeScript encoder's ``extras`` slot).
    A key that reuses a §8.3 field name raises ``ValueError``.
    """
    if extras:
        clash = sorted(_RESERVED_KEYS.intersection(extras))
        if clash:
            raise ValueError(
                f"heartbeat extras must not reuse the §8.3 field names {clash}; "
                "those are set from the subject and the service"
            )
    return HeartbeatPayload(
        agent=subject.agent,
        owner=subject.owner,
        session=subject.session_name,
        instance_id=instance_id,
        ts=now_iso(),
        interval_s=interval_s,
        **(extras or {}),
    )


async def publish_one(
    nc: NATSClient,
    subject: AgentSubject,
    interval_s: int,
    instance_id: str,
    extras: Mapping[str, object] | None = None,
) -> None:
    """Publish a single heartbeat frame to the agent's heartbeat subject."""
    payload = build_heartbeat_payload(subject, interval_s, instance_id, extras)
    data = payload.model_dump_json().encode("utf-8")
    await nc.publish(subject.heartbeat, data)


async def run_publisher(
    nc: NATSClient,
    subject: AgentSubject,
    interval_s: int,
    instance_id: str,
    stop: asyncio.Event,
    extras: ExtrasProvider | None = None,
) -> None:
    """Periodically publish heartbeats until `stop` is set.

    ``extras``, when given, is called before each beat for the extra
    fields to carry on it (see :func:`build_heartbeat_payload`). A
    provider that raises, or extras the payload cannot carry (a reserved
    key, a value that does not serialise), cost that beat its extras and
    nothing more: the beat still goes out, the failure is logged, and the
    publisher keeps running — a bad extra must never take the agent's
    liveness down with it.

    A failed publish (e.g. ``ConnectionClosedError`` after a broker
    restart) MUST NOT crash the publisher task with a non-cancellation
    exception: that would (a) make the agent go dark while the micro
    service still appears registered, and (b) cause :meth:`AgentService.stop`
    to re-raise on teardown. The publisher logs the failure and exits
    cleanly so ``stop()`` can complete; the surrounding service decides
    whether to recover.
    """
    log.debug("heartbeat publisher starting for %s (interval=%ss)", subject.inbox, interval_s)

    async def beat() -> None:
        fields: Mapping[str, object] | None = None
        if extras is not None:
            try:
                fields = extras()
            except Exception:
                log.exception("heartbeat extras provider failed; publishing without extras")
        try:
            await publish_one(nc, subject, interval_s, instance_id, fields)
        except (TypeError, ValueError) as exc:
            # A reserved key or an unserialisable value — a fault in the
            # extras, not in the transport (pydantic's serialisation error
            # is a ValueError). Transport errors propagate to the caller.
            if fields is None:
                raise
            log.error("heartbeat extras rejected (%s); publishing without extras", exc)
            await publish_one(nc, subject, interval_s, instance_id)

    try:
        # Emit one heartbeat immediately so callers that subscribe-then-discover
        # observe liveness without waiting a full interval (§8.5).
        await beat()
        while not stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval_s)
            if stop.is_set():
                break
            await beat()
    except Exception:
        log.exception("heartbeat publisher failed for %s; exiting", subject.inbox)
        return
    log.debug("heartbeat publisher stopped for %s", subject.inbox)


__all__ = [
    "ExtrasProvider",
    "build_heartbeat_payload",
    "publish_one",
    "run_publisher",
]
