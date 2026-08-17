"""E2E for trace propagation: derived thread ids.

Against a real nats-server: registers an agent whose handler records its
:class:`PromptStream` trace identity, prompts it through the client SDK,
and asserts the two ends independently derive the same thread id — the
core zero-wire-surface property of the design.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest
from synadia_ai.agents import Agents, ResponseChunk, derive_thread_id, is_thread_id

from synadia_ai.agent_service import AgentService, PromptStream

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable

    from nats.aio.client import Client as NATSClient
    from synadia_ai.agents import Envelope

    from tests.harness.evidence import EvidenceRecorder

AGENT = "test"
OWNER = "pytest"
SESSION_NAME = "trace"


@asynccontextmanager
async def _running_service(
    nc: NATSClient,
    session_name: str,
    handler: Callable[[Envelope, PromptStream], Awaitable[None]],
) -> AsyncIterator[AgentService]:
    """A started :class:`AgentService` serving ``handler``; stopped on exit."""
    service = AgentService(
        agent=AGENT, owner=OWNER, session_name=session_name, nc=nc, heartbeat_interval_s=30
    )
    service.on_prompt(handler)
    await service.start()
    try:
        yield service
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_thread_identity(nc: NATSClient, evidence: EvidenceRecorder) -> None:
    recorded: list[dict[str, Any]] = []

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        recorded.append({"thread_id": stream.thread_id})
        await stream.send("ok")

    agents = Agents(nc=nc)
    try:
        async with _running_service(nc, SESSION_NAME, handler):
            found = await agents.discover()
            assert len(found) == 1
            agent = found[0]

            handle = agent.prompt("root prompt")
            async for msg in handle:
                if isinstance(msg, ResponseChunk):
                    assert msg.text == "ok"
            obs = recorded[-1]

            # Both ends derived the same thread id with nothing exchanged.
            assert obs["thread_id"] == handle.thread_id

            evidence.write_json("trace-observations.json", recorded)
    finally:
        await agents.close()


@pytest.mark.asyncio
async def test_replyless_prompts_get_distinct_random_thread_ids(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    """Fire-and-forget prompts (no reply subject) must NOT share a thread id.

    Hashing the empty reply subject would merge every reply-less request
    on every agent into one well-known constant thread
    (sha256('')[:16]); instead each gets a random shape-valid id.
    """
    seen: list[dict[str, Any]] = []

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        # No send — there is nowhere to reply to; ack/terminator failures
        # are logged and swallowed by the service.
        seen.append({"thread_id": stream.thread_id})

    async with _running_service(nc, "fireforget", handler) as service:
        for _ in range(2):
            await nc.publish(service.subject.prompt, b'{"prompt": "x"}')  # no reply=
        deadline = asyncio.get_event_loop().time() + 2.0
        while len(seen) < 2 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)
        assert len(seen) == 2, f"expected 2 handled prompts, saw {len(seen)}"

        first, second = seen
        assert first["thread_id"] != second["thread_id"]
        for obs in seen:
            assert is_thread_id(obs["thread_id"])
            assert obs["thread_id"] != derive_thread_id("")  # not the constant phantom
        evidence.write_json("replyless-thread-ids.json", seen)
