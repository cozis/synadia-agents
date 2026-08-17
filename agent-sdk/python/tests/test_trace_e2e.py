"""E2E for trace propagation: derived thread ids + root_id forwarding.

Against a real nats-server: registers an agent whose handler records its
:class:`PromptStream` trace identity, prompts it through the client SDK,
and asserts the two ends independently derive the same thread id — the
core zero-wire-surface property of the design — plus root forwarding
and the root test.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest
from synadia_ai.agents import (
    Agents,
    ResponseChunk,
    TraceContext,
    derive_thread_id,
    is_thread_id,
)

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
async def test_thread_identity_and_root_forwarding(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    recorded: list[dict[str, Any]] = []

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        recorded.append(
            {
                "thread_id": stream.thread_id,
                "root_id": stream.root_id,
                "is_root": stream.is_root,
            }
        )
        await stream.send("ok")

    agents = Agents(nc=nc)
    try:
        async with _running_service(nc, SESSION_NAME, handler):
            found = await agents.discover()
            assert len(found) == 1
            agent = found[0]

            # --- root prompt (no trace) --------------------------------
            handle = agent.prompt("root prompt")
            async for msg in handle:
                if isinstance(msg, ResponseChunk):
                    assert msg.text == "ok"
            root_obs = recorded[-1]

            # Both ends derived the same thread id with nothing exchanged.
            assert root_obs["thread_id"] == handle.thread_id
            # A traceless prompt starts a new tree rooted at itself.
            assert root_obs["root_id"] == handle.thread_id == handle.root_id
            assert root_obs["is_root"] is True

            # --- spawned prompt (forwarded trace) ----------------------
            parent_root = root_obs["root_id"]
            handle2 = agent.prompt("spawned prompt", trace=TraceContext(root_id=parent_root))
            async for _msg in handle2:
                pass
            child_obs = recorded[-1]

            assert child_obs["thread_id"] == handle2.thread_id
            assert child_obs["thread_id"] != root_obs["thread_id"]
            assert child_obs["root_id"] == parent_root == handle2.root_id
            assert child_obs["is_root"] is False

            evidence.write_json("trace-observations.json", recorded)
    finally:
        await agents.close()


@pytest.mark.asyncio
async def test_hostile_root_id_rejected_at_decode(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    """A root_id outside the 16-lowercase-hex shape 400s at decode.

    Security boundary: root_id flows into HTTP header values agent-side,
    so a CRLF-bearing value must be rejected before the handler (and
    thus any header emission) ever sees it.
    """
    handled: list[str] = []

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        handled.append(envelope.prompt)

    async with _running_service(nc, "hostile", handler) as service:
        inbox = nc.new_inbox()
        sub = await nc.subscribe(inbox)
        payload = json.dumps({"prompt": "hi", "root_id": "x\r\nx-evil: 1"}).encode()
        await nc.publish(service.subject.prompt, payload, reply=inbox)
        msg = await sub.next_msg(timeout=2.0)
        await sub.unsubscribe()

        headers = dict(msg.headers or {})
        assert headers.get("Nats-Service-Error-Code") == "400"
        assert handled == [], "handler MUST NOT run on a malformed root_id"
        evidence.write_json("hostile-root-id.json", {"headers": headers})


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
        seen.append(
            {"thread_id": stream.thread_id, "root_id": stream.root_id, "is_root": stream.is_root}
        )

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
            assert obs["root_id"] == obs["thread_id"]  # provisional root of its own tree
            assert obs["is_root"] is True
        evidence.write_json("replyless-thread-ids.json", seen)
