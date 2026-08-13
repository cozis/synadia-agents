"""E2E for trace propagation: derived thread ids.

Against a real nats-server: registers an agent whose handler records its
:class:`PromptStream` trace identity, prompts it through the client SDK,
and asserts the two ends independently derive the same thread id — the
core zero-wire-surface property of the design.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from synadia_ai.agents import Agents, ResponseChunk

from synadia_ai.agent_service import AgentService, PromptStream

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient
    from synadia_ai.agents import Envelope

    from tests.harness.evidence import EvidenceRecorder

AGENT = "test"
OWNER = "pytest"
SESSION_NAME = "trace"


@pytest.mark.asyncio
async def test_thread_identity(nc: NATSClient, evidence: EvidenceRecorder) -> None:
    recorded: list[dict[str, Any]] = []

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        recorded.append(
            {
                "thread_id": stream.thread_id,
                "headers": stream.trace_headers(),
            }
        )
        await stream.send("ok")

    service = AgentService(
        agent=AGENT,
        owner=OWNER,
        session_name=SESSION_NAME,
        nc=nc,
        description="trace e2e agent",
        heartbeat_interval_s=30,
    )
    service.on_prompt(handler)
    await service.start()
    agents = Agents(nc=nc)
    try:
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
        assert obs["headers"]["x-agent-thread-id"] == handle.thread_id

        evidence.write_json("trace-observations.json", recorded)
    finally:
        await agents.close()
        await service.stop()
