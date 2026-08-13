"""E2E for trace propagation: derived thread ids + root_id forwarding.

Against a real nats-server: registers an agent whose handler records its
:class:`PromptStream` trace identity, prompts it through the client SDK,
and asserts the two ends independently derive the same thread id — the
core zero-wire-surface property of the design — plus root forwarding,
the root test, and spawn-edge accumulation/drain on ``trace_headers``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from synadia_ai.agents import Agents, ResponseChunk, TraceContext

from synadia_ai.agent_service import AgentService, PromptStream

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient
    from synadia_ai.agents import Envelope

    from tests.harness.evidence import EvidenceRecorder

AGENT = "test"
OWNER = "pytest"
SESSION_NAME = "trace"


@pytest.mark.asyncio
async def test_thread_identity_and_root_forwarding(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    recorded: list[dict[str, Any]] = []

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        marker = stream.record_spawn(
            "cafebabe00000000", tool_call_id="toolu_test", edge_type="tool_call"
        )
        first = stream.trace_headers()
        second = stream.trace_headers()  # spawn entry must have drained
        recorded.append(
            {
                "thread_id": stream.thread_id,
                "root_id": stream.root_id,
                "is_root": stream.is_root,
                "marker": marker,
                "headers_first": first,
                "headers_second": second,
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

        # --- header shapes ------------------------------------------
        for obs in (root_obs, child_obs):
            marker = obs["marker"]
            assert marker["x-agent-event"] == "spawn"
            assert marker["x-agent-thread-id"] == obs["thread_id"]
            assert marker["x-agent-root-id"] == obs["root_id"]
            assert marker["x-agent-spawned"] == "cafebabe00000000:toolu_test:tool_call"
            # Completion-report channel: present once, then drained.
            assert obs["headers_first"]["x-agent-spawned"] == marker["x-agent-spawned"]
            assert obs["headers_first"]["x-agent-thread-id"] == obs["thread_id"]
            assert "x-agent-spawned" not in obs["headers_second"]

        evidence.write_json("trace-observations.json", recorded)
    finally:
        await agents.close()
        await service.stop()
