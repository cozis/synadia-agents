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
from synadia_ai.agents import Agents, DiscoverFilter, ResponseChunk, TraceContext, tool_scope

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


@pytest.mark.asyncio
async def test_ambient_nested_spawn(nc: NATSClient, evidence: EvidenceRecorder) -> None:
    """A parent handler spawns a child with ZERO explicit trace plumbing.

    The agent-sdk binds the ambient ActiveTrace around the handler; the
    client-sdk's prompt() picks it up (root forwarding + spawn edge with
    the ambient tool_scope id) — the core of the implicit-correlation
    layer, over a real broker.
    """
    agents = Agents(nc=nc)
    child_obs: dict[str, Any] = {}
    parent_obs: dict[str, Any] = {}

    async def child_handler(envelope: Envelope, stream: PromptStream) -> None:
        child_obs.update(thread_id=stream.thread_id, root_id=stream.root_id, is_root=stream.is_root)
        await stream.send("child ok")

    async def parent_handler(envelope: Envelope, stream: PromptStream) -> None:
        found = await agents.discover(filter=DiscoverFilter(session_name="child"))
        assert found, "child agent not discovered"

        # Scopeless ambient spawn — edge is honest about having no tool.
        scopeless = found[0].prompt("fire and observe")
        async for _ in scopeless:
            pass
        assert scopeless.spawn_marker_headers is not None
        expected_programmatic = f"{scopeless.thread_id}::programmatic"
        assert scopeless.spawn_marker_headers["x-agent-spawned"] == expected_programmatic
        stream.trace_headers()  # drain the programmatic entry before the scoped one

        with tool_scope("toolu_ambient"):
            # No trace=, no record_spawn — everything ambient.
            handle = found[0].prompt("sub-task")
            async for _ in handle:
                pass
        parent_obs.update(
            thread_id=stream.thread_id,
            root_id=stream.root_id,
            child_handle_thread=handle.thread_id,
            marker=handle.spawn_marker_headers,
            headers_after=stream.trace_headers(),
        )
        await stream.send("parent ok")

    child_svc = AgentService(
        agent=AGENT, owner=OWNER, session_name="child", nc=nc, heartbeat_interval_s=30
    )
    child_svc.on_prompt(child_handler)
    parent_svc = AgentService(
        agent=AGENT, owner=OWNER, session_name="parent", nc=nc, heartbeat_interval_s=30
    )
    parent_svc.on_prompt(parent_handler)
    await child_svc.start()
    await parent_svc.start()

    outer = Agents(nc=nc)
    try:
        found = await outer.discover(filter=DiscoverFilter(session_name="parent"))
        assert len(found) == 1
        root_handle = found[0].prompt("do the thing")
        async for _ in root_handle:
            pass

        # Tree: root prompt -> parent thread -> (ambient spawn) -> child thread.
        assert parent_obs["thread_id"] == root_handle.thread_id
        assert parent_obs["root_id"] == root_handle.thread_id  # parent is the root
        assert child_obs["root_id"] == parent_obs["root_id"]  # forwarded implicitly
        assert child_obs["thread_id"] == parent_obs["child_handle_thread"]
        assert child_obs["is_root"] is False

        # Edge auto-recorded with the ambient tool id, on both channels.
        expected_edge = f"{child_obs['thread_id']}:toolu_ambient:tool_call"
        assert parent_obs["marker"]["x-agent-event"] == "spawn"
        assert parent_obs["marker"]["x-agent-spawned"] == expected_edge
        assert parent_obs["headers_after"]["x-agent-spawned"] == expected_edge

        evidence.write_json("ambient-nested-spawn.json", {"parent": parent_obs, "child": child_obs})
    finally:
        await outer.close()
        await agents.close()
        await parent_svc.stop()
        await child_svc.stop()
