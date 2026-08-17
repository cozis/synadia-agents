"""E2E for trace propagation: derived thread ids + root_id forwarding.

Against a real nats-server: registers an agent whose handler records its
:class:`PromptStream` trace identity, prompts it through the client SDK,
and asserts the two ends independently derive the same thread id — the
core zero-wire-surface property of the design — plus root forwarding,
the root test, and spawn-edge accumulation/drain on ``trace_headers``.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import pytest
from synadia_ai.agents import (
    Agents,
    DiscoverFilter,
    ResponseChunk,
    TraceContext,
    derive_thread_id,
    is_thread_id,
    tool_scope,
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


def _expected_identity_path(session_name: str, instance_id: str) -> str:
    """The base-URL prefix a harness composes for this test's agents."""
    return f"synadia/{AGENT}/{OWNER}/{session_name}/{instance_id}"


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

    agents = Agents(nc=nc)
    try:
        async with _running_service(nc, SESSION_NAME, handler) as service:
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
                expected_trace = f"{obs['root_id']}:{obs['thread_id']}"
                assert marker["x-synadia-event"] == "spawn"
                assert marker["x-synadia-trace"] == expected_trace
                assert marker["x-synadia-spawned"] == "cafebabe00000000:toolu_test:tool_call"
                # Completion-report channel: present once, then drained.
                assert obs["headers_first"]["x-synadia-spawned"] == marker["x-synadia-spawned"]
                assert obs["headers_first"]["x-synadia-trace"] == expected_trace
                assert "x-synadia-spawned" not in obs["headers_second"]

            # §3.2 attribution travels in the client's base-URL path, not in
            # headers — values match what discovery advertises (§8.3 instance
            # id included), so proxy node labels line up with heartbeats.
            assert service.identity_path == _expected_identity_path(SESSION_NAME, agent.instance_id)

            evidence.write_json("trace-observations.json", recorded)
    finally:
        await agents.close()


@pytest.mark.asyncio
async def test_hostile_root_id_rejected_at_decode(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    """A root_id outside the 16-lowercase-hex shape 400s at decode.

    Security boundary: root_id flows into HTTP header values via
    trace_headers(), so a CRLF-bearing value must be rejected before the
    handler (and thus any header emission) ever sees it.
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


@pytest.mark.asyncio
async def test_task_outliving_request_spawns_marker_only(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    """An ambient spawn from a task that outlives its request: marker only.

    The task inherits the ambient trace via its PEP 567 context copy;
    once the request finishes the completion-report channel is closed,
    so the late spawn delivers via the spawn-time marker (still joining
    the parent's tree) and nothing accretes in the finished stream's
    pending set.
    """
    agents = Agents(nc=nc)
    captured: dict[str, Any] = {}
    release = asyncio.Event()
    late_done = asyncio.Event()

    async def child_handler(envelope: Envelope, stream: PromptStream) -> None:
        await stream.send("child ok")

    async def parent_handler(envelope: Envelope, stream: PromptStream) -> None:
        captured["stream"] = stream  # test-only: inspect post-completion state

        async def late_spawn() -> None:
            await release.wait()  # deterministically after the request finished
            found = await agents.discover(filter=DiscoverFilter(session_name="late-child"))
            handle = found[0].prompt("late sub-task")
            async for _ in handle:
                pass
            captured["marker"] = handle.spawn_marker_headers
            captured["late_root"] = handle.root_id
            late_done.set()

        captured["task"] = asyncio.create_task(late_spawn())
        await stream.send("parent ok")

    outer = Agents(nc=nc)
    try:
        async with (
            _running_service(nc, "late-child", child_handler),
            _running_service(nc, "late-parent", parent_handler),
        ):
            found = await outer.discover(filter=DiscoverFilter(session_name="late-parent"))
            assert len(found) == 1
            root_handle = found[0].prompt("go")
            async for _ in root_handle:
                pass
            # Terminator consumed ⇒ the service closed the ledger before emitting it.
            release.set()
            await asyncio.wait_for(late_done.wait(), timeout=5.0)

            marker = captured["marker"]
            assert marker is not None  # the marker channel still delivers the edge
            # The parent is the tree root: both trace slots carry its thread id.
            assert marker["x-synadia-trace"] == f"{root_handle.thread_id}:{root_handle.thread_id}"
            assert captured["late_root"] == root_handle.thread_id  # still joins the tree
            headers_after = captured["stream"].trace_headers()
            assert "x-synadia-spawned" not in headers_after  # no post-completion accretion
            evidence.write_json(
                "late-spawn-marker-only.json", {"marker": marker, "headers_after": headers_after}
            )
    finally:
        await outer.close()
        await agents.close()


@pytest.mark.asyncio
async def test_forwarded_envelope_keeps_spawn_edge(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    """A handler forwarding its received envelope verbatim keeps its edge.

    The forwarded envelope carries the SDK-stamped root_id of the
    parent's own tree — a same-tree spawn, so the ambient auto-record
    must fire on both edge channels even though the envelope's root_id
    is non-None (the one-line way to preserve attachments must not
    silently orphan the child)."""
    agents = Agents(nc=nc)
    child_obs: dict[str, Any] = {}
    parent_obs: dict[str, Any] = {}

    async def child_handler(envelope: Envelope, stream: PromptStream) -> None:
        child_obs.update(thread_id=stream.thread_id, root_id=stream.root_id)
        await stream.send("child ok")

    async def parent_handler(envelope: Envelope, stream: PromptStream) -> None:
        found = await agents.discover(filter=DiscoverFilter(session_name="fwd-child"))
        assert found, "child agent not discovered"
        with tool_scope("toolu_fwd"):
            handle = found[0].prompt(envelope)  # verbatim — root_id is SDK-stamped
            async for _ in handle:
                pass
        parent_obs.update(
            root_id=stream.root_id,
            child_thread=handle.thread_id,
            child_root=handle.root_id,
            marker=handle.spawn_marker_headers,
            headers_after=stream.trace_headers(),
        )
        await stream.send("parent ok")

    outer = Agents(nc=nc)
    try:
        async with (
            _running_service(nc, "fwd-child", child_handler),
            _running_service(nc, "fwd-parent", parent_handler),
        ):
            found = await outer.discover(filter=DiscoverFilter(session_name="fwd-parent"))
            assert len(found) == 1
            root_handle = found[0].prompt("delegate this verbatim")
            async for _ in root_handle:
                pass

            # Same tree throughout: root prompt -> parent -> forwarded child.
            assert parent_obs["root_id"] == root_handle.thread_id
            assert parent_obs["child_root"] == parent_obs["root_id"]
            assert child_obs["root_id"] == parent_obs["root_id"]
            assert child_obs["thread_id"] == parent_obs["child_thread"]

            # The edge survived the non-None envelope root, on both channels.
            expected_edge = f"{child_obs['thread_id']}:toolu_fwd:tool_call"
            assert parent_obs["marker"] is not None
            assert parent_obs["marker"]["x-synadia-spawned"] == expected_edge
            assert parent_obs["headers_after"]["x-synadia-spawned"] == expected_edge

            evidence.write_json(
                "forwarded-envelope-edge.json", {"parent": parent_obs, "child": child_obs}
            )
    finally:
        await outer.close()
        await agents.close()


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
        assert scopeless.spawn_marker_headers["x-synadia-spawned"] == expected_programmatic
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

    outer = Agents(nc=nc)
    try:
        async with (
            _running_service(nc, "child", child_handler),
            _running_service(nc, "parent", parent_handler) as parent_svc,
        ):
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
            assert parent_obs["marker"]["x-synadia-event"] == "spawn"
            assert parent_obs["marker"]["x-synadia-spawned"] == expected_edge
            assert parent_obs["headers_after"]["x-synadia-spawned"] == expected_edge

            # Attribution names the SPAWNER: the marker rides the parent's
            # provider client, whose base-URL identity path is the parent's.
            assert parent_svc.identity_path == _expected_identity_path(
                "parent", found[0].instance_id
            )

            evidence.write_json(
                "ambient-nested-spawn.json", {"parent": parent_obs, "child": child_obs}
            )
    finally:
        await outer.close()
        await agents.close()
