"""Observability traces end-to-end: the service mints each execution's id,
announces it on the flat trace subject (``afo.threads``), exposes provider headers, and
binds the ambient trace so a coordinator's nested prompt joins the tree.

Evidence per test under ``tests/_evidence/<test>/``: ``trace-records.jsonl``
is the literal sequence observed on ``afo.threads``.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest
from synadia_ai.agents import (
    HEADER_PARENT,
    HEADER_TRACE,
    Agents,
    Envelope,
    ProtocolError,
    ResponseChunk,
    TraceRecord,
    decode,
    trace_headers,
)

from synadia_ai.agent_service import AgentService, PromptStream

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription

    from tests.harness.evidence import EvidenceRecorder

OWNER = "pytest"


class _TraceSpy:
    """Collects every TraceRecord published while attached."""

    def __init__(self, nc: NATSClient, subject: str = "afo.threads") -> None:
        self._nc = nc
        self._subject = subject
        self.records: list[TraceRecord] = []
        self.subjects: list[str] = []
        self._sub: Subscription | None = None

    async def __aenter__(self) -> _TraceSpy:
        async def on_msg(msg: Msg) -> None:
            self.subjects.append(msg.subject)
            self.records.append(TraceRecord.model_validate_json(msg.data))

        self._sub = await self._nc.subscribe(self._subject, cb=on_msg)
        return self

    async def __aexit__(self, *_: object) -> None:
        assert self._sub is not None
        await self._sub.unsubscribe()

    async def wait_for(self, count: int, timeout: float = 2.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while len(self.records) < count and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.02)
        assert len(self.records) >= count, f"saw {len(self.records)} records, wanted {count}"

    def dump(self, evidence: EvidenceRecorder) -> None:
        evidence.write_jsonl(
            "trace-records.jsonl",
            [
                {"subject": s, **json.loads(r.model_dump_json(exclude_none=True))}
                for s, r in zip(self.subjects, self.records, strict=True)
            ],
        )


async def _prompt_text(agents: Agents, service: AgentService, text: str) -> str:
    found = await agents.discover(timeout=1.0)
    agent = next(a for a in found if a.prompt_subject == service.subject.prompt)
    out: list[str] = []
    async for msg in agent.prompt(text, timeout=5.0):
        if isinstance(msg, ResponseChunk):
            out.append(msg.text)
    return "".join(out)


@pytest.mark.asyncio
async def test_root_prompt_publishes_record_and_headers(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    seen: dict[str, object] = {}

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        seen["stream_headers"] = stream.trace_headers()
        seen["ambient_headers"] = trace_headers()
        seen["prompt_id"] = stream.prompt_id
        seen["is_root"] = stream.is_root
        await stream.send(envelope.prompt)

    service = AgentService(agent="test", owner=OWNER, session_name="root", nc=nc)
    service.on_prompt(handler)
    await service.start()
    agents = Agents(nc=nc)
    try:
        async with _TraceSpy(nc) as spy:
            assert await _prompt_text(agents, service, "hello") == "hello"
            await spy.wait_for(1)
        spy.dump(evidence)

        record = spy.records[0]
        assert record.prompt_id == seen["prompt_id"]
        assert record.root_id == record.prompt_id, "a root execution is its own root"
        assert record.parent_prompt_id is None and record.tool_call_id is None
        assert (record.agent, record.owner, record.session) == ("test", OWNER, "root")
        found = await agents.discover(timeout=1.0)
        live = next(a for a in found if a.prompt_subject == service.subject.prompt)
        assert record.instance_id == live.instance_id
        assert record.ts.endswith("Z")
        assert spy.subjects[0] == "afo.threads"
        # The wire form omits the absent lineage fields entirely.
        assert "parent_prompt_id" not in json.loads(record.encode())

        assert seen["is_root"] is True
        expected = {HEADER_TRACE: f"{record.root_id}:{record.prompt_id}"}
        assert seen["stream_headers"] == expected
        assert seen["ambient_headers"] == expected, "ambient trace bound around the handler"
        assert trace_headers() == {}, "binding does not leak past the handler"
    finally:
        await agents.close()
        await service.stop()


@pytest.mark.asyncio
async def test_coordinator_spawn_joins_the_tree(nc: NATSClient, evidence: EvidenceRecorder) -> None:
    """Coordinator prompts a worker from inside its handler with a tool id:
    the worker's record and headers carry the coordinator's lineage."""
    worker_headers: dict[str, str] = {}

    async def worker_handler(envelope: Envelope, stream: PromptStream) -> None:
        worker_headers.update(stream.trace_headers())
        await stream.send(envelope.prompt.upper())

    worker = AgentService(agent="worker", owner=OWNER, session_name="w", nc=nc)
    worker.on_prompt(worker_handler)

    agents = Agents(nc=nc)

    async def coordinator_handler(envelope: Envelope, stream: PromptStream) -> None:
        found = await agents.discover(timeout=1.0)
        w = next(a for a in found if a.prompt_subject == worker.subject.prompt)
        # No trace plumbing: the ambient binding forwards parent/root.
        async for msg in w.prompt(envelope.prompt, timeout=5.0, tool_call_id="call_42"):
            if isinstance(msg, ResponseChunk):
                await stream.send(f"worker says: {msg.text}")

    coordinator = AgentService(agent="coordinator", owner=OWNER, session_name="c", nc=nc)
    coordinator.on_prompt(coordinator_handler)

    await worker.start()
    await coordinator.start()
    try:
        async with _TraceSpy(nc) as spy:
            assert await _prompt_text(agents, coordinator, "hi") == "worker says: HI"
            await spy.wait_for(2)
        spy.dump(evidence)

        by_agent = {r.agent: r for r in spy.records}
        root, child = by_agent["coordinator"], by_agent["worker"]
        assert spy.records[0] is root, "parent is announced before its child"
        assert root.parent_prompt_id is None
        assert child.parent_prompt_id == root.prompt_id
        assert child.root_id == root.root_id == root.prompt_id
        assert child.tool_call_id == "call_42"
        assert set(spy.subjects) == {"afo.threads"}, "flat subject"
        assert worker_headers == {
            HEADER_TRACE: f"{root.root_id}:{child.prompt_id}",
            HEADER_PARENT: f"{root.prompt_id}:call_42",
        }
    finally:
        await agents.close()
        await coordinator.stop()
        await worker.stop()


@pytest.mark.asyncio
async def test_custom_trace_subject(nc: NATSClient, evidence: EvidenceRecorder) -> None:
    service = AgentService(
        agent="test", owner=OWNER, session_name="custom", nc=nc, trace_subject="obs.traces"
    )

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        await stream.send("ok")

    service.on_prompt(handler)
    await service.start()
    agents = Agents(nc=nc)
    try:
        async with _TraceSpy(nc, "obs.traces") as spy, _TraceSpy(nc) as default_spy:
            assert await _prompt_text(agents, service, "x") == "ok"
            await spy.wait_for(1)
        spy.dump(evidence)
        assert spy.subjects == ["obs.traces"]
        assert default_spy.records == []
    finally:
        await agents.close()
        await service.stop()


def test_empty_trace_subject_rejected() -> None:
    with pytest.raises(ValueError, match="trace_subject"):
        AgentService(agent="test", owner=OWNER, session_name="x", nc=None, trace_subject="")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_trace_subject_none_is_silent(nc: NATSClient) -> None:
    service = AgentService(
        agent="test", owner=OWNER, session_name="quiet", nc=nc, trace_subject=None
    )
    ids: list[str] = []

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        ids.append(stream.prompt_id)
        await stream.send("ok")

    service.on_prompt(handler)
    await service.start()
    agents = Agents(nc=nc)
    try:
        async with _TraceSpy(nc) as spy:
            assert await _prompt_text(agents, service, "x") == "ok"
            await asyncio.sleep(0.2)
        assert spy.records == []
        assert len(ids) == 1, "identity still minted locally — headers keep working"
    finally:
        await agents.close()
        await service.stop()


@pytest.mark.asyncio
async def test_hostile_lineage_is_rejected_with_400(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    """A raw-inbox ``root_id`` (or CRLF in ``tool_call_id``) never reaches
    the handler, the record, or a header."""
    service = AgentService(agent="test", owner=OWNER, session_name="hostile", nc=nc)
    called = False

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        nonlocal called
        called = True

    service.on_prompt(handler)
    await service.start()
    try:
        async with _TraceSpy(nc) as spy:
            for field, value in (
                ("root_id", "_INBOX.someone.elses"),
                ("tool_call_id", "x\r\ny: 1"),
            ):
                payload = json.dumps({"prompt": "hi", field: value}).encode()
                reply = await nc.request(service.subject.prompt, payload, timeout=2.0)
                evidence.write_json(f"reject-{field}.json", dict(reply.headers or {}))
                assert (reply.headers or {}).get("Nats-Service-Error-Code") == "400"
            await asyncio.sleep(0.2)
        assert not called
        assert spy.records == []
    finally:
        await service.stop()


def test_protocol_error_is_the_400_path() -> None:
    # Guards the assumption above: decode failures surface as ProtocolError.
    with pytest.raises(ProtocolError):
        decode(b'{"prompt":"hi","root_id":"nope"}')
