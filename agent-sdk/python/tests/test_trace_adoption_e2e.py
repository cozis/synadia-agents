"""Observability: the service adopts the envelope's (thread, root) or mints.

Design: ``synadia-agent-fabric-docs/docs/observability.md``. Three wire
scenarios against a real broker:

* **adoption** — an envelope carrying ``thread_id`` + ``root_id`` reaches
  the handler with exactly those values on ``stream.trace``, and the same
  :class:`ActiveTrace` is bound as the ambient trace for the handler.
* **mint-if-absent** — an ID-less envelope (NATS CLI, legacy 0.3) gets a
  service-minted root: valid shape, ``thread_id == root_id``. No record
  is written (the root is advertised implicitly).
* **partial lineage** — a lone ``thread_id`` is a malformed envelope:
  ``error(400)`` + terminator, handler never runs.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING

import pytest
from synadia_ai.agents import (
    ActiveTrace,
    Envelope,
    TraceOptions,
    active_trace,
    inherited_trace_options,
    is_thread_id,
)

from synadia_ai.agent_service import AgentService, PromptStream

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient
    from nats.aio.msg import Msg

    from tests.harness.evidence import EvidenceRecorder

AGENT = "test"
OWNER = "pytest"
HEARTBEAT_INTERVAL_S = 30


async def _drain_reply(nc: NATSClient, subject: str, payload: bytes) -> list[Msg]:
    inbox = nc.new_inbox()
    sub = await nc.subscribe(inbox)
    try:
        await nc.publish(subject, payload, reply=inbox)
        collected: list[Msg] = []
        deadline = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < deadline:
            try:
                msg = await sub.next_msg(timeout=0.5)
            except TimeoutError:
                break
            collected.append(msg)
            if msg.data == b"" and not msg.headers:
                break
        return collected
    finally:
        await sub.unsubscribe()


async def _start_capturing_service(
    nc: NATSClient, session_name: str, seen: list[dict[str, ActiveTrace | None]]
) -> AgentService:
    async def _handler(envelope: Envelope, stream: PromptStream) -> None:
        seen.append({"stream": stream.trace, "ambient": active_trace()})
        await stream.send(envelope.prompt)

    service = AgentService(
        agent=AGENT,
        owner=OWNER,
        session_name=session_name,
        nc=nc,
        heartbeat_interval_s=HEARTBEAT_INTERVAL_S,
        keepalive_interval_s=None,
    )
    service.on_prompt(_handler)
    await service.start()
    return service


@pytest.mark.asyncio
async def test_service_adopts_envelope_lineage(nc: NATSClient, evidence: EvidenceRecorder) -> None:
    seen: list[dict[str, ActiveTrace | None]] = []
    service = await _start_capturing_service(nc, "trace-adopt", seen)
    try:
        tid = "9f2c4b1e8a7d33051c6e0b42d78a91f0"
        rid = "5b8e2a90c4f1d7631e0a9b3c82d45f17"
        payload = json.dumps({"prompt": "hi", "thread_id": tid, "root_id": rid}).encode()
        frames = await _drain_reply(nc, service.subject.prompt, payload)
        evidence.write_json(
            "adopted-trace.json",
            {"sent": {"thread_id": tid, "root_id": rid}, "frames": len(frames)},
        )
        assert seen == [
            {
                "stream": ActiveTrace(thread_id=tid, root_id=rid),
                "ambient": ActiveTrace(thread_id=tid, root_id=rid),
            }
        ]
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_service_mints_a_root_for_an_id_less_envelope(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    seen: list[dict[str, ActiveTrace | None]] = []
    service = await _start_capturing_service(nc, "trace-mint", seen)
    try:
        frames = await _drain_reply(nc, service.subject.prompt, b'{"prompt":"hi"}')
        assert len(seen) == 1
        stream_trace = seen[0]["stream"]
        assert stream_trace is not None
        evidence.write_json(
            "minted-trace.json",
            {"thread_id": stream_trace.thread_id, "frames": len(frames)},
        )
        assert is_thread_id(stream_trace.thread_id)
        assert stream_trace.root_id == stream_trace.thread_id
        assert seen[0]["ambient"] == stream_trace
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_service_hands_its_trace_config_down_to_clients(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    """A service's ``trace=`` config reaches an unconfigured nested client."""
    seen: list[TraceOptions | None] = []

    async def _handler(envelope: Envelope, stream: PromptStream) -> None:
        seen.append(inherited_trace_options())
        await stream.send(envelope.prompt)

    options = TraceOptions(edge_subject="TRACE.edges")
    service = AgentService(
        agent=AGENT,
        owner=OWNER,
        session_name="trace-passdown",
        nc=nc,
        heartbeat_interval_s=HEARTBEAT_INTERVAL_S,
        keepalive_interval_s=None,
        trace=options,
    )
    service.on_prompt(_handler)
    await service.start()
    try:
        await _drain_reply(nc, service.subject.prompt, b'{"prompt":"hi"}')
        evidence.write_json("inherited-config.json", {"edge_subject": options.edge_subject})
        assert seen == [options]
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_service_without_trace_config_hands_down_nothing(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    seen: list[TraceOptions | None] = []

    async def _handler(envelope: Envelope, stream: PromptStream) -> None:
        seen.append(inherited_trace_options())
        await stream.send(envelope.prompt)

    service = AgentService(
        agent=AGENT,
        owner=OWNER,
        session_name="trace-nopassdown",
        nc=nc,
        heartbeat_interval_s=HEARTBEAT_INTERVAL_S,
        keepalive_interval_s=None,
    )
    service.on_prompt(_handler)
    await service.start()
    try:
        await _drain_reply(nc, service.subject.prompt, b'{"prompt":"hi"}')
        evidence.write_json("inherited-config.json", {"inherited": None})
        assert seen == [None]
    finally:
        await service.stop()


@pytest.mark.asyncio
async def test_service_rejects_partial_lineage_with_400(
    nc: NATSClient, evidence: EvidenceRecorder
) -> None:
    seen: list[dict[str, ActiveTrace | None]] = []
    service = await _start_capturing_service(nc, "trace-400", seen)
    try:
        payload = json.dumps(
            {"prompt": "hi", "thread_id": "9f2c4b1e8a7d33051c6e0b42d78a91f0"}
        ).encode()
        frames = await _drain_reply(nc, service.subject.prompt, payload)
        evidence.write_jsonl(
            "frames.jsonl",
            [{"headers": dict(m.headers or {}), "data_len": len(m.data)} for m in frames],
        )
        codes = [(m.headers or {}).get("Nats-Service-Error-Code") for m in frames if m.headers]
        assert "400" in codes
        assert frames[-1].data == b"" and not frames[-1].headers
        assert seen == []
    finally:
        await service.stop()
