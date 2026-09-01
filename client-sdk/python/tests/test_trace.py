"""Tracing opt-in switch, thread-ID minting, and envelope lineage.

Mirrors ``client-sdk/typescript/test/unit/trace-options.test.ts`` and
``trace-ids.test.ts``.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import TYPE_CHECKING, cast

import pytest

import synadia_ai.agents.agent as agent_module
from synadia_ai.agents import (
    DEFAULT_EDGE_SUBJECT,
    EDGE_RECORD_VERSION,
    HEADER_ROOT_ID,
    HEADER_THREAD_ID,
    THREAD_ID_HEX_LEN,
    ActiveTrace,
    Agent,
    Agents,
    Envelope,
    Identity,
    ProtocolError,
    TraceOptions,
    active_trace,
    active_turn_count,
    bind_active_trace,
    build_agent_info,
    decode,
    encode,
    format_trace_headers,
    is_thread_id,
    is_tool_call_id,
    random_thread_id,
    trace_headers,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from nats.aio.client import Client as NATSClient

    from synadia_ai.agents import SenderSigner


class _StubConnection:
    """Weak-referenceable stand-in — the publisher registry keys on identity."""


# Tracker/resolver only store the connection at construction time, so a
# bare stub is enough for option-plumbing tests.
_NC = cast("NATSClient", _StubConnection())


def _signing_identity() -> Identity:
    """Edge records are only published with a signer; the fake never signs
    (the publisher is stubbed out in these tests)."""
    return Identity(signer=cast("SenderSigner", object()))


def _info() -> dict[str, object]:
    return {
        "name": "agents",
        "id": "VMKS6MHK71PCPWGY38A7N5",
        "version": "1.0.0",
        "description": "test agent",
        "metadata": {
            "agent": "echo",
            "owner": "test",
            "session": "main",
            "protocol_version": "0.3",
        },
        "endpoints": [
            {
                "name": "prompt",
                "subject": "agents.prompt.echo.test.main",
                "queue_group": "agents",
                "metadata": {"max_payload": "1MB", "attachments_ok": "true"},
            }
        ],
    }


def test_tracing_is_off_when_trace_is_omitted() -> None:
    agents = Agents(nc=_NC)
    assert agents.trace is None


def test_tracing_is_on_when_a_trace_option_is_passed() -> None:
    options = TraceOptions()
    agents = Agents(nc=_NC, trace=options)
    assert agents.trace is options


def test_trace_option_reaches_agent_handles() -> None:
    info = build_agent_info(_info())
    assert info is not None
    off = Agent(_NC, info)
    assert off.tracing_enabled is False
    on = Agent(_NC, info, trace=TraceOptions())
    assert on.tracing_enabled is True


# --- thread ids -------------------------------------------------------


def test_random_thread_id_shape_and_uniqueness() -> None:
    a = random_thread_id()
    b = random_thread_id()
    assert len(a) == THREAD_ID_HEX_LEN
    assert re.fullmatch(r"[0-9a-f]{32}", a)
    assert a != b


def test_is_thread_id_validates_the_normative_shape() -> None:
    assert is_thread_id(random_thread_id())
    assert not is_thread_id("")
    assert not is_thread_id("9f2c4b1e8a7d33051c6e0b42d78a91f")  # 31 chars
    assert not is_thread_id("9f2c4b1e8a7d33051c6e0b42d78a91f00")  # 33 chars
    assert not is_thread_id("9F2C4B1E8A7D33051C6E0B42D78A91F0")  # upper case
    assert not is_thread_id("gf2c4b1e8a7d33051c6e0b42d78a91f0")  # non-hex


# --- envelope lineage fields ------------------------------------------


def test_encode_emits_lineage_when_present_and_omits_when_absent() -> None:
    tid = random_thread_id()
    with_lineage = json.loads(encode(Envelope(prompt="hi", thread_id=tid, root_id=tid)))
    assert with_lineage == {"prompt": "hi", "thread_id": tid, "root_id": tid}
    without = json.loads(encode(Envelope(prompt="hi")))
    assert without == {"prompt": "hi"}


# --- prompt minting ---------------------------------------------------


class _FakePublisher:
    """Records what `prompt()` enqueues instead of delivering it."""

    def __init__(self) -> None:
        self.enqueued: list[tuple[str, bytes, str]] = []

    def enqueue(
        self, subject: str, payload: bytes, record_id: str, sign: object = None
    ) -> None:
        self.enqueued.append((subject, payload, record_id))


def _captured_envelope(
    monkeypatch: pytest.MonkeyPatch, agent: Agent, text: str | Envelope
) -> Envelope:
    """Run the sync half of ``prompt()`` and capture the envelope it encodes."""
    seen: list[Envelope] = []

    def capture(envelope: Envelope) -> bytes:
        seen.append(envelope)
        return encode(envelope)

    monkeypatch.setattr(agent_module, "encode", capture)
    # Keep delivery out of these tests: the publisher would start a drain
    # task, and there is no running loop in the sync half.
    monkeypatch.setattr(agent_module, "edge_publisher_for", lambda *a, **k: _FakePublisher())
    agent.prompt(text)  # sync half builds + encodes; iterator never started
    assert len(seen) == 1
    return seen[0]


def test_prompt_adds_no_lineage_when_tracing_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    info = build_agent_info(_info())
    assert info is not None
    envelope = _captured_envelope(monkeypatch, Agent(_NC, info), "hello")
    assert envelope.thread_id is None
    assert envelope.root_id is None


def test_prompt_mints_a_root_when_tracing_is_on(monkeypatch: pytest.MonkeyPatch) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions(), identity=_signing_identity())
    envelope = _captured_envelope(monkeypatch, agent, "hello")
    assert envelope.thread_id is not None
    assert is_thread_id(envelope.thread_id)
    assert envelope.root_id == envelope.thread_id


def test_prompt_mints_fresh_ids_per_call(monkeypatch: pytest.MonkeyPatch) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions(), identity=_signing_identity())
    first = _captured_envelope(monkeypatch, agent, "hello")
    monkeypatch.undo()
    second = _captured_envelope(monkeypatch, agent, "hello")
    assert first.thread_id != second.thread_id


def test_decode_adopts_a_valid_lineage_pair() -> None:
    tid = random_thread_id()
    rid = random_thread_id()
    decoded = decode(encode(Envelope(prompt="hi", thread_id=tid, root_id=rid)))
    assert decoded.thread_id == tid
    assert decoded.root_id == rid


def test_decode_rejects_a_lone_lineage_field() -> None:
    tid = random_thread_id()
    with pytest.raises(ProtocolError, match="sent together"):
        decode(json.dumps({"prompt": "hi", "thread_id": tid}).encode())
    with pytest.raises(ProtocolError, match="sent together"):
        decode(json.dumps({"prompt": "hi", "root_id": tid}).encode())


def test_decode_rejects_malformed_thread_ids() -> None:
    tid = random_thread_id()
    with pytest.raises(ProtocolError, match="thread id"):
        decode(json.dumps({"prompt": "hi", "thread_id": "nope", "root_id": tid}).encode())


def test_ambient_trace_binds_and_restores() -> None:
    assert active_trace() is None
    trace = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())
    with bind_active_trace(trace):
        assert active_trace() is trace
        inner = ActiveTrace(thread_id=random_thread_id(), root_id=trace.root_id)
        with bind_active_trace(inner):
            assert active_trace() is inner
        assert active_trace() is trace
    assert active_trace() is None


async def test_ambient_trace_flows_into_awaited_work() -> None:
    trace = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())

    async def probe() -> ActiveTrace | None:
        await asyncio.sleep(0)
        return active_trace()

    with bind_active_trace(trace):
        assert await probe() is trace


def test_turn_count_hint_is_omitted_when_the_parent_made_no_model_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions(), identity=_signing_identity())
    _, edge = _captured_edge(monkeypatch, agent, "hello")
    assert edge is not None
    assert "turn_count_hint" not in json.loads(edge[1])


def test_turn_count_hint_carries_the_parents_model_call_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions(), identity=_signing_identity())
    ambient = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())
    with bind_active_trace(ambient):
        trace_headers()
        trace_headers()
        _, first = _captured_edge(monkeypatch, agent, "hello")  # after two turns
        trace_headers()
        _, second = _captured_edge(monkeypatch, agent, "hello")  # ... and a third
    assert first is not None
    assert second is not None
    assert json.loads(first[1])["turn_count_hint"] == 2
    assert json.loads(second[1])["turn_count_hint"] == 3


def test_format_trace_headers_names_the_thread_and_root() -> None:
    trace = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())
    assert format_trace_headers(trace) == {
        HEADER_THREAD_ID: trace.thread_id,
        HEADER_ROOT_ID: trace.root_id,
    }


async def test_trace_headers_is_empty_outside_a_handler_and_populated_inside() -> None:
    assert trace_headers() == {}
    trace = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())

    async def probe() -> dict[str, str]:
        await asyncio.sleep(0)
        return trace_headers()

    with bind_active_trace(trace):
        assert await probe() == format_trace_headers(trace)
    assert trace_headers() == {}


async def test_turn_counter_ticks_per_trace_headers_call() -> None:
    assert active_turn_count() == 0
    trace_headers()  # outside a handler: no scope to count against
    assert active_turn_count() == 0

    trace = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())
    with bind_active_trace(trace):
        assert active_turn_count() == 0
        trace_headers()
        trace_headers()
        assert active_turn_count() == 2
    assert active_turn_count() == 0


def test_is_tool_call_id_bounds() -> None:
    assert is_tool_call_id("call_9xJ2")
    assert is_tool_call_id("x" * 256)
    assert not is_tool_call_id("")
    assert not is_tool_call_id("x" * 257)
    assert not is_tool_call_id("has space")
    assert not is_tool_call_id("newline\n")
    assert not is_tool_call_id("émoji")


def test_prompt_rejects_an_invalid_tool_synchronously(monkeypatch: pytest.MonkeyPatch) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions(), identity=_signing_identity())
    with pytest.raises(ValueError, match="tool call id"):
        agent.prompt("hello", tool="bad tool")


def _captured_edge(
    monkeypatch: pytest.MonkeyPatch,
    agent: Agent,
    text: str | Envelope,
    tool: str | None = None,
) -> tuple[Envelope, tuple[str, bytes] | None]:
    """Run the sync half of ``prompt()``; capture the envelope and edge record."""
    seen: list[Envelope] = []
    publisher = _FakePublisher()

    def capture(envelope: Envelope) -> bytes:
        seen.append(envelope)
        return encode(envelope)

    def capture_stream(*args: object, **kw: object) -> AsyncIterator[object]:
        async def _empty() -> AsyncIterator[object]:
            return
            yield  # pragma: no cover

        return _empty()

    monkeypatch.setattr(agent_module, "encode", capture)
    monkeypatch.setattr(agent_module, "edge_publisher_for", lambda *a, **k: publisher)
    monkeypatch.setattr(agent, "_stream_prompt", capture_stream)
    agent.prompt(text, tool=tool)
    assert len(seen) == 1
    edge = (publisher.enqueued[0][0], publisher.enqueued[0][1]) if publisher.enqueued else None
    return seen[0], edge


def test_prompt_publishes_a_root_edge_record(monkeypatch: pytest.MonkeyPatch) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions(), identity=_signing_identity())
    envelope, edge = _captured_edge(monkeypatch, agent, "hello", tool="call_9xJ2")
    assert edge is not None
    subject, payload = edge
    assert subject == DEFAULT_EDGE_SUBJECT
    record = json.loads(payload)
    assert record["version"] == EDGE_RECORD_VERSION
    assert record["thread_id"] == envelope.thread_id
    assert record["root_id"] == envelope.thread_id
    assert record["parent_id"] is None
    assert record["tool_call_id"] == "call_9xJ2"
    assert is_thread_id(record["record_id"])
    assert isinstance(record["ts"], int)


def test_prompt_inherits_ambient_lineage(monkeypatch: pytest.MonkeyPatch) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions(), identity=_signing_identity())
    ambient = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())
    with bind_active_trace(ambient):
        envelope, edge = _captured_edge(monkeypatch, agent, "hello")

    # The envelope carries only (thread, root); the parent rides the edge.
    assert envelope.root_id == ambient.root_id
    assert envelope.thread_id != ambient.thread_id
    assert "parent_id" not in json.loads(encode(envelope))
    assert edge is not None
    record = json.loads(edge[1])
    assert record["thread_id"] == envelope.thread_id
    assert record["root_id"] == ambient.root_id
    assert record["parent_id"] == ambient.thread_id


def test_propagate_only_mints_without_publishing(monkeypatch: pytest.MonkeyPatch) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions(edge_subject=None), identity=_signing_identity())
    envelope, edge = _captured_edge(monkeypatch, agent, "hello")
    assert envelope.thread_id is not None
    assert edge is None


def test_unconfigured_handle_traces_when_the_service_handed_config_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, identity=_signing_identity())  # no config of its own
    assert agent.tracing_enabled is False
    trace = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())
    with bind_active_trace(trace, TraceOptions()):
        envelope, edge = _captured_edge(monkeypatch, agent, "hello")
    assert envelope.thread_id is not None
    assert envelope.root_id == trace.root_id
    assert edge is not None
    assert edge[0] == DEFAULT_EDGE_SUBJECT


def test_tracing_stays_off_when_neither_side_configured_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info)
    trace = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())
    with bind_active_trace(trace, None):
        envelope, edge = _captured_edge(monkeypatch, agent, "hello")
    assert envelope.thread_id is None
    assert edge is None


def test_own_configuration_wins_over_the_inherited_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(
        _NC, info, trace=TraceOptions(edge_subject="TRACE.own"), identity=_signing_identity()
    )
    trace = ActiveTrace(thread_id=random_thread_id(), root_id=random_thread_id())
    with bind_active_trace(trace, TraceOptions(edge_subject="TRACE.inherited")):
        _, edge = _captured_edge(monkeypatch, agent, "hello")
    assert edge is not None
    assert edge[0] == "TRACE.own"


def test_no_edge_is_published_without_a_signer(monkeypatch: pytest.MonkeyPatch) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions())  # tracing on, no signer
    envelope, edge = _captured_edge(monkeypatch, agent, "hello")
    # Minting and envelope lineage need no identity and still happen.
    assert envelope.thread_id is not None
    assert edge is None


def test_prompt_publishes_no_edge_when_tracing_is_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    info = build_agent_info(_info())
    assert info is not None
    envelope, edge = _captured_edge(monkeypatch, Agent(_NC, info), "hello", tool="call_9xJ2")
    assert envelope.thread_id is None
    assert edge is None


def test_explicit_envelope_lineage_wins_over_minting(monkeypatch: pytest.MonkeyPatch) -> None:
    info = build_agent_info(_info())
    assert info is not None
    agent = Agent(_NC, info, trace=TraceOptions(), identity=_signing_identity())
    tid = random_thread_id()
    rid = random_thread_id()
    explicit = Envelope(prompt="hi", thread_id=tid, root_id=rid)
    envelope = _captured_envelope(monkeypatch, agent, explicit)
    assert envelope.thread_id == tid
    assert envelope.root_id == rid
