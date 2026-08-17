"""Unit tests for the trace primitives.

Covers the normative thread-id derivation, the optional ``root_id``
envelope field (wire round-trip + legacy compatibility), and the
``TraceContext`` shape. The end-to-end path (handle thread_id matching
the agent-side derivation over a real broker) is exercised by the
agent-sdk's e2e suite, which owns the server-side half.
"""

from __future__ import annotations

import hashlib
import json
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError as PydanticValidationError

from synadia_ai.agents import (
    HEADER_EVENT,
    HEADER_SPAWNED,
    THREAD_ID_HEX_LEN,
    ActiveTrace,
    Agent,
    Envelope,
    ProtocolError,
    TraceContext,
    active_trace,
    bind_active_trace,
    current_tool_call_id,
    decode,
    derive_thread_id,
    encode,
    format_spawn_entry,
    is_thread_id,
    random_thread_id,
    tool_scope,
)
from tests.harness.agent_info import make_agent_info


class TestDeriveThreadId:
    def test_matches_normative_convention(self) -> None:
        subject = "_INBOX.agents.MUXNUID.TOKEN"
        expected = hashlib.sha256(subject.encode("utf-8")).hexdigest()[:THREAD_ID_HEX_LEN]
        assert derive_thread_id(subject) == expected

    def test_is_deterministic_and_subject_sensitive(self) -> None:
        a = derive_thread_id("_INBOX.agents.m.t1")
        assert a == derive_thread_id("_INBOX.agents.m.t1")
        assert a != derive_thread_id("_INBOX.agents.m.t2")

    def test_length_and_alphabet(self) -> None:
        tid = derive_thread_id("_INBOX.x.y")
        assert len(tid) == THREAD_ID_HEX_LEN
        assert set(tid) <= set("0123456789abcdef")

    def test_random_thread_id_shape_and_uniqueness(self) -> None:
        a, b = random_thread_id(), random_thread_id()
        assert is_thread_id(a)
        assert is_thread_id(b)
        assert a != b


class TestEnvelopeRootId:
    def test_round_trip(self) -> None:
        env = Envelope(prompt="hi", root_id="a" * 16)
        wire = encode(env)
        assert json.loads(wire)["root_id"] == "a" * 16
        assert decode(wire).root_id == "a" * 16

    def test_omitted_from_wire_when_none(self) -> None:
        wire = encode(Envelope(prompt="hi"))
        assert "root_id" not in json.loads(wire)

    def test_legacy_payload_decodes_to_none(self) -> None:
        # A pre-trace caller's payload — no root_id field at all.
        assert decode(b'{"prompt": "hi"}').root_id is None

    def test_plain_text_shorthand_decodes_to_none(self) -> None:
        assert decode(b"just text").root_id is None

    @pytest.mark.parametrize(
        "bad",
        [
            "x\r\nx-evil: 1",  # CRLF header injection
            "a" * 15,  # too short
            "a" * 17,  # too long
            "A" * 16,  # uppercase
            "g" * 16,  # non-hex
            "é" * 16,  # non-latin-1
            "",  # empty
        ],
    )
    def test_malformed_root_id_rejected_at_decode(self, bad: str) -> None:
        # Security boundary: root_id reaches HTTP header values agent-side,
        # so anything outside the 16-lowercase-hex shape must 400 at decode.
        payload = json.dumps({"prompt": "hi", "root_id": bad}).encode()
        with pytest.raises(ProtocolError, match="root_id"):
            decode(payload)

    def test_malformed_root_id_rejected_at_construction(self) -> None:
        with pytest.raises(PydanticValidationError, match="root_id"):
            Envelope(prompt="hi", root_id="not-a-thread-id")

    def test_is_thread_id_accepts_derived_ids(self) -> None:
        assert is_thread_id(derive_thread_id("_INBOX.agents.m.t1"))
        assert not is_thread_id("A" * 16)


class TestTraceContext:
    def test_frozen_carrier(self) -> None:
        ctx = TraceContext(root_id="b" * 16)
        assert ctx.root_id == "b" * 16


class TestFormatSpawnEntry:
    """The edge policy: ambient tool default + edge-type honesty."""

    def test_explicit_tool_id(self) -> None:
        assert format_spawn_entry("c1", "toolu_x") == "c1:toolu_x:tool_call"

    def test_ambient_tool_id(self) -> None:
        with tool_scope("toolu_ambient"):
            assert format_spawn_entry("c1") == "c1:toolu_ambient:tool_call"

    def test_no_tool_is_programmatic(self) -> None:
        assert format_spawn_entry("c1") == "c1::programmatic"

    def test_explicit_edge_type_wins(self) -> None:
        assert format_spawn_entry("c1", None, "handoff") == "c1::handoff"

    def test_empty_tool_id_counts_as_no_tool(self) -> None:
        # `.get('id','')`-style defaults must not claim a tool edge.
        assert format_spawn_entry("c1", "") == "c1::programmatic"
        with tool_scope(""):
            assert format_spawn_entry("c1") == "c1::programmatic"

    def test_reserved_chars_percent_encoded(self) -> None:
        # ':' and ',' would corrupt the entry / comma-joined report.
        assert format_spawn_entry("c1", "a:b,c") == "c1:a%3Ab%2Cc:tool_call"

    def test_plain_provider_ids_pass_through_unchanged(self) -> None:
        assert format_spawn_entry("c1", "toolu_01AbC") == "c1:toolu_01AbC:tool_call"


def _make_agent(nc: MagicMock) -> Agent:
    nc.max_payload = 0  # "not declared" — §5.4 validator falls back to endpoint value
    return Agent(nc, make_agent_info("agents.prompt.test.pytest.s"))


class TestAmbientContext:
    """The contextvars layer: fallback, auto-record, tool scope."""

    def test_no_ambient_outside_binding(self) -> None:
        assert active_trace() is None
        assert current_tool_call_id() is None

    def test_bind_and_tool_scope_nest_and_reset(self) -> None:
        with bind_active_trace(ActiveTrace(thread_id="p", root_id="r")):
            assert active_trace() is not None
            with tool_scope("toolu_outer"):
                assert current_tool_call_id() == "toolu_outer"
                with tool_scope("toolu_inner"):
                    assert current_tool_call_id() == "toolu_inner"
                assert current_tool_call_id() == "toolu_outer"
            assert current_tool_call_id() is None
        assert active_trace() is None

    def test_prompt_joins_ambient_tree_and_auto_records(self) -> None:
        # The recorder runs synchronously in the spawner's context, so it
        # resolves the ambient tool scope itself — record what it saw.
        recorded: list[tuple[str, str | None]] = []

        def recorder(child: str) -> dict[str, str]:
            recorded.append((child, current_tool_call_id()))
            return {HEADER_EVENT: "spawn", HEADER_SPAWNED: format_spawn_entry(child)}

        agent = _make_agent(MagicMock())
        with (
            bind_active_trace(
                ActiveTrace(thread_id="a" * 16, root_id="b" * 16, record_spawn=recorder)
            ),
            tool_scope("toolu_x"),
        ):
            handle = agent.prompt("hi")

        assert handle.root_id == "b" * 16
        assert recorded == [(handle.thread_id, "toolu_x")]
        assert handle.spawn_marker_headers is not None
        assert handle.spawn_marker_headers[HEADER_EVENT] == "spawn"
        assert (
            handle.spawn_marker_headers[HEADER_SPAWNED] == f"{handle.thread_id}:toolu_x:tool_call"
        )

    def test_explicit_trace_wins_and_disables_auto_record(self) -> None:
        recorded: list[tuple[str, str | None]] = []

        def recorder(child: str) -> dict[str, str]:
            recorded.append((child, current_tool_call_id()))
            return {}

        agent = _make_agent(MagicMock())
        with bind_active_trace(
            ActiveTrace(thread_id="a" * 16, root_id="c" * 16, record_spawn=recorder)
        ):
            handle = agent.prompt("hi", trace=TraceContext(root_id="d" * 16))

        assert handle.root_id == "d" * 16
        assert recorded == []
        assert handle.spawn_marker_headers is None

    def test_forwarded_envelope_same_tree_keeps_auto_record(self) -> None:
        # A handler forwarding its received envelope verbatim: the
        # envelope's root_id (SDK-stamped upstream) names the ambient
        # tree, so this is a same-tree spawn and the edge must survive.
        recorded: list[str] = []

        def recorder(child: str) -> dict[str, str]:
            recorded.append(child)
            return {HEADER_EVENT: "spawn", HEADER_SPAWNED: format_spawn_entry(child)}

        agent = _make_agent(MagicMock())
        forwarded = Envelope(prompt="hi", root_id="b" * 16)
        with bind_active_trace(
            ActiveTrace(thread_id="a" * 16, root_id="b" * 16, record_spawn=recorder)
        ):
            handle = agent.prompt(forwarded)

        assert handle.root_id == "b" * 16
        assert recorded == [handle.thread_id]
        assert handle.spawn_marker_headers is not None

    def test_envelope_root_of_foreign_tree_disables_auto_record(self) -> None:
        # A root naming a different tree: honored explicitly, and no
        # edge is recorded — there are no cross-tree edges.
        recorded: list[str] = []

        def recorder(child: str) -> dict[str, str]:
            recorded.append(child)
            return {}

        agent = _make_agent(MagicMock())
        foreign = Envelope(prompt="hi", root_id="f" * 16)
        with bind_active_trace(
            ActiveTrace(thread_id="a" * 16, root_id="b" * 16, record_spawn=recorder)
        ):
            handle = agent.prompt(foreign)

        assert handle.root_id == "f" * 16
        assert recorded == []
        assert handle.spawn_marker_headers is None

    def test_prompt_without_any_context_roots_itself(self) -> None:
        handle = _make_agent(MagicMock()).prompt("hi")
        assert handle.root_id == handle.thread_id
        assert handle.spawn_marker_headers is None
