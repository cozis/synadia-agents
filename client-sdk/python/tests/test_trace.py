"""Observability trace primitives — shape, headers, ambient layer."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError as PydanticValidationError

from synadia_ai.agents import (
    HEADER_PARENT,
    HEADER_TRACE,
    ActiveTrace,
    Envelope,
    ProtocolError,
    TraceRecord,
    active_trace,
    bind_active_trace,
    decode,
    encode,
    trace_headers,
)
from synadia_ai.agents.trace import (
    format_trace_headers,
    is_prompt_id,
    is_tool_call_id,
    random_prompt_id,
)

ROOT = "a" * 16
PARENT = "b" * 16
CHILD = "c" * 16


# --- ids ----------------------------------------------------------------


def test_random_prompt_id_shape() -> None:
    ids = {random_prompt_id() for _ in range(64)}
    assert len(ids) == 64
    assert all(is_prompt_id(i) for i in ids)


@pytest.mark.parametrize("bad", ["", "A" * 16, "a" * 15, "a" * 17, "_INBOX.x.y", "g" * 16])
def test_prompt_id_rejects_off_shape(bad: str) -> None:
    assert not is_prompt_id(bad)


@pytest.mark.parametrize("ok", ["call_abc123", "toolu_01X", "a", "x" * 256, "!'()*%"])
def test_tool_call_id_accepts_visible_ascii(ok: str) -> None:
    assert is_tool_call_id(ok)


@pytest.mark.parametrize("bad", ["", "a b", "a\r\nb", "x" * 257, "é"])
def test_tool_call_id_rejects_header_unsafe(bad: str) -> None:
    assert not is_tool_call_id(bad)


def test_trace_record_wire_form_omits_absent_lineage() -> None:
    record = TraceRecord(
        prompt_id=CHILD, root_id=ROOT, agent="a", owner="o", session="s", instance_id="i", ts="t"
    )
    assert "parent_prompt_id" not in json.loads(record.encode())


# --- headers ------------------------------------------------------------


def test_root_headers_carry_only_trace_with_equal_slots() -> None:
    headers = format_trace_headers(ActiveTrace(prompt_id=ROOT, root_id=ROOT))
    assert headers == {HEADER_TRACE: f"{ROOT}:{ROOT}"}


def test_spawned_headers_carry_parent_and_percent_encoded_tool() -> None:
    trace = ActiveTrace(
        prompt_id=CHILD, root_id=ROOT, parent_prompt_id=PARENT, tool_call_id="call/1!'()*"
    )
    headers = format_trace_headers(trace)
    assert headers[HEADER_TRACE] == f"{ROOT}:{CHILD}"
    # Normative: RFC 3986 with no safe characters — the TS SDK must match byte-for-byte.
    assert headers[HEADER_PARENT] == f"{PARENT}:call%2F1%21%27%28%29%2A"


def test_programmatic_spawn_has_empty_tool_slot() -> None:
    trace = ActiveTrace(prompt_id=CHILD, root_id=ROOT, parent_prompt_id=PARENT)
    assert format_trace_headers(trace)[HEADER_PARENT] == f"{PARENT}:"


# --- ambient layer ------------------------------------------------------


def test_ambient_trace_is_scoped_to_the_block() -> None:
    assert active_trace() is None
    assert trace_headers() == {}
    trace = ActiveTrace(prompt_id=ROOT, root_id=ROOT)
    with bind_active_trace(trace):
        assert active_trace() is trace
        assert trace_headers() == format_trace_headers(trace)
        inner = ActiveTrace(prompt_id=CHILD, root_id=ROOT, parent_prompt_id=ROOT)
        with bind_active_trace(inner):
            assert active_trace() is inner
        assert active_trace() is trace
    assert active_trace() is None


# --- envelope lineage fields --------------------------------------------


def test_root_envelope_wire_form_is_unchanged() -> None:
    assert encode(Envelope(prompt="hi")) == b'{"prompt":"hi"}'


def test_lineage_fields_round_trip() -> None:
    env = Envelope(prompt="hi", parent_prompt_id=PARENT, root_id=ROOT, tool_call_id="call_1")
    wire = json.loads(encode(env))
    assert wire == {
        "prompt": "hi",
        "parent_prompt_id": PARENT,
        "root_id": ROOT,
        "tool_call_id": "call_1",
    }
    back = decode(encode(env))
    assert (back.parent_prompt_id, back.root_id, back.tool_call_id) == (PARENT, ROOT, "call_1")


@pytest.mark.parametrize(
    "field,value",
    [
        ("parent_prompt_id", "_INBOX.raw.subject"),
        ("root_id", "A" * 16),
        ("tool_call_id", "evil\r\nx-injected: 1"),
    ],
)
def test_hostile_lineage_dies_at_decode(field: str, value: str) -> None:
    payload = json.dumps({"prompt": "hi", field: value}).encode()
    with pytest.raises(ProtocolError, match="malformed envelope"):
        decode(payload)
    with pytest.raises(PydanticValidationError):
        Envelope(prompt="hi", **{field: value})
