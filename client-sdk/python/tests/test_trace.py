"""Unit tests for the trace primitives.

Covers the normative thread-id derivation and the optional ``root_id``
envelope field (wire round-trip + legacy compatibility). The end-to-end
path (handle thread_id matching the agent-side derivation over a real
broker) is exercised by the agent-sdk's e2e suite, which owns the
server-side half.
"""

from __future__ import annotations

import hashlib
import json

from synadia_ai.agents import (
    THREAD_ID_HEX_LEN,
    Envelope,
    decode,
    derive_thread_id,
    encode,
)


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
