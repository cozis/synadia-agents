"""Unit tests for the trace primitives.

Covers the normative thread-id derivation. The end-to-end path (handle
thread_id matching the agent-side derivation over a real broker) is
exercised by the agent-sdk's e2e suite, which owns the server-side half.
"""

from __future__ import annotations

import hashlib

from synadia_ai.agents import (
    THREAD_ID_HEX_LEN,
    derive_thread_id,
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
