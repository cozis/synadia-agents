"""Unit tests for the spawn-ledger lifecycle (stream finish semantics).

A handler-spawned task that outlives its request inherits the ambient
``ActiveTrace`` (PEP 567 copies the context at task creation). Once the
request completes, the stream's completion-report channel closes: late
spawns stop accreting into the undrainable pending set and deliver via
the spawn-time marker only. Broker-less — these paths never touch the
``Request`` or the NATS client.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from synadia_ai.agent_service import PromptStream


def _make_stream() -> PromptStream:
    return PromptStream(
        MagicMock(),
        MagicMock(),
        reply_subject="_INBOX.agents.mux.token",
    )


class TestSpawnLedgerLifecycle:
    def test_live_stream_records_on_both_channels(self) -> None:
        stream = _make_stream()
        marker = stream.record_spawn("cafebabe00000000")
        assert marker["x-synadia-event"] == "spawn"
        assert stream.trace_headers()["x-synadia-spawned"] == "cafebabe00000000::programmatic"

    def test_finish_closes_completion_report_channel(self) -> None:
        stream = _make_stream()
        stream._finish()
        marker = stream.record_spawn("cafebabe00000000")
        # The marker channel still delivers the late edge…
        assert marker["x-synadia-spawned"] == "cafebabe00000000::programmatic"
        # …but the completion report stays empty: no accretion.
        assert "x-synadia-spawned" not in stream.trace_headers()

    def test_ambient_recorder_respects_finish(self) -> None:
        # The recorder closure a detached task holds via its context copy.
        stream = _make_stream()
        recorder = stream._as_active_trace().record_spawn
        assert recorder is not None
        stream._finish()
        marker = recorder("cafebabe00000000")
        assert marker["x-synadia-event"] == "spawn"
        assert "x-synadia-spawned" not in stream.trace_headers()

    def test_empty_tool_call_id_counts_as_none(self) -> None:
        # `.get('id','')`-style defaults must not claim a tool edge.
        stream = _make_stream()
        marker = stream.record_spawn("cafebabe00000000", tool_call_id="")
        assert marker["x-synadia-spawned"] == "cafebabe00000000::programmatic"

    def test_entries_recorded_before_finish_still_drain(self) -> None:
        # close() blocks new inserts only — an entry recorded while the
        # request was live is still handed out if trace_headers is called.
        stream = _make_stream()
        stream.record_spawn("cafebabe00000000")
        stream._finish()
        assert stream.trace_headers()["x-synadia-spawned"] == "cafebabe00000000::programmatic"
