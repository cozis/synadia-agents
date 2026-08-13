"""Observability trace primitives.

A **thread** is one prompt execution: one request published to
``agents.prompt.{a}.{o}.{n}`` and its §6 response stream. Its identity is
*derived*, never exchanged: both ends already share the prompt's reply
inbox (the caller mints it; the agent receives it as the request's reply
subject), and it is unique per prompt, so a hash of it identifies the
thread with zero extra wire surface.

``root_id`` is the thread_id of the tree's *root* thread, forwarded down
the tree via an optional envelope field (§5.6 tolerates unknown fields).
It is always the hash — never the raw inbox subject, which would hand
every descendant the root caller's live reply subject (an injection
surface). Root test: a thread is root iff its ``root_id`` equals the hash
of its own reply subject.

Parent/child *edges* are not carried on NATS at all — the parent (the
only party that knows the spawning tool_call_id) reports them to the
observing HTTP proxy. See the agent-sdk's ``PromptStream.trace_headers``.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

# Length (hex chars) of derived thread ids. 16 hex chars = 64 bits —
# collision-safe for observability purposes and short enough to eyeball
# in HTTP header dumps.
THREAD_ID_HEX_LEN = 16

# Provider-facing header vocabulary — normative, like the derivation
# below: the TS SDK and any observing proxy must reproduce these
# byte-for-byte, so they live here (the shared-primitives home) rather
# than as literals at the emission sites.
HEADER_THREAD_ID = "x-agent-thread-id"
HEADER_ROOT_ID = "x-agent-root-id"
HEADER_TOOL_CALL_ID = "x-agent-tool-call-id"
HEADER_SPAWNED = "x-agent-spawned"
HEADER_EVENT = "x-agent-event"


def derive_thread_id(reply_subject: str) -> str:
    """Derive a thread id from a prompt's reply subject.

    Normative convention (must match every SDK implementation):
    lowercase hex of ``sha256(reply_subject)`` truncated to
    :data:`THREAD_ID_HEX_LEN` chars, hashing the exact full subject
    string as UTF-8 — e.g. ``_INBOX.agents.<mux>.<token>``.
    """
    return hashlib.sha256(reply_subject.encode("utf-8")).hexdigest()[:THREAD_ID_HEX_LEN]


def format_spawn_entry(
    child_thread_id: str,
    tool_call_id: str | None = None,
    edge_type: str | None = None,
) -> str:
    """Format one spawn-edge claim: ``<child>:<tool_call_id>:<edge_type>``.

    This is where the edge policy lives, so every entry point agrees: an
    unspecified ``edge_type`` is honest about what was observed —
    ``tool_call`` when a tool id was given, ``programmatic`` when none
    was (don't claim a tool edge with no tool to point at).
    """
    if edge_type is None:
        edge_type = "tool_call" if tool_call_id is not None else "programmatic"
    return f"{child_thread_id}:{tool_call_id or ''}:{edge_type}"


def spawn_marker_headers(thread_id: str, root_id: str, entry: str) -> dict[str, str]:
    """Headers for the spawn-time idempotent marker request (one edge)."""
    return {
        HEADER_EVENT: "spawn",
        HEADER_THREAD_ID: thread_id,
        HEADER_ROOT_ID: root_id,
        HEADER_SPAWNED: entry,
    }


@dataclass(frozen=True, slots=True)
class TraceContext:
    """Trace identity forwarded from a parent thread to a spawned prompt.

    Produced agent-side (``PromptStream.child_trace()``) and passed to
    :meth:`Agent.prompt(trace=...) <synadia_ai.agents.Agent.prompt>` when
    an agent spawns a sub-agent. Carries only what the child cannot
    derive itself.
    """

    root_id: str
