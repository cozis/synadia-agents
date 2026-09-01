"""Observability tracing — opt-in configuration.

Design: ``synadia-agent-fabric-docs/docs/observability.md``. Tracing is
off unless the caller passes ``trace=TraceOptions(...)`` to
:class:`~synadia_ai.agents.Agents` (or an ``AgentService`` passes its
config down). Omission means byte-identical protocol-0.3 prompts: no
thread IDs minted, no lineage on the wire. Configuration fields (edge
subject, delivery tuning) land as the tracing feature is built out.

Mirrors the TS SDK's ``TraceOptions``.
"""

from __future__ import annotations

import re
import secrets
from dataclasses import dataclass

#: Length in hex characters of a thread ID — 128 bits. A thread ID names
#: one prompt execution; it is minted by the caller at ``prompt()`` time
#: and adopted by the receiving service. ``root_id`` shares the shape (it
#: IS a thread ID: the tree's first execution).
THREAD_ID_HEX_LEN = 32

_THREAD_ID_RE = re.compile(rf"^[0-9a-f]{{{THREAD_ID_HEX_LEN}}}$")


def is_thread_id(value: str) -> bool:
    """``True`` iff ``value`` has the normative thread-ID shape (32 lowercase hex)."""
    return _THREAD_ID_RE.fullmatch(value) is not None


def random_thread_id() -> str:
    """Mint a fresh 128-bit random thread ID."""
    return secrets.token_hex(THREAD_ID_HEX_LEN // 2)


# Tool-call IDs flow into edge records (and eventually header values), so
# they are bounded to a single line of visible ASCII.

#: Maximum length of a tool-call ID.
TOOL_CALL_ID_MAX_LEN = 256

_TOOL_CALL_ID_RE = re.compile(r"^[\x21-\x7e]+$")


def is_tool_call_id(value: str) -> bool:
    """``True`` iff ``value`` is a valid tool-call ID: 1-256 visible-ASCII characters."""
    return len(value) <= TOOL_CALL_ID_MAX_LEN and _TOOL_CALL_ID_RE.fullmatch(value) is not None


@dataclass(frozen=True, slots=True)
class TraceOptions:
    """Opt-in tracing configuration; passing an instance enables tracing."""


def send_edge_record(*, thread_id: str, tool_call_id: str | None = None) -> None:
    """Hand one edge record to the (future) publisher.

    One record per traced prompt, written by the caller before the prompt
    is sent (observability.md, Trace Log). Not implemented yet —
    deliberately a no-op so call sites and tests can land first; later
    commits add the remaining fields, signing, and delivery.
    """


__all__ = [
    "THREAD_ID_HEX_LEN",
    "TOOL_CALL_ID_MAX_LEN",
    "TraceOptions",
    "is_thread_id",
    "is_tool_call_id",
    "random_thread_id",
]
