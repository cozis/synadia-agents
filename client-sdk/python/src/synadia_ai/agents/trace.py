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
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

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


@dataclass(frozen=True, slots=True)
class ActiveTrace:
    """Identity of the prompt execution running in the current async context.

    ``thread_id`` is this execution's own thread ID — the parent of any
    thread it spawns; ``root_id`` is the tree's root, inherited unchanged
    (equal to ``thread_id`` on a root).
    """

    thread_id: str
    root_id: str


# The agent-sdk binds the execution's ActiveTrace around each prompt
# handler; nested client calls read it to inherit lineage without any
# plumbing through user code. contextvars flow into awaited work and
# tasks created inside the bound scope.

_active_trace: ContextVar[ActiveTrace | None] = ContextVar("synadia_active_trace", default=None)


def active_trace() -> ActiveTrace | None:
    """The ambient :class:`ActiveTrace`, or ``None`` outside a bound handler."""
    return _active_trace.get()


@contextmanager
def bind_active_trace(trace: ActiveTrace) -> Iterator[None]:
    """Run the block with ``trace`` as the ambient execution (used by the agent-sdk)."""
    token = _active_trace.set(trace)
    try:
        yield
    finally:
        _active_trace.reset(token)


def send_edge_record(
    *,
    thread_id: str,
    root_id: str,
    parent_id: str | None = None,
    tool_call_id: str | None = None,
) -> None:
    """Hand one edge record to the (future) publisher.

    One record per traced prompt, written by the caller before the prompt
    is sent (observability.md, Trace Log). ``thread_id`` is the spawned
    thread (minted by this caller); ``root_id`` the tree's root (equal to
    ``thread_id`` when the spawn starts a tree); ``parent_id`` the calling
    execution's own thread from the ambient trace, absent on a root. Not
    implemented yet — deliberately a no-op so call sites and tests can
    land first; later commits add the remaining fields, signing, and
    delivery.
    """


__all__ = [
    "THREAD_ID_HEX_LEN",
    "TOOL_CALL_ID_MAX_LEN",
    "ActiveTrace",
    "TraceOptions",
    "active_trace",
    "bind_active_trace",
    "is_thread_id",
    "is_tool_call_id",
    "random_thread_id",
]
