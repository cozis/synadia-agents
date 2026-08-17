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
import re
import secrets
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from urllib.parse import quote

# Length (hex chars) of derived thread ids. 16 hex chars = 64 bits —
# collision-safe for observability purposes and short enough to eyeball
# in HTTP header dumps.
THREAD_ID_HEX_LEN = 16

_THREAD_ID_RE = re.compile(rf"^[0-9a-f]{{{THREAD_ID_HEX_LEN}}}$")


def is_thread_id(value: str) -> bool:
    """True iff ``value`` has the normative derived-thread-id shape."""
    return _THREAD_ID_RE.fullmatch(value) is not None


# Normative header vocabulary — the TS SDK and any observing proxy must
# reproduce these byte-for-byte.
HEADER_TRACE = "x-synadia-trace"  # "<root_id>:<thread_id>"
HEADER_SPAWNED = "x-synadia-spawned"  # comma-joined spawn entries (format_spawn_entry)
HEADER_EVENT = "x-synadia-event"  # "spawn" tags the spawn-time marker request

IDENTITY_PATH_MARKER = "synadia"  # reserved leading segment of identity_path()


def identity_path(
    *,
    agent: str,
    owner: str,
    session_name: str,
    instance_id: str | None = None,
) -> str:
    """``synadia/<agent>/<owner>/<session>/<instance_id>`` — base-URL path
    prefix attributing a provider client's traffic to an agent (``-`` when
    there is no instance id). Normative."""
    return f"{IDENTITY_PATH_MARKER}/{agent}/{owner}/{session_name}/{instance_id or '-'}"


def derive_thread_id(reply_subject: str) -> str:
    """Normative: lowercase hex of ``sha256(reply_subject)``, truncated."""
    return hashlib.sha256(reply_subject.encode("utf-8")).hexdigest()[:THREAD_ID_HEX_LEN]


def random_thread_id() -> str:
    return secrets.token_hex(THREAD_ID_HEX_LEN // 2)


def format_spawn_entry(
    child_thread_id: str,
    tool_call_id: str | None = None,
    edge_type: str | None = None,
) -> str:
    """One spawn-edge claim: ``<child>:<tool_call_id>:<edge_type>``. Normative.

    ``tool_call_id`` defaults from the ambient :func:`tool_scope` and empty
    means "no tool"; ``edge_type`` defaults to ``tool_call``/``programmatic``
    to match. The tool slot is percent-encoded; consumers ``unquote`` it.
    """
    if not tool_call_id:
        tool_call_id = current_tool_call_id()
    if edge_type is None:
        edge_type = "tool_call" if tool_call_id else "programmatic"
    encoded_tool = quote(tool_call_id, safe="") if tool_call_id else ""
    return f"{child_thread_id}:{encoded_tool}:{edge_type}"


@dataclass(frozen=True, slots=True)
class TraceContext:
    """Trace identity a parent thread forwards to a spawned prompt
    (``PromptStream.child_trace()`` → ``Agent.prompt(trace=...)``)."""

    root_id: str


# --- ambient (async-context) layer -------------------------------------
#
# Plain PEP 567 contextvars making correlation implicit where threading
# arguments through is impossible or noisy: the agent-sdk binds an
# ActiveTrace around each prompt handler, and Agent.prompt() consults it
# when no explicit ``trace`` is passed — a spawn inside a handler joins
# the parent's tree and records its edge automatically.

SpawnRecorder = Callable[[str], dict[str, str]]
"""``(child_thread_id) -> spawn-marker headers`` — the parent stream's
edge recorder; runs in the spawner's context, so it resolves the
ambient tool scope itself."""


@dataclass(frozen=True, slots=True)
class ActiveTrace:
    """The prompt execution currently running in this async context."""

    thread_id: str
    root_id: str
    record_spawn: SpawnRecorder | None = None


_active_trace: ContextVar[ActiveTrace | None] = ContextVar(
    "synadia_agents_active_trace", default=None
)
_active_tool_call: ContextVar[str | None] = ContextVar(
    "synadia_agents_active_tool_call", default=None
)


def active_trace() -> ActiveTrace | None:
    """The ambient :class:`ActiveTrace`, or None outside a bound handler."""
    return _active_trace.get()


def current_tool_call_id() -> str | None:
    """The innermost ambient :func:`tool_scope` id, or None outside one."""
    return _active_tool_call.get()


@contextmanager
def bind_active_trace(trace: ActiveTrace) -> Iterator[None]:
    """Bind ``trace`` as the ambient context for the enclosed scope (the
    agent-sdk calls this around each prompt-handler invocation)."""
    token = _active_trace.set(trace)
    try:
        yield
    finally:
        _active_trace.reset(token)


@contextmanager
def tool_scope(tool_call_id: str) -> Iterator[None]:
    """Mark the enclosed scope as executing one tool invocation: ambient
    spawns inside it label their edge with ``tool_call_id``. Scopes
    nest; the innermost wins."""
    token = _active_tool_call.set(tool_call_id)
    try:
        yield
    finally:
        _active_tool_call.reset(token)
