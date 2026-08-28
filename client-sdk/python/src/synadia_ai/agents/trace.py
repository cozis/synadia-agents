"""Observability trace primitives — the shared vocabulary for both SDK halves.

A **prompt execution** is one request on ``agents.prompt.{a}.{o}.{n}`` and
its §6 response stream. Its ``prompt_id`` is minted by the *agent that runs
it*, at receive time; the caller never chooses an id. A root prompt
therefore carries nothing on the wire — it is byte-identical to a legacy
prompt — and "root" simply means "no ``parent_prompt_id``".

Lineage rides three optional envelope fields (§5.6 tolerates unknown
fields), sent only by callers that are themselves inside a prompt handler:
``parent_prompt_id`` (the calling execution's own id), ``root_id`` (forwarded
unchanged), ``tool_call_id`` (the model tool call being served — explicit on
``Agent.prompt``). The receiving service publishes one :class:`TraceRecord`
per execution on the (flat, configurable) trace subject and puts the same lineage on every
model request via :func:`trace_headers`, so a NATS observer and an HTTP
proxy can each rebuild the tree without seeing the other's traffic.

Mirrors the TS SDK; ids, subjects and header bytes are normative.
"""

from __future__ import annotations

import re
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict

# Length (hex chars) of prompt ids. 16 hex chars = 64 bits — collision-safe
# for observability, short enough to eyeball in header dumps.
PROMPT_ID_HEX_LEN = 16

_PROMPT_ID_RE = re.compile(rf"^[0-9a-f]{{{PROMPT_ID_HEX_LEN}}}$")

# Tool-call ids flow into HTTP header values and a NATS subject-adjacent
# record, so they are bounded to a single line of visible ASCII.
TOOL_CALL_ID_MAX_LEN = 256
_TOOL_CALL_ID_RE = re.compile(r"^[\x21-\x7e]+$")

# Default trace subject: one TraceRecord per execution, published flat on
# this exact subject. Deployments pick their own via
# ``AgentService(trace_subject=...)``.
DEFAULT_TRACE_SUBJECT = "afo.threads"

# Normative header vocabulary — reproduced byte-for-byte by the TS SDK and
# any observing proxy.
HEADER_TRACE = "x-synadia-trace"  # "<root_id>:<prompt_id>"; slots equal ⇔ root
HEADER_PARENT = "x-synadia-parent"  # "<parent_prompt_id>:<tool_call_id>", non-root only


def is_prompt_id(value: str) -> bool:
    """True iff ``value`` has the normative prompt-id shape."""
    return _PROMPT_ID_RE.fullmatch(value) is not None


def is_tool_call_id(value: str) -> bool:
    """True iff ``value`` is header-safe: 1-256 visible-ASCII characters."""
    return len(value) <= TOOL_CALL_ID_MAX_LEN and _TOOL_CALL_ID_RE.fullmatch(value) is not None


def random_prompt_id() -> str:
    return secrets.token_hex(PROMPT_ID_HEX_LEN // 2)


@dataclass(frozen=True, slots=True)
class ActiveTrace:
    """Identity of the prompt execution running in the current async context."""

    prompt_id: str
    root_id: str
    parent_prompt_id: str | None = None
    tool_call_id: str | None = None

    @property
    def is_root(self) -> bool:
        return self.parent_prompt_id is None


class TraceRecord(BaseModel):
    """One prompt execution, as published on the trace subject.

    Wire shape (``None`` fields omitted)::

        {"prompt_id": ..., "root_id": ..., "parent_prompt_id"?: ...,
         "tool_call_id"?: ..., "agent": ..., "owner": ..., "session": ...,
         "instance_id": ..., "ts": "<UTC ISO 8601>"}
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    prompt_id: str
    root_id: str
    parent_prompt_id: str | None = None
    tool_call_id: str | None = None
    agent: str
    owner: str
    session: str
    instance_id: str
    ts: str

    def encode(self) -> bytes:
        return self.model_dump_json(exclude_none=True).encode("utf-8")


def format_trace_headers(trace: ActiveTrace) -> dict[str, str]:
    """Headers a harness attaches to every model request of ``trace``.

    The tool slot of ``x-synadia-parent`` is percent-encoded (RFC 3986,
    no safe characters) and empty when the spawn was programmatic.
    """
    headers = {HEADER_TRACE: f"{trace.root_id}:{trace.prompt_id}"}
    if trace.parent_prompt_id is not None:
        tool = quote(trace.tool_call_id or "", safe="")
        headers[HEADER_PARENT] = f"{trace.parent_prompt_id}:{tool}"
    return headers


# --- ambient (async-context) layer -------------------------------------
#
# The agent-sdk binds the execution's ActiveTrace around each prompt handler;
# Agent.prompt() reads it to forward lineage, and trace_headers() reads it
# so harnesses that build their HTTP client deep inside a tool need no
# plumbing. contextvars flow into awaited work and tasks created inside.

_active_trace: ContextVar[ActiveTrace | None] = ContextVar("synadia_active_trace", default=None)


def active_trace() -> ActiveTrace | None:
    """The ambient :class:`ActiveTrace`, or ``None`` outside a bound handler."""
    return _active_trace.get()


@contextmanager
def bind_active_trace(trace: ActiveTrace) -> Iterator[None]:
    """Run the block with ``trace`` as the ambient execution."""
    token = _active_trace.set(trace)
    try:
        yield
    finally:
        _active_trace.reset(token)


def trace_headers() -> dict[str, str]:
    """:func:`format_trace_headers` for the ambient execution; ``{}`` outside one."""
    trace = active_trace()
    return format_trace_headers(trace) if trace is not None else {}


__all__ = [
    "DEFAULT_TRACE_SUBJECT",
    "HEADER_PARENT",
    "HEADER_TRACE",
    "PROMPT_ID_HEX_LEN",
    "TOOL_CALL_ID_MAX_LEN",
    "ActiveTrace",
    "TraceRecord",
    "active_trace",
    "bind_active_trace",
    "format_trace_headers",
    "is_prompt_id",
    "is_tool_call_id",
    "random_prompt_id",
    "trace_headers",
]
