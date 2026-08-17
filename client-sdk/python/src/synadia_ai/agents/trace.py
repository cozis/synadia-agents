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
"""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass

# Length (hex chars) of derived thread ids. 16 hex chars = 64 bits —
# collision-safe for observability purposes and short enough to eyeball
# in HTTP header dumps.
THREAD_ID_HEX_LEN = 16

_THREAD_ID_RE = re.compile(rf"^[0-9a-f]{{{THREAD_ID_HEX_LEN}}}$")


def is_thread_id(value: str) -> bool:
    """True iff ``value`` has the normative derived-thread-id shape."""
    return _THREAD_ID_RE.fullmatch(value) is not None


def derive_thread_id(reply_subject: str) -> str:
    """Normative: lowercase hex of ``sha256(reply_subject)``, truncated."""
    return hashlib.sha256(reply_subject.encode("utf-8")).hexdigest()[:THREAD_ID_HEX_LEN]


def random_thread_id() -> str:
    return secrets.token_hex(THREAD_ID_HEX_LEN // 2)


@dataclass(frozen=True, slots=True)
class TraceContext:
    """Trace identity a parent thread forwards to a spawned prompt
    (``PromptStream.child_trace()`` → ``Agent.prompt(trace=...)``)."""

    root_id: str
