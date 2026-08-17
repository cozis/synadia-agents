"""Observability trace primitives.

A **thread** is one prompt execution: one request published to
``agents.prompt.{a}.{o}.{n}`` and its §6 response stream. Its identity is
*derived*, never exchanged: both ends already share the prompt's reply
inbox (the caller mints it; the agent receives it as the request's reply
subject), and it is unique per prompt, so a hash of it identifies the
thread with zero extra wire surface.
"""

from __future__ import annotations

import hashlib
import re
import secrets

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
