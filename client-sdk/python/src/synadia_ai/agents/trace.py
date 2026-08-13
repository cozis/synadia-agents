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

# Length (hex chars) of derived thread ids. 16 hex chars = 64 bits —
# collision-safe for observability purposes and short enough to eyeball
# in HTTP header dumps.
THREAD_ID_HEX_LEN = 16

# Provider-facing header vocabulary — normative, like the derivation
# below: the TS SDK and any observing proxy must reproduce these
# byte-for-byte, so they live here (the shared-primitives home) rather
# than as literals at the emission sites.
HEADER_THREAD_ID = "x-agent-thread-id"


def derive_thread_id(reply_subject: str) -> str:
    """Derive a thread id from a prompt's reply subject.

    Normative convention (must match every SDK implementation):
    lowercase hex of ``sha256(reply_subject)`` truncated to
    :data:`THREAD_ID_HEX_LEN` chars, hashing the exact full subject
    string as UTF-8 — e.g. ``_INBOX.agents.<mux>.<token>``.
    """
    return hashlib.sha256(reply_subject.encode("utf-8")).hexdigest()[:THREAD_ID_HEX_LEN]
