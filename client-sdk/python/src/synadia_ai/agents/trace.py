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

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TraceOptions:
    """Opt-in tracing configuration; passing an instance enables tracing."""


__all__ = ["TraceOptions"]
