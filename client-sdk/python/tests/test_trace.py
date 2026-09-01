"""Tracing opt-in switch — off unless a ``trace=`` option is passed.

Mirrors ``client-sdk/typescript/test/unit/trace-options.test.ts``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from synadia_ai.agents import Agent, Agents, TraceOptions, build_agent_info

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient

# Tracker/resolver only store the connection at construction time, so a
# bare stub is enough for option-plumbing tests.
_NC = cast("NATSClient", object())


def _info() -> dict[str, object]:
    return {
        "name": "agents",
        "id": "VMKS6MHK71PCPWGY38A7N5",
        "version": "1.0.0",
        "description": "test agent",
        "metadata": {
            "agent": "echo",
            "owner": "test",
            "session": "main",
            "protocol_version": "0.3",
        },
        "endpoints": [
            {
                "name": "prompt",
                "subject": "agents.prompt.echo.test.main",
                "queue_group": "agents",
                "metadata": {"max_payload": "1MB", "attachments_ok": "true"},
            }
        ],
    }


def test_tracing_is_off_when_trace_is_omitted() -> None:
    agents = Agents(nc=_NC)
    assert agents.trace is None


def test_tracing_is_on_when_a_trace_option_is_passed() -> None:
    options = TraceOptions()
    agents = Agents(nc=_NC, trace=options)
    assert agents.trace is options


def test_trace_option_reaches_agent_handles() -> None:
    info = build_agent_info(_info())
    assert info is not None
    off = Agent(_NC, info)
    assert off.tracing_enabled is False
    on = Agent(_NC, info, trace=TraceOptions())
    assert on.tracing_enabled is True
