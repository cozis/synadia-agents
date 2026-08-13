"""Shared test fixture: build an :class:`AgentInfo` without discovery.

Unit tests that own both ends of the wire (or need no wire at all)
build the record directly instead of round-tripping ``$SRV.INFO``.
Lifted from the identical private helpers in ``test_prompt_max_wait.py``
/ ``test_prompt_cancel_race.py`` / ``test_mux_wire_economy.py`` — new
tests should import this one rather than adding another copy.
"""

from __future__ import annotations

from types import MappingProxyType

from synadia_ai.agents import AgentInfo, EndpointInfo


def make_agent_info(
    prompt_subject: str,
    *,
    agent: str = "test-agent",
    owner: str = "pytest",
    session_name: str = "test",
) -> AgentInfo:
    """An :class:`AgentInfo` pointing at a test-controlled prompt subject."""
    prompt_endpoint = EndpointInfo(
        name="prompt",
        subject=prompt_subject,
        queue_group="agents",
        metadata=MappingProxyType({}),
        max_payload_bytes=None,
        attachments_ok=True,
    )
    return AgentInfo(
        instance_id="test-instance",
        agent=agent,
        owner=owner,
        session_name=session_name,
        protocol_version="0.3",
        description="",
        version="0.0.0",
        metadata=MappingProxyType({"agent": agent, "owner": owner}),
        endpoints=(prompt_endpoint,),
        prompt_endpoint=prompt_endpoint,
    )
