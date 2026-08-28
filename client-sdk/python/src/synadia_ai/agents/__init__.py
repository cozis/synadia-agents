"""Python SDK for the Synadia Agent Protocol for NATS.

See https://github.com/synadia-ai/synadia-agent-sdk-docs/blob/main/core-protocol.md
for the wire spec.

Public API entry points:

* :class:`Agents` — caller-side; owns the heartbeat wildcard, hands out
  live :class:`Agent` instances from :meth:`Agents.discover`.
* :class:`Agent` — a discovered agent with flat ``$SRV.INFO`` metadata
  fields and a :meth:`Agent.prompt` method.
* :func:`load_context_options` — translate a ``nats`` CLI context into
  kwargs for :func:`nats.connect`.
* :func:`parse_nats_url` — parse a NATS URL (with optional userinfo
  for token / user:password) into kwargs for :func:`nats.connect`.
* :mod:`synadia_ai.agents.trace` — observability vocabulary shared with
  the agent-sdk: :class:`TraceRecord` (published per prompt execution on
  the configurable trace subject, default ``afo.threads``), :func:`trace_headers`
  (the ambient execution's ``x-synadia-*`` headers for model requests).

The agent-host surface (``AgentService``, ``PromptStream``,
``PromptHandler``) lives in the sibling package
:mod:`synadia_ai.agent_service` (distribution
``synadia-ai-agent-service``); install that package alongside this one
when authoring an agent harness.

The SDK does NOT open NATS connections — callers build a
:class:`~nats.aio.client.Client` and hand it to :class:`Agents`. This
matches the TS SDK's PR #7 surface and the broader ``@nats-io/*``
convention (``jetstream(nc)``, ``Svcm(nc)``, ``Kvm(nc)``…).
"""

from __future__ import annotations

from .agent import (
    DEFAULT_PROMPT_MAX_WAIT_S,
    DEFAULT_STREAM_INACTIVITY_TIMEOUT_S,
    Agent,
    Query,
    StreamMessage,
)
from .agents import Agents
from .context import load_context_options, parse_nats_url
from .discovery import (
    DEFAULT_DISCOVER_MAX_WAIT_S,
    DEFAULT_DISCOVER_STALL_S,
    PROMPT_ENDPOINT_NAME,
    PROMPT_QUEUE_GROUP,
    SERVICE_NAME,
    STATUS_ENDPOINT_NAME,
    STATUS_QUEUE_GROUP,
    AgentInfo,
    DiscoverFilter,
    EndpointInfo,
    build_agent_info,
)
from .envelope import Attachment, Envelope, decode, encode
from .errors import (
    AgentNotFound,
    AgentsClosedError,
    AttachmentsNotSupportedError,
    InvalidSubjectToken,
    NatsAgentError,
    NatsContextError,
    PayloadTooLargeError,
    PromptEmptyError,
    ProtocolError,
    QueryTimeout,
    StreamMaxWaitExceededError,
    StreamStalledError,
    ValidationError,
)
from .heartbeat import (
    DEFAULT_LIVENESS_SLACK,
    HEARTBEAT_SUBJECT,
    HeartbeatPayload,
    Liveness,
)
from .messages import Chunk, QueryChunk, ResponseChunk, StatusChunk
from .subjects import AgentSubject
from .trace import (
    DEFAULT_TRACE_SUBJECT,
    HEADER_PARENT,
    HEADER_TRACE,
    ActiveTrace,
    TraceRecord,
    active_trace,
    bind_active_trace,
    trace_headers,
)

__all__ = [
    "DEFAULT_DISCOVER_MAX_WAIT_S",
    "DEFAULT_DISCOVER_STALL_S",
    "DEFAULT_LIVENESS_SLACK",
    "DEFAULT_PROMPT_MAX_WAIT_S",
    "DEFAULT_STREAM_INACTIVITY_TIMEOUT_S",
    "DEFAULT_TRACE_SUBJECT",
    "HEADER_PARENT",
    "HEADER_TRACE",
    "HEARTBEAT_SUBJECT",
    "PROMPT_ENDPOINT_NAME",
    "PROMPT_QUEUE_GROUP",
    "SERVICE_NAME",
    "STATUS_ENDPOINT_NAME",
    "STATUS_QUEUE_GROUP",
    "ActiveTrace",
    "Agent",
    "AgentInfo",
    "AgentNotFound",
    "AgentSubject",
    "Agents",
    "AgentsClosedError",
    "Attachment",
    "AttachmentsNotSupportedError",
    "Chunk",
    "DiscoverFilter",
    "EndpointInfo",
    "Envelope",
    "HeartbeatPayload",
    "InvalidSubjectToken",
    "Liveness",
    "NatsAgentError",
    "NatsContextError",
    "PayloadTooLargeError",
    "PromptEmptyError",
    "ProtocolError",
    "Query",
    "QueryChunk",
    "QueryTimeout",
    "ResponseChunk",
    "StatusChunk",
    "StreamMaxWaitExceededError",
    "StreamMessage",
    "StreamStalledError",
    "TraceRecord",
    "ValidationError",
    "active_trace",
    "bind_active_trace",
    "build_agent_info",
    "decode",
    "encode",
    "load_context_options",
    "parse_nats_url",
    "trace_headers",
]
