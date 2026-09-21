"""Prompt interceptors — the caller-side hook around :meth:`Agent.prompt`.

An interceptor runs before a prompt is published, once per prompt, in the
order the client lists them. It sees the target agent, the prompt text and
the opaque ``context`` the caller passed to :meth:`Agent.prompt`, and may
publish messages of its own first — signed with the prompting client's
identity, through ``ctx.identity`` — before it returns extra envelope
fields and extra headers for the prompt to carry, or ``None``.

The SDK gives those fields and headers no meaning. §5.6 obliges a receiver
to tolerate unknown top-level envelope fields; a host built on
:mod:`synadia_ai.agent_service` reads them back from
:attr:`Envelope.extras` and the request's headers in its own request
interceptors.

When it runs: at publish time — on the stream's first ``__anext__``, after
the prompt's sender identity is resolved and before its ``Agent-Sender``
header is signed over the final envelope — so a prompt that is never
iterated, or that fails validation, runs no interceptor. It runs in a copy
of the :mod:`contextvars` context :meth:`Agent.prompt` was called in, not
the one the stream happens to be iterated in, so an interceptor that reads
a ``ContextVar`` sees the caller's value. The TypeScript SDK's
``PromptInterceptor`` is the same hook.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Protocol

from .errors import NatsAgentError
from .identity.options import Identity, self_id_for
from .identity.sender_header import AGENT_SENDER_HEADER
from .identity.signed_publish import signed_publish_headers, to_bytes

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient

    from .agent import Agent
    from .identity.agent_id import AgentId


class PromptSigning:
    """Signing with the prompting client's identity.

    The one that signs the prompt's own ``Agent-Sender`` header; the same
    rules as the :class:`~synadia_ai.agents.Agents` methods of the same
    names.
    """

    def __init__(self, nc: NATSClient, identity: Identity | None) -> None:
        self._nc = nc
        self._identity = identity

    @property
    def can_sign(self) -> bool:
        """``True`` iff a signer is configured, so :meth:`publish_signed` can sign."""
        return self._identity is not None and self._identity.signer is not None

    async def self_id(self) -> AgentId:
        """The client's own agent ID (``{account}.{user}``), as ``Agents.self_id()``."""
        return await self_id_for(self._identity, self._nc)

    async def publish_signed(
        self,
        subject: str,
        payload: bytes | str,
        *,
        sub: str | None = None,
        headers: Mapping[str, str] | None = None,
        nonce: str | None = None,
    ) -> None:
        """Sign and publish one message, as ``Agents.publish_signed()``.

        Its ``Agent-Sender`` header, and ``Nats-Msg-Id`` set to the nonce.
        Pass ``nonce`` for a body that carries its own id.
        """
        data = to_bytes(payload)
        hdrs = await signed_publish_headers(
            self._identity, self._nc, subject, data, sub=sub, headers=headers, nonce=nonce
        )
        await self._nc.publish(subject, data, headers=hdrs)


@dataclass(frozen=True, slots=True)
class PromptInterceptorContext:
    """What a :class:`PromptInterceptor` sees."""

    #: The agent the prompt is addressed to.
    agent: Agent
    #: The prompt text.
    prompt: str
    #: ``Agent.prompt(context=...)``, verbatim; empty when the caller passed none.
    context: Mapping[str, object]
    #: The connection the prompt goes out on.
    connection: NATSClient
    #: Signing with the prompting client's identity.
    identity: PromptSigning


@dataclass(frozen=True, slots=True)
class PromptExtras:
    """What a :class:`PromptInterceptor` adds to the prompt.

    ``fields`` are extra top-level envelope fields, by wire name — a field
    the envelope defines (``prompt``, ``attachments``) is refused.
    ``headers`` are extra message headers — ``Agent-Sender`` belongs to the
    SDK and is refused.
    """

    fields: Mapping[str, object] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)


class PromptInterceptor(Protocol):
    """A caller-side hook that runs before each prompt is published.

    An exception fails the prompt: it surfaces from the stream's first
    ``__anext__``, and the prompt is not sent.
    """

    async def before_prompt(self, ctx: PromptInterceptorContext) -> PromptExtras | None: ...


EMPTY_CONTEXT: Mapping[str, object] = MappingProxyType({})


async def collect_extras(
    interceptors: tuple[PromptInterceptor, ...],
    ctx: PromptInterceptorContext,
    envelope_fields: frozenset[str],
) -> PromptExtras:
    """Run ``interceptors`` in order and merge what they add.

    A later one wins a key an earlier one also set. A field in
    ``envelope_fields`` (the ones the envelope codec owns), or the
    ``Agent-Sender`` header, is refused rather than silently dropped: an
    interceptor that sets one has a bug worth hearing about.
    """
    fields: dict[str, object] = {}
    headers: dict[str, str] = {}
    for interceptor in interceptors:
        extras = await interceptor.before_prompt(ctx)
        if extras is None:
            continue
        for key, value in extras.fields.items():
            if key in envelope_fields:
                raise NatsAgentError(f"prompt interceptor: envelope field `{key}` is not an extra")
            fields[key] = value
        for key, value in extras.headers.items():
            if key.lower() == AGENT_SENDER_HEADER.lower():
                raise NatsAgentError(
                    f"prompt interceptor: the {AGENT_SENDER_HEADER} header is the SDK's"
                )
            headers[key] = value
    return PromptExtras(fields=fields, headers=headers)


__all__ = [
    "PromptExtras",
    "PromptInterceptor",
    "PromptInterceptorContext",
    "PromptSigning",
]
