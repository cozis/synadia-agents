"""Server-side protocol-compliant agent per the §12 implementation checklist.

An :class:`AgentService` registers as a NATS micro service with the
protocol's required metadata, serves the prompt inbox, and publishes
heartbeats at the configured interval. Agent authors register a prompt
handler via :meth:`AgentService.on_prompt`; the handler receives the
decoded envelope and a :class:`PromptStream` on which to emit response
chunks.

The **client side** of the protocol (discover-and-prompt) lives in the
sibling distribution :mod:`synadia_ai.agents` as
:class:`~synadia_ai.agents.Agent` returned from
:meth:`~synadia_ai.agents.Agents.discover`.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Awaitable, Callable
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from typing import TYPE_CHECKING

from nats.micro import ServiceConfig, add_service
from nats.micro.service import EndpointConfig
from synadia_ai.agents import (
    HEADER_ROOT_ID,
    HEADER_SPAWNED,
    HEADER_THREAD_ID,
    HEADER_TOOL_CALL_ID,
    PROMPT_ENDPOINT_NAME,
    PROMPT_QUEUE_GROUP,
    SERVICE_NAME,
    STATUS_ENDPOINT_NAME,
    STATUS_QUEUE_GROUP,
    ActiveTrace,
    AgentSubject,
    Attachment,
    Chunk,
    Envelope,
    ProtocolError,
    QueryChunk,
    QueryTimeout,
    ResponseChunk,
    StatusChunk,
    TraceContext,
    bind_active_trace,
    current_tool_call_id,
    decode,
    derive_thread_id,
    format_spawn_entry,
    spawn_marker_headers,
)
from synadia_ai.agents.messages import encode_chunk

from ._bytes import format_human_bytes, parse_human_bytes
from ._inbox import new_inbox
from ._logging import get_logger
from .heartbeat import build_heartbeat_payload, run_publisher

if TYPE_CHECKING:
    from nats.aio.client import Client as NATSClient
    from nats.micro.service import Request, Service


log = get_logger(__name__)

PromptHandler = Callable[["Envelope", "PromptStream"], Awaitable[None]]

# §3.2 + §11.1: metadata.protocol_version is MAJOR.MINOR only.
_PROTOCOL_VERSION = "0.3"


def _resolve_sdk_version() -> str:
    """Read the installed package version so pyproject.toml is the single source.

    Falls back to ``"0.0.0+unknown"`` when the package is not installed
    (e.g. running from a source tree without ``uv sync``) — `$SRV.INFO`
    still emits a syntactically valid version field.
    """
    try:
        return _pkg_version("synadia-ai-agent-service")
    except PackageNotFoundError:
        return "0.0.0+unknown"


_SDK_VERSION = _resolve_sdk_version()

# §2.1: prompt endpoint metadata defaults.
#
# ``DEFAULT_MAX_PAYLOAD`` is used only when ``nc.max_payload`` is missing or
# zero (an unconnected client, or a server that did not include the field in
# its INFO block) — the broker's negotiated value is the real cap, so when
# it's available we always advertise that. The TS harnesses share the same
# constant under the same name (``agents/{claude-code,openclaw,pi}``).
#
# ``attachments_ok`` defaults to True so envelopes with ``attachments`` just
# work out of the box.
DEFAULT_MAX_PAYLOAD = "1MB"
DEFAULT_ATTACHMENTS_OK = True

# §6.4: callers may wire a stream-inactivity timeout — the TS SDK defaults to
# 60 s — so an agent that goes quiet for too long looks dead even if its
# handler is still working. To avoid that, every TS reference harness
# (`agents/pi/`, `agents/claude-code/`, `agents/openclaw/`) emits
# `{type:"status",data:"ack"}` periodically while a request is in flight. We
# match that behaviour by default; agent authors who don't want it pass
# `keepalive_interval_s=None`.
#
# Note: the §6.4-MUST *leading* ack is emitted unconditionally by
# ``_on_prompt_request`` before the handler runs — it is independent of
# this keep-alive cadence, and disabling keep-alive does NOT disable the
# leading ack.
DEFAULT_KEEPALIVE_INTERVAL_S: float = 30.0


class PromptStream:
    """Handle given to a prompt handler for emitting response chunks.

    The :class:`AgentService` owns stream termination (empty-payload
    terminator per §6.5). Handlers should ``send(...)`` zero or more
    chunks and return; raising an exception converts to a service error
    per §9.

    Also carries the request's observability identity:
    :attr:`thread_id` is derived from the request's
    reply subject, :attr:`root_id` comes from the envelope's optional
    ``root_id`` field (falling back to :attr:`thread_id` for legacy
    callers — a provisional root). :meth:`trace_headers` yields the
    HTTP headers a harness passes on every outbound model request;
    :meth:`record_spawn` / :meth:`child_trace` support spawning
    sub-agents with correct edge reporting.
    """

    def __init__(
        self,
        request: Request,
        nc: NATSClient,
        *,
        reply_subject: str,
        root_id: str | None = None,
    ) -> None:
        self._request = request
        self._nc = nc
        self._thread_id = derive_thread_id(reply_subject)
        self._root_id = root_id if root_id is not None else self._thread_id
        # Edges recorded via record_spawn(), drained by the next
        # trace_headers() call (the parent's next model request).
        # Dict-as-ordered-set: O(1) idempotent inserts, insertion order
        # preserved for the drained report.
        self._pending_spawns: dict[str, None] = {}

    # --- observability identity ----------------------------------------

    @property
    def thread_id(self) -> str:
        """This prompt execution's derived thread id."""
        return self._thread_id

    @property
    def root_id(self) -> str:
        """Root thread id of the tree this prompt belongs to."""
        return self._root_id

    @property
    def is_root(self) -> bool:
        """True iff this thread is its tree's root.

        Exact when the caller sent ``root_id`` (any client-SDK caller);
        provisionally true for legacy callers that sent none.
        """
        return self._root_id == self._thread_id

    def _identity_headers(self) -> dict[str, str]:
        return {
            HEADER_THREAD_ID: self._thread_id,
            HEADER_ROOT_ID: self._root_id,
        }

    def trace_headers(self, *, tool_call_id: str | None = None) -> dict[str, str]:
        """Headers for an outbound model request issued by this thread.

        Includes thread + root identity, any spawn edges recorded since
        the previous call (drained — the marker channel provides the
        redundant delivery), and the tool invocation this outbound call
        serves — ``tool_call_id`` explicitly, else the ambient
        :func:`~synadia_ai.agents.tool_scope` when inside one.
        """
        if tool_call_id is None:
            tool_call_id = current_tool_call_id()
        headers = self._identity_headers()
        if tool_call_id is not None:
            headers[HEADER_TOOL_CALL_ID] = tool_call_id
        if self._pending_spawns:
            headers[HEADER_SPAWNED] = ",".join(self._pending_spawns)
            self._pending_spawns.clear()
        return headers

    def record_spawn(
        self,
        child_thread_id: str,
        *,
        tool_call_id: str | None = None,
        edge_type: str | None = None,
    ) -> dict[str, str]:
        """Record a spawned child thread; returns spawn-marker headers.

        Registers the edge for the completion-report channel (the next
        :meth:`trace_headers` call) and returns the headers for the
        spawn-time idempotent marker request (``x-agent-event: spawn``)
        — fire it through any provider client, e.g.
        ``client.models.list(extra_headers=...)``, as fire-and-forget
        telemetry. An observing proxy consumes marker-tagged requests
        without forwarding them upstream.

        Defaults follow :func:`~synadia_ai.agents.format_spawn_entry`:
        ``tool_call_id`` resolves from the ambient
        :func:`~synadia_ai.agents.tool_scope`, and an unspecified
        ``edge_type`` is ``tool_call`` iff a tool id resolved, else
        ``programmatic`` — so explicit and ambient call sites agree.
        """
        entry = format_spawn_entry(child_thread_id, tool_call_id, edge_type)
        self._pending_spawns[entry] = None  # idempotent insert, order preserved
        return spawn_marker_headers(self._thread_id, self._root_id, entry)

    def child_trace(self) -> TraceContext:
        """Trace context to pass to ``Agent.prompt(trace=...)`` when spawning."""
        return TraceContext(root_id=self._root_id)

    def _as_active_trace(self) -> ActiveTrace:
        """Ambient-context view of this stream (bound around the handler).

        Captures only the identity strings and the pending-spawns dict —
        deliberately NOT ``self`` — so a handler-spawned task that
        outlives the request doesn't pin the ``Request``/``Msg`` (and
        its attachment payload) via the contextvar binding.
        """
        thread_id, root_id = self._thread_id, self._root_id
        pending = self._pending_spawns

        def _record(child_thread_id: str) -> dict[str, str]:
            entry = format_spawn_entry(child_thread_id)
            pending[entry] = None
            return spawn_marker_headers(thread_id, root_id, entry)

        return ActiveTrace(
            thread_id=thread_id,
            root_id=root_id,
            record_spawn=_record,
        )

    async def send(self, chunk: str | Chunk) -> None:
        """Publish one chunk to the caller's reply subject.

        A ``str`` is wrapped in a :class:`ResponseChunk` and emitted as the
        §6.3 bare-string form ``{"type":"response","data":"<text>"}``. §6.2
        explicitly forbids plain-text shorthand on the response side — every
        non-terminating chunk MUST be a JSON object with a ``type`` field.
        """
        if isinstance(chunk, str):
            payload = encode_chunk(ResponseChunk(text=chunk))
        elif isinstance(chunk, ResponseChunk | StatusChunk | QueryChunk):
            payload = encode_chunk(chunk)
        else:
            raise TypeError(f"unsupported chunk type: {type(chunk).__name__}")
        await self._request.respond(payload)

    async def ask(
        self,
        prompt: str | Envelope,
        *,
        timeout: float,
        attachments: list[Attachment] | None = None,
    ) -> Envelope:
        """Ask the caller a mid-stream question and await the reply (§7).

        Allocates a fresh reply inbox, publishes a ``query`` chunk into the
        response stream, and waits for exactly one reply. The response stream
        stays open across the round-trip — the caller keeps iterating the
        prompt's async iterator while the handler awaits here.

        ``prompt`` is either a bare string or an :class:`Envelope` whose
        ``prompt`` + ``attachments`` become the query's fields. Raises
        :class:`QueryTimeout` if no reply arrives within ``timeout`` seconds;
        per §7.3 the handler decides whether to abort the stream or proceed.
        """
        if isinstance(prompt, str):
            prompt_text = prompt
            query_attachments = list(attachments) if attachments else None
        elif isinstance(prompt, Envelope):
            prompt_text = prompt.prompt
            merged = list(prompt.attachments or [])
            if attachments:
                merged.extend(attachments)
            query_attachments = merged or None
        else:
            raise TypeError(f"unsupported prompt type: {type(prompt).__name__}")

        reply_subject = new_inbox()
        sub = await self._nc.subscribe(reply_subject)
        query = QueryChunk(
            id=uuid.uuid4().hex,
            reply_subject=reply_subject,
            prompt=prompt_text,
            attachments=query_attachments,
        )
        try:
            await self.send(query)
            try:
                msg = await sub.next_msg(timeout=timeout)
            except TimeoutError as exc:
                raise QueryTimeout(
                    f"no reply on {reply_subject} within {timeout}s (query id={query.id})"
                ) from exc
            return decode(msg.data)
        finally:
            with contextlib.suppress(Exception):
                await sub.unsubscribe()


class AgentService:
    """A protocol-compliant agent (§12 implementation checklist).

    Construct with ``agent``/``owner``/``session_name`` plus a live NATS
    client, register a prompt handler via :meth:`on_prompt`, then call
    :meth:`start`. The agent keeps a background heartbeat task running
    until :meth:`stop` is awaited.

    ``heartbeat_interval_s`` defaults to 30 s per §8.2. Under v0.3 the
    session lives in the subject (token 5 — the ``session_name``); a
    worker that wants to serve N sessions registers N services.

    ``keepalive_interval_s`` controls the per-request keep-alive
    *cadence*: while a handler is running, the agent emits
    ``{"type":"status","data":"ack"}`` every ``keepalive_interval_s``
    seconds so callers using a stream inactivity timeout (the TS SDK
    default is 60 s) don't fire on slow handlers. Defaults to 30 s,
    matching the TS reference harnesses. Pass ``None`` to disable the
    periodic cadence — for example when the handler emits its own
    status chunks at a finer cadence. Note: the §6.4 *leading* ack
    (emitted before the handler runs) is mandatory and fires
    unconditionally regardless of this flag; ``None`` disables only
    the periodic mid-stream cadence, not the leading ack.

    ``max_payload`` is honored up to the connected server's negotiated
    limit (``nc.max_payload``). If you pass a value *larger* than the
    server allows, :meth:`start` clamps the advertised metadata down to
    the server's limit and logs a warning — the broker would reject
    anything larger before it ever reached your handler, so
    over-advertising would only break callers. Smaller overrides are
    honored (use case: shed expensive prompts before they reach the
    handler).
    """

    def __init__(
        self,
        *,
        agent: str,
        owner: str,
        session_name: str,
        nc: NATSClient,
        description: str = "",
        heartbeat_interval_s: int = 30,
        max_payload: str = DEFAULT_MAX_PAYLOAD,
        attachments_ok: bool = DEFAULT_ATTACHMENTS_OK,
        keepalive_interval_s: float | None = DEFAULT_KEEPALIVE_INTERVAL_S,
    ) -> None:
        if heartbeat_interval_s <= 0:
            raise ValueError("heartbeat_interval_s must be > 0 (heartbeat is mandatory in v0.3)")
        if keepalive_interval_s is not None and keepalive_interval_s <= 0:
            raise ValueError("keepalive_interval_s must be > 0 or None (None disables keep-alive)")
        # Validate max_payload eagerly so misconfiguration fails at construction
        # rather than surfacing later via caller-side validation (§5.4).
        parse_human_bytes(max_payload)
        self.subject = AgentSubject.new(agent=agent, owner=owner, session_name=session_name)
        self._nc = nc
        self._description = description or f"{agent} agent {self.subject.session_name}"
        self._heartbeat_interval_s = heartbeat_interval_s
        self._max_payload = max_payload
        self._effective_max_payload_value = max_payload
        self._attachments_ok = attachments_ok
        self._keepalive_interval_s = keepalive_interval_s
        self._prompt_handler: PromptHandler | None = None
        self._service: Service | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._heartbeat_stop = asyncio.Event()

    def on_prompt(self, handler: PromptHandler) -> None:
        """Register the prompt handler. Must be called before :meth:`start`."""
        self._prompt_handler = handler

    def _effective_max_payload(self) -> str:
        """Return the value to advertise on the prompt endpoint.

        Constructor-supplied ``max_payload`` is honored **unless it
        exceeds** the connected server's negotiated limit
        (``nc.max_payload``). When the override is larger, the broker
        would reject any request that fits the override but not the
        server cap, so we clamp down to the server value, log a warning,
        and advertise the clamped value.

        ``nc.max_payload == 0`` (or the attribute missing — older
        ``nats-py`` builds) is treated as "no INFO available" and the
        override stands as configured.
        """
        override_bytes = parse_human_bytes(self._max_payload)
        server_bytes = getattr(self._nc, "max_payload", 0) or 0
        if server_bytes <= 0:
            return self._max_payload
        if override_bytes <= server_bytes:
            return self._max_payload
        clamped = format_human_bytes(server_bytes)
        log.warning(
            "max_payload=%s (%d bytes) exceeds server limit %s (%d bytes); "
            "clamping advertised value to %s — anything larger would be "
            "rejected by the broker before reaching the handler",
            self._max_payload,
            override_bytes,
            clamped,
            server_bytes,
            clamped,
        )
        return clamped

    async def start(self) -> None:
        if self._prompt_handler is None:
            raise RuntimeError("register a prompt handler via on_prompt() before start()")
        if self._service is not None:
            raise RuntimeError("agent already started")

        # Resolve and clamp ``max_payload`` against the server's negotiated
        # limit (§2.1). See ``_effective_max_payload`` for the rule.
        max_payload_str = self._effective_max_payload()
        self._effective_max_payload_value = max_payload_str

        # §3.2: metadata.session matches the 5th subject token. For session-
        # less harnesses (e.g. openclaw) the spec allows omitting the field
        # OR setting it to "default"; the Python constructor takes a required
        # `session_name` (defaulting callers pass "default"), so we always
        # advertise it. Callers that filter on metadata.session see a
        # consistent shape across session-aware and session-less agents.
        metadata: dict[str, str] = {
            "agent": self.subject.agent,
            "owner": self.subject.owner,
            "session": self.subject.session_name,
            "protocol_version": _PROTOCOL_VERSION,
        }
        config = ServiceConfig(
            name=SERVICE_NAME,
            version=_SDK_VERSION,
            description=self._description,
            metadata=metadata,
        )
        self._service = await add_service(self._nc, config)
        await self._service.add_endpoint(
            EndpointConfig(
                name=PROMPT_ENDPOINT_NAME,
                subject=self.subject.prompt,
                handler=self._on_prompt_request,
                # §3.3: the `prompt` endpoint MUST use queue group `"agents"`
                # so multiple instances of the same logical agent load-balance
                # requests. Framework defaults differ between SDKs, which
                # would break interop — pin to the spec value explicitly.
                queue_group=PROMPT_QUEUE_GROUP,
                # §2.1: endpoint metadata is a `Record<string, string>` on the
                # wire — booleans are encoded as "true" / "false".
                metadata={
                    "max_payload": max_payload_str,
                    "attachments_ok": "true" if self._attachments_ok else "false",
                },
            )
        )
        # v0.3 §-TBD: the status endpoint returns a freshly-built heartbeat-
        # shaped payload. Same queue group as `prompt` so callers load-balance
        # to one responder per logical agent.
        await self._service.add_endpoint(
            EndpointConfig(
                name=STATUS_ENDPOINT_NAME,
                subject=self.subject.status,
                handler=self._on_status_request,
                queue_group=STATUS_QUEUE_GROUP,
            )
        )

        self._heartbeat_stop.clear()
        # §8.3 instance_id matches the micro-service instance id assigned by
        # nats-py; this is what callers correlate heartbeats against.
        self._heartbeat_task = asyncio.create_task(
            run_publisher(
                self._nc,
                self.subject,
                self._heartbeat_interval_s,
                self._service.id,
                self._heartbeat_stop,
            ),
            name=f"heartbeat-{self.subject.inbox}",
        )
        log.info("agent started on %s (instance_id=%s)", self.subject.inbox, self._service.id)

    async def stop(self) -> None:
        self._heartbeat_stop.set()
        if self._heartbeat_task is not None:
            # The publisher catches its own exceptions and exits cleanly
            # (see ``run_publisher``); broaden the suppress to ``Exception``
            # anyway so a publisher that died of an unforeseen error before
            # we got here can't break teardown.
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._heartbeat_task
            self._heartbeat_task = None
        if self._service is not None:
            await self._service.stop()
            self._service = None
        log.info("agent stopped on %s", self.subject.inbox)

    async def _on_status_request(self, request: Request) -> None:
        """Reply with a freshly-built §8.3 heartbeat payload (v0.3 §-TBD).

        Request body is ignored — there is no request schema yet; future
        PRs will extend the response with richer metadata and may add a
        request shape at that time. ``self._service`` is set in
        :meth:`start` before the endpoint is registered, so the
        ``RuntimeError`` guard below should be unreachable in normal
        operation; it survives ``python -O`` (which strips ``assert``)
        and gives a clearer signal than ``AttributeError`` from the
        following ``.id`` access if invariants ever drift.
        """
        if self._service is None:  # pragma: no cover — defensive
            raise RuntimeError("status handler invoked before start()")
        try:
            payload = build_heartbeat_payload(
                self.subject,
                self._heartbeat_interval_s,
                self._service.id,
            )
            data = payload.model_dump_json().encode("utf-8")
            await request.respond(data)
        except Exception as exc:
            # A respond() failure (broker dropped, request torn down, encode
            # error in a future richer payload) MUST surface as a §9.1 error
            # to the caller, not silently propagate into nats-py's framework
            # — mirroring _on_prompt_request's explicit error path.
            log.exception("status handler failed on %s", request.subject)
            with contextlib.suppress(Exception):
                await request.respond_error(
                    "500", _sanitize_error_desc(f"status handler error: {exc}")
                )

    async def _on_prompt_request(self, request: Request) -> None:
        keepalive_task: asyncio.Task[None] | None = None
        try:
            try:
                envelope = decode(request.data)
            except ProtocolError as exc:
                log.warning("rejecting malformed prompt on %s: %s", request.subject, exc)
                await request.respond_error("400", _sanitize_error_desc(str(exc)))
                return

            max_payload_bytes = parse_human_bytes(self._effective_max_payload_value)
            if len(request.data) > max_payload_bytes:
                log.warning(
                    "rejecting oversized prompt on %s: %d bytes exceeds %s",
                    request.subject,
                    len(request.data),
                    self._effective_max_payload_value,
                )
                await request.respond_error(
                    "400",
                    _sanitize_error_desc(
                        f"prompt payload exceeds max_payload {self._effective_max_payload_value}"
                    ),
                )
                return
            if envelope.attachments and not self._attachments_ok:
                log.warning(
                    "rejecting attachments on %s: endpoint advertised attachments_ok=false",
                    request.subject,
                )
                await request.respond_error(
                    "400",
                    _sanitize_error_desc("attachments are not supported by this endpoint"),
                )
                return

            # §6.4: emit the leading ack BEFORE any handler work so warm-up
            # latency stays inside the §6.6 budget and the stream is observable
            # to plain `nats req --wait-for-empty`. Best-effort, mirroring the
            # terminator path below — if respond() fails, log and continue;
            # the next send (handler chunk or terminator) will surface it.
            try:
                await request.respond(encode_chunk(StatusChunk(status="ack")))
            except Exception:
                log.exception("failed to emit leading ack on %s", request.subject)

            # nats.micro's Request has no public reply accessor — this is
            # the one place we reach into its Msg, at the framework
            # boundary, so PromptStream stays constructible from plain data.
            stream = PromptStream(
                request,
                self._nc,
                reply_subject=request._msg.reply,
                root_id=envelope.root_id,
            )
            handler = self._prompt_handler
            if handler is None:  # pragma: no cover — start() rejects this path
                raise RuntimeError("prompt handler invoked before on_prompt() registered one")

            if self._keepalive_interval_s is not None:
                keepalive_task = asyncio.create_task(
                    _keepalive_loop(request, self._keepalive_interval_s),
                    name=f"keepalive-{request.subject}",
                )

            try:
                # Bind the ambient trace context (PEP 567 contextvars) for
                # the handler's entire async call tree: Agent.prompt()
                # calls inside it join this thread's tree and auto-record
                # spawn edges without explicit plumbing. Inherited by
                # tasks the handler spawns; scoped strictly to this
                # request — concurrent handlers (TS today, Python after
                # the concurrency fix) each see their own binding.
                with bind_active_trace(stream._as_active_trace()):
                    await handler(envelope, stream)
            except ProtocolError as exc:
                log.warning(
                    "prompt handler rejected protocol input on %s: %s",
                    request.subject,
                    exc,
                )
                await _stop_keepalive(keepalive_task)
                keepalive_task = None
                await request.respond_error("400", _sanitize_error_desc(str(exc)))
            except Exception as exc:
                log.exception("prompt handler raised on %s", request.subject)
                # Stop keep-alive BEFORE the §9 error frame so the keepalive
                # task can't race an ack chunk in between error(500) and the
                # §6.5 terminator emitted in the outer `finally`. Ditto in
                # the success path right below.
                await _stop_keepalive(keepalive_task)
                keepalive_task = None
                await request.respond_error("500", _sanitize_error_desc(f"handler error: {exc}"))
            else:
                await _stop_keepalive(keepalive_task)
                keepalive_task = None
        finally:
            # Belt-and-braces: if anything above failed before we could stop
            # the keepalive (e.g. respond_error itself raised), don't leak the
            # task and don't let it slip an ack past the terminator. No-op
            # when keepalive_task is already None.
            await _stop_keepalive(keepalive_task)
            # §6.5 + §9.3: every stream — successful or errored — ends with a
            # zero-byte body message that carries NO NATS headers. The error
            # frame emitted by `respond_error` above is NOT the terminator;
            # this final `respond(b"")` is.
            try:
                await request.respond(b"")
            except Exception:
                log.exception("failed to emit stream terminator on %s", request.subject)


async def _stop_keepalive(task: asyncio.Task[None] | None) -> None:
    """Cancel and await a keep-alive task, swallowing the resulting CancelledError.

    No-op when ``task`` is ``None``, so the outer ``finally`` can call this
    unconditionally as a belt-and-braces guard after the success/error paths
    have already cleared their own task handles.
    """
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def _keepalive_loop(request: Request, interval_s: float) -> None:
    """Emit a `status="ack"` chunk every `interval_s` seconds until cancelled.

    Runs as a sibling task to the user's prompt handler; cancelled by
    :meth:`AgentService._on_prompt_request` once the handler returns or
    raises, so the ack stream never extends past the §6.5 terminator.
    """
    payload = encode_chunk(StatusChunk(status="ack"))
    while True:
        await asyncio.sleep(interval_s)
        try:
            await request.respond(payload)
        except Exception:
            # An emit failure (broker dropped, request reply already torn down,
            # etc.) is best-effort — log and stop. The terminator path will
            # fail loudly enough on its own if the request is truly dead.
            log.exception("keepalive emit failed on %s", request.subject)
            return


# NATS message headers are single-line (CR/LF delimited in the wire format),
# so any description passed to `respond_error` MUST be stripped of newlines
# or the server will truncate subsequent headers. 200 chars is plenty for
# §9.1 ("short human-readable description"); richer context belongs in the
# JSON body per §9.1.
_MAX_ERROR_DESC_LEN = 200


def _sanitize_error_desc(desc: str) -> str:
    flat = " | ".join(line.strip() for line in desc.splitlines() if line.strip())
    if len(flat) > _MAX_ERROR_DESC_LEN:
        # ASCII "..." (not U+2026) — some NATS header parsers choke on multi-
        # byte UTF-8 in header values.
        flat = flat[: _MAX_ERROR_DESC_LEN - 3] + "..."
    return flat


__all__ = [
    "DEFAULT_ATTACHMENTS_OK",
    "DEFAULT_KEEPALIVE_INTERVAL_S",
    "DEFAULT_MAX_PAYLOAD",
    "AgentService",
    "PromptHandler",
    "PromptStream",
]
