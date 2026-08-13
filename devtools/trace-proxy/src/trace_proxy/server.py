"""Dummy observing proxy for the Synadia agent trace headers.

Sits between an agent harness and its LLM provider: point the harness's
base URL (e.g. ``OLLAMA_URL``) at this proxy and it forwards everything
upstream while capturing the ``x-agent-*`` trace vocabulary. Implements
the trace plan's §1.5 discrimination rule, checked BEFORE the
header-stripping step:

    x-agent-event header present → consume: record the edge, respond
                                    locally, never forward upstream
    otherwise                    → record, strip x-agent-*, forward

Captured events stream to a live tree page (``GET /``) over SSE.
Everything is in-memory — this is a devtool for testing the trace
design's assumptions, not a production gateway.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import time
from collections.abc import AsyncIterator, Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

from aiohttp import ClientError, ClientSession, ClientTimeout, web

# Trace vocabulary — mirrors `synadia_ai.agents.trace`. Duplicated by
# design: the proxy observes the SDK from outside and must not import it.
HEADER_PREFIX = "x-agent-"
HEADER_THREAD_ID = "x-agent-thread-id"
HEADER_ROOT_ID = "x-agent-root-id"
HEADER_TOOL_CALL_ID = "x-agent-tool-call-id"
HEADER_SPAWNED = "x-agent-spawned"
HEADER_EVENT = "x-agent-event"

# Hop-by-hop headers (RFC 9110 §7.6.1) plus entity headers the proxy
# recomputes; never forwarded in either direction.
_SKIP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
    # The aiohttp client transparently decompresses; forwarding the
    # original content-encoding with decompressed bytes would corrupt.
    "content-encoding",
}

_EDGE_PARTS = 3  # <child>:<tool_call_id>:<edge_type>

# Credential-bearing headers are redacted in the traffic dump — the dump
# is for inspecting trace propagation, not for holding API keys on disk.
_REDACT_HEADERS = {"authorization", "proxy-authorization", "cookie", "x-api-key", "api-key"}

_INDEX_PATH = Path(__file__).parent / "index.html"


def parse_spawned(value: str) -> list[dict[str, str | None]]:
    """Parse ``child:tool:type[,child:tool:type…]`` edge claims.

    Fail-open: malformed entries are skipped, well-formed siblings kept.
    """
    edges: list[dict[str, str | None]] = []
    for raw in value.split(","):
        parts = raw.strip().split(":")
        if len(parts) != _EDGE_PARTS or not parts[0]:
            continue
        child, tool, edge_type = parts
        edges.append({"child": child, "tool_call_id": tool or None, "edge_type": edge_type})
    return edges


# Conversation extraction is best-effort display material: turns are
# clipped so the dashboard stays readable, and any parse failure just
# means "no contents shown", never a proxy error.
_TURN_TEXT_LIMIT = 600
_TURNS_LIMIT = 30


def _clip(text: str) -> str:
    text = text.strip()
    return text if len(text) <= _TURN_TEXT_LIMIT else text[:_TURN_TEXT_LIMIT] + " …"


def _content_text(content: Any) -> str:
    """Flatten a chat `content` field (string or typed parts) to text."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [p.get("text", "") for p in content if isinstance(p, dict)]
        return " ".join(p for p in parts if p)
    return ""


def _structured_tool_calls(calls: Any) -> list[dict[str, Any]] | None:
    """Normalize a tool_calls array to [{id, name, arguments}] — shared by
    the request-side and response-side extractors so anchoring ids match."""
    if not isinstance(calls, list):
        return None
    structured = []
    for call in calls:
        if not isinstance(call, dict):
            continue
        fn = call.get("function") or {}
        structured.append(
            {
                "id": str(call.get("id") or ""),
                "name": str(fn.get("name") or "?"),
                "arguments": _clip(str(fn.get("arguments") or "")),
            }
        )
    return structured or None


def conversation_from_request(body: bytes) -> list[dict[str, Any]] | None:
    """Best-effort turn list from a completion request body.

    Understands OpenAI-style ``messages`` (string or parts content) and
    Ollama's bare ``prompt``. Tool calls stay structured — id, name,
    arguments per call, and ``tool_call_id`` on result turns — so the
    dashboard can anchor spawned child threads at the exact tool-call
    box that spawned them. Each request carries the whole history, so
    the latest request IS the conversation so far.
    """
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("prompt"), str):  # Ollama /api/generate
        return [{"role": "user", "text": _clip(payload["prompt"])}]
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return None
    turns: list[dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        turn: dict[str, Any] = {
            "role": str(message.get("role", "?")),
            "text": _clip(_content_text(message.get("content"))),
        }
        structured = _structured_tool_calls(message.get("tool_calls"))
        if structured:
            turn["tool_calls"] = structured
        if message.get("tool_call_id"):  # a tool-result turn
            turn["tool_call_id"] = str(message["tool_call_id"])
        turns.append(turn)
    return turns[-_TURNS_LIMIT:] or None


def _parse_sse_response(text: str) -> tuple[str | None, list[dict[str, Any]] | None]:
    """Assemble (reply, tool calls) from an OpenAI SSE stream.

    Streamed tool calls arrive fragmented — id and name once, arguments
    split across deltas keyed by index — so accumulate per slot.
    """
    parts: list[str] = []
    slots: dict[int, dict[str, Any]] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line.startswith("data:"):
            continue
        data = line[len("data:") :].strip()
        if data in ("", "[DONE]"):
            continue
        try:
            delta = json.loads(data)["choices"][0]["delta"]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError):
            continue
        if not isinstance(delta, dict):
            continue
        token = delta.get("content")
        if token:
            parts.append(str(token))
        for fragment in delta.get("tool_calls") or []:
            if not isinstance(fragment, dict):
                continue
            slot = slots.setdefault(
                int(fragment.get("index") or 0), {"id": "", "name": "", "arguments": ""}
            )
            if fragment.get("id"):
                slot["id"] = str(fragment["id"])
            fn = fragment.get("function") or {}
            if fn.get("name"):
                slot["name"] = str(fn["name"])
            if fn.get("arguments"):
                slot["arguments"] += str(fn["arguments"])
    calls = [
        {"id": s["id"], "name": s["name"] or "?", "arguments": _clip(s["arguments"])}
        for _, s in sorted(slots.items())
    ]
    return _clip("".join(parts)) or None, calls or None


def parse_response(
    content_type: str, body: bytes
) -> tuple[str | None, list[dict[str, Any]] | None]:
    """Best-effort (assistant reply, structured tool calls) from a response.

    Handles OpenAI SSE streams, Ollama NDJSON streams, and plain JSON
    bodies from either provider shape. Tool calls are the interesting
    half: the response is where the model DECIDES to call a tool, one
    request earlier than the decision shows up in the conversation — so
    surfacing them here lets spawned children anchor immediately.
    """
    text = body.decode("utf-8", "replace")
    kind = content_type.lower()
    if "text/event-stream" in kind:
        return _parse_sse_response(text)
    if "ndjson" in kind:
        parts: list[str] = []
        calls: list[dict[str, Any]] | None = None
        for raw in text.splitlines():
            with contextlib.suppress(json.JSONDecodeError, AttributeError, TypeError):
                obj = json.loads(raw)
                message = obj.get("message") or {}
                token = obj.get("response") or message.get("content")
                if token:
                    parts.append(str(token))
                calls = calls or _structured_tool_calls(message.get("tool_calls"))
        return _clip("".join(parts)) or None, calls
    with contextlib.suppress(json.JSONDecodeError, KeyError, IndexError, TypeError):
        obj = json.loads(text)
        choices = obj.get("choices")
        if isinstance(choices, list) and choices:  # OpenAI non-streamed
            message = choices[0].get("message") or {}
            reply = _clip(_content_text(message.get("content"))) or None
            return reply, _structured_tool_calls(message.get("tool_calls"))
        message = obj.get("message")  # Ollama /api/chat non-streamed
        if isinstance(message, dict):
            reply = _clip(_content_text(message.get("content"))) or None
            return reply, _structured_tool_calls(message.get("tool_calls"))
        if isinstance(obj.get("response"), str):  # Ollama /api/generate
            return _clip(obj["response"]) or None, None
    return None, None


def _dump_headers(prefix: str, headers: Iterable[tuple[str, str]]) -> list[str]:
    lines = []
    for k, v in headers:
        value = f"<redacted, {len(v)} chars>" if k.lower() in _REDACT_HEADERS else v
        lines.append(f"{prefix} {k}: {value}")
    return lines


def _dump_body(prefix: str, body: bytes) -> list[str]:
    if not body:
        return []
    text = body.decode("utf-8", "replace")
    # Pretty-print JSON bodies so completion payloads read at a glance;
    # anything else (SSE/NDJSON streams, plain text) is kept verbatim.
    with contextlib.suppress(json.JSONDecodeError):
        text = json.dumps(json.loads(text), indent=2)
    return [prefix, *(f"{prefix} {line}" for line in text.splitlines())]


class TrafficDumper:
    """Human-readable dump of every proxied exchange, to a file and stdout.

    Entries are written whole, under a lock, once the exchange finishes —
    concurrent streams never interleave inside an entry.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()

    def format_entry(  # noqa: PLR0913 — one kwarg per part of the exchange
        self,
        *,
        seq: int,
        request: web.Request,
        request_body: bytes,
        note: str | None,
        status: int | str,
        response_headers: Iterable[tuple[str, str]],
        response_body: bytes,
    ) -> str:
        stamp = datetime.now().astimezone().isoformat(timespec="milliseconds")
        lines = [f"=== #{seq} {stamp} {request.method} {request.path_qs}"]
        if note:
            lines.append(f"=== {note}")
        lines += _dump_headers(">", request.headers.items())
        lines += _dump_body(">", request_body)
        lines.append(f"--- response {status}")
        lines += _dump_headers("<", response_headers)
        lines += _dump_body("<", response_body)
        lines.append(f"=== end #{seq}")
        return "\n".join(lines) + "\n\n"

    async def write(self, entry: str) -> None:
        async with self._lock:
            print(entry, end="", flush=True)
            with self._path.open("a", encoding="utf-8") as f:
                f.write(entry)


class ThreadStore:
    """The plan's accumulating forest, buffered server-side by thread id.

    Threads are upserted as claims arrive: identity from request/marker
    headers, parent edges from ``x-agent-spawned`` entries. Edge claims
    are idempotent — the spawn-time marker and the drained report
    describe the same edge, so the first claim wins. Every change
    broadcasts the full thread record; SSE consumers treat messages as
    upserts, so replay/live races are harmless.
    """

    def __init__(self) -> None:
        self._threads: dict[str, dict[str, Any]] = {}
        self._untraced = 0
        self._queues: set[asyncio.Queue[dict[str, Any]]] = set()

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._queues.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._queues.discard(queue)

    def _broadcast(self, msg: dict[str, Any]) -> None:
        for queue in self._queues:
            queue.put_nowait(msg)

    def _meta(self) -> dict[str, Any]:
        return {"type": "meta", "untraced": self._untraced}

    def snapshot(self) -> list[dict[str, Any]]:
        return [self._meta(), *(dict(t) for t in self._threads.values())]

    def _ensure(self, thread_id: str, root_id: str | None) -> dict[str, Any]:
        thread = self._threads.get(thread_id)
        if thread is None:
            now = time.time()
            thread = {
                "type": "thread",
                "thread_id": thread_id,
                "root_id": root_id or thread_id,
                "parent": None,
                "edge": None,
                "requests": 0,
                "markers": 0,
                "first_seen": now,
                "last_seen": now,
                "last_path": None,
                "last_status": None,
                "conversation": None,
                "reply": None,
            }
            self._threads[thread_id] = thread
        return thread

    def observe(  # noqa: PLR0913 — one kwarg per header-derived fact
        self,
        *,
        kind: str,
        thread_id: str | None,
        root_id: str | None,
        spawned: list[dict[str, str | None]],
        path: str,
        conversation: list[dict[str, Any]] | None = None,
    ) -> None:
        """Fold one proxied request (or consumed marker) into the forest."""
        if thread_id is None:
            self._untraced += 1
            self._broadcast(self._meta())
            return
        thread = self._ensure(thread_id, root_id)
        thread["last_seen"] = time.time()
        thread["last_path"] = path
        if kind == "marker":
            thread["markers"] += 1
        else:
            thread["requests"] += 1
            thread["last_status"] = None  # in flight until note_response()
        if conversation is not None:
            # Each model request carries the whole history — the latest
            # request is the fullest snapshot of this conversation.
            thread["conversation"] = conversation
        for edge in spawned:
            child_id = edge["child"]
            if not child_id:
                continue
            # A spawn claim: buffer the child immediately (it may not have
            # made a model request yet) and attach it under this thread.
            child = self._ensure(child_id, thread["root_id"])
            if child["parent"] is None:
                child["parent"] = thread_id
                child["edge"] = {
                    "tool_call_id": edge["tool_call_id"],
                    "edge_type": edge["edge_type"],
                    "via": kind,
                }
                self._broadcast(dict(child))
        self._broadcast(dict(thread))

    def note_response(
        self,
        thread_id: str | None,
        status: int,
        reply: str | None,
        tool_calls: list[dict[str, Any]] | None = None,
    ) -> None:
        if thread_id is None:
            return
        thread = self._threads.get(thread_id)
        if thread is None:
            return
        thread["last_status"] = status
        if reply is not None:
            thread["reply"] = reply
        if tool_calls:
            # The model just DECIDED these calls. Buffer the assistant turn
            # now so spawned children can anchor in their tool-call box
            # immediately — one request before the parent's follow-up
            # re-states the same turn (which then simply replaces this
            # snapshot; no duplication, no visual snap).
            convo = list(thread.get("conversation") or [])
            convo.append({"role": "assistant", "text": "", "tool_calls": tool_calls})
            thread["conversation"] = convo
        self._broadcast(dict(thread))


class TraceStore:
    """Append-only in-memory event log with SSE fan-out."""

    def __init__(self) -> None:
        self._events: list[dict[str, Any]] = []
        self._queues: set[asyncio.Queue[dict[str, Any]]] = set()
        self._seq = 0

    def record(self, event: dict[str, Any]) -> dict[str, Any]:
        self._seq += 1
        event["seq"] = self._seq
        event["ts"] = time.time()
        self._events.append(event)
        for queue in self._queues:
            queue.put_nowait(event)
        return event

    def snapshot(self) -> list[dict[str, Any]]:
        return list(self._events)

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._queues.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._queues.discard(queue)


async def _index(request: web.Request) -> web.Response:
    # Read per request so the page can be edited without restarting.
    return web.Response(text=_INDEX_PATH.read_text("utf-8"), content_type="text/html")


async def _events_json(request: web.Request) -> web.Response:
    store: TraceStore = request.app["store"]
    return web.json_response(store.snapshot())


async def _sse_write(resp: web.StreamResponse, event: dict[str, Any]) -> None:
    await resp.write(f"data: {json.dumps(event)}\n\n".encode())


async def _events_sse(request: web.Request) -> web.StreamResponse:
    """Replay the full log, then stream live events (SSE)."""
    store: TraceStore = request.app["store"]
    resp = web.StreamResponse(
        headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
    )
    await resp.prepare(request)

    # Subscribe BEFORE snapshotting so nothing falls between; dedupe by seq.
    queue = store.subscribe()
    try:
        last_seq = 0
        for event in store.snapshot():
            await _sse_write(resp, event)
            last_seq = event["seq"]
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=15)
            except TimeoutError:
                await resp.write(b": keep-alive\n\n")
                continue
            if event["seq"] <= last_seq:
                continue
            await _sse_write(resp, event)
    except (ConnectionResetError, ConnectionError):
        return resp
    finally:
        store.unsubscribe(queue)


async def _threads_json(request: web.Request) -> web.Response:
    threads: ThreadStore = request.app["threads"]
    return web.json_response(threads.snapshot())


async def _threads_sse(request: web.Request) -> web.StreamResponse:
    """Replay every thread record, then stream live upserts (SSE)."""
    threads: ThreadStore = request.app["threads"]
    resp = web.StreamResponse(
        headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
    )
    await resp.prepare(request)

    # Subscribe before snapshotting; thread messages are idempotent
    # upserts, so a record delivered by both paths is harmless.
    queue = threads.subscribe()
    try:
        for msg in threads.snapshot():
            await _sse_write(resp, msg)
        while True:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=15)
            except TimeoutError:
                await resp.write(b": keep-alive\n\n")
                continue
            await _sse_write(resp, msg)
    except (ConnectionResetError, ConnectionError):
        return resp
    finally:
        threads.unsubscribe(queue)


async def _proxy(request: web.Request) -> web.StreamResponse:
    store: TraceStore = request.app["store"]
    threads: ThreadStore = request.app["threads"]
    trace = {
        k.lower(): v for k, v in request.headers.items() if k.lower().startswith(HEADER_PREFIX)
    }
    base: dict[str, Any] = {
        "method": request.method,
        "path": request.path_qs,
        "thread_id": trace.get(HEADER_THREAD_ID),
        "root_id": trace.get(HEADER_ROOT_ID),
        "tool_call_id": trace.get(HEADER_TOOL_CALL_ID),
        "spawned": parse_spawned(trace.get(HEADER_SPAWNED, "")),
    }

    dumper: TrafficDumper = request.app["dumper"]
    body = await request.read()  # completion payloads are small; no need to stream up

    is_marker = HEADER_EVENT in trace
    threads.observe(
        kind="marker" if is_marker else "request",
        thread_id=base["thread_id"],
        root_id=base["root_id"],
        spawned=base["spawned"],
        path=request.path_qs,
        conversation=None if is_marker else conversation_from_request(body),
    )

    # §1.5 discrimination rule: a marker request is consumed — recorded,
    # answered locally, never forwarded (so it is never billed upstream).
    if HEADER_EVENT in trace:
        event = store.record({**base, "kind": "marker", "event": trace[HEADER_EVENT]})
        local_body = json.dumps({"ok": True, "consumed": trace[HEADER_EVENT]}).encode()
        await dumper.write(
            dumper.format_entry(
                seq=event["seq"],
                request=request,
                request_body=body,
                note=f"{trace[HEADER_EVENT]} marker — consumed by proxy, NOT forwarded upstream",
                status=200,
                response_headers=[("Content-Type", "application/json")],
                response_body=local_body,
            )
        )
        return web.json_response(body=local_body)

    event = store.record({**base, "kind": "request", "status": None})

    forward_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in _SKIP_HEADERS and not k.lower().startswith(HEADER_PREFIX)
    }
    return await _forward(
        request,
        event_seq=event["seq"],
        thread_id=base["thread_id"],
        body=body,
        forward_headers=forward_headers,
    )


async def _relay(
    request: web.Request, resp: web.StreamResponse, upstream_resp: Any
) -> tuple[bytes, str | None, bool]:
    """Relay the upstream body to the client, draining it fully even if
    the client goes away. Returns (captured body, note, cancelled)."""
    note: str | None = None
    response_body = bytearray()
    client_gone = False
    cancelled = False
    try:
        try:
            await resp.prepare(request)
        except OSError:
            client_gone = True
        async for chunk in upstream_resp.content.iter_chunked(8192):
            response_body.extend(chunk)
            if client_gone:
                continue  # keep draining upstream so the capture is complete
            try:
                await resp.write(chunk)
            except OSError:
                client_gone = True
        if not client_gone:
            with contextlib.suppress(OSError):
                await resp.write_eof()
    except (ClientError, OSError) as exc:
        note = f"upstream stream aborted mid-response: {exc}"
    except asyncio.CancelledError:
        # aiohttp cancels the handler when the peer disconnects hard;
        # the caller bookkeeps, then re-raises the cancellation.
        client_gone = True
        cancelled = True
    if client_gone and note is None:
        note = "agent disconnected before end of stream (upstream drained for capture)"
    return bytes(response_body), note, cancelled


async def _forward(
    request: web.Request,
    *,
    event_seq: int,
    thread_id: str | None,
    body: bytes,
    forward_headers: dict[str, str],
) -> web.StreamResponse:
    """Stream one request upstream and back, recording as we go.

    Failure domains are separate on purpose. Upstream unreachable → 502.
    But a DOWNSTREAM disconnect (the agent's SDK closes its socket the
    moment it has consumed the stream terminator, racing our last write)
    is not an upstream failure: keep the real status, drain the rest of
    the stream, and still record the reply — otherwise every race loses
    the conversation's final entry and paints the thread red.
    """
    store: TraceStore = request.app["store"]
    threads: ThreadStore = request.app["threads"]
    dumper: TrafficDumper = request.app["dumper"]
    client: ClientSession = request.app["client"]
    upstream: str = request.app["upstream"]
    try:
        upstream_ctx = client.request(
            request.method, upstream + request.path_qs, headers=forward_headers, data=body
        )
        upstream_resp = await upstream_ctx.__aenter__()
    except (ClientError, OSError) as exc:
        store.record({"kind": "status", "for_seq": event_seq, "status": 502, "error": str(exc)})
        threads.note_response(thread_id, 502, None, None)
        await dumper.write(
            dumper.format_entry(
                seq=event_seq,
                request=request,
                request_body=body,
                note=f"upstream request failed: {exc}",
                status=502,
                response_headers=[],
                response_body=b"",
            )
        )
        return web.json_response({"error": f"upstream request failed: {exc}"}, status=502)

    store.record({"kind": "status", "for_seq": event_seq, "status": upstream_resp.status})
    resp = web.StreamResponse(status=upstream_resp.status)
    for k, v in upstream_resp.headers.items():
        if k.lower() not in _SKIP_HEADERS:
            resp.headers[k] = v

    try:
        response_body, note, cancelled = await _relay(request, resp, upstream_resp)
    finally:
        await upstream_ctx.__aexit__(None, None, None)
    reply, tool_calls = parse_response(
        upstream_resp.headers.get("Content-Type", ""), bytes(response_body)
    )
    threads.note_response(thread_id, upstream_resp.status, reply, tool_calls)
    await dumper.write(
        dumper.format_entry(
            seq=event_seq,
            request=request,
            request_body=body,
            note=note,
            status=upstream_resp.status,
            response_headers=upstream_resp.headers.items(),
            response_body=bytes(response_body),
        )
    )
    if cancelled:
        raise asyncio.CancelledError
    return resp


async def _client_ctx(app: web.Application) -> AsyncIterator[None]:
    # total=None: streamed completions run arbitrarily long by design.
    app["client"] = ClientSession(timeout=ClientTimeout(total=None, connect=10))
    yield
    await app["client"].close()


def make_app(upstream: str, dump_path: Path) -> web.Application:
    app = web.Application()
    app["store"] = TraceStore()
    app["threads"] = ThreadStore()
    app["upstream"] = upstream.rstrip("/")
    app["dumper"] = TrafficDumper(dump_path)
    app.cleanup_ctx.append(_client_ctx)
    # Exact UI routes win over the catch-all proxy route. `/trace/...`
    # is the collision-safe spelling; `/` is served too for convenience
    # (no completion provider serves its API at the bare root).
    app.router.add_get("/", _index)
    app.router.add_get("/trace", _index)
    app.router.add_get("/trace/events", _events_sse)
    app.router.add_get("/trace/events.json", _events_json)
    app.router.add_get("/trace/threads", _threads_sse)
    app.router.add_get("/trace/threads.json", _threads_json)
    app.router.add_route("*", "/{tail:.*}", _proxy)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dummy observing proxy: captures x-agent-* headers, renders the live tree."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument(
        "--upstream",
        default="http://127.0.0.1:11434",
        help="base URL non-marker requests are forwarded to (default: local Ollama)",
    )
    parser.add_argument(
        "--dump",
        default="dump.txt",
        type=Path,
        help="file all proxied HTTP traffic is appended to, human-readable "
        "(also echoed to stdout; credential headers redacted)",
    )
    args = parser.parse_args()

    print(f"trace-proxy: http://{args.host}:{args.port} → {args.upstream}")
    print(f"live tree:   http://{args.host}:{args.port}/trace")
    print(f"traffic dump: {args.dump} (+ stdout)")
    with contextlib.suppress(KeyboardInterrupt):
        web.run_app(make_app(args.upstream, args.dump), host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
