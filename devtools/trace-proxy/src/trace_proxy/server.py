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
from collections.abc import AsyncIterator
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


async def _proxy(request: web.Request) -> web.StreamResponse:
    store: TraceStore = request.app["store"]
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

    # §1.5 discrimination rule: a marker request is consumed — recorded,
    # answered locally, never forwarded (so it is never billed upstream).
    if HEADER_EVENT in trace:
        store.record({**base, "kind": "marker", "event": trace[HEADER_EVENT]})
        return web.json_response({"ok": True, "consumed": trace[HEADER_EVENT]})

    event = store.record({**base, "kind": "request", "status": None})

    forward_headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in _SKIP_HEADERS and not k.lower().startswith(HEADER_PREFIX)
    }
    body = await request.read()  # completion payloads are small; no need to stream up
    client: ClientSession = request.app["client"]
    upstream: str = request.app["upstream"]
    try:
        async with client.request(
            request.method, upstream + request.path_qs, headers=forward_headers, data=body
        ) as upstream_resp:
            store.record(
                {"kind": "status", "for_seq": event["seq"], "status": upstream_resp.status}
            )
            resp = web.StreamResponse(status=upstream_resp.status)
            for k, v in upstream_resp.headers.items():
                if k.lower() not in _SKIP_HEADERS:
                    resp.headers[k] = v
            await resp.prepare(request)
            async for chunk in upstream_resp.content.iter_chunked(8192):
                await resp.write(chunk)
            await resp.write_eof()
            return resp
    except (ClientError, OSError) as exc:
        store.record({"kind": "status", "for_seq": event["seq"], "status": 502, "error": str(exc)})
        return web.json_response({"error": f"upstream request failed: {exc}"}, status=502)


async def _client_ctx(app: web.Application) -> AsyncIterator[None]:
    # total=None: streamed completions run arbitrarily long by design.
    app["client"] = ClientSession(timeout=ClientTimeout(total=None, connect=10))
    yield
    await app["client"].close()


def make_app(upstream: str) -> web.Application:
    app = web.Application()
    app["store"] = TraceStore()
    app["upstream"] = upstream.rstrip("/")
    app.cleanup_ctx.append(_client_ctx)
    # Exact UI routes win over the catch-all proxy route. `/trace/...`
    # is the collision-safe spelling; `/` is served too for convenience
    # (no completion provider serves its API at the bare root).
    app.router.add_get("/", _index)
    app.router.add_get("/trace", _index)
    app.router.add_get("/trace/events", _events_sse)
    app.router.add_get("/trace/events.json", _events_json)
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
    args = parser.parse_args()

    print(f"trace-proxy: http://{args.host}:{args.port} → {args.upstream}")
    print(f"live tree:   http://{args.host}:{args.port}/trace")
    with contextlib.suppress(KeyboardInterrupt):
        web.run_app(make_app(args.upstream), host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
