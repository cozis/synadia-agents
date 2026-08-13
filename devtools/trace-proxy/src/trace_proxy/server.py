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

    dumper: TrafficDumper = request.app["dumper"]
    body = await request.read()  # completion payloads are small; no need to stream up

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
            response_body = bytearray()
            async for chunk in upstream_resp.content.iter_chunked(8192):
                response_body.extend(chunk)
                await resp.write(chunk)
            await resp.write_eof()
            await dumper.write(
                dumper.format_entry(
                    seq=event["seq"],
                    request=request,
                    request_body=body,
                    note=None,
                    status=upstream_resp.status,
                    response_headers=upstream_resp.headers.items(),
                    response_body=bytes(response_body),
                )
            )
            return resp
    except (ClientError, OSError) as exc:
        store.record({"kind": "status", "for_seq": event["seq"], "status": 502, "error": str(exc)})
        await dumper.write(
            dumper.format_entry(
                seq=event["seq"],
                request=request,
                request_body=body,
                note=f"upstream request failed: {exc}",
                status=502,
                response_headers=[],
                response_body=b"",
            )
        )
        return web.json_response({"error": f"upstream request failed: {exc}"}, status=502)


async def _client_ctx(app: web.Application) -> AsyncIterator[None]:
    # total=None: streamed completions run arbitrarily long by design.
    app["client"] = ClientSession(timeout=ClientTimeout(total=None, connect=10))
    yield
    await app["client"].close()


def make_app(upstream: str, dump_path: Path) -> web.Application:
    app = web.Application()
    app["store"] = TraceStore()
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
