"""Offline demo for trace-proxy: stub provider + simulated agent tree.

Starts a stub completion provider (Ollama- and OpenAI-shaped paths) on
``--listen-port``, then plays a simulated three-agent tree through the
proxy every ``--interval`` seconds:

    root ──(tool_call toolu_…)──> worker ──(programmatic)──> sub-worker

Header shapes match the SDK's trace vocabulary exactly, including the
two edge channels (spawn-time marker + drained ``x-agent-spawned`` on
the parent's next request). Run the proxy pointed at the stub first:

    uv run trace-proxy --port 8100 --upstream http://127.0.0.1:8199
    uv run python scripts/demo_traffic.py

The stub records every request it receives; ``GET /_seen`` on the stub
lists them with any x-agent-* headers that leaked through — the list
must show zero markers and zero x-agent-* headers, which is exactly
the §1.5 contract (markers consumed, trace headers stripped).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import re
import secrets
from http import HTTPStatus
from typing import Any

from aiohttp import ClientError, ClientSession, web

_seen: list[dict[str, Any]] = []


def _record(request: web.Request) -> None:
    _seen.append(
        {
            "method": request.method,
            "path": request.path_qs,
            "x_agent_headers": [k for k in request.headers if k.lower().startswith("x-agent-")],
        }
    )


async def _chat(request: web.Request) -> web.Response:
    _record(request)
    return web.json_response({"message": {"role": "assistant", "content": "stub reply"}})


async def _generate(request: web.Request) -> web.StreamResponse:
    _record(request)
    try:
        payload: dict[str, Any] = await request.json()
    except json.JSONDecodeError:
        payload = {}
    # Test knobs: `stub_chunks` / `stub_delay_s` in the request body turn
    # the reply into a slow N-chunk stream — used to exercise proxy
    # behaviour when a client disconnects mid-stream.
    chunks = int(payload.get("stub_chunks") or 0)
    delay = float(payload.get("stub_delay_s") or 0)
    resp = web.StreamResponse(headers={"Content-Type": "application/x-ndjson"})
    await resp.prepare(request)
    if chunks:
        for i in range(chunks - 1):
            await resp.write(f'{{"response": "tok{i} "}}\n'.encode())
            if delay:
                await asyncio.sleep(delay)
        await resp.write(b'{"response": "end", "done": true}\n')
    else:
        await resp.write(b'{"response": "stub "}\n')
        await resp.write(b'{"response": "reply", "done": true}\n')
    await resp.write_eof()
    return resp


async def _completions(request: web.Request) -> web.StreamResponse:
    """Fake chat-completions model, streamed and non-streamed.

    Just enough model behaviour to exercise the openai_agent tool loop
    offline: when tools are offered, no tool result is present yet, and
    the user prompt mentions "delegate", the fake model answers with a
    prompt_agent tool call (targeting agent 'openai', session 'worker');
    otherwise it replies "stub reply".
    """
    _record(request)
    try:
        body: dict[str, Any] = await request.json()
    except json.JSONDecodeError:
        body = {}
    messages: list[dict[str, Any]] = body.get("messages", [])
    user_text = " ".join(
        str(m.get("content", "")) for m in messages if m.get("role") == "user"
    ).lower()
    wants_tool = (
        bool(body.get("tools"))
        and "delegate" in user_text
        and not any(m.get("role") == "tool" for m in messages)
    )

    if not body.get("stream"):
        if wants_tool:
            # Fan out to every `agent '<name>'` the prompt mentions (with
            # an optional `in session '<name>'`); fall back to the classic
            # single openai/worker target so older demo commands keep
            # working. One tool call per target → parallel branches.
            targets = re.findall(r"agent '([a-z0-9-]+)'(?: in session '([a-z0-9-]+)')?", user_text)
            if not targets:
                targets = [("openai", "worker")]
            message: dict[str, Any] = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": f"call_stub{i:04d}",
                        "type": "function",
                        "function": {
                            "name": "prompt_agent",
                            "arguments": json.dumps(
                                {
                                    "agent": agent,
                                    **({"session_name": session} if session else {}),
                                    "prompt": "say hello",
                                }
                            ),
                        },
                    }
                    for i, (agent, session) in enumerate(targets, start=1)
                ],
            }
        else:
            message = {"role": "assistant", "content": "stub reply"}
        return web.json_response(
            {
                "id": "chatcmpl-stub",
                "object": "chat.completion",
                "created": 0,
                "model": "stub",
                "choices": [
                    {
                        "index": 0,
                        "message": message,
                        "finish_reason": "tool_calls" if wants_tool else "stop",
                    }
                ],
            }
        )

    # Full chunk shape so strict clients (the official openai SDK) parse it.
    chunk = (
        '{"id":"chatcmpl-stub","object":"chat.completion.chunk","created":0,"model":"stub",'
        '"choices":[{"index":0,"delta":{"content":"stub reply"},"finish_reason":null}]}'
    )
    resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
    await resp.prepare(request)
    await resp.write(f"data: {chunk}\n\n".encode())
    await resp.write(b"data: [DONE]\n\n")
    await resp.write_eof()
    return resp


async def _models(request: web.Request) -> web.Response:
    _record(request)
    return web.json_response({"data": []})


async def _seen_endpoint(request: web.Request) -> web.Response:
    return web.json_response(_seen)


def make_stub() -> web.Application:
    app = web.Application()
    app.router.add_post("/api/chat", _chat)
    app.router.add_post("/api/generate", _generate)
    app.router.add_post("/v1/chat/completions", _completions)
    app.router.add_get("/v1/models", _models)
    app.router.add_get("/_seen", _seen_endpoint)
    return app


def _tid() -> str:
    return secrets.token_hex(8)  # same shape as derive_thread_id output


async def _model_request(
    client: ClientSession,
    proxy: str,
    *,
    thread: str,
    root: str,
    tool: str | None = None,
    spawned: str | None = None,
) -> None:
    headers = {"x-agent-thread-id": thread, "x-agent-root-id": root}
    if tool is not None:
        headers["x-agent-tool-call-id"] = tool
    if spawned is not None:
        headers["x-agent-spawned"] = spawned
    body = {"model": "stub", "messages": [{"role": "user", "content": "hi"}]}
    async with client.post(f"{proxy}/api/chat", json=body, headers=headers) as resp:
        await resp.read()


async def _marker(client: ClientSession, proxy: str, *, thread: str, root: str, entry: str) -> None:
    headers = {
        "x-agent-event": "spawn",
        "x-agent-thread-id": thread,
        "x-agent-root-id": root,
        "x-agent-spawned": entry,
    }
    async with client.get(f"{proxy}/v1/models", headers=headers) as resp:
        await resp.read()


async def _wait_for_proxy(client: ClientSession, proxy: str) -> None:
    """Poll until the proxy answers, so terminal start order doesn't matter."""
    for _ in range(40):
        with contextlib.suppress(ClientError, OSError):
            async with client.get(f"{proxy}/trace/events.json") as resp:
                if resp.status == HTTPStatus.OK:
                    return
        await asyncio.sleep(0.25)
    raise RuntimeError(f"proxy not reachable at {proxy} after 10s — is trace-proxy running?")


async def play_round(client: ClientSession, proxy: str) -> None:
    """One simulated tree: root → (tool) worker → (programmatic) sub-worker."""
    root, worker, sub = _tid(), _tid(), _tid()
    tool = f"toolu_{secrets.token_hex(4)}"

    # Root thread: first model request, then it spawns the worker.
    await _model_request(client, proxy, thread=root, root=root)
    await _marker(client, proxy, thread=root, root=root, entry=f"{worker}:{tool}:tool_call")

    # Worker thread runs; mid-way it spawns a sub-worker with no tool scope.
    await _model_request(client, proxy, thread=worker, root=root)
    await _marker(client, proxy, thread=worker, root=root, entry=f"{sub}::programmatic")
    await _model_request(client, proxy, thread=sub, root=root)
    await _model_request(client, proxy, thread=worker, root=root, spawned=f"{sub}::programmatic")

    # Root's next request serves the tool result and drains its edge report.
    await _model_request(
        client, proxy, thread=root, root=root, tool=tool, spawned=f"{worker}:{tool}:tool_call"
    )


async def run(proxy: str, listen_port: int, interval: float, rounds: int) -> None:
    runner = web.AppRunner(make_stub())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", listen_port)
    await site.start()
    print(f"stub provider on http://127.0.0.1:{listen_port} (introspection: GET /_seen)")
    print(f"playing a simulated tree through {proxy} every {interval}s")

    played = 0
    try:
        async with ClientSession() as client:
            await _wait_for_proxy(client, proxy)
            while rounds == 0 or played < rounds:
                await play_round(client, proxy)
                played += 1
                print(f"round {played}: tree played ({len(_seen)} requests reached the stub)")
                if rounds != 0 and played >= rounds:
                    break
                await asyncio.sleep(interval)
    finally:
        # Self-verify the §1.5 contract from the provider's point of view:
        # nothing that reached the stub may carry x-agent-* headers, and no
        # marker (GET /v1/models with x-agent-event) may leak through.
        leaks = [s for s in _seen if s["x_agent_headers"]]
        markers = [s for s in _seen if s["path"].startswith("/v1/models")]
        print(
            f"stub saw {len(_seen)} request(s); "
            f"x-agent header leaks: {len(leaks)}; leaked markers: {len(markers)}"
        )
        if leaks or markers:
            print("CONTRACT VIOLATION — the proxy forwarded what it should have consumed/stripped")
        await runner.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--proxy", default="http://127.0.0.1:8100")
    parser.add_argument("--listen-port", type=int, default=8199, help="stub provider port")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--rounds", type=int, default=0, help="0 = play forever")
    args = parser.parse_args()
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run(args.proxy, args.listen_port, args.interval, args.rounds))


if __name__ == "__main__":
    main()
