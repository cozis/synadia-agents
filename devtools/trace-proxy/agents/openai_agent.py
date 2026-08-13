"""A real protocol agent backed by a cheap OpenAI model, for exercising trace-proxy.

Registers a spec-compliant agent (via the local agent-sdk checkout) whose
handler answers each prompt with an OpenAI chat completion, streamed token
by token through the official ``openai`` SDK. This mirrors exactly how a
real harness integrates the trace design: the SDK client is constructed
with ``base_url`` pointing at the proxy, and every request carries
``extra_headers=stream.trace_headers()``. (A spawning harness would fire
its spawn markers through the same client — ``client.models.list(
extra_headers=handle.spawn_marker_headers)`` — this agent doesn't spawn.)

Environment:

    OPENAI_API_KEY=sk-...                       # required
    OPENAI_MODEL=gpt-4o-mini                    # default; any cheap chat model
    OPENAI_BASE_URL=http://127.0.0.1:8100/v1    # default: the local trace-proxy

Full loop (three terminals; needs `uv sync --extra agents` here first):

    uv run trace-proxy --port 8100 --upstream https://api.openai.com
    OPENAI_API_KEY=sk-... uv run python agents/openai_agent.py --url nats://127.0.0.1:4222
    # then prompt it from client-sdk/python:
    uv run python examples/02-prompt-text.py "say hi" --url nats://127.0.0.1:4222

Watch http://127.0.0.1:8100/trace — each prompt appears as a thread whose
id matches what the client printed, with one /v1/chat/completions request
under it. No key handy? Point the proxy at the demo stub instead
(`--upstream http://127.0.0.1:8199`, run scripts/demo_traffic.py, any
OPENAI_API_KEY value) — the wire shape is identical.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import getpass
import os
import signal
import sys

import nats
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionMessageParam
from synadia_ai.agent_service import AgentService, PromptStream
from synadia_ai.agents import Envelope

DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_BASE_URL = "http://127.0.0.1:8100/v1"


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Protocol agent answering prompts with a cheap OpenAI model via trace-proxy."
    )
    parser.add_argument("--url", default=os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    parser.add_argument("--agent", default="openai", help="§2 agent token (default: openai)")
    parser.add_argument("--owner", default=os.environ.get("NATS_AGENT_OWNER") or getpass.getuser())
    parser.add_argument("--session-name", default="main")
    parser.add_argument("--heartbeat-interval", type=float, default=30.0)
    args = parser.parse_args()

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY is not set — get one at https://platform.openai.com", file=sys.stderr)
        sys.exit(1)
    model = os.environ.get("OPENAI_MODEL", DEFAULT_MODEL)
    base_url = os.environ.get("OPENAI_BASE_URL", DEFAULT_BASE_URL).rstrip("/")

    nc = await nats.connect(servers=args.url)
    # The harness's one LLM client, constructed against the proxy. All
    # trace metadata rides per-request extra_headers — no other plumbing.
    client = AsyncOpenAI(api_key=api_key, base_url=base_url)

    service = AgentService(
        agent=args.agent,
        owner=args.owner,
        session_name=args.session_name,
        nc=nc,
        description=f"trace-proxy demo agent — answers prompts with OpenAI '{model}'",
        heartbeat_interval_s=args.heartbeat_interval,
    )

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        print(
            f"prompt thread={stream.thread_id} root={stream.root_id} is_root={stream.is_root}",
            flush=True,
        )
        messages: list[ChatCompletionMessageParam] = [{"role": "user", "content": envelope.prompt}]
        # Every outbound model request carries this thread's trace headers;
        # the proxy records them, strips them, and forwards to OpenAI.
        completion = await client.chat.completions.create(
            model=model,
            messages=messages,
            stream=True,
            extra_headers=stream.trace_headers(),
        )
        async for chunk in completion:
            token = chunk.choices[0].delta.content if chunk.choices else None
            if token:
                await stream.send(token)

    service.on_prompt(handler)
    await service.start()
    print(f"openai agent listening on {service.subject.prompt}")
    print(f"model '{model}' via {base_url} (put trace-proxy here)")
    print("press Ctrl+C to stop")

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for _sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(_sig, stop.set)
    try:
        await stop.wait()
    finally:
        print("\nshutting down…")
        await service.stop()
        await client.close()
        await nc.close()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
