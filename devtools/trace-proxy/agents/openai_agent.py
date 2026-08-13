"""A real protocol agent backed by a cheap OpenAI model, for exercising trace-proxy.

Registers a spec-compliant agent (via the local agent-sdk checkout) whose
handler answers each prompt with an OpenAI chat completion, streamed token
by token through the official ``openai`` SDK. This mirrors exactly how a
real harness integrates the trace design: the SDK client is constructed
with ``base_url`` pointing at the proxy, and every request carries
``extra_headers=stream.trace_headers()``.

The model gets one tool, ``prompt_agent(agent, session_name, prompt)``,
which delegates to another agent on the NATS network — so this agent can
spawn REAL sub-agents when the model decides to. The tool loop is the
reference shape for the whole trace story:

- each tool execution runs inside ``tool_scope(call.id)``, so the spawn
  edge is labeled with the model's real tool-call id;
- the spawn inside the tool auto-records via the ambient trace context
  (zero explicit plumbing), and the spawn marker fires through the same
  OpenAI client — ``client.models.list(extra_headers=...)`` — which the
  proxy consumes without forwarding;
- the follow-up completion (returning the tool result to the model)
  drains the ``x-agent-spawned`` report on its headers.

One round of tool calls per prompt (plenty for the demo), then a final
streamed answer.

Environment:

    OPENAI_API_KEY=sk-...                       # required
    OPENAI_MODEL=gpt-4o-mini                    # default; any cheap chat model
    OPENAI_BASE_URL=http://127.0.0.1:8100/v1    # default: the local trace-proxy

Sub-agent loop (after `uv sync --extra agents`; add `--env-file .env`
to the agent commands if your key lives there):

    uv run trace-proxy --port 8100 --upstream https://api.openai.com
    uv run python agents/openai_agent.py --session-name coordinator --url nats://127.0.0.1:4222
    uv run python agents/openai_agent.py --session-name worker --url nats://127.0.0.1:4222
    # from client-sdk/python — a prompt that makes the model delegate:
    uv run python examples/02-prompt-text.py --session coordinator \
        "Please delegate to the agent 'openai' in session 'worker': \
    ask it for a haiku about NATS" --url nats://127.0.0.1:4222

Watch http://127.0.0.1:8100/trace: the coordinator's thread shows the
spawn marker and the drained edge report, with the worker's thread as a
child labeled by the real tool-call id. No key handy? Point the proxy at
the demo stub (`--upstream http://127.0.0.1:8199`, run
scripts/demo_traffic.py, any OPENAI_API_KEY value) — its fake model
requests the tool whenever the prompt mentions "delegate".
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import getpass
import json
import os
import signal
import sys

import nats
from openai import AsyncOpenAI
from openai.types.chat import (
    ChatCompletionMessageFunctionToolCall,
    ChatCompletionMessageParam,
    ChatCompletionToolParam,
)
from synadia_ai.agent_service import AgentService, PromptStream
from synadia_ai.agents import Agents, DiscoverFilter, Envelope, ResponseChunk, tool_scope

DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_BASE_URL = "http://127.0.0.1:8100/v1"

TOOLS: list[ChatCompletionToolParam] = [
    {
        "type": "function",
        "function": {
            "name": "prompt_agent",
            "description": (
                "Delegate a task to another agent on the NATS network and return its "
                "reply. Use this only when the user asks you to delegate, forward, or "
                "consult another agent."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "agent": {
                        "type": "string",
                        "description": "agent token to target, e.g. 'openai'",
                    },
                    "session_name": {
                        "type": "string",
                        "description": "session to target (5th subject token); omit for any",
                    },
                    "prompt": {"type": "string", "description": "the task for the other agent"},
                },
                "required": ["agent", "prompt"],
            },
        },
    },
]


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
    # Caller-side SDK handle for the prompt_agent tool's delegations.
    agents_api = Agents(nc=nc)

    service = AgentService(
        agent=args.agent,
        owner=args.owner,
        session_name=args.session_name,
        nc=nc,
        description=f"trace-proxy demo agent — answers prompts with OpenAI '{model}'",
        heartbeat_interval_s=args.heartbeat_interval,
    )

    async def run_prompt_agent(arguments: str) -> str:
        """The prompt_agent tool: discover the target and forward the prompt.

        Runs inside the caller's ``tool_scope``, so the spawn below joins
        this thread's tree with the model's tool-call id as edge label —
        all via the ambient trace context, no explicit wiring.
        """
        try:
            parsed = json.loads(arguments)
        except json.JSONDecodeError:
            return "error: tool arguments were not valid JSON"
        target, prompt = parsed.get("agent"), parsed.get("prompt")
        session = parsed.get("session_name")
        if not isinstance(target, str) or not isinstance(prompt, str):
            return "error: 'agent' and 'prompt' are required strings"

        filt = DiscoverFilter(
            agent=target,
            owner=args.owner,
            session_name=session if isinstance(session, str) else None,
        )
        found = await agents_api.discover(filter=filt)
        # Never delegate to ourselves — same agent token + session would
        # recurse (this process answering its own prompt forever).
        candidates = [
            a for a in found if not (a.agent == args.agent and a.session_name == args.session_name)
        ]
        if not candidates:
            return f"error: no agent found (agent={target!r}, session={session!r})"
        worker = candidates[0]

        handle = worker.prompt(prompt)
        # Spawn-time marker, fired through the harness's own OpenAI client
        # (fire-and-forget telemetry): the proxy consumes it; without a
        # proxy it degrades to a harmless real GET /v1/models.
        if handle.spawn_marker_headers is not None:
            with contextlib.suppress(Exception):
                await client.models.list(extra_headers=handle.spawn_marker_headers)

        parts: list[str] = []
        async for msg in handle:
            if isinstance(msg, ResponseChunk):
                parts.append(msg.text)
        return "".join(parts) or "(the delegated agent returned no text)"

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        print(
            f"prompt thread={stream.thread_id} root={stream.root_id} is_root={stream.is_root}",
            flush=True,
        )
        messages: list[ChatCompletionMessageParam] = [{"role": "user", "content": envelope.prompt}]

        # Round 1 — non-streamed with tools, so tool_calls come back whole.
        first = await client.chat.completions.create(
            model=model,
            messages=messages,
            tools=TOOLS,
            extra_headers=stream.trace_headers(),
        )
        decision = first.choices[0].message
        tool_calls = decision.tool_calls or []
        if not tool_calls:
            if decision.content:
                await stream.send(decision.content)
            return

        messages.append(
            {
                "role": "assistant",
                "content": decision.content,
                "tool_calls": [
                    {
                        "id": c.id,
                        "type": "function",
                        "function": {
                            "name": c.function.name,
                            "arguments": c.function.arguments,
                        },
                    }
                    for c in tool_calls
                    if isinstance(c, ChatCompletionMessageFunctionToolCall)
                ],
            }
        )
        for call in tool_calls:
            fn = call.function if isinstance(call, ChatCompletionMessageFunctionToolCall) else None
            if fn is None or fn.name != "prompt_agent":
                result = f"error: unknown tool {getattr(fn, 'name', call.type)!r}"
            else:
                # The harness contract: each tool execution runs inside
                # tool_scope(call.id) so spawns inside it get labeled with
                # the model's REAL tool-call id.
                print(f"tool {call.id}: prompt_agent({fn.arguments})", flush=True)
                with tool_scope(call.id):
                    result = await run_prompt_agent(fn.arguments)
            messages.append({"role": "tool", "tool_call_id": call.id, "content": result})

        # Final round — streamed. Its headers also drain the x-agent-spawned
        # report for the edges recorded above (the redundant edge channel).
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
        await agents_api.close()
        await client.close()
        await nc.close()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
