# 06 · Sub-agent delegation — an agent that prompts another agent, with trace edges.
#
# New rung on the ladder: 05-tools gave the agent a microservice-backed tool;
# here the capability is another AGENT. The coordinator forwards each prompt
# to a worker agent and streams the worker's answer back — a minimal
# multi-agent tree. The worker runs in this same process only to keep the
# demo self-contained; in production it is any protocol-compliant agent,
# anywhere on the network.
#
# The observability story: every prompt execution (a "thread") has a derived
# id, and the coordinator's delegation is a parent→child edge an observing
# proxy reconstructs into an activity tree. The SDK binds this thread's
# trace context around the handler, so the delegation needs ZERO explicit
# plumbing — prompting the worker from inside the handler automatically
# joins the tree and records the spawn edge. tool_scope(...) labels the
# edge with the tool invocation it serves:
#
#   with tool_scope("delegate-1"):
#       handle = worker.prompt(text)   # joins the tree, edge auto-recorded
#
# (The explicit equivalent — worker.prompt(text, trace=stream.child_trace())
# plus stream.record_spawn(handle.thread_id) — remains available for spawns
# outside a handler's context.)
#
# Try it — run this file, then prompt the coordinator from
# client-sdk/python/examples (the worker serves session "worker", so
# `--session main` selects the coordinator):
#
#   uv run python examples/06-subagent.py --url nats://127.0.0.1:4222
#   uv run python examples/02-prompt-text.py "hello" --session main \
#       --url nats://127.0.0.1:4222
#
# The thread/root ids the client prints line up with what this process logs.
#
# Identity/heartbeat via --owner/--session-name/--heartbeat-interval or the
# matching NATS_AGENT_* env vars. Connection: --context/--url, else
# $NATS_CONTEXT/$NATS_URL, else the selected `nats` context. Run -h for flags.

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from synadia_ai.agents import Agents, DiscoverFilter, Envelope, ResponseChunk, tool_scope

from examples._connect_cli import (
    add_agent_identity_flags,
    add_connection_flags,
    connect_from_cli,
)
from synadia_ai.agent_service import AgentService, PromptStream

# The worker's fixed session name. Distinct from the coordinator's (default
# "main") so a client can select either agent by `--session`.
WORKER_SESSION = "worker"


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Coordinator agent that delegates each prompt to a worker agent."
    )
    add_connection_flags(parser)
    add_agent_identity_flags(parser, agent="coordinator")
    args = parser.parse_args()

    nc = await connect_from_cli(args)

    # The worker: a stand-in for any other agent on the network. It shares
    # this process (and connection) only to keep the demo self-contained.
    worker_svc = AgentService(
        agent="worker",
        owner=args.owner,
        session_name=WORKER_SESSION,
        nc=nc,
        description="worker agent — shouts the prompt back",
        heartbeat_interval_s=args.heartbeat_interval,
    )

    async def worker_handler(envelope: Envelope, stream: PromptStream) -> None:
        await stream.send(envelope.prompt.upper())

    worker_svc.on_prompt(worker_handler)

    # One Agents client for the coordinator's outbound prompts. In a real
    # harness this lives wherever the harness keeps its NATS state.
    agents = Agents(nc=nc)

    coordinator = AgentService(
        agent="coordinator",
        owner=args.owner,
        session_name=args.session_name,
        nc=nc,
        description="coordinator agent — delegates every prompt to the worker",
        heartbeat_interval_s=args.heartbeat_interval,
    )

    async def handler(envelope: Envelope, stream: PromptStream) -> None:
        found = await agents.discover(filter=DiscoverFilter(agent="worker", owner=args.owner))
        if not found:
            raise ValueError("worker agent not found — did its registration fail?")
        worker = found[0]

        # Spawn the worker as a child thread of this one — zero plumbing:
        # the SDK bound this thread's trace context around the handler, so
        # prompt() forwards the tree root and records the parent→child edge
        # automatically. tool_scope() labels the edge as the tool invocation
        # it serves; without it the edge is honestly `programmatic`. The
        # handle carries the spawn-marker headers (fired through the
        # harness's LLM client as fire-and-forget telemetry); the same edge
        # also drains into this thread's next trace_headers() call.
        with tool_scope("delegate-1"):
            handle = worker.prompt(envelope.prompt)
        print(f"delegating to worker thread {handle.thread_id}")
        print(f"  spawn marker: {handle.spawn_marker_headers}")

        async for msg in handle:
            if isinstance(msg, ResponseChunk):
                await stream.send(f"worker says: {msg.text}")

    coordinator.on_prompt(handler)

    await worker_svc.start()
    await coordinator.start()
    print(f"coordinator listening on {coordinator.subject.prompt}")
    print(f"worker listening on {worker_svc.subject.prompt}")
    print("press Ctrl+C to stop")

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for _sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(_sig, stop.set)
    try:
        await stop.wait()
    finally:
        print("\nshutting down…")
        await coordinator.stop()
        await worker_svc.stop()
        await agents.close()
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
