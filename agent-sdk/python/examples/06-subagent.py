# 06 · Sub-agent delegation — an agent that prompts another agent, traced.
#
# New rung: 05-tools gave the agent a microservice-backed tool; here the
# capability is another AGENT. The coordinator forwards each prompt to a worker
# agent and streams the worker's answer back — a minimal multi-agent tree. The
# worker shares this process only to keep the demo self-contained.
#
# Observability: the service running each prompt mints a prompt_id and
# announces it on the trace subject (default afo.threads). The
# coordinator's handler runs with its trace bound as ambient context, so the
# nested worker.prompt(...) forwards parent/root automatically; tool_call_id=
# labels the edge with the model tool call being served. Watch the tree form:
#
#   nats sub afo.threads
#   uv run python examples/06-subagent.py --url nats://127.0.0.1:4222
#   nats req agents.prompt.coordinator.<owner>.main "hello" --replies=0 --reply-timeout=10s
#
# Connection: --context / --url, else $NATS_CONTEXT / $NATS_URL, else the
# selected `nats` context. Run with -h for the full flag list.

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from synadia_ai.agents import Agents, DiscoverFilter, Envelope, ResponseChunk

from examples._connect_cli import (
    add_agent_identity_flags,
    add_connection_flags,
    connect_from_cli,
)
from synadia_ai.agent_service import AgentService, PromptStream


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Coordinator agent — delegates every prompt to a worker agent."
    )
    add_connection_flags(parser)
    add_agent_identity_flags(parser, agent="coordinator")
    args = parser.parse_args()

    nc = await connect_from_cli(args)

    # The worker: a stand-in for any other agent on the network.
    worker = AgentService(
        agent="worker",
        owner=args.owner,
        session_name="worker",
        nc=nc,
        description="worker agent — shouts the prompt back",
        heartbeat_interval_s=args.heartbeat_interval,
    )

    async def worker_handler(envelope: Envelope, stream: PromptStream) -> None:
        # A real worker would pass stream.trace_headers() to its model client.
        print(f"  worker    prompt={stream.prompt_id} parent={stream.trace.parent_prompt_id}")
        await stream.send(envelope.prompt.upper())

    worker.on_prompt(worker_handler)

    # One Agents client for the coordinator's outbound prompts.
    agents = Agents(nc=nc)
    await agents.start_tracking()

    coordinator = AgentService(
        agent="coordinator",
        owner=args.owner,
        session_name=args.session_name,
        nc=nc,
        description="coordinator agent — delegates every prompt to the worker",
        heartbeat_interval_s=args.heartbeat_interval,
    )

    async def coordinator_handler(envelope: Envelope, stream: PromptStream) -> None:
        print(f"coordinator prompt={stream.prompt_id} root={stream.root_id}")
        # Demo simplicity: re-discover the worker on every prompt. Production
        # handlers discover once and cache the Agent handle.
        found = await agents.discover(
            filter=DiscoverFilter(agent="worker", owner=args.owner), timeout=1.0
        )
        if not found:
            raise RuntimeError("worker agent not found — did its registration fail?")

        # Spawn the worker as a child execution — zero plumbing: the ambient
        # trace forwards parent/root; tool_call_id labels the edge as the tool
        # invocation it serves (omit it for a programmatic spawn).
        async for msg in found[0].prompt(envelope.prompt, tool_call_id="delegate-1"):
            if isinstance(msg, ResponseChunk):
                await stream.send(f"worker says: {msg.text}")

    coordinator.on_prompt(coordinator_handler)

    await worker.start()
    await coordinator.start()
    print(f"coordinator listening on {coordinator.subject.prompt}")
    print(f"worker listening on {worker.subject.prompt}")
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
        await worker.stop()
        await agents.close()
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
