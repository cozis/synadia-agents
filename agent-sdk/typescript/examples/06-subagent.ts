// 06 · Sub-agent delegation — an agent that prompts another agent, with trace edges.
//
// New rung on the ladder: 05-tools gave the agent a microservice-backed tool;
// here the capability is another AGENT. The coordinator forwards each prompt
// to a worker agent and streams the worker's answer back — a minimal
// multi-agent tree. The worker shares this process only to keep the demo
// self-contained.
//
// Observability: every prompt execution (a "thread") has a derived id, and
// the delegation is a parent→child edge an observing proxy reconstructs into
// an activity tree. The SDK binds the trace context around the handler, so
// prompting the worker inside toolScope(...) joins the tree and records the
// labeled edge with zero explicit plumbing. (The explicit equivalent —
// prompt(text, { trace: response.childTrace() }) + response.recordSpawn(...) —
// remains available outside a handler's context.)
//
// Try it — run this file, then prompt the coordinator (name "main"; the
// worker serves name "worker") from client-sdk/typescript/examples:
//
//   npx tsx examples/06-subagent.ts
//   npx tsx examples/02-prompt-text.ts "hello"
//
// Connection resolution: $NATS_CONTEXT > $NATS_URL > nats://127.0.0.1:4222.

import { connect as natsConnect } from "@nats-io/transport-node";
import { Agents, loadContextOptions, parseNatsUrl, toolScope } from "@synadia-ai/agents";
import { AgentService } from "@synadia-ai/agent-service";

async function main(): Promise<void> {
  const opts = process.env["NATS_CONTEXT"]
    ? await loadContextOptions(process.env["NATS_CONTEXT"])
    : process.env["NATS_URL"]
      ? parseNatsUrl(process.env["NATS_URL"])
      : { servers: "nats://127.0.0.1:4222" };
  const nc = await natsConnect(opts);

  const owner =
    process.env["SYNADIA_COORDINATOR_OWNER"] ??
    process.env["SYNADIA_OWNER"] ??
    process.env["NATS_AGENT_OWNER"] ??
    process.env["USER"] ??
    "anon";

  // The worker: a stand-in for any other agent on the network. It shares
  // this process (and connection) only to keep the demo self-contained.
  const workerSvc = new AgentService({
    nc,
    agent: "worker",
    owner,
    name: "worker",
    description: "worker agent — shouts the prompt back",
  });
  workerSvc.onPrompt(async (envelope, response) => {
    await response.send(envelope.prompt.toUpperCase());
  });

  // One Agents client for the coordinator's outbound prompts.
  const agents = new Agents({ nc });
  await agents.startTracking();

  const coordinator = new AgentService({
    nc,
    agent: "coordinator",
    owner,
    name: process.env["SYNADIA_NAME"] ?? process.env["NATS_AGENT_NAME"] ?? "main",
    description: "coordinator agent — delegates every prompt to the worker",
  });

  coordinator.onPrompt(async (envelope, response) => {
    // Demo simplicity: re-discover the worker on every prompt (no startup
    // ordering, survives worker restarts). Production handlers should
    // discover once and cache the Agent handle.
    const found = await agents.discover({ filter: { agent: "worker", owner } });
    if (found.length === 0) {
      throw new Error("worker agent not found — did its registration fail?");
    }
    const worker = found[0]!;

    // Spawn the worker as a child thread — zero plumbing: the ambient trace
    // bound around this handler forwards the tree root and records the
    // parent→child edge; toolScope() labels the edge as the tool invocation
    // it serves (without it: `programmatic`).
    const stream = await toolScope("delegate-1", () => worker.prompt(envelope.prompt));
    console.log(`delegating to worker thread ${stream.threadId}`);
    console.log(`  spawn marker: ${JSON.stringify(stream.spawnMarkerHeaders)}`);

    for await (const msg of stream) {
      if (msg.type === "response") {
        await response.send(`worker says: ${msg.text}`);
      }
    }
  });

  await workerSvc.start();
  await coordinator.start();
  console.log(`coordinator listening on ${coordinator.subject.prompt}`);
  console.log(`worker listening on ${workerSvc.subject.prompt}`);
  console.log("press Ctrl+C to stop");

  const shutdown = async (): Promise<void> => {
    console.log("\nshutting down…");
    await coordinator.stop();
    await workerSvc.stop();
    await agents.close();
    await nc.close();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown());
  process.on("SIGTERM", () => void shutdown());
}

void main().catch((err: unknown) => {
  console.error("subagent demo failed:", err);
  process.exit(1);
});
