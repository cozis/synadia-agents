// E2E for trace propagation: derived thread ids.
//
// Against a real nats-server: registers an agent whose handler records its
// PromptResponse trace identity, prompts it through the client SDK, and
// asserts the two ends independently derive the same thread id — the core
// zero-wire-surface property of the design. Mirrors the Python SDK's
// test_trace_e2e.py.

import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, inject, it } from "vitest";
import { connect as natsConnect } from "@nats-io/transport-node";
import type { NatsConnection } from "@nats-io/nats-core";
import { Agents, deriveThreadId, isThreadId } from "@synadia-ai/agents";
import { AgentService, type PromptResponse } from "../../src/service.js";

const natsUrl = inject("natsUrl");

const AGENT = "trace-test";
const OWNER = "testers";

describe.skipIf(!natsUrl)("trace propagation — derived thread ids", () => {
  let nc: NatsConnection;
  let client: Agents;
  const services: AgentService[] = [];

  beforeAll(async () => {
    nc = await natsConnect({ servers: natsUrl! });
  });

  afterAll(async () => {
    await nc.close();
  });

  beforeEach(async () => {
    client = new Agents({ nc });
    await client.startTracking();
  });

  afterEach(async () => {
    await client.close();
    await Promise.all(services.splice(0).map((s) => s.stop()));
  });

  async function startService(
    name: string,
    handler: (envelope: { prompt: string }, response: PromptResponse) => Promise<void> | void,
  ): Promise<AgentService> {
    const service = new AgentService({
      nc,
      agent: AGENT,
      owner: OWNER,
      name,
      heartbeatIntervalS: 30,
    });
    service.onPrompt(handler);
    await service.start();
    services.push(service);
    return service;
  }

  it("both ends derive the same thread id, and root forwarding works", async () => {
    const recorded: { threadId: string; rootId: string; isRoot: boolean }[] = [];
    await startService("identity", async (_envelope, response) => {
      recorded.push({
        threadId: response.threadId,
        rootId: response.rootId,
        isRoot: response.isRoot,
      });
      await response.send("ok");
    });

    const agents = await client.discover({ filter: { agent: AGENT } });
    expect(agents).toHaveLength(1);
    const agent = agents[0]!;

    // --- root prompt (no trace) — a traceless prompt roots its own tree.
    const stream = await agent.prompt("root prompt");
    expect(isThreadId(stream.threadId)).toBe(true);
    for await (const _msg of stream) {
      /* drain */
    }
    const rootObs = recorded.at(-1)!;
    expect(rootObs.threadId).toBe(stream.threadId);
    expect(rootObs.rootId).toBe(stream.threadId);
    expect(stream.rootId).toBe(stream.threadId);
    expect(rootObs.isRoot).toBe(true);

    // --- spawned prompt (forwarded trace) — joins the parent's tree.
    const child = await agent.prompt("spawned prompt", { trace: { rootId: rootObs.rootId } });
    for await (const _msg of child) {
      /* drain */
    }
    const childObs = recorded.at(-1)!;
    expect(childObs.threadId).toBe(child.threadId);
    expect(childObs.threadId).not.toBe(rootObs.threadId);
    expect(childObs.rootId).toBe(rootObs.rootId);
    expect(child.rootId).toBe(rootObs.rootId);
    expect(childObs.isRoot).toBe(false);
  });

  it("rejects a hostile root_id at decode with a 400, before the handler runs", async () => {
    const handled: string[] = [];
    const service = await startService("hostile", (envelope) => {
      handled.push(envelope.prompt);
    });

    const inbox = `_INBOX.trace-hostile.${Math.random().toString(36).slice(2)}`;
    const sub = nc.subscribe(inbox);
    const payload = JSON.stringify({ prompt: "hi", root_id: "x\r\nx-evil: 1" });
    nc.publish(service.subject.prompt, new TextEncoder().encode(payload), { reply: inbox });

    for await (const msg of sub) {
      expect(msg.headers?.get("Nats-Service-Error-Code")).toBe("400");
      break;
    }
    sub.unsubscribe();
    expect(handled).toHaveLength(0);
  });

  it("reply-less prompts get distinct random thread ids", async () => {
    const seen: string[] = [];
    const service = await startService("fireforget", (_envelope, response) => {
      seen.push(response.threadId);
    });

    for (let i = 0; i < 2; i++) {
      nc.publish(service.subject.prompt, new TextEncoder().encode('{"prompt": "x"}')); // no reply
    }
    await nc.flush();
    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(seen).toHaveLength(2);
    expect(seen[0]).not.toBe(seen[1]);
    for (const tid of seen) {
      expect(isThreadId(tid)).toBe(true);
      expect(tid).not.toBe(deriveThreadId("")); // not the constant phantom
    }
  });
});
