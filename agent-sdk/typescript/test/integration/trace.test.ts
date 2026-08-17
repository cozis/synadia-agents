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

  it("both ends derive the same thread id with nothing exchanged", async () => {
    const recorded: string[] = [];
    await startService("identity", async (_envelope, response) => {
      recorded.push(response.threadId);
      await response.send("ok");
    });

    const agents = await client.discover({ filter: { agent: AGENT } });
    expect(agents).toHaveLength(1);
    const stream = await agents[0]!.prompt("root prompt");
    expect(isThreadId(stream.threadId)).toBe(true);
    for await (const _msg of stream) {
      /* drain */
    }
    expect(recorded).toHaveLength(1);
    expect(recorded[0]).toBe(stream.threadId);
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
