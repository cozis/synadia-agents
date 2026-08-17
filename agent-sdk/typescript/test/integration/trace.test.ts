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
import { Agents, deriveThreadId, isThreadId, toolScope } from "@synadia-ai/agents";
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
    const recorded: {
      threadId: string;
      rootId: string;
      isRoot: boolean;
      marker: Record<string, string>;
      headersFirst: Record<string, string>;
      headersSecond: Record<string, string>;
    }[] = [];
    await startService("identity", async (_envelope, response) => {
      const marker = response.recordSpawn("cafebabe00000000", {
        toolCallId: "toolu_test",
        edgeType: "tool_call",
      });
      recorded.push({
        threadId: response.threadId,
        rootId: response.rootId,
        isRoot: response.isRoot,
        marker,
        headersFirst: response.traceHeaders(),
        headersSecond: response.traceHeaders(), // spawn entry must have drained
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

    // --- header shapes: marker + completion-report drain semantics.
    for (const obs of [rootObs, childObs]) {
      const expectedTrace = `${obs.rootId}:${obs.threadId}`;
      expect(obs.marker["x-synadia-event"]).toBe("spawn");
      expect(obs.marker["x-synadia-trace"]).toBe(expectedTrace);
      expect(obs.marker["x-synadia-spawned"]).toBe("cafebabe00000000:toolu_test:tool_call");
      expect(obs.headersFirst["x-synadia-spawned"]).toBe(obs.marker["x-synadia-spawned"]);
      expect(obs.headersFirst["x-synadia-trace"]).toBe(expectedTrace);
      expect(obs.headersSecond["x-synadia-spawned"]).toBeUndefined();
    }
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

describe.skipIf(!natsUrl)("trace propagation — ambient spawns", () => {
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

  it("a parent handler spawns a child with zero explicit trace plumbing", async () => {
    const childObs: Record<string, unknown> = {};
    const parentObs: Record<string, unknown> = {};

    const childSvc = new AgentService({ nc, agent: AGENT, owner: OWNER, name: "amb-child" });
    childSvc.onPrompt(async (_envelope, response) => {
      childObs["threadId"] = response.threadId;
      childObs["rootId"] = response.rootId;
      childObs["isRoot"] = response.isRoot;
      await response.send("child ok");
    });
    await childSvc.start();
    services.push(childSvc);

    const parentSvc = new AgentService({ nc, agent: AGENT, owner: OWNER, name: "amb-parent" });
    parentSvc.onPrompt(async (envelope, response) => {
      if (envelope.prompt !== "do the thing") return;
      const found = await client.discover({ filter: { name: "amb-child" } });
      const child = found[0]!;

      // Scopeless ambient spawn — edge is honest about having no tool.
      const scopeless = await child.prompt("fire and observe");
      for await (const _m of scopeless) {
        /* drain */
      }
      const expectedProgrammatic = `${scopeless.threadId}::programmatic`;
      if (scopeless.spawnMarkerHeaders?.["x-synadia-spawned"] !== expectedProgrammatic) {
        throw new Error("scopeless spawn missing programmatic marker");
      }
      response.traceHeaders(); // drain the programmatic entry before the scoped one

      // No trace, no recordSpawn — everything ambient, labeled by toolScope.
      const stream = await toolScope("toolu_ambient", () => child.prompt("sub-task"));
      for await (const _m of stream) {
        /* drain */
      }
      parentObs["threadId"] = response.threadId;
      parentObs["rootId"] = response.rootId;
      parentObs["childHandleThread"] = stream.threadId;
      parentObs["marker"] = stream.spawnMarkerHeaders;
      parentObs["headersAfter"] = response.traceHeaders();
      await response.send("parent ok");
    });
    await parentSvc.start();
    services.push(parentSvc);

    const found = await client.discover({ filter: { name: "amb-parent" } });
    const rootStream = await found[0]!.prompt("do the thing");
    for await (const _m of rootStream) {
      /* drain */
    }

    // Tree: root prompt -> parent thread -> (ambient spawn) -> child thread.
    expect(parentObs["threadId"]).toBe(rootStream.threadId);
    expect(parentObs["rootId"]).toBe(rootStream.threadId);
    expect(childObs["rootId"]).toBe(rootStream.threadId); // forwarded implicitly
    expect(childObs["threadId"]).toBe(parentObs["childHandleThread"]);
    expect(childObs["isRoot"]).toBe(false);

    // Edge auto-recorded with the ambient tool id, on both channels.
    const expectedEdge = `${childObs["threadId"] as string}:toolu_ambient:tool_call`;
    const marker = parentObs["marker"] as Record<string, string>;
    expect(marker["x-synadia-event"]).toBe("spawn");
    expect(marker["x-synadia-spawned"]).toBe(expectedEdge);
    const headersAfter = parentObs["headersAfter"] as Record<string, string>;
    expect(headersAfter["x-synadia-spawned"]).toBe(expectedEdge);

    // Attribution names the SPAWNER: the marker rides the parent's provider
    // client, whose base-URL identity path is the parent's.
    const parentSvc2 = services.find((s) => s.subject.name === "amb-parent")!;
    expect(parentSvc2.identityPath).toBe(
      `synadia/${AGENT}/${OWNER}/amb-parent/${parentSvc2.instanceId}`,
    );
  });

  it("an ambient spawn from a task outliving its request: marker only", async () => {
    // The detached task inherits the ambient trace via AsyncLocalStorage;
    // once the request finishes the completion-report channel is closed, so
    // the late spawn delivers via the spawn-time marker (still joining the
    // parent's tree) and nothing accretes in the finished response.
    const captured: Record<string, unknown> = {};
    let release!: () => void;
    const released = new Promise<void>((resolve) => (release = resolve));
    let lateDone!: () => void;
    const lateFinished = new Promise<void>((resolve) => (lateDone = resolve));

    const childSvc = new AgentService({ nc, agent: AGENT, owner: OWNER, name: "late-child" });
    childSvc.onPrompt(async (_envelope, response) => {
      await response.send("child ok");
    });
    await childSvc.start();
    services.push(childSvc);

    const parentSvc = new AgentService({ nc, agent: AGENT, owner: OWNER, name: "late-parent" });
    parentSvc.onPrompt(async (_envelope, response) => {
      captured["response"] = response; // test-only: inspect post-completion state
      void (async (): Promise<void> => {
        await released; // deterministically after the request finished
        const found = await client.discover({ filter: { name: "late-child" } });
        const stream = await found[0]!.prompt("late sub-task");
        for await (const _m of stream) {
          /* drain */
        }
        captured["marker"] = stream.spawnMarkerHeaders;
        captured["lateRoot"] = stream.rootId;
        lateDone();
      })();
      await response.send("parent ok");
    });
    await parentSvc.start();
    services.push(parentSvc);

    const found = await client.discover({ filter: { name: "late-parent" } });
    const rootStream = await found[0]!.prompt("go");
    for await (const _m of rootStream) {
      /* drain */
    }
    // Terminator consumed ⇒ the service closed the ledger before emitting it.
    release();
    await lateFinished;

    const marker = captured["marker"] as Record<string, string>;
    // The marker channel still delivers the edge; the parent is the root.
    expect(marker["x-synadia-trace"]).toBe(`${rootStream.threadId}:${rootStream.threadId}`);
    expect(captured["lateRoot"]).toBe(rootStream.threadId); // still joins the tree
    const headersAfter = (captured["response"] as PromptResponse).traceHeaders();
    expect(headersAfter["x-synadia-spawned"]).toBeUndefined(); // no accretion
  });

  it("a handler forwarding its envelope verbatim keeps its spawn edge", async () => {
    const childObs: Record<string, unknown> = {};
    const parentObs: Record<string, unknown> = {};

    const childSvc = new AgentService({ nc, agent: AGENT, owner: OWNER, name: "fwd-child" });
    childSvc.onPrompt(async (_envelope, response) => {
      childObs["threadId"] = response.threadId;
      childObs["rootId"] = response.rootId;
      await response.send("child ok");
    });
    await childSvc.start();
    services.push(childSvc);

    const parentSvc = new AgentService({ nc, agent: AGENT, owner: OWNER, name: "fwd-parent" });
    parentSvc.onPrompt(async (envelope, response) => {
      const found = await client.discover({ filter: { name: "fwd-child" } });
      // Verbatim forward — rootId is SDK-stamped, naming this same tree.
      const stream = await toolScope("toolu_fwd", () => found[0]!.prompt(envelope));
      for await (const _m of stream) {
        /* drain */
      }
      parentObs["rootId"] = response.rootId;
      parentObs["childThread"] = stream.threadId;
      parentObs["childRoot"] = stream.rootId;
      parentObs["marker"] = stream.spawnMarkerHeaders;
      parentObs["headersAfter"] = response.traceHeaders();
      await response.send("parent ok");
    });
    await parentSvc.start();
    services.push(parentSvc);

    const found = await client.discover({ filter: { name: "fwd-parent" } });
    const rootStream = await found[0]!.prompt("delegate this verbatim");
    for await (const _m of rootStream) {
      /* drain */
    }

    // Same tree throughout: root prompt -> parent -> forwarded child.
    expect(parentObs["rootId"]).toBe(rootStream.threadId);
    expect(parentObs["childRoot"]).toBe(parentObs["rootId"]);
    expect(childObs["rootId"]).toBe(parentObs["rootId"]);
    expect(childObs["threadId"]).toBe(parentObs["childThread"]);

    // The edge survived the non-undefined envelope root, on both channels.
    const expectedEdge = `${childObs["threadId"] as string}:toolu_fwd:tool_call`;
    expect((parentObs["marker"] as Record<string, string>)["x-synadia-spawned"]).toBe(expectedEdge);
    expect((parentObs["headersAfter"] as Record<string, string>)["x-synadia-spawned"]).toBe(
      expectedEdge,
    );
  });
});
