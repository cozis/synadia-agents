// prompt() × the ambient trace: root precedence + implicit spawn recording.
// Broker-less — the publish is lazy, so a stub NatsConnection suffices.

import { describe, expect, it } from "vitest";
import type { NatsConnection } from "@nats-io/nats-core";
import {
  Agent,
  bindActiveTrace,
  currentToolCallId,
  formatSpawnEntry,
  toolScope,
  type AgentInfo,
  type EndpointInfo,
} from "../../src/index.js";

function makeAgent(): Agent {
  const promptEndpoint: EndpointInfo = {
    name: "prompt",
    subject: "agents.prompt.test.testers.s",
    queueGroup: "agents",
    metadata: {},
    attachmentsOk: true,
  };
  const info: AgentInfo = {
    instanceId: "test-instance",
    agent: "test",
    owner: "testers",
    name: "s",
    protocolVersion: "0.3",
    description: "",
    version: "0.0.0",
    metadata: { agent: "test", owner: "testers" },
    endpoints: [promptEndpoint],
    promptEndpoint,
  };
  return new Agent({} as NatsConnection, info, 60_000);
}

describe("prompt() × ambient trace", () => {
  it("joins the ambient tree and auto-records the spawn edge", async () => {
    const recorded: { child: string; tool: string | undefined }[] = [];
    const recordSpawn = (child: string): Record<string, string> => {
      // Runs synchronously in the spawner's context — sees the tool scope.
      recorded.push({ child, tool: currentToolCallId() });
      return { "x-synadia-event": "spawn", "x-synadia-spawned": formatSpawnEntry(child) };
    };

    const stream = await bindActiveTrace(
      { threadId: "a".repeat(16), rootId: "b".repeat(16), recordSpawn },
      () => toolScope("toolu_x", () => makeAgent().prompt("hi")),
    );

    expect(stream.rootId).toBe("b".repeat(16));
    expect(recorded).toEqual([{ child: stream.threadId, tool: "toolu_x" }]);
    expect(stream.spawnMarkerHeaders?.["x-synadia-event"]).toBe("spawn");
    expect(stream.spawnMarkerHeaders?.["x-synadia-spawned"]).toBe(
      `${stream.threadId}:toolu_x:tool_call`,
    );
  });

  it("explicit trace wins and disables the auto-record", async () => {
    const recorded: string[] = [];
    const recordSpawn = (child: string): Record<string, string> => {
      recorded.push(child);
      return {};
    };

    const stream = await bindActiveTrace(
      { threadId: "a".repeat(16), rootId: "c".repeat(16), recordSpawn },
      () => makeAgent().prompt("hi", { trace: { rootId: "d".repeat(16) } }),
    );

    expect(stream.rootId).toBe("d".repeat(16));
    expect(recorded).toEqual([]);
    expect(stream.spawnMarkerHeaders).toBeUndefined();
  });

  it("a forwarded envelope naming the ambient tree keeps the auto-record", async () => {
    const recorded: string[] = [];
    const recordSpawn = (child: string): Record<string, string> => {
      recorded.push(child);
      return { "x-synadia-event": "spawn" };
    };

    const stream = await bindActiveTrace(
      { threadId: "a".repeat(16), rootId: "b".repeat(16), recordSpawn },
      () => makeAgent().prompt({ prompt: "hi", rootId: "b".repeat(16) }),
    );

    expect(stream.rootId).toBe("b".repeat(16));
    expect(recorded).toEqual([stream.threadId]);
    expect(stream.spawnMarkerHeaders).toBeDefined();
  });

  it("an envelope root naming a foreign tree disables the auto-record", async () => {
    const recorded: string[] = [];
    const recordSpawn = (child: string): Record<string, string> => {
      recorded.push(child);
      return {};
    };

    const stream = await bindActiveTrace(
      { threadId: "a".repeat(16), rootId: "b".repeat(16), recordSpawn },
      () => makeAgent().prompt({ prompt: "hi", rootId: "f".repeat(16) }),
    );

    expect(stream.rootId).toBe("f".repeat(16));
    expect(recorded).toEqual([]);
    expect(stream.spawnMarkerHeaders).toBeUndefined();
  });

  it("a prompt without any context roots itself", async () => {
    const stream = await makeAgent().prompt("hi");
    expect(stream.rootId).toBe(stream.threadId);
    expect(stream.spawnMarkerHeaders).toBeUndefined();
  });
});
