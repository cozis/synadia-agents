import { describe, expect, test } from "bun:test";
import {
  activeTrace,
  type Agent,
  type Agents,
  type StreamMessage,
  type TraceScope,
} from "@synadia-ai/agents";
import { AsyncPromptManager } from "../src/agent-tools.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function fakeAgent(messages: () => AsyncIterable<StreamMessage>): Agent {
  return {
    instanceId: "instance-1",
    agent: "test",
    owner: "owner",
    name: "name",
    description: "test agent",
    version: "1.0.0",
    protocolVersion: "0.3",
    promptSubject: "agents.prompt.test.owner.name",
    promptEndpoint: {
      subject: "agents.prompt.test.owner.name",
      attachmentsOk: true,
    },
    idSigVerified: true,
    prompt: async () => messages(),
  } as unknown as Agent;
}

function clientFor(agent: Agent): Pick<Agents, "lookupInstance"> {
  return {
    lookupInstance: async (id: string) =>
      id === agent.instanceId ? agent : undefined,
  } as Pick<Agents, "lookupInstance">;
}

describe("AsyncPromptManager", () => {
  test("returns pending, polls, then collects a reply", async () => {
    const release = deferred<void>();
    const agent = fakeAgent(async function* () {
      yield { type: "status", status: "ack" };
      await release.promise;
      yield { type: "response", text: "hello" };
    });
    const manager = new AsyncPromptManager();
    const started = await manager.promptAgent(clientFor(agent), {
      instance_id: agent.instanceId,
      prompt: "hello",
    });
    const polled = await manager.waitForReply({
      prompt_ids: [started.prompt_id],
      timeout_ms: 0,
    });
    expect(polled).toEqual({ timed_out: true });

    const timed = await manager.waitForReply({
      prompt_ids: [started.prompt_id],
      timeout_ms: 5,
    });
    expect(timed).toEqual({ timed_out: true });

    release.resolve();
    const completed = await manager.waitForReply({
      prompt_ids: [started.prompt_id],
      timeout_ms: 100,
    });
    expect(completed.timed_out).toBe(false);
    expect(completed).toMatchObject({
      prompt_id: started.prompt_id,
      state: "completed",
      response: "hello",
    });
  });

  test("returns when the first of multiple handles settles", async () => {
    const releases = [deferred<void>(), deferred<void>()];
    let call = 0;
    const agent = fakeAgent(() => {
      const current = call++;
      return (async function* () {
        await releases[current]!.promise;
        yield { type: "response", text: String(current + 1) };
      })();
    });
    const manager = new AsyncPromptManager();
    const first = await manager.promptAgent(clientFor(agent), {
      instance_id: agent.instanceId,
      prompt: "first",
    });
    const second = await manager.promptAgent(clientFor(agent), {
      instance_id: agent.instanceId,
      prompt: "second",
    });
    setTimeout(() => releases[0]!.resolve(), 5);
    const result = await manager.waitForReply({
      prompt_ids: [first.prompt_id, second.prompt_id],
      timeout_ms: 100,
    });
    expect(result.timed_out).toBe(false);
    expect(result).toMatchObject({
      prompt_id: first.prompt_id,
      state: "completed",
      response: "1",
    });
    expect(result).not.toHaveProperty("prompts");

    releases[1]!.resolve();
    const remaining = await manager.waitForReply({
      prompt_ids: [second.prompt_id],
      timeout_ms: 100,
    });
    expect(remaining).toMatchObject({
      prompt_id: second.prompt_id,
      state: "completed",
      response: "2",
    });
  });

  test("returns background errors and rejects unknown handles", async () => {
    const agent = fakeAgent(async function* () {
      throw new Error("remote failed");
    });
    const manager = new AsyncPromptManager();
    const started = await manager.promptAgent(clientFor(agent), {
      instance_id: agent.instanceId,
      prompt: "fail",
    });
    const result = await manager.waitForReply({
      prompt_ids: [started.prompt_id],
      timeout_ms: 100,
    });
    expect(result).toMatchObject({
      prompt_id: started.prompt_id,
      state: "error",
      error: "remote failed",
    });
    await expect(
      manager.waitForReply({ prompt_ids: ["missing"], timeout_ms: 0 }),
    ).rejects.toThrow('pending prompt "missing" was not found');
  });

  test("notifies once when a prompt settles without an active waiter", async () => {
    const release = deferred<void>();
    const notified = deferred<void>();
    const events: Array<{ prompt_id: string; state: string }> = [];
    const agent = fakeAgent(async function* () {
      await release.promise;
      yield { type: "response", text: "done" };
    });
    const manager = new AsyncPromptManager();
    const started = await manager.promptAgent(
      clientFor(agent),
      { instance_id: agent.instanceId, prompt: "background" },
      {
        onSettled: (event) => {
          events.push(event);
          notified.resolve();
        },
      },
    );

    release.resolve();
    await notified.promise;
    expect(events).toEqual([
      { prompt_id: started.prompt_id, state: "completed" },
    ]);
  });

  test("an active wait consumes completion without a duplicate notification", async () => {
    const release = deferred<void>();
    const events: Array<{ prompt_id: string; state: string }> = [];
    const agent = fakeAgent(async function* () {
      await release.promise;
      yield { type: "response", text: "done" };
    });
    const manager = new AsyncPromptManager();
    const started = await manager.promptAgent(
      clientFor(agent),
      { instance_id: agent.instanceId, prompt: "wait" },
      { onSettled: (event) => events.push(event) },
    );
    const waiting = manager.waitForReply({
      prompt_ids: [started.prompt_id],
      timeout_ms: 100,
    });

    release.resolve();
    expect(await waiting).toMatchObject({
      timed_out: false,
      prompt_id: started.prompt_id,
    });
    await Promise.resolve();
    expect(events).toEqual([]);
  });

  test("keeps the initiating trace active in background collection", async () => {
    const seen: Array<TraceScope | undefined> = [];
    const scope: TraceScope = {
      threadId: "a".repeat(32),
      rootId: "b".repeat(32),
      turnCountHint: 1,
    };
    const agent = fakeAgent(async function* () {
      seen.push(activeTrace());
      yield { type: "response", text: "ok" };
    });
    const originalPrompt = agent.prompt.bind(agent);
    Object.defineProperty(agent, "prompt", {
      value: async (...args: Parameters<Agent["prompt"]>) => {
        seen.push(activeTrace());
        return originalPrompt(...args);
      },
    });
    const manager = new AsyncPromptManager();
    const started = await manager.promptAgent(
      clientFor(agent),
      { instance_id: agent.instanceId, prompt: "trace" },
      { traceScope: scope },
    );
    await manager.waitForReply({ prompt_ids: [started.prompt_id], timeout_ms: 100 });
    expect(seen).toEqual([scope, scope]);
  });
});
