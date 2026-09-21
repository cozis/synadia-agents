import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  StreamMaxWaitExceededError,
  activeTrace,
  type Agent,
  type Agents,
  type StreamMessage,
  type TraceScope,
} from "@synadia-ai/agents";
import {
  AsyncPromptManager,
  PromptToolError,
} from "../extensions/agent-tools.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function fakeAgent(
  messages: () => AsyncIterable<StreamMessage>,
  onPrompt?: (text: string, options: any) => void,
): Agent {
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
    prompt: async (text: string, options: any) => {
      onPrompt?.(text, options);
      return messages();
    },
  } as unknown as Agent;
}

function clientFor(agent: Agent): Pick<Agents, "discover"> {
  return {
    discover: async () => [agent],
  } as Pick<Agents, "discover">;
}

async function start(
  manager: AsyncPromptManager,
  agent: Agent,
  label = "Test prompt",
) {
  await manager.discoverAgents(clientFor(agent));
  return manager.promptAgent({
    prompt_endpoint: agent.promptSubject,
    label,
    text: "hello",
  });
}

describe("AsyncPromptManager", () => {
  test("uses short handles, lists pending work, and keeps results readable", async () => {
    const release = deferred<void>();
    const agent = fakeAgent(async function* () {
      yield { type: "status", status: "ack" };
      await release.promise;
      yield { type: "response", text: "hello" };
    });
    const manager = new AsyncPromptManager();

    const started = await start(manager, agent);
    expect(started).toMatchObject({
      prompt_id: "p1",
      label: "Test prompt",
      state: "pending",
      prompt_endpoint: agent.promptSubject,
    });
    expect(manager.listPendingPrompts()).toEqual([started]);
    expect(
      await manager.waitForPrompt({
        prompt_ids: [started.prompt_id],
        timeout_ms: 0,
      }),
    ).toEqual({ type: "timeout", pending_prompt_ids: ["p1"] });

    release.resolve();
    const completed = await manager.waitForPrompt({
      prompt_ids: [started.prompt_id],
      timeout_ms: 100,
    });
    expect(completed).toMatchObject({
      type: "prompt_result",
      prompt_id: "p1",
      label: "Test prompt",
      state: "completed",
      response_text: "hello",
      remaining_prompt_ids: [],
    });
    expect(manager.listPendingPrompts()).toEqual([]);
    expect(
      await manager.waitForPrompt({ prompt_ids: ["p1"], timeout_ms: 0 }),
    ).toEqual(completed);
  });

  test("wait_for_prompt returns only the first terminal result", async () => {
    const releases = [deferred<void>(), deferred<void>()];
    let call = 0;
    const agent = fakeAgent(() => {
      const current = call++;
      return (async function* () {
        yield { type: "status", status: "ack" };
        await releases[current]!.promise;
        yield { type: "response", text: String(current + 1) };
      })();
    });
    const manager = new AsyncPromptManager();
    const a = await start(manager, agent, "first");
    const b = await start(manager, agent, "second");

    const waiting = manager.waitForPrompt({
      prompt_ids: [a.prompt_id, b.prompt_id],
      timeout_ms: 100,
    });
    releases[1]!.resolve();
    expect(await waiting).toMatchObject({
      prompt_id: "p2",
      response_text: "2",
      remaining_prompt_ids: ["p1"],
    });

    releases[0]!.resolve();
    expect(
      await manager.waitForPrompt({ prompt_ids: ["p1"], timeout_ms: 100 }),
    ).toMatchObject({ prompt_id: "p1", response_text: "1" });
  });

  test("stores failures as terminal results and reports unknown handles", async () => {
    const agent = fakeAgent(async function* () {
      yield { type: "status", status: "ack" };
      throw new Error("remote failed");
    });
    const manager = new AsyncPromptManager();
    const started = await start(manager, agent);
    expect(
      await manager.waitForPrompt({
        prompt_ids: [started.prompt_id],
        timeout_ms: 100,
      }),
    ).toMatchObject({
      prompt_id: "p1",
      state: "failed",
      error: { code: "remote_error", message: "remote failed" },
    });
    try {
      await manager.waitForPrompt({ prompt_ids: ["missing"], timeout_ms: 0 });
      throw new Error("expected an error");
    } catch (error) {
      expect(error).toBeInstanceOf(PromptToolError);
      expect((error as PromptToolError).code).toBe("prompt_not_found");
    }
  });

  test("does not create a handle before acceptance and records deadline expiry", async () => {
    const rejected = fakeAgent(async function* () {
      throw new Error("acceptance failed");
    });
    const manager = new AsyncPromptManager();
    await expect(start(manager, rejected)).rejects.toThrow("acceptance failed");
    expect(manager.listPendingPrompts()).toEqual([]);

    const expired = fakeAgent(async function* () {
      yield { type: "status", status: "ack" };
      throw new StreamMaxWaitExceededError(10);
    });
    const started = await start(manager, expired);
    expect(
      await manager.waitForPrompt({
        prompt_ids: [started.prompt_id],
        timeout_ms: 100,
      }),
    ).toMatchObject({
      state: "expired",
      error: { code: "deadline_exceeded" },
    });
  });

  test("resolves the exact prompt endpoint and denies interactive queries", async () => {
    let queryReply = "";
    let discoveryOptions: unknown;
    const agent = fakeAgent(async function* () {
      yield { type: "status", status: "ack" };
      yield {
        type: "query",
        id: "q1",
        prompt: "May I continue?",
        reply: async (answer: string) => {
          queryReply = answer;
        },
      };
    });
    const client = {
      discover: async (options: unknown) => {
        discoveryOptions = options;
        return [agent];
      },
    } as Pick<Agents, "discover">;
    const manager = new AsyncPromptManager();
    await manager.discoverAgents(client);
    const started = await manager.promptAgent({
      prompt_endpoint: agent.promptSubject,
      label: "query",
      text: "ask",
    });
    await manager.waitForPrompt({
      prompt_ids: [started.prompt_id],
      timeout_ms: 100,
    });

    expect(discoveryOptions).toEqual({});
    expect(queryReply).toBe(
      "This caller cannot answer interactive queries; deny or continue without approval.",
    );
    await expect(
      new AsyncPromptManager().promptAgent({
        prompt_endpoint: agent.promptSubject,
        label: "missing",
        text: "ask",
      }),
    ).rejects.toMatchObject({ code: "agent_not_found" });
  });

  test("notifies only when no active waiter receives the completion", async () => {
    const release = deferred<void>();
    const notified = deferred<void>();
    const events: unknown[] = [];
    const agent = fakeAgent(async function* () {
      yield { type: "status", status: "ack" };
      await release.promise;
    });
    const manager = new AsyncPromptManager();
    await manager.discoverAgents(clientFor(agent));
    const background = await manager.promptAgent(
      { prompt_endpoint: agent.promptSubject, label: "background", text: "go" },
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
      {
        event: "agent_prompt_finished",
        prompt_id: background.prompt_id,
        state: "completed",
      },
    ]);

    const waitedRelease = deferred<void>();
    const waitedEvents: unknown[] = [];
    const waitedAgent = fakeAgent(async function* () {
      yield { type: "status", status: "ack" };
      await waitedRelease.promise;
    });
    await manager.discoverAgents(clientFor(waitedAgent));
    const waited = await manager.promptAgent(
      {
        prompt_endpoint: waitedAgent.promptSubject,
        label: "waited",
        text: "go",
      },
      {
        onSettled: (event) => {
          waitedEvents.push(event);
        },
      },
    );
    const waiting = manager.waitForPrompt({
      prompt_ids: [waited.prompt_id],
      timeout_ms: 100,
    });
    waitedRelease.resolve();
    await waiting;
    await Promise.resolve();
    expect(waitedEvents).toEqual([]);
  });

  test("cancels prompts, evicts terminal results, and rejects an all-pending limit", async () => {
    const releases = [deferred<void>(), deferred<void>(), deferred<void>()];
    let call = 0;
    const agent = fakeAgent(() => {
      const current = call++;
      return (async function* () {
        yield { type: "status", status: "ack" };
        await releases[current]!.promise;
      })();
    });
    const manager = new AsyncPromptManager({ maxTrackedPrompts: 1 });
    const first = await start(manager, agent, "first");
    releases[0]!.resolve();
    await manager.waitForPrompt({
      prompt_ids: [first.prompt_id],
      timeout_ms: 100,
    });
    const second = await start(manager, agent, "second");
    await expect(
      manager.waitForPrompt({ prompt_ids: [first.prompt_id], timeout_ms: 0 }),
    ).rejects.toMatchObject({ code: "prompt_not_found" });
    expect(
      manager.cancelPrompts({ prompt_ids: [second.prompt_id, "missing"] }),
    ).toEqual([
      { prompt_id: "p2", outcome: "cancelled" },
      { prompt_id: "missing", outcome: "not_found" },
    ]);
    expect(
      await manager.waitForPrompt({ prompt_ids: ["p2"], timeout_ms: 0 }),
    ).toMatchObject({ prompt_id: "p2", state: "cancelled" });

    const allPending = new AsyncPromptManager({ maxTrackedPrompts: 1 });
    await start(allPending, agent);
    await expect(start(allPending, agent)).rejects.toMatchObject({
      code: "prompt_limit_reached",
    });
    allPending.cancelAll();
  });

  test("reserves capacity while awaiting acceptance and cancels startup on shutdown", async () => {
    const release = deferred<void>();
    const agent = fakeAgent(async function* () {
      await release.promise;
      yield { type: "status", status: "ack" };
    });
    const manager = new AsyncPromptManager({ maxTrackedPrompts: 1 });
    await manager.discoverAgents(clientFor(agent));
    const input = {
      prompt_endpoint: agent.promptSubject,
      label: "Test prompt",
      text: "hello",
    };
    const starting = manager.promptAgent(input);
    await Promise.resolve();
    expect(manager.listPendingPrompts()).toEqual([]);
    await expect(manager.promptAgent(input)).rejects.toMatchObject({
      code: "prompt_limit_reached",
    });
    manager.cancelAll();
    release.resolve();
    await expect(starting).rejects.toMatchObject({ code: "prompt_cancelled" });
    expect(manager.listPendingPrompts()).toEqual([]);
  });

  test("loads request attachments and materializes response attachments", async () => {
    const sourceDir = mkdtempSync(join(tmpdir(), "prompt-input-"));
    const sourcePath = join(sourceDir, "input.txt");
    writeFileSync(sourcePath, "request bytes");
    let promptOptions: any;
    const agent = fakeAgent(
      async function* () {
        yield { type: "status", status: "ack" };
        yield {
          type: "response",
          text: "attached",
          attachments: [
            {
              filename: "../answer.txt",
              content: Buffer.from("response bytes").toString("base64"),
            },
          ],
        };
      },
      (_text, options) => {
        promptOptions = options;
      },
    );
    const manager = new AsyncPromptManager();
    try {
      await manager.discoverAgents(clientFor(agent));
      const started = await manager.promptAgent({
        prompt_endpoint: agent.promptSubject,
        label: "attachments",
        text: "inspect this",
        attachments: [{ path: sourcePath, filename: "renamed.txt" }],
      });
      expect(promptOptions.attachments[0].filename).toBe("renamed.txt");
      expect(Buffer.from(promptOptions.attachments[0].content).toString()).toBe(
        "request bytes",
      );
      const result: any = await manager.waitForPrompt({
        prompt_ids: [started.prompt_id],
        timeout_ms: 100,
      });
      expect(result.attachments[0]).toMatchObject({
        filename: "answer.txt",
        size_bytes: 14,
      });
      expect(readFileSync(result.attachments[0].path, "utf8")).toBe(
        "response bytes",
      );
      expect(result.attachments[0].path).toContain("/p1/");
    } finally {
      manager.cancelAll();
      rmSync(sourceDir, { recursive: true, force: true });
    }
  });

  test("propagates the initiating trace scope through prompt acceptance", async () => {
    const seen: Array<TraceScope | undefined> = [];
    const scope: TraceScope = {
      threadId: "a".repeat(32),
      rootId: "b".repeat(32),
      turnCountHint: 1,
    };
    const agent = fakeAgent(async function* () {
      seen.push(activeTrace());
      yield { type: "status", status: "ack" };
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
    await manager.discoverAgents(clientFor(agent));
    const started = await manager.promptAgent(
      { prompt_endpoint: agent.promptSubject, label: "trace", text: "trace" },
      { traceScope: scope },
    );
    await manager.waitForPrompt({
      prompt_ids: [started.prompt_id],
      timeout_ms: 100,
    });
    expect(seen).toEqual([scope, scope, scope]);
  });
});
