import { Empty, type Msg, type NatsConnection } from "@nats-io/nats-core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Agent } from "../../src/agent.js";
import { buildAgentInfo, type RawServiceInfo } from "../../src/discovery/agent-info.js";
import { decodeEnvelope, encodeEnvelope } from "../../src/prompt/envelope.js";
import { activeTrace, bindActiveTrace, type ActiveTrace } from "../../src/trace/context.js";
import { isThreadId, isToolCallId, randomThreadId, THREAD_ID_HEX_LEN } from "../../src/trace/ids.js";

// The edge sender is a no-op stub for now; mock it so tests can assert the
// call shape `prompt()` hands it.
interface CapturedEdge {
  threadId: string;
  rootId: string;
  parentId?: string;
  toolCallId?: string;
}
const edgeCalls = vi.hoisted(() => [] as CapturedEdge[]);
vi.mock("../../src/trace/edge.js", () => ({
  sendEdgeRecord: (fields: CapturedEdge) => {
    edgeCalls.push(fields);
  },
}));

beforeEach(() => {
  edgeCalls.length = 0;
});

function info(): RawServiceInfo {
  return {
    name: "agents",
    id: "VMKS6MHK71PCPWGY38A7N5",
    version: "1.0.0",
    description: "test agent",
    metadata: {
      agent: "echo",
      owner: "test",
      session: "main",
      protocol_version: "0.3",
    },
    endpoints: [
      {
        name: "prompt",
        subject: "agents.prompt.echo.test.main",
        queue_group: "agents",
        metadata: { max_payload: "1MB", attachments_ok: "true" },
      },
    ],
  };
}

/** Fake connection capturing the published prompt payload. */
function captureNc(sink: { payload?: Uint8Array }): NatsConnection {
  return {
    info: { max_payload: 1024 * 1024 },
    requestMany: (_subject: string, payload: Uint8Array) => {
      sink.payload = payload;
      const messages = (async function* (): AsyncGenerator<Msg> {
        await Promise.resolve();
        yield { data: Empty } as Msg;
      })();
      return Object.assign(messages, { stop: () => undefined });
    },
  } as unknown as NatsConnection;
}

async function promptedWire(
  agent: Agent,
  sink: { payload?: Uint8Array },
  opts: Parameters<Agent["prompt"]>[1] = {},
): Promise<unknown> {
  const stream = await agent.prompt("hello", opts);
  for await (const _ of stream) {
    // drain the single terminator
  }
  expect(sink.payload).toBeDefined();
  return JSON.parse(new TextDecoder().decode(sink.payload));
}

describe("thread ids", () => {
  it("mints 32 lowercase hex chars, unique per call", () => {
    const a = randomThreadId();
    const b = randomThreadId();
    expect(a).toHaveLength(THREAD_ID_HEX_LEN);
    expect(a).toMatch(/^[0-9a-f]{32}$/);
    expect(a).not.toBe(b);
  });

  it("validates the normative shape", () => {
    expect(isThreadId(randomThreadId())).toBe(true);
    expect(isThreadId("")).toBe(false);
    expect(isThreadId("9f2c4b1e8a7d33051c6e0b42d78a91f")).toBe(false); // 31 chars
    expect(isThreadId("9F2C4B1E8A7D33051C6E0B42D78A91F0".toLowerCase() + "0")).toBe(false); // 33
    expect(isThreadId("9F2C4B1E8A7D33051C6E0B42D78A91F0")).toBe(false); // upper case
    expect(isThreadId("gf2c4b1e8a7d33051c6e0b42d78a91f0")).toBe(false); // non-hex
  });
});

describe("envelope lineage fields", () => {
  it("emits thread_id/root_id when present and omits them when absent", () => {
    const tid = randomThreadId();
    const withLineage: unknown = JSON.parse(
      new TextDecoder().decode(encodeEnvelope({ prompt: "hi", threadId: tid, rootId: tid })),
    );
    expect(withLineage).toEqual({ prompt: "hi", thread_id: tid, root_id: tid });

    const without: unknown = JSON.parse(new TextDecoder().decode(encodeEnvelope({ prompt: "hi" })));
    expect(without).toEqual({ prompt: "hi" });
  });
});

describe("prompt minting", () => {
  it("adds no lineage when tracing is off (byte-identical 0.3)", async () => {
    const sink: { payload?: Uint8Array } = {};
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000);
    const wire = (await promptedWire(agent, sink)) as Record<string, unknown>;
    expect(wire).toEqual({ prompt: "hello" });
  });

  it("mints thread_id and root_id (equal — a root) when tracing is on", async () => {
    const sink: { payload?: Uint8Array } = {};
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000, undefined, undefined, {});
    const wire = (await promptedWire(agent, sink)) as Record<string, unknown>;
    expect(wire["prompt"]).toBe("hello");
    const threadId = wire["thread_id"] as string;
    expect(isThreadId(threadId)).toBe(true);
    expect(wire["root_id"]).toBe(threadId);
  });

  it("mints a fresh thread_id per prompt", async () => {
    const sink: { payload?: Uint8Array } = {};
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000, undefined, undefined, {});
    const first = (await promptedWire(agent, sink)) as Record<string, unknown>;
    const second = (await promptedWire(agent, sink)) as Record<string, unknown>;
    expect(first["thread_id"]).not.toBe(second["thread_id"]);
  });

  it("inherits root and parent from the ambient trace; the parent never transits the child", async () => {
    const ambient: ActiveTrace = { threadId: randomThreadId(), rootId: randomThreadId() };
    const sink: { payload?: Uint8Array } = {};
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000, undefined, undefined, {});
    const wire = (await bindActiveTrace(ambient, () =>
      promptedWire(agent, sink, { tool: "call_9xJ2" }),
    )) as Record<string, unknown>;

    expect(wire["root_id"]).toBe(ambient.rootId);
    expect(wire["thread_id"]).not.toBe(ambient.threadId);
    expect(wire).not.toHaveProperty("parent_id");

    expect(edgeCalls).toHaveLength(1);
    expect(edgeCalls[0]).toEqual({
      threadId: wire["thread_id"],
      rootId: ambient.rootId,
      parentId: ambient.threadId,
      toolCallId: "call_9xJ2",
    });
  });

});

describe("tool call ids", () => {
  it("accepts 1-256 visible-ASCII characters", () => {
    expect(isToolCallId("call_9xJ2")).toBe(true);
    expect(isToolCallId("x".repeat(256))).toBe(true);
    expect(isToolCallId("")).toBe(false);
    expect(isToolCallId("x".repeat(257))).toBe(false);
    expect(isToolCallId("has space")).toBe(false);
    expect(isToolCallId("newline\n")).toBe(false);
    expect(isToolCallId("émoji")).toBe(false);
  });

  it("prompt() rejects an invalid tool option synchronously", () => {
    const sink: { payload?: Uint8Array } = {};
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000, undefined, undefined, {});
    expect(() => agent.prompt("hello", { tool: "bad tool" })).toThrow(/tool call id/);
    expect(sink.payload).toBeUndefined();
    expect(edgeCalls).toHaveLength(0);
  });

  it("prompt() hands thread and tool ids to the edge sender when tracing is on", async () => {
    const sink: { payload?: Uint8Array } = {};
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000, undefined, undefined, {});
    const wire = (await promptedWire(agent, sink, { tool: "call_9xJ2" })) as Record<
      string,
      unknown
    >;
    expect(edgeCalls).toHaveLength(1);
    expect(edgeCalls[0]!.threadId).toBe(wire["thread_id"]);
    expect(edgeCalls[0]!.toolCallId).toBe("call_9xJ2");
  });

  it("prompt() does not touch the edge sender when tracing is off", async () => {
    const sink: { payload?: Uint8Array } = {};
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000);
    await promptedWire(agent, sink, { tool: "call_9xJ2" });
    expect(edgeCalls).toHaveLength(0);
  });
});

describe("decodeEnvelope lineage", () => {
  it("adopts a valid thread_id/root_id pair", () => {
    const tid = randomThreadId();
    const rid = randomThreadId();
    const decoded = decodeEnvelope(encodeEnvelope({ prompt: "hi", threadId: tid, rootId: rid }));
    expect(decoded).toEqual({ prompt: "hi", threadId: tid, rootId: rid });
  });

  it("leaves both fields absent on a plain envelope", () => {
    const decoded = decodeEnvelope(encodeEnvelope({ prompt: "hi" }));
    expect(decoded.threadId).toBeUndefined();
    expect(decoded.rootId).toBeUndefined();
  });

  it("rejects a lone thread_id or root_id", () => {
    const tid = randomThreadId();
    const enc = (obj: unknown): Uint8Array => new TextEncoder().encode(JSON.stringify(obj));
    expect(() => decodeEnvelope(enc({ prompt: "hi", thread_id: tid }))).toThrow(/sent together/);
    expect(() => decodeEnvelope(enc({ prompt: "hi", root_id: tid }))).toThrow(/sent together/);
  });

  it("rejects malformed thread ids", () => {
    const enc = (obj: unknown): Uint8Array => new TextEncoder().encode(JSON.stringify(obj));
    const tid = randomThreadId();
    expect(() =>
      decodeEnvelope(enc({ prompt: "hi", thread_id: "nope", root_id: tid })),
    ).toThrow(/thread id/);
    expect(() =>
      decodeEnvelope(enc({ prompt: "hi", thread_id: tid, root_id: 42 })),
    ).toThrow(/thread id/);
  });
});

describe("ambient trace context", () => {
  it("is undefined outside a bound scope and visible inside, including across awaits", async () => {
    expect(activeTrace()).toBeUndefined();
    const trace: ActiveTrace = { threadId: randomThreadId(), rootId: randomThreadId() };
    const seen = await bindActiveTrace(trace, async () => {
      await Promise.resolve();
      return activeTrace();
    });
    expect(seen).toBe(trace);
    expect(activeTrace()).toBeUndefined();
  });

  it("nests: the inner binding wins and the outer is restored", async () => {
    const outer: ActiveTrace = { threadId: randomThreadId(), rootId: randomThreadId() };
    const inner: ActiveTrace = { threadId: randomThreadId(), rootId: outer.rootId };
    await bindActiveTrace(outer, async () => {
      expect(activeTrace()).toBe(outer);
      await bindActiveTrace(inner, async () => {
        await Promise.resolve();
        expect(activeTrace()).toBe(inner);
      });
      expect(activeTrace()).toBe(outer);
    });
  });
});
