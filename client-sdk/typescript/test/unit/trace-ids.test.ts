import { Empty, type Msg, type MsgHdrs, type NatsConnection } from "@nats-io/nats-core";
import { describe, expect, it } from "vitest";
import { Agent } from "../../src/agent.js";
import { createUser } from "@nats-io/nkeys";
import { buildAgentInfo, type RawServiceInfo } from "../../src/discovery/agent-info.js";
import { newAgentId } from "../../src/identity/agent-id.js";
import type { IdentityContext } from "../../src/identity/context.js";
import { buildClaimHeader } from "../../src/identity/sender-header.js";
import { decodeEnvelope, encodeEnvelope } from "../../src/prompt/envelope.js";
import {
  activeTrace,
  activeTurnCount,
  bindActiveTrace,
  type ActiveTrace,
} from "../../src/trace/context.js";
import { DEFAULT_EDGE_SUBJECT, EDGE_RECORD_VERSION } from "../../src/trace/edge.js";
import {
  formatTraceHeaders,
  HEADER_ROOT_ID,
  HEADER_THREAD_ID,
  traceHeaders,
} from "../../src/trace/headers.js";
import type { TraceOptions } from "../../src/trace/options.js";
import { isThreadId, isToolCallId, randomThreadId, THREAD_ID_HEX_LEN } from "../../src/trace/ids.js";

interface CapturedEdge {
  subject: string;
  record: Record<string, unknown>;
  signed: boolean;
  msgId: string | undefined;
}

// Edge records are only published when a signer is configured (consumers
// ignore unsigned ones), so traced handles in these tests carry a fake
// identity whose plan produces a claim header.
function fakeIdentity(): IdentityContext {
  const id = newAgentId("$G", createUser().getPublicKey());
  return {
    signer: {} as unknown as IdentityContext["signer"],
    name: undefined,
    sendUnsignedClaim: false,
    mayAttachHeader: () => false,
    plan: (sub: string) =>
      Promise.resolve({
        id,
        signed: true,
        sub,
        wireBytes: 256,
        build: () => Promise.resolve(buildClaimHeader({ id })),
      }),
  } as unknown as IdentityContext;
}

/** A traced handle: tracing on, identity able to sign its edge records. */
function tracedAgent(sink: Sink, trace: TraceOptions = {}): Agent {
  return new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000, undefined, fakeIdentity(), trace);
}

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

interface Sink {
  payload?: Uint8Array;
  edges: CapturedEdge[];
}

function newSink(): Sink {
  return { edges: [] };
}

/** Fake connection capturing the prompt payload and any edge publishes. */
function captureNc(sink: Sink): NatsConnection {
  return {
    info: { max_payload: 1024 * 1024 },
    // Edge records travel as acked requests through the background
    // publisher; record them and ack immediately.
    request: (subject: string, payload: Uint8Array, opts?: { headers?: MsgHdrs }) => {
      sink.edges.push({
        subject,
        record: JSON.parse(new TextDecoder().decode(payload)) as Record<string, unknown>,
        signed: opts?.headers?.get("Agent-Sender") !== undefined,
        msgId: opts?.headers?.get("Nats-Msg-Id"),
      });
      return Promise.resolve({} as Msg);
    },
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
  sink: Sink,
  opts: Parameters<Agent["prompt"]>[1] = {},
): Promise<unknown> {
  const stream = await agent.prompt("hello", opts);
  for await (const _ of stream) {
    // drain the single terminator
  }
  expect(sink.payload).toBeDefined();
  // The edge publish is a background drain; give it its microtask turns
  // so assertions on `sink.edges` are deterministic.
  await settle();
  return JSON.parse(new TextDecoder().decode(sink.payload));
}

/** Let the publisher's drain run to completion. */
function settle(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 5));
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
    const sink = newSink();
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000);
    const wire = (await promptedWire(agent, sink)) as Record<string, unknown>;
    expect(wire).toEqual({ prompt: "hello" });
  });

  it("mints thread_id and root_id (equal — a root) when tracing is on", async () => {
    const sink = newSink();
    const agent = tracedAgent(sink);
    const wire = (await promptedWire(agent, sink)) as Record<string, unknown>;
    expect(wire["prompt"]).toBe("hello");
    const threadId = wire["thread_id"] as string;
    expect(isThreadId(threadId)).toBe(true);
    expect(wire["root_id"]).toBe(threadId);
  });

  it("mints a fresh thread_id per prompt", async () => {
    const sink = newSink();
    const agent = tracedAgent(sink);
    const first = (await promptedWire(agent, sink)) as Record<string, unknown>;
    const second = (await promptedWire(agent, sink)) as Record<string, unknown>;
    expect(first["thread_id"]).not.toBe(second["thread_id"]);
  });

  it("inherits root and parent from the ambient trace; the parent never transits the child", async () => {
    const ambient: ActiveTrace = { threadId: randomThreadId(), rootId: randomThreadId() };
    const sink = newSink();
    const agent = tracedAgent(sink);
    const wire = (await bindActiveTrace(ambient, () =>
      promptedWire(agent, sink, { tool: "call_9xJ2" }),
    )) as Record<string, unknown>;

    expect(wire["root_id"]).toBe(ambient.rootId);
    expect(wire["thread_id"]).not.toBe(ambient.threadId);
    expect(wire).not.toHaveProperty("parent_id");

    expect(sink.edges).toHaveLength(1);
    expect(sink.edges[0]!.record).toMatchObject({
      thread_id: wire["thread_id"],
      root_id: ambient.rootId,
      parent_id: ambient.threadId,
      tool_call_id: "call_9xJ2",
    });
  });

  it("keeps the minted id internal — the envelope and the edge agree, nothing is exposed", async () => {
    const sink = newSink();
    const agent = tracedAgent(sink);
    const wire = (await promptedWire(agent, sink)) as Record<string, unknown>;
    expect(sink.edges).toHaveLength(1);
    expect(sink.edges[0]!.record["thread_id"]).toBe(wire["thread_id"]);
  });
});

describe("signing gate", () => {
  it("publishes no edge records when tracing is on but no signer is configured", async () => {
    const sink = newSink();
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000, undefined, undefined, {});
    const wire = (await promptedWire(agent, sink)) as Record<string, unknown>;
    // Minting and envelope lineage need no identity and still happen.
    expect(isThreadId(wire["thread_id"] as string)).toBe(true);
    expect(sink.edges).toHaveLength(0);
  });

  it("signs each record it publishes", async () => {
    const sink = newSink();
    await promptedWire(tracedAgent(sink), sink);
    expect(sink.edges).toHaveLength(1);
    expect(sink.edges[0]!.signed).toBe(true);
    expect(sink.edges[0]!.msgId).toBe(sink.edges[0]!.record["record_id"]);
  });
});

describe("turn_count_hint on edges", () => {
  it("is omitted when the parent has made no model call", async () => {
    const sink = newSink();
    const agent = tracedAgent(sink);
    await promptedWire(agent, sink);
    expect(sink.edges[0]!.record).not.toHaveProperty("turn_count_hint");
  });

  it("carries the parent's model-call count at spawn time", async () => {
    const ambient: ActiveTrace = { threadId: randomThreadId(), rootId: randomThreadId() };
    const sink = newSink();
    const agent = tracedAgent(sink);
    await bindActiveTrace(ambient, async () => {
      traceHeaders();
      traceHeaders();
      await promptedWire(agent, sink); // spawned after two model turns
      traceHeaders();
      await promptedWire(agent, sink); // ... and after a third
    });
    expect(sink.edges.map((e) => e.record["turn_count_hint"])).toEqual([2, 3]);
  });
});

describe("trace headers", () => {
  it("names the thread and the root", () => {
    const trace: ActiveTrace = { threadId: randomThreadId(), rootId: randomThreadId() };
    expect(formatTraceHeaders(trace)).toEqual({
      [HEADER_THREAD_ID]: trace.threadId,
      [HEADER_ROOT_ID]: trace.rootId,
    });
  });

  it("is empty outside a handler and populated inside", async () => {
    expect(traceHeaders()).toEqual({});
    const trace: ActiveTrace = { threadId: randomThreadId(), rootId: randomThreadId() };
    const inside = await bindActiveTrace(trace, async () => {
      await Promise.resolve();
      return traceHeaders();
    });
    expect(inside).toEqual(formatTraceHeaders(trace));
    expect(traceHeaders()).toEqual({});
  });
});

describe("turn counter", () => {
  it("counts one turn per traceHeaders() call and stays at zero outside a handler", async () => {
    expect(activeTurnCount()).toBe(0);
    traceHeaders(); // outside a handler: no scope to count against
    expect(activeTurnCount()).toBe(0);

    const trace: ActiveTrace = { threadId: randomThreadId(), rootId: randomThreadId() };
    await bindActiveTrace(trace, async () => {
      await Promise.resolve();
      expect(activeTurnCount()).toBe(0);
      traceHeaders();
      traceHeaders();
      expect(activeTurnCount()).toBe(2);
    });
    expect(activeTurnCount()).toBe(0);
  });
});

describe("inherited tracing configuration", () => {
  const bind = <T>(fn: () => T, options: TraceOptions | undefined): T =>
    bindActiveTrace({ threadId: randomThreadId(), rootId: randomThreadId() }, fn, options);

  it("traces an unconfigured handle when the enclosing service handed config down", async () => {
    const sink = newSink();
    const agent = new Agent(
      captureNc(sink),
      buildAgentInfo(info())!,
      1_000,
      undefined,
      fakeIdentity(),
    );
    expect(agent.tracingEnabled).toBe(false); // no config of its own
    const wire = (await bind(() => promptedWire(agent, sink), {})) as Record<string, unknown>;
    expect(isThreadId(wire["thread_id"] as string)).toBe(true);
    expect(sink.edges).toHaveLength(1);
  });

  it("stays off when neither the handle nor the enclosing service configured tracing", async () => {
    const sink = newSink();
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000);
    const wire = (await bind(() => promptedWire(agent, sink), undefined)) as Record<
      string,
      unknown
    >;
    expect(wire).toEqual({ prompt: "hello" });
    expect(sink.edges).toHaveLength(0);
  });

  it("prefers the handle's own configuration over the inherited one", async () => {
    const sink = newSink();
    const agent = tracedAgent(sink, { edgeSubject: "TRACE.own" });
    await bind(() => promptedWire(agent, sink), { edgeSubject: "TRACE.inherited" });
    expect(sink.edges).toHaveLength(1);
    expect(sink.edges[0]!.subject).toBe("TRACE.own");
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
    const sink = newSink();
    const agent = tracedAgent(sink);
    expect(() => agent.prompt("hello", { tool: "bad tool" })).toThrow(/tool call id/);
    expect(sink.payload).toBeUndefined();
    expect(sink.edges).toHaveLength(0);
  });

  it("prompt() hands thread and tool ids to the edge sender when tracing is on", async () => {
    const sink = newSink();
    const agent = tracedAgent(sink);
    const wire = (await promptedWire(agent, sink, { tool: "call_9xJ2" })) as Record<
      string,
      unknown
    >;
    expect(sink.edges).toHaveLength(1);
    expect(sink.edges[0]!.subject).toBe(DEFAULT_EDGE_SUBJECT);
    expect(sink.edges[0]!.record).toMatchObject({
      version: EDGE_RECORD_VERSION,
      thread_id: wire["thread_id"],
      root_id: wire["root_id"],
      parent_id: null,
      tool_call_id: "call_9xJ2",
    });
    expect(isThreadId(sink.edges[0]!.record["record_id"] as string)).toBe(true);
    expect(typeof sink.edges[0]!.record["ts"]).toBe("number");
  });

  it("prompt() does not touch the edge sender when tracing is off", async () => {
    const sink = newSink();
    const agent = new Agent(captureNc(sink), buildAgentInfo(info())!, 1_000);
    await promptedWire(agent, sink, { tool: "call_9xJ2" });
    expect(sink.edges).toHaveLength(0);
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
