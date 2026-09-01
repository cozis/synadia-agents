import { Empty, type Msg, type NatsConnection } from "@nats-io/nats-core";
import { describe, expect, it } from "vitest";
import { Agent } from "../../src/agent.js";
import { buildAgentInfo, type RawServiceInfo } from "../../src/discovery/agent-info.js";
import { decodeEnvelope, encodeEnvelope } from "../../src/prompt/envelope.js";
import { isThreadId, randomThreadId, THREAD_ID_HEX_LEN } from "../../src/trace/ids.js";

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

async function promptedWire(agent: Agent, sink: { payload?: Uint8Array }): Promise<unknown> {
  const stream = await agent.prompt("hello");
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
});

describe("decodeEnvelope (pre-adoption)", () => {
  it("still decodes a lineage-bearing envelope's prompt (fields adopted in a later commit)", () => {
    const tid = randomThreadId();
    const bytes = encodeEnvelope({ prompt: "hi", threadId: tid, rootId: tid });
    expect(decodeEnvelope(bytes).prompt).toBe("hi");
  });
});
