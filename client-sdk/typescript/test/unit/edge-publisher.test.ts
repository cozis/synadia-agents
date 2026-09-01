import type { Msg, MsgHdrs, NatsConnection } from "@nats-io/nats-core";
import { describe, expect, it } from "vitest";
import {
  EdgePublisher,
  msgIdHeaders,
  type EdgePublisherOptions,
} from "../../src/trace/publisher.js";

/** Stand-in for the identity signing step; the publisher only awaits it. */
const sign = (recordId: string) => (): Promise<MsgHdrs> => {
  const h = msgIdHeaders(recordId);
  h.set("Agent-Sender", '{"v":1,"account":"$G","user":"U"}');
  return Promise.resolve(h);
};

// Delays are ~0 so the retry schedule is observable without waiting out
// real backoff intervals.
const FAST: EdgePublisherOptions = {
  ackTimeoutMs: 50,
  initialRetryDelayMs: 1,
  maxRetryDelayMs: 4,
  closeDrainMs: 1_000,
};

interface Recorded {
  subject: string;
  payload: Uint8Array;
  msgId: string | undefined;
  signed: boolean;
}

/** Fake connection recording requests; fails the first `failTimes` of them. */
function fakeNc(failTimes = 0): { nc: NatsConnection; requests: Recorded[] } {
  const requests: Recorded[] = [];
  let remaining = failTimes;
  const nc = {
    request: (
      subject: string,
      payload: Uint8Array,
      opts?: { headers?: MsgHdrs },
    ): Promise<Msg> => {
      requests.push({
        subject,
        payload,
        msgId: opts?.headers?.get("Nats-Msg-Id"),
        signed: opts?.headers?.get("Agent-Sender") !== undefined,
      });
      if (remaining > 0) {
        remaining -= 1;
        return Promise.reject(new Error("no ack"));
      }
      return Promise.resolve({} as Msg);
    },
  } as unknown as NatsConnection;
  return { nc, requests };
}

async function drained(publisher: EdgePublisher, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (publisher.queued > 0) {
    if (Date.now() > deadline) throw new Error(`queue did not drain: ${publisher.queued} left`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

const text = (bytes: Uint8Array): string => new TextDecoder().decode(bytes);

describe("EdgePublisher", () => {
  it("publishes request-style with the record id as Nats-Msg-Id", async () => {
    const { nc, requests } = fakeNc();
    const publisher = new EdgePublisher(nc, FAST);
    publisher.enqueue("TRACE.edges", new TextEncoder().encode('{"x":1}'), "abc123", sign("abc123"));
    await drained(publisher);
    expect(requests).toHaveLength(1);
    expect(requests[0]!.subject).toBe("TRACE.edges");
    expect(text(requests[0]!.payload)).toBe('{"x":1}');
    expect(requests[0]!.msgId).toBe("abc123");
    expect(requests[0]!.signed).toBe(true);
    expect(publisher.dropped).toBe(0);
  });

  it("retries the same bytes until acked", async () => {
    const { nc, requests } = fakeNc(2);
    const publisher = new EdgePublisher(nc, FAST);
    publisher.enqueue("TRACE.edges", new TextEncoder().encode('{"x":1}'), "abc123", sign("abc123"));
    await drained(publisher);
    // Three attempts, byte-identical: the retry is idempotent at the
    // stream via Nats-Msg-Id.
    expect(requests).toHaveLength(3);
    expect(new Set(requests.map((r) => text(r.payload)))).toEqual(new Set(['{"x":1}']));
    expect(new Set(requests.map((r) => r.msgId))).toEqual(new Set(["abc123"]));
    expect(publisher.dropped).toBe(0);
  });

  it("preserves FIFO order across records", async () => {
    const { nc, requests } = fakeNc();
    const publisher = new EdgePublisher(nc, FAST);
    for (let i = 0; i < 5; i++) {
      publisher.enqueue(
        "TRACE.edges",
        new TextEncoder().encode(String(i)),
        `rec${i}`,
        sign(`rec${i}`),
      );
    }
    await drained(publisher);
    expect(requests.map((r) => text(r.payload))).toEqual(["0", "1", "2", "3", "4"]);
  });

  it("evicts the oldest record when the ring is full and counts it", async () => {
    // A connection that never acks, so the queue fills instead of draining.
    const { nc } = fakeNc(10_000);
    const publisher = new EdgePublisher(nc, { ...FAST, queueCapacity: 3 });
    for (let i = 0; i < 5; i++) {
      publisher.enqueue(
        "TRACE.edges",
        new TextEncoder().encode(String(i)),
        `rec${i}`,
        sign(`rec${i}`),
      );
    }
    expect(publisher.queued).toBe(3);
    expect(publisher.dropped).toBe(2);
    await publisher.close();
  });

  it("ignores records enqueued after close", async () => {
    const { nc, requests } = fakeNc();
    const publisher = new EdgePublisher(nc, FAST);
    await publisher.close();
    publisher.enqueue("TRACE.edges", new TextEncoder().encode("x"), "rec", sign("rec"));
    await new Promise((r) => setTimeout(r, 20));
    expect(requests).toHaveLength(0);
    expect(publisher.queued).toBe(0);
  });

  it("flushes what is queued on close", async () => {
    const { nc, requests } = fakeNc();
    const publisher = new EdgePublisher(nc, FAST);
    for (let i = 0; i < 3; i++) {
      publisher.enqueue(
        "TRACE.edges",
        new TextEncoder().encode(String(i)),
        `rec${i}`,
        sign(`rec${i}`),
      );
    }
    await publisher.close();
    expect(requests).toHaveLength(3);
    expect(publisher.queued).toBe(0);
  });
});
