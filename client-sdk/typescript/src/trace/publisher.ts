// Best-effort edge-record delivery.
//
// The wire contract is best-effort (observability.md, Delivery
// Guarantees); this publisher is how far *this* SDK takes it. `prompt()`
// only enqueues — every wait happens in a background drain that nothing
// awaits, so nothing in the observability layer can slow an agent.
//
// One bounded FIFO ring per connection. The drain publishes
// request-style and awaits the stream's PubAck, which is relayed back
// across the account import; on failure it backs off exponentially and
// retries the same bytes (idempotent within the stream's duplicate
// window via `Nats-Msg-Id`). Records are lost only to ring eviction,
// which is counted.

import { headers as natsHeaders, type MsgHdrs, type NatsConnection } from "@nats-io/nats-core";

/** Records held before the oldest is evicted to make room. */
export const DEFAULT_EDGE_QUEUE_CAPACITY = 100;
/** How long one publish waits for its PubAck. */
export const DEFAULT_EDGE_ACK_TIMEOUT_MS = 2_000;
/** First retry delay after a failure; also the value reset to on success. */
export const DEFAULT_EDGE_INITIAL_RETRY_DELAY_MS = 1_000;
/** Ceiling on the exponential backoff. */
export const DEFAULT_EDGE_MAX_RETRY_DELAY_MS = 60_000;
/** Multiplier applied to the delay after each failed round. */
export const DEFAULT_EDGE_RETRY_DELAY_FACTOR = 2;
/** How long `close()` gives the drain to flush what is queued. */
export const DEFAULT_EDGE_CLOSE_DRAIN_MS = 2_000;

/** Backoff jitter, ±15%, so a fleet does not retry in lockstep after an outage. */
const JITTER = 0.15;

export interface EdgePublisherOptions {
  readonly queueCapacity?: number;
  readonly ackTimeoutMs?: number;
  readonly initialRetryDelayMs?: number;
  readonly maxRetryDelayMs?: number;
  readonly retryDelayFactor?: number;
  readonly closeDrainMs?: number;
}

interface QueuedEdge {
  readonly subject: string;
  readonly payload: Uint8Array;
  readonly recordId: string;
  /** Builds the record's `Agent-Sender` header; run once, then cached. */
  readonly sign: (payload: Uint8Array) => Promise<MsgHdrs>;
  headers?: MsgHdrs;
}

export class EdgePublisher {
  readonly #nc: NatsConnection;
  readonly #capacity: number;
  readonly #ackTimeoutMs: number;
  readonly #initialDelayMs: number;
  readonly #maxDelayMs: number;
  readonly #factor: number;
  readonly #closeDrainMs: number;

  readonly #queue: QueuedEdge[] = [];
  #draining: Promise<void> | null = null;
  #delayMs: number;
  #dropped = 0;
  #closed = false;

  constructor(nc: NatsConnection, opts: EdgePublisherOptions = {}) {
    this.#nc = nc;
    this.#capacity = opts.queueCapacity ?? DEFAULT_EDGE_QUEUE_CAPACITY;
    this.#ackTimeoutMs = opts.ackTimeoutMs ?? DEFAULT_EDGE_ACK_TIMEOUT_MS;
    this.#initialDelayMs = opts.initialRetryDelayMs ?? DEFAULT_EDGE_INITIAL_RETRY_DELAY_MS;
    this.#maxDelayMs = opts.maxRetryDelayMs ?? DEFAULT_EDGE_MAX_RETRY_DELAY_MS;
    this.#factor = opts.retryDelayFactor ?? DEFAULT_EDGE_RETRY_DELAY_FACTOR;
    this.#closeDrainMs = opts.closeDrainMs ?? DEFAULT_EDGE_CLOSE_DRAIN_MS;
    this.#delayMs = this.#initialDelayMs;
  }

  /** Records dropped so far — always ring evictions, never silent losses. */
  get dropped(): number {
    return this.#dropped;
  }

  /** Records waiting to be delivered. */
  get queued(): number {
    return this.#queue.length;
  }

  /**
   * Hand one record to the publisher. Never blocks, never throws: a full
   * ring evicts its oldest entry (counted) to make room for the newest.
   */
  enqueue(
    subject: string,
    payload: Uint8Array,
    recordId: string,
    sign: (payload: Uint8Array) => Promise<MsgHdrs>,
  ): void {
    if (this.#closed) return;
    if (this.#queue.length >= this.#capacity) {
      this.#queue.shift();
      this.#dropped += 1;
    }
    this.#queue.push({ subject, payload, recordId, sign });
    this.#draining ??= this.#drain().finally(() => {
      this.#draining = null;
    });
  }

  /** Flush what is queued, then stop accepting records. */
  async close(): Promise<void> {
    if (this.#closed) return;
    const inFlight = this.#draining;
    if (inFlight) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const deadline = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, this.#closeDrainMs);
        timer.unref?.();
      });
      await Promise.race([inFlight, deadline]);
      if (timer !== undefined) clearTimeout(timer);
    }
    this.#closed = true;
  }

  async #drain(): Promise<void> {
    while (this.#queue.length > 0 && !this.#closed) {
      const head = this.#queue[0]!;
      try {
        // Signed once and cached, so a retry re-sends byte-identical
        // headers — consumers verify in stored mode, where freshness is
        // not checked.
        head.headers ??= await head.sign(head.payload);
        // Request-style: the reply is the stream's PubAck, relayed back
        // through the account import. A duplicate (a retry after a lost
        // ack) is absorbed by the stream's `Nats-Msg-Id` window.
        await this.#nc.request(head.subject, head.payload, {
          timeout: this.#ackTimeoutMs,
          headers: head.headers,
        });
        this.#queue.shift();
        this.#delayMs = this.#initialDelayMs;
      } catch {
        // Ambiguous by construction: the record may be stored with the
        // ack lost. Retrying the same bytes is safe, so back off and
        // keep the record queued — only eviction discards one.
        await sleep(withJitter(this.#delayMs));
        this.#delayMs = Math.min(this.#delayMs * this.#factor, this.#maxDelayMs);
      }
    }
  }
}

function withJitter(delayMs: number): number {
  return delayMs * (1 + (Math.random() * 2 - 1) * JITTER);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    timer.unref?.();
  });
}

/** `Nats-Msg-Id` = the record id, so stream de-duplication absorbs retries. */
export function msgIdHeaders(recordId: string): MsgHdrs {
  const h = natsHeaders();
  h.set("Nats-Msg-Id", recordId);
  return h;
}

// --- per-connection registry -----------------------------------------
//
// One publisher per NatsConnection, so every client on that connection
// shares one queue and one drain. The first enabler's configuration wins;
// a differing later configuration warns rather than silently applying.

const registry = new WeakMap<NatsConnection, EdgePublisher>();

const configured = new WeakMap<NatsConnection, EdgePublisherOptions>();

export function edgePublisherFor(
  nc: NatsConnection,
  opts: EdgePublisherOptions = {},
): EdgePublisher {
  const existing = registry.get(nc);
  if (existing) {
    const first = configured.get(nc) ?? {};
    if (!sameOptions(first, opts)) {
      console.warn(
        "@synadia-ai/agents: edge publisher for this connection is already configured; " +
          "ignoring the differing delivery options",
      );
    }
    return existing;
  }
  const publisher = new EdgePublisher(nc, opts);
  registry.set(nc, publisher);
  configured.set(nc, opts);
  return publisher;
}

function sameOptions(a: EdgePublisherOptions, b: EdgePublisherOptions): boolean {
  const keys: (keyof EdgePublisherOptions)[] = [
    "queueCapacity",
    "ackTimeoutMs",
    "initialRetryDelayMs",
    "maxRetryDelayMs",
    "retryDelayFactor",
    "closeDrainMs",
  ];
  return keys.every((k) => a[k] === b[k]);
}

/** Flush and drop the connection's publisher, if it has one. */
export async function closeEdgePublisherFor(nc: NatsConnection): Promise<void> {
  const publisher = registry.get(nc);
  if (!publisher) return;
  registry.delete(nc);
  configured.delete(nc);
  await publisher.close();
}
