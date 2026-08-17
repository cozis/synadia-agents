// Shared per-connection reply mux for prompt streams.
//
// One wildcard subscription (`_INBOX.agents.<nuid>.*`) per NatsConnection
// routes every prompt's reply stream by its final subject token, replacing
// `nc.requestMany` — whose internally-minted inbox the SDK cannot see.
// Owning the inbox lets `mintToken()` expose a stream's reply subject (and
// the thread id derived from it) BEFORE anything is published. Mirrors the
// Python SDK's `_mux.py`.

import {
  createInbox,
  nuid,
  type Msg,
  type NatsConnection,
  type Subscription,
} from "@nats-io/nats-core";
import { SDK_INBOX_PREFIX } from "./inbox.js";

/** Minimal single-consumer async queue of inbound reply messages. */
export class MsgQueue implements AsyncIterable<Msg> {
  #items: Msg[] = [];
  #resolvers: ((r: IteratorResult<Msg, void>) => void)[] = [];
  #ended = false;

  push(msg: Msg): void {
    if (this.#ended) return;
    const resolve = this.#resolvers.shift();
    if (resolve) resolve({ value: msg, done: false });
    else this.#items.push(msg);
  }

  /** End the queue: pending and future `next()` calls resolve done. Idempotent. */
  end(): void {
    this.#ended = true;
    for (const resolve of this.#resolvers.splice(0)) {
      resolve({ value: undefined, done: true });
    }
  }

  next(): Promise<IteratorResult<Msg, void>> {
    const head = this.#items.shift();
    if (head !== undefined) return Promise.resolve({ value: head, done: false });
    if (this.#ended) return Promise.resolve({ value: undefined, done: true });
    return new Promise((resolve) => this.#resolvers.push(resolve));
  }

  [Symbol.asyncIterator](): AsyncIterator<Msg, void> {
    return { next: () => this.next() };
  }
}

export class MuxInbox {
  readonly #nc: NatsConnection;
  readonly #base: string;
  readonly #routes = new Map<string, MsgQueue>();
  #sub: Subscription | null = null;

  constructor(nc: NatsConnection) {
    this.#nc = nc;
    this.#base = createInbox(SDK_INBOX_PREFIX);
  }

  /** Mint (but do not register) a fresh per-stream token, so a stream's
   * reply subject — and anything derived from it, e.g. the observability
   * thread id — is known before {@link register} and the publish. */
  mintToken(): string {
    return nuid.next();
  }

  replySubjectFor(token: string): string {
    return `${this.#base}.${token}`;
  }

  /** Subscribe the shared wildcard (idempotent; pays SUB+flush once). */
  async start(): Promise<void> {
    if (this.#sub !== null) return;
    this.#sub = this.#nc.subscribe(`${this.#base}.*`, {
      callback: (err, msg) => {
        if (err) return;
        this.#route(msg);
      },
    });
    await this.#nc.flush();
  }

  #route(msg: Msg): void {
    const token = msg.subject.slice(this.#base.length + 1);
    this.#routes.get(token)?.push(msg);
  }

  /** Reserve the routing slot for a minted token. Callers MUST
   * {@link unregister} when the stream completes. */
  register(token: string): MsgQueue {
    const queue = new MsgQueue();
    this.#routes.set(token, queue);
    return queue;
  }

  /** Drop the routing entry for `token`. Idempotent. */
  unregister(token: string): void {
    this.#routes.delete(token);
  }
}

// Weak-keyed so a dropped/closed connection releases its mux (the
// subscription dies with the connection).
const MUX_CACHE = new WeakMap<NatsConnection, MuxInbox>();

export function muxFor(nc: NatsConnection): MuxInbox {
  let mux = MUX_CACHE.get(nc);
  if (mux === undefined) {
    mux = new MuxInbox(nc);
    MUX_CACHE.set(nc, mux);
  }
  return mux;
}
