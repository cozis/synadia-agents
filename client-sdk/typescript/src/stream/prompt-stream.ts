// PromptStream — the user-facing stream of typed events yielded by
// `Agent.prompt`. Implements `AsyncIterable<StreamMessage>` so the
// caller writes `for await (const msg of stream) { ... }`.
//
// Wire behavior:
//   - Replies ride the per-connection shared mux (`internal/mux.ts`): the
//     stream's reply subject is minted before the request is published, so
//     the observability `threadId` (derived from it) is known up front.
//   - Yields `{ type: "response" }`, `{ type: "status" }`, `QueryEvent` per
//     §6.3–§7; a synthetic `{ type: "status", status: "done" }` marks the
//     wire terminator (empty body + no headers, §6.5).
//   - Throws `ServiceError` on a `Nats-Service-Error-Code` header (§9.1),
//     `StreamStalledError` on inactivity timeout (§6.6), and
//     `StreamMaxWaitExceededError` when `maxWaitMs` elapses without a
//     terminator.
//   - `cancel()` and early break from `for await` both stop the iterator
//     cleanly.

import type { Msg, NatsConnection } from "@nats-io/nats-core";
import {
  ServiceError,
  StreamMaxWaitExceededError,
  StreamStalledError,
  type ServiceErrorBody,
} from "../errors.js";
import { abortError } from "../internal/abort.js";
import type { MsgQueue, MuxInbox } from "../internal/mux.js";
import { encodeEnvelope, type RequestEnvelope } from "../prompt/envelope.js";
import { buildQueryEvent, type QueryEvent } from "../query/query-event.js";
import { decodeChunk, type DecodedAttachment, type DecodedChunk } from "./chunk-decoder.js";
import { withInactivityTimeout } from "./inactivity.js";
import { isErrorSignal, isTerminator } from "./terminator.js";

export type ResponseAttachment = DecodedAttachment;

export type StreamMessage =
  | {
      readonly type: "response";
      readonly text: string;
      readonly attachments?: ReadonlyArray<ResponseAttachment>;
    }
  | { readonly type: "status"; readonly status: string }
  | QueryEvent;

/** Constructor inputs — assembled by `Agent.prompt`; not part of the caller API. */
export interface PromptStreamInit {
  readonly nc: NatsConnection;
  readonly mux: MuxInbox;
  readonly token: string;
  readonly requestSubject: string;
  readonly envelope: RequestEnvelope;
  readonly inactivityTimeoutMs: number;
  readonly maxWaitMs: number;
  readonly signal?: AbortSignal | undefined;
  readonly threadId: string;
  readonly rootId: string;
  readonly spawnMarkerHeaders?: Record<string, string> | undefined;
}

export class PromptStream implements AsyncIterable<StreamMessage> {
  /** This prompt execution's derived thread id (observability identity). */
  readonly threadId: string;

  /** Root thread id of the tree this prompt joined (== `threadId` when it rooted a new tree). */
  readonly rootId: string;

  /** Headers for the spawn-time marker request, set when the ambient trace
   * auto-recorded this prompt as a spawn; undefined when there is nothing
   * to mark. Fire through any provider client as fire-and-forget telemetry. */
  readonly spawnMarkerHeaders: Record<string, string> | undefined;

  readonly #nc: NatsConnection;
  readonly #mux: MuxInbox;
  readonly #token: string;
  readonly #requestSubject: string;
  readonly #envelope: RequestEnvelope;
  readonly #inactivityTimeoutMs: number;
  readonly #maxWaitMs: number;
  readonly #signal: AbortSignal | undefined;
  #queue: MsgQueue | null = null;
  #iterated = false;
  #cancelled = false;

  constructor(init: PromptStreamInit) {
    this.#nc = init.nc;
    this.#mux = init.mux;
    this.#token = init.token;
    this.#requestSubject = init.requestSubject;
    this.#envelope = init.envelope;
    this.#inactivityTimeoutMs = init.inactivityTimeoutMs;
    this.#maxWaitMs = init.maxWaitMs;
    this.#signal = init.signal;
    this.threadId = init.threadId;
    this.rootId = init.rootId;
    this.spawnMarkerHeaders = init.spawnMarkerHeaders;
  }

  /**
   * Stop the underlying reply queue and end the stream cleanly.
   * Subsequent `for await` iterations over this stream exit without throwing.
   */
  cancel(): void {
    this.#cancelled = true;
    this.#queue?.end();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<StreamMessage> {
    if (this.#iterated) {
      throw new Error("PromptStream is single-use: a stream cannot be iterated more than once");
    }
    this.#iterated = true;
    if (this.#cancelled) return;
    if (this.#signal?.aborted) throw abortError(this.#signal);

    await this.#mux.start(); // idempotent; pays SUB+flush on the first prompt
    const queue = this.#mux.register(this.#token);
    this.#queue = queue;
    // cancel()/abort may have fired during the mux start await.
    if (this.#cancelled) {
      this.#mux.unregister(this.#token);
      return;
    }
    if (this.#signal?.aborted) {
      this.#mux.unregister(this.#token);
      throw abortError(this.#signal);
    }

    let onAbort: (() => void) | undefined;
    if (this.#signal) {
      onAbort = (): void => {
        this.#cancelled = true; // mark so we distinguish "closed by abort" vs "stalled"
        queue.end();
      };
      this.#signal.addEventListener("abort", onAbort, { once: true });
    }
    const maxWaitTimer = setTimeout(() => queue.end(), this.#maxWaitMs);
    maxWaitTimer.unref?.();

    try {
      this.#nc.publish(this.#requestSubject, encodeEnvelope(this.#envelope), {
        reply: this.#mux.replySubjectFor(this.#token),
      });
      const timed = withInactivityTimeout(
        queue,
        this.#inactivityTimeoutMs,
        () => new StreamStalledError(this.#inactivityTimeoutMs),
      );
      for await (const msg of timed) {
        if (this.#signal?.aborted) throw abortError(this.#signal);
        if (isErrorSignal(msg)) {
          throw buildServiceErrorFromMsg(msg);
        }
        if (isTerminator(msg)) {
          yield { type: "status", status: "done" };
          return;
        }
        let decoded: DecodedChunk | null;
        try {
          decoded = decodeChunk(msg.data);
        } catch {
          // Malformed chunk — §6.6 says drop unknown types silently. We
          // treat a malformed KNOWN chunk the same way: log would help
          // debugging but we don't want to take down the stream.
          continue;
        }
        if (!decoded) continue; // unknown `type` silently dropped per §6.6
        yield toStreamMessage(decoded, this.#nc);
      }
      // The terminator branch above always `return`s, so reaching here
      // means the queue drained without one: abort → throw its reason,
      // cancel() → exit cleanly, maxWait elapsed → StreamMaxWaitExceededError.
      if (this.#signal?.aborted) {
        throw abortError(this.#signal);
      }
      if (this.#cancelled) return;
      throw new StreamMaxWaitExceededError(this.#maxWaitMs);
    } finally {
      if (onAbort && this.#signal) this.#signal.removeEventListener("abort", onAbort);
      clearTimeout(maxWaitTimer);
      queue.end();
      this.#mux.unregister(this.#token);
      this.#queue = null;
    }
  }
}

function toStreamMessage(decoded: DecodedChunk, nc: NatsConnection): StreamMessage {
  switch (decoded.type) {
    case "response":
      return decoded.attachments !== undefined
        ? { type: "response", text: decoded.text, attachments: decoded.attachments }
        : { type: "response", text: decoded.text };
    case "status":
      return { type: "status", status: decoded.status };
    case "query":
      return buildQueryEvent(nc, {
        id: decoded.id,
        replySubject: decoded.replySubject,
        prompt: decoded.prompt,
        ...(decoded.attachments !== undefined ? { attachments: decoded.attachments } : {}),
      });
  }
}

function buildServiceErrorFromMsg(msg: Msg): ServiceError {
  const h = msg.headers;
  const codeStr = h?.get("Nats-Service-Error-Code") ?? "500";
  const code = Number(codeStr);
  const description = h?.get("Nats-Service-Error") ?? "";
  let body: ServiceErrorBody | undefined;
  if (msg.data.length > 0) {
    try {
      const parsed = JSON.parse(new TextDecoder().decode(msg.data)) as unknown;
      if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
        body = parsed as ServiceErrorBody;
      }
    } catch {
      /* non-JSON body is allowed per §9.1 — leave body undefined */
    }
  }
  return new ServiceError(Number.isFinite(code) ? code : 500, description, body);
}
