// Edge records — one per traced prompt, written by the caller before the
// prompt is sent (observability.md, Trace Log). Delivery is currently a
// plain fire-and-forget core publish; later commits add signing and the
// queued, acked best-effort publisher.

import { randomThreadId } from "./ids.js";

/** Edge record schema version. */
export const EDGE_RECORD_VERSION = 1;

/**
 * Default subject edge records are published to — the tenant-side short
 * form; the account's import qualifies it to `TRACE.{account}.edges`.
 */
export const DEFAULT_EDGE_SUBJECT = "TRACE.edges";

/** Per-spawn fields of one edge record (the writer's identity lands with signing). */
export interface EdgeFields {
  /** The spawned thread — minted by this caller. */
  readonly threadId: string;
  /** The tree's root; equals `threadId` when this spawn starts a tree. */
  readonly rootId: string;
  /** The calling execution's own thread, from the ambient trace; absent on a root. */
  readonly parentId?: string | undefined;
  readonly toolCallId?: string | undefined;
}

/** One built edge record: its wire bytes and the id that de-duplicates it. */
export interface BuiltEdgeRecord {
  readonly recordId: string;
  readonly payload: Uint8Array;
}

/**
 * Build one edge record. Nulls are explicit (a root's `parent_id` is
 * `null`, not omitted); `record_id` shares the 128-bit lowercase-hex
 * shape of thread IDs and travels as the JetStream `Nats-Msg-Id`, so a
 * retry after a lost ack is absorbed by the stream's duplicate window.
 * `ts` is unix seconds.
 */
export function buildEdgeRecord(fields: EdgeFields): BuiltEdgeRecord {
  const recordId = randomThreadId();
  const record = {
    version: EDGE_RECORD_VERSION,
    record_id: recordId,
    ts: Math.floor(Date.now() / 1000),
    thread_id: fields.threadId,
    parent_id: fields.parentId ?? null,
    root_id: fields.rootId,
    tool_call_id: fields.toolCallId ?? null,
  };
  return { recordId, payload: new TextEncoder().encode(JSON.stringify(record)) };
}
