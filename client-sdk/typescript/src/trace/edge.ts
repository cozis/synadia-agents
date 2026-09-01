// Edge records — one per traced prompt, written by the caller before the
// prompt is sent (observability.md, Trace Log). This module grows into
// the real publisher (signing, queueing, acked delivery); for now it only
// fixes the call shape.

/** Fields of one edge record known so far; later commits add the rest. */
export interface EdgeFields {
  /** The spawned thread — minted by this caller. */
  readonly threadId: string;
  /** The tree's root; equals `threadId` when this spawn starts a tree. */
  readonly rootId: string;
  /** The calling execution's own thread, from the ambient trace; absent on a root. */
  readonly parentId?: string | undefined;
  readonly toolCallId?: string | undefined;
}

/**
 * Hand one edge record to the (future) publisher. Not implemented yet —
 * deliberately a no-op so call sites and tests can land first.
 */
export function sendEdgeRecord(_fields: EdgeFields): void {
  // Intentionally empty until the fire-and-forget publish lands.
}
