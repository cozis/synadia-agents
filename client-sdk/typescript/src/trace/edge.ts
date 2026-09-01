// Edge records — one per traced prompt, written by the caller before the
// prompt is sent (observability.md, Trace Log). This module grows into
// the real publisher (signing, queueing, acked delivery); for now it only
// fixes the call shape.

/** Fields of one edge record known so far; later commits add the rest. */
export interface EdgeFields {
  readonly threadId: string;
  readonly toolCallId?: string | undefined;
}

/**
 * Hand one edge record to the (future) publisher. Not implemented yet —
 * deliberately a no-op so call sites and tests can land first.
 */
export function sendEdgeRecord(_fields: EdgeFields): void {
  // Intentionally empty until the fire-and-forget publish lands.
}
