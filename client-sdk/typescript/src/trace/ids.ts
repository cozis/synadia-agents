// Thread identifiers for observability tracing (design:
// synadia-agent-fabric-docs/docs/observability.md).
//
// A thread ID names one prompt execution. It is minted by the caller at
// `prompt()` time — 128-bit random, 32 lowercase hex chars — and the
// receiving service adopts it. `root_id` shares the same shape (it IS a
// thread ID: the tree's first execution).

/** Length in hex characters of a thread ID — 128 bits. */
export const THREAD_ID_HEX_LEN = 32;

const THREAD_ID_RE = /^[0-9a-f]{32}$/;

/** `true` iff `value` has the normative thread-ID shape (32 lowercase hex). */
export function isThreadId(value: string): boolean {
  return THREAD_ID_RE.test(value);
}

/** Mint a fresh 128-bit random thread ID. */
export function randomThreadId(): string {
  const bytes = new Uint8Array(THREAD_ID_HEX_LEN / 2);
  crypto.getRandomValues(bytes);
  let out = "";
  for (const b of bytes) out += b.toString(16).padStart(2, "0");
  return out;
}
