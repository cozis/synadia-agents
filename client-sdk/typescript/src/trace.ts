// Observability trace primitives.
//
// A **thread** is one prompt execution: one request published to
// `agents.prompt.{a}.{o}.{n}` and its §6 response stream. Its identity is
// *derived*, never exchanged: both ends already share the prompt's reply
// inbox (the caller mints it; the agent receives it as the request's reply
// subject), and it is unique per prompt, so a hash of it identifies the
// thread with zero extra wire surface.
//
// Mirrors the Python SDK's `synadia_ai/agents/trace.py` — the derivation
// and vocabulary are normative and must match byte-for-byte.

import { createHash, randomBytes } from "node:crypto";

// Length (hex chars) of derived thread ids. 16 hex chars = 64 bits —
// collision-safe for observability purposes and short enough to eyeball
// in HTTP header dumps.
export const THREAD_ID_HEX_LEN = 16;

const THREAD_ID_RE = /^[0-9a-f]{16}$/;

/** True iff `value` has the normative derived-thread-id shape. */
export function isThreadId(value: string): boolean {
  return THREAD_ID_RE.test(value);
}

/** Normative: lowercase hex of `sha256(replySubject)` (UTF-8), truncated. */
export function deriveThreadId(replySubject: string): string {
  return createHash("sha256")
    .update(replySubject, "utf8")
    .digest("hex")
    .slice(0, THREAD_ID_HEX_LEN);
}

export function randomThreadId(): string {
  return randomBytes(THREAD_ID_HEX_LEN / 2).toString("hex");
}
