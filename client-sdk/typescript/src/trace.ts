// Observability trace primitives.
//
// A **thread** is one prompt execution: one request published to
// `agents.prompt.{a}.{o}.{n}` and its §6 response stream. Its identity is
// *derived*, never exchanged: both ends already share the prompt's reply
// inbox (the caller mints it; the agent receives it as the request's reply
// subject), and it is unique per prompt, so a hash of it identifies the
// thread with zero extra wire surface.
//
// `rootId` is the thread id of the tree's *root* thread, forwarded down
// the tree via an optional envelope field (§5.6 tolerates unknown fields).
// It is always the hash — never the raw inbox subject, which would hand
// every descendant the root caller's live reply subject (an injection
// surface). Root test: a thread is root iff its rootId equals the hash of
// its own reply subject.
//
// Mirrors the Python SDK's `synadia_ai/agents/trace.py` — the derivation
// and vocabulary are normative and must match byte-for-byte.

import { AsyncLocalStorage } from "node:async_hooks";
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

// Normative header vocabulary — the Python SDK and any observing proxy
// reproduce these byte-for-byte.
export const HEADER_TRACE = "x-synadia-trace"; // "<root_id>:<thread_id>"
export const HEADER_SPAWNED = "x-synadia-spawned"; // comma-joined spawn entries (formatSpawnEntry)
export const HEADER_EVENT = "x-synadia-event"; // "spawn" tags the spawn-time marker request

export const IDENTITY_PATH_MARKER = "synadia"; // reserved leading segment of identityPath()

/** `synadia/<agent>/<owner>/<name>/<instanceId>` — base-URL path prefix
 * attributing a provider client's traffic to an agent (`-` when there is
 * no instance id). Normative. */
export function identityPath(opts: {
  readonly agent: string;
  readonly owner: string;
  readonly name: string;
  readonly instanceId?: string;
}): string {
  return `${IDENTITY_PATH_MARKER}/${opts.agent}/${opts.owner}/${opts.name}/${opts.instanceId || "-"}`;
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

/** One spawn-edge claim: `<child>:<toolCallId>:<edgeType>`. Normative.
 *
 * `toolCallId` defaults from the ambient {@link toolScope} and empty means
 * "no tool"; `edgeType` defaults to `tool_call`/`programmatic` to match.
 * The tool slot is percent-encoded; consumers decode it. */
export function formatSpawnEntry(
  childThreadId: string,
  toolCallId?: string,
  edgeType?: string,
): string {
  const tool = toolCallId || currentToolCallId() || "";
  const type = edgeType ?? (tool ? "tool_call" : "programmatic");
  return `${childThreadId}:${percentEncodeStrict(tool)}:${type}`;
}

// RFC 3986 with NO safe characters beyond unreserved — must match Python's
// `urllib.parse.quote(s, safe="")` byte-for-byte, so the `!'()*` set that
// encodeURIComponent leaves bare is escaped too.
function percentEncodeStrict(s: string): string {
  return encodeURIComponent(s).replace(
    /[!'()*]/g,
    (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase().padStart(2, "0"),
  );
}

/** Trace identity a parent thread forwards to a spawned prompt
 * (`PromptResponse.childTrace()` → `Agent.prompt(..., { trace })`). */
export interface TraceContext {
  readonly rootId: string;
}

// --- ambient (async-context) layer -------------------------------------
//
// Plain AsyncLocalStorage (the Node analogue of Python's contextvars)
// making correlation implicit where threading arguments through is
// impossible or noisy: the agent-sdk binds an ActiveTrace around each
// prompt handler, and Agent.prompt() consults it when no explicit trace
// is passed — a spawn inside a handler joins the parent's tree and
// records its edge automatically.

/** `(childThreadId) -> spawn-marker headers` — the parent stream's edge
 * recorder; runs in the spawner's context, so it resolves the ambient
 * tool scope itself. */
export type SpawnRecorder = (childThreadId: string) => Record<string, string>;

/** The prompt execution currently running in this async context. */
export interface ActiveTrace {
  readonly threadId: string;
  readonly rootId: string;
  readonly recordSpawn?: SpawnRecorder;
}

const activeTraceStorage = new AsyncLocalStorage<ActiveTrace>();
const toolCallStorage = new AsyncLocalStorage<string>();

/** The ambient {@link ActiveTrace}, or undefined outside a bound handler. */
export function activeTrace(): ActiveTrace | undefined {
  return activeTraceStorage.getStore();
}

/** The innermost ambient {@link toolScope} id, or undefined outside one. */
export function currentToolCallId(): string | undefined {
  return toolCallStorage.getStore();
}

/** Run `fn` with `trace` as the ambient context (the agent-sdk calls this
 * around each prompt-handler invocation). */
export function bindActiveTrace<T>(trace: ActiveTrace, fn: () => T): T {
  return activeTraceStorage.run(trace, fn);
}

/** Run `fn` as one tool invocation: ambient spawns inside it label their
 * edge with `toolCallId`. Scopes nest; the innermost wins. */
export function toolScope<T>(toolCallId: string, fn: () => T): T {
  return toolCallStorage.run(toolCallId, fn);
}
