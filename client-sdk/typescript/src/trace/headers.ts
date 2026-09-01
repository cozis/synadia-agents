// Trace headers for model calls.
//
// An agent stamps these on every completion request it issues, so the
// model proxy can file the call under the right thread and tree without
// seeing any NATS traffic (observability.md, Model Proxy). The proxy
// needs no parent or tool-call header — hierarchy is the edge records'
// job.

import { activeTrace, type ActiveTrace } from "./context.js";

/** The execution's own thread ID. */
export const HEADER_THREAD_ID = "X-Synadia-Thread-ID";
/** The tree's root thread ID; equals the thread ID on a root. */
export const HEADER_ROOT_ID = "X-Synadia-Root-ID";

/** Headers naming `trace` — attach to every model request of that execution. */
export function formatTraceHeaders(trace: ActiveTrace): Record<string, string> {
  return {
    [HEADER_THREAD_ID]: trace.threadId,
    [HEADER_ROOT_ID]: trace.rootId,
  };
}

/**
 * {@link formatTraceHeaders} for the ambient execution; `{}` outside a
 * prompt handler, so harness code that builds its HTTP client deep inside
 * a tool needs no plumbing and degrades to nothing when untraced.
 */
export function traceHeaders(): Record<string, string> {
  const trace = activeTrace();
  return trace !== undefined ? formatTraceHeaders(trace) : {};
}
