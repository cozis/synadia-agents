// Ambient (async-local) trace context.
//
// The agent-sdk binds the execution's ActiveTrace around each prompt
// handler; nested client calls read it to inherit lineage without any
// plumbing through user code. AsyncLocalStorage flows into awaited work
// and tasks created inside the bound scope (Node and Bun).
//
// The binding also carries the service's tracing configuration, so an
// agent configured once passes tracing down to every client it uses
// inside the handler.

import { AsyncLocalStorage } from "node:async_hooks";
import type { TraceOptions } from "./options.js";

/** Identity of the prompt execution running in the current async context. */
export interface ActiveTrace {
  /** This execution's own thread ID — the parent of any thread it spawns. */
  readonly threadId: string;
  /** The tree's root thread ID, inherited unchanged; equals `threadId` on a root. */
  readonly rootId: string;
}

interface TraceScope {
  readonly trace: ActiveTrace;
  readonly options: TraceOptions | undefined;
  /** Model-call count for this execution — mutable, shared by the scope. */
  readonly turns: { count: number };
}

const storage = new AsyncLocalStorage<TraceScope>();

/** The ambient {@link ActiveTrace}, or `undefined` outside a bound handler. */
export function activeTrace(): ActiveTrace | undefined {
  return storage.getStore()?.trace;
}

/**
 * Tracing configuration handed down by the enclosing `AgentService`, or
 * `undefined` when the service has none (or outside a handler). A client
 * with no configuration of its own inherits this one.
 */
export function inheritedTraceOptions(): TraceOptions | undefined {
  return storage.getStore()?.options;
}

/**
 * Run `fn` with `trace` as the ambient execution (used by the agent-sdk).
 * `options`, when given, is the service's tracing configuration and is
 * inherited by clients used inside `fn`.
 */
export function bindActiveTrace<T>(
  trace: ActiveTrace,
  fn: () => T,
  options: TraceOptions | undefined = undefined,
): T {
  return storage.run({ trace, options, turns: { count: 0 } }, fn);
}

/**
 * Model calls this execution has made so far, counted by `traceHeaders()`;
 * `0` outside a bound handler. A prompt spawned now carries this as its
 * edge record's `turn_count_hint`.
 */
export function activeTurnCount(): number {
  return storage.getStore()?.turns.count ?? 0;
}

/** Count one model call against the ambient execution; no-op outside one. */
export function countTurn(): void {
  const scope = storage.getStore();
  if (scope !== undefined) scope.turns.count += 1;
}
