// Ambient (async-local) trace context.
//
// The agent-sdk binds the execution's ActiveTrace around each prompt
// handler; nested client calls read it to inherit lineage without any
// plumbing through user code. AsyncLocalStorage flows into awaited work
// and tasks created inside the bound scope (Node and Bun).

import { AsyncLocalStorage } from "node:async_hooks";

/** Identity of the prompt execution running in the current async context. */
export interface ActiveTrace {
  /** This execution's own thread ID — the parent of any thread it spawns. */
  readonly threadId: string;
  /** The tree's root thread ID, inherited unchanged; equals `threadId` on a root. */
  readonly rootId: string;
}

const storage = new AsyncLocalStorage<ActiveTrace>();

/** The ambient {@link ActiveTrace}, or `undefined` outside a bound handler. */
export function activeTrace(): ActiveTrace | undefined {
  return storage.getStore();
}

/** Run `fn` with `trace` as the ambient execution (used by the agent-sdk). */
export function bindActiveTrace<T>(trace: ActiveTrace, fn: () => T): T {
  return storage.run(trace, fn);
}
