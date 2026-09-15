/**
 * The OpenClaw session a NATS prompt ran in — the harness thread id the
 * plugin's `served` records name as `openclaw:<session id>`.
 *
 * OpenClaw files every message of a conversation under one session: the
 * route's `sessionKey` names the conversation, the session store maps it
 * to the `sessionId` of the session currently bound to it — a new one
 * after `/new`, `/reset`, or a daily or idle reset. The id is therefore
 * only certain once OpenClaw has taken the prompt: the dispatch helper
 * returns the route and store it recorded the message in, and the store
 * is read then. Reading it any earlier could name a session the reset
 * policy is about to retire.
 *
 * The store is read through the plugin runtime, which the supported
 * OpenClaw releases expose in two shapes: `getSessionEntry` under the
 * runtime's `agent.session` namespace (2026.8 and later, also the
 * SQLite-backed store), or `loadSessionStore` under `channel.session`
 * (2026.5.4, the JSON store). Both namespaces are consulted for either
 * reader, so a release that moves one does not silently lose tracing.
 */

export interface OpenClawDispatchRoute {
  readonly agentId: string;
  readonly sessionKey: string;
}

type SessionEntryLike = { readonly sessionId?: unknown };

interface SessionRuntimeLike {
  readonly getSessionEntry?: (params: {
    agentId: string;
    sessionKey: string;
    storePath: string;
  }) => SessionEntryLike | undefined | null;
  readonly loadSessionStore?: (
    storePath: string,
  ) => Record<string, SessionEntryLike | undefined> | undefined | null;
}

/** The subset of the plugin runtime this lookup uses. */
export interface SessionRuntimeSource {
  readonly agent?: { readonly session?: SessionRuntimeLike };
  readonly channel?: { readonly session?: SessionRuntimeLike };
}

/**
 * The session id bound to `route.sessionKey` in the store at `storePath`,
 * or `undefined` when the runtime offers no session reader, the entry is
 * missing, or the id is blank — none of which is worth failing the
 * prompt over; the caller publishes no `served` records for it. A reader
 * that throws propagates so the caller can log why.
 */
export function resolveOpenClawSessionId(
  runtime: SessionRuntimeSource,
  route: OpenClawDispatchRoute,
  storePath: string,
): string | undefined {
  const readers = [runtime.agent?.session, runtime.channel?.session];
  for (const session of readers) {
    if (typeof session?.getSessionEntry === "function") {
      return sessionIdOf(
        session.getSessionEntry({
          agentId: route.agentId,
          sessionKey: route.sessionKey,
          storePath,
        }),
      );
    }
  }
  for (const session of readers) {
    if (typeof session?.loadSessionStore === "function") {
      return sessionIdOf(session.loadSessionStore(storePath)?.[route.sessionKey]);
    }
  }
  return undefined;
}

function sessionIdOf(entry: SessionEntryLike | undefined | null): string | undefined {
  const id = entry?.sessionId;
  return typeof id === "string" && id.length > 0 ? id : undefined;
}
