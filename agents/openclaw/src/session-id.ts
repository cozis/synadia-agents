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
 * The store is read through the plugin runtime, which every supported
 * OpenClaw release provides in one of two shapes: `getSessionEntry`
 * (2026.8 and later, also the SQLite-backed store) or `loadSessionStore`
 * (older releases, the JSON store).
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
  const session = runtime.channel?.session;
  if (!session) return undefined;
  let entry: SessionEntryLike | undefined | null;
  if (typeof session.getSessionEntry === "function") {
    entry = session.getSessionEntry({
      agentId: route.agentId,
      sessionKey: route.sessionKey,
      storePath,
    });
  } else if (typeof session.loadSessionStore === "function") {
    entry = session.loadSessionStore(storePath)?.[route.sessionKey];
  } else {
    return undefined;
  }
  const id = entry?.sessionId;
  return typeof id === "string" && id.length > 0 ? id : undefined;
}
