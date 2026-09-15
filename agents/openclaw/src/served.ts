/**
 * The `served` record — this plugin's binding of one prompt execution to
 * the OpenClaw session it ran in.
 *
 * An SDK-built agent stamps the thread on every model request it makes,
 * so those requests carry the thread the caller minted. A channel plugin
 * cannot do that to OpenClaw's provider requests, and OpenClaw sends no
 * session header of its own, so nothing on those calls names the thread.
 * The plugin publishes the binding instead: one record at `start`, stamped
 * with the prompt's arrival and naming the thread and `openclaw:<session
 * id>`, and one at `end` with the outcome. Between the two, the agent's
 * model calls belong to the thread.
 *
 * Both records are signed with `Agent-Sender` by the host identity and
 * carry one id as body `record_id`, header nonce and `Nats-Msg-Id`, like
 * the SDK's edge records, and count toward the same process-wide trace
 * record counts the service reports on its heartbeat. Publishing is
 * asynchronous and fail-open: a prompt is never delayed or failed by it.
 */

import { headers as createHeaders, type NatsConnection } from "@nats-io/nats-core";
import {
  AGENT_SENDER_HEADER,
  countTraceRecordDropped,
  countTraceRecordPublished,
  randomThreadId,
  serializeSenderHeader,
  signSenderHeader,
  type AgentId,
  type Logger,
  type SenderSigner,
  type TraceScope,
} from "@synadia-ai/agents";

// Bump every time the served record schema changes.
export const SERVED_RECORD_VERSION = 1;

/** The harness namespace this plugin's records live in. */
export const HARNESS = "openclaw";

export type ServedPhase = "start" | "end";
export type ServedStatus = "ok" | "error";

/** Longest accepted session id, in Unicode code points. */
export const SESSION_ID_MAX = 256;

// Same class the SDK uses for subjects: the session id ends up in a JSON
// field matched against what OpenClaw itself reports, so anything a
// session id could never be — empty, whitespace, control characters — is
// refused rather than published.
const FORBIDDEN =
  /[\u0000-\u0020\u007f\u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]/;

/** `true` iff `value` can be the session id a served record names. */
export function validSessionId(value: unknown): value is string {
  if (typeof value !== "string" || value.length === 0) return false;
  if (FORBIDDEN.test(value)) return false;
  return Array.from(value).length <= SESSION_ID_MAX;
}

/** Now, in unix seconds — the served record's `ts` resolution. */
export function unixSeconds(): number {
  return Math.floor(Date.now() / 1000);
}

/** One built served record: its wire bytes and the id that de-duplicates it. */
export interface BuiltServedRecord {
  readonly recordId: string;
  readonly payload: Uint8Array;
}

/**
 * One served record, ready to publish. The record id comes back alongside
 * the payload so the publisher can stamp it as `Nats-Msg-Id` and sign
 * with it as the `Agent-Sender` nonce. `agent` is the host identity, in
 * canonical `{account}.{user}` form — the identity that signs the record.
 * `status` is required on `end` and must be absent on `start`. `ts` is
 * unix seconds — when the prompt arrived for `start`, when the turn ended
 * for `end`.
 */
export function buildServedRecord(
  agent: AgentId,
  threadId: string,
  rootId: string,
  sessionId: string,
  phase: ServedPhase,
  status?: ServedStatus,
  ts: number = unixSeconds(),
): BuiltServedRecord {
  if (phase === "end" && status === undefined) {
    throw new Error("buildServedRecord: an end record needs a status");
  }
  if (phase === "start" && status !== undefined) {
    throw new Error("buildServedRecord: a start record carries no status");
  }
  if (!Number.isInteger(ts) || ts < 0) {
    throw new Error(
      "buildServedRecord: ts must be a non-negative integer of unix seconds",
    );
  }
  const recordId = randomThreadId();
  const record = {
    version: SERVED_RECORD_VERSION,
    kind: "served",
    record_id: recordId,
    ts,
    agent,
    thread_id: threadId,
    root_id: rootId,
    harness: HARNESS,
    harness_thread_id: `${HARNESS}:${sessionId}`,
    phase,
    ...(status !== undefined ? { status } : {}),
  };
  return {
    recordId,
    payload: new TextEncoder().encode(JSON.stringify(record)),
  };
}

export interface ServedPublisherOptions {
  readonly nc: NatsConnection;
  /** Where the records go — the SDK's trace subject. */
  readonly subject: string;
  /** The host's signer; without one nothing is published. */
  readonly signer: SenderSigner | undefined;
  /** The host identity, read per record: it is known only once the service started. */
  readonly identity: () => AgentId | undefined;
  readonly logger: Logger;
}

/**
 * One prompt's served pair. `bind` names the session once and publishes
 * `start` stamped with the prompt's arrival; `settle` records the outcome
 * when the turn ends and publishes `end` if the session is known. A
 * session bound after settling is refused: the pair is the turn's window,
 * not something to backfill.
 */
export interface ServedTurn {
  bind(sessionId: string): void;
  settle(status: ServedStatus): void;
}

const NATS_MSG_ID_HEADER = "Nats-Msg-Id";

export class ServedPublisher {
  readonly #options: ServedPublisherOptions;
  #unsignedWarned = false;

  constructor(options: ServedPublisherOptions) {
    this.#options = options;
  }

  /**
   * Start a prompt's pair. `scope` is the prompt's trace scope — the
   * ambient one inside the service's handler; `undefined` (an untraced
   * service) means no pair for this prompt.
   */
  beginTurn(scope: TraceScope | undefined): ServedTurn | undefined {
    if (scope === undefined) return undefined;
    const arrivedAt = unixSeconds();
    let sessionId: string | undefined;
    let settled = false;
    return {
      bind: (id: string): void => {
        if (settled) {
          this.#options.logger.warn(
            "served: session bound after the turn ended; no served record",
          );
          return;
        }
        if (sessionId !== undefined) {
          if (id !== sessionId) {
            this.#options.logger.warn(
              "served: session already bound for this prompt; ignored",
            );
          }
          return;
        }
        if (!validSessionId(id)) {
          this.#options.logger.warn(
            "served: unusable session id bound; no served record",
          );
          return;
        }
        sessionId = id;
        this.publish(scope, id, "start", undefined, arrivedAt);
      },
      settle: (status: ServedStatus): void => {
        if (settled) return;
        settled = true;
        if (sessionId !== undefined) {
          this.publish(scope, sessionId, "end", status, unixSeconds());
        }
      },
    };
  }

  /**
   * Publish one signed record. Fail-open and asynchronous. Without a
   * signer or a host identity nothing is published — an unsigned record
   * cannot be attributed — the publisher warns once, and the record counts
   * as dropped. Every record that goes out or fails to moves the
   * process-wide trace record counts.
   */
  publish(
    scope: TraceScope,
    sessionId: string,
    phase: ServedPhase,
    status: ServedStatus | undefined,
    ts: number,
  ): void {
    const { nc, subject, signer, logger } = this.#options;
    const id = this.#options.identity();
    if (signer === undefined || id === undefined) {
      countTraceRecordDropped();
      if (!this.#unsignedWarned) {
        this.#unsignedWarned = true;
        logger.warn(
          "served: no host identity signer; served records are not published " +
            "(an unsigned record cannot be attributed). Set senderIdentity to signed.",
        );
      }
      return;
    }
    void (async (): Promise<void> => {
      try {
        const record = buildServedRecord(
          id,
          scope.threadId,
          scope.rootId,
          sessionId,
          phase,
          status,
          ts,
        );
        const header = await signSenderHeader({
          signer,
          id,
          sub: subject,
          payload: record.payload,
          nonce: record.recordId,
        });
        const hdrs = createHeaders();
        hdrs.set(AGENT_SENDER_HEADER, serializeSenderHeader(header));
        hdrs.set(NATS_MSG_ID_HEADER, record.recordId);
        nc.publish(subject, record.payload, { headers: hdrs });
        countTraceRecordPublished();
      } catch (err) {
        countTraceRecordDropped();
        logger.warn("served: failed to publish served record", {
          subject,
          phase,
          reason: err instanceof Error ? err.message : String(err),
        });
      }
    })();
  }
}
