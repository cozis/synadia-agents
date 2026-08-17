// `Agent` — a live handle returned by `Agents.discover()`. Carries the
// metadata parsed from `$SRV.INFO` (spec §4.3) and the `NatsConnection`
// needed to prompt it. Every public field is read-only; all selection is
// done inline by the caller via native `Array` / `Map.groupBy` / `filter`.

import type { NatsConnection } from "@nats-io/nats-core";
import type { AgentInfo } from "./discovery/agent-info.js";
import type { EndpointInfo } from "./discovery/endpoint-info.js";
import { combineAbortSignals } from "./internal/abort.js";
import { muxFor } from "./internal/mux.js";
import { normalizeAttachments } from "./prompt/attachments.js";
import { encodedEnvelopeSize, type RequestEnvelope } from "./prompt/envelope.js";
import { DEFAULT_PROMPT_MAX_WAIT_MS, type PromptOptions } from "./prompt/options.js";
import {
  assertAttachmentsAllowed,
  assertPromptNonEmpty,
  assertWithinMaxPayload,
} from "./prompt/validate.js";
import { PromptStream } from "./stream/prompt-stream.js";
import { activeTrace, deriveThreadId } from "./trace.js";

export class Agent {
  // Identity from $SRV.INFO metadata — always populated.
  readonly instanceId: string;
  readonly agent: string;
  readonly owner: string;
  readonly name: string;
  readonly session: string | undefined;
  readonly protocolVersion: string;
  readonly description: string;
  readonly version: string;

  // Prompt addressing + capability metadata.
  readonly promptEndpoint: EndpointInfo;
  readonly metadata: Readonly<Record<string, string>>;
  readonly endpoints: ReadonlyArray<EndpointInfo>;

  readonly #nc: NatsConnection;
  readonly #defaultInactivityTimeoutMs: number;
  readonly #closeSignal: AbortSignal | undefined;

  constructor(
    nc: NatsConnection,
    info: AgentInfo,
    defaultInactivityTimeoutMs: number,
    closeSignal: AbortSignal | undefined = undefined,
  ) {
    this.#nc = nc;
    this.#defaultInactivityTimeoutMs = defaultInactivityTimeoutMs;
    this.#closeSignal = closeSignal;
    this.instanceId = info.instanceId;
    this.agent = info.agent;
    this.owner = info.owner;
    this.name = info.name;
    this.session = info.session;
    this.protocolVersion = info.protocolVersion;
    this.description = info.description;
    this.version = info.version;
    this.promptEndpoint = info.promptEndpoint;
    this.metadata = info.metadata;
    this.endpoints = info.endpoints;
  }

  /** The prompt endpoint subject — taken verbatim from `$SRV.INFO` (§4.3). */
  get promptSubject(): string {
    return this.promptEndpoint.subject;
  }

  /** The `NatsConnection` this agent uses (shared with its `Agents`). */
  get connection(): NatsConnection {
    return this.#nc;
  }

  /**
   * Send a prompt (optionally with attachments) and return a
   * {@link PromptStream} to iterate the response.
   *
   * Errors rejected BEFORE any wire I/O:
   *   - {@link PromptEmptyError}             — empty prompt (§5.1).
   *   - {@link AttachmentsNotSupportedError} — `attachments_ok=false` (§5.4).
   *   - {@link PayloadTooLargeError}         — envelope exceeds `max_payload` (§5.4).
   *
   * Wire errors thrown from the iterator:
   *   - {@link ServiceError}              — `Nats-Service-Error-Code` header (§9.1).
   *   - {@link StreamStalledError}        — inactivity timeout (§6.6).
   *   - {@link StreamMaxWaitExceededError} — total response time exceeded
   *     `maxWaitMs` (default {@link DEFAULT_PROMPT_MAX_WAIT_MS}, 10 minutes)
   *     without seeing the wire terminator.
   */
  prompt(text: string | RequestEnvelope, opts: PromptOptions = {}): Promise<PromptStream> {
    const promptText = typeof text === "string" ? text : text.prompt;
    assertPromptNonEmpty(promptText);
    const attachmentInputs = opts.attachments ?? [];
    const baseAttachments = typeof text === "string" ? [] : (text.attachments ?? []);
    const hasAttachments = attachmentInputs.length > 0 || baseAttachments.length > 0;
    if (hasAttachments) {
      assertAttachmentsAllowed(true, this.promptEndpoint);
    }
    // Root resolution: explicit envelope field > explicit trace > new tree
    // rooted at this prompt (resolved against the minted thread id inside
    // #buildStream).
    const explicitRoot = typeof text === "string" ? undefined : text.rootId;

    // Fast path: text-only — max_payload check is sync.
    if (!hasAttachments) {
      return Promise.resolve(this.#buildStream({ prompt: promptText }, explicitRoot, opts));
    }

    // With attachments: load files, then check max_payload on the final encoded size.
    return (async (): Promise<PromptStream> => {
      const extra = await normalizeAttachments(attachmentInputs);
      const attachments = [...baseAttachments, ...extra];
      return this.#buildStream({ prompt: promptText, attachments }, explicitRoot, opts);
    })();
  }

  #buildStream(
    base: RequestEnvelope,
    explicitRoot: string | undefined,
    opts: PromptOptions,
  ): PromptStream {
    const signal = combineAbortSignals([opts.signal, this.#closeSignal]);
    // Mint the mux token up front (pure, no wire I/O) so the reply
    // subject — and the thread id derived from it — are known before
    // anything is published; the publish stays lazy inside the stream.
    const mux = muxFor(this.#nc);
    const token = mux.mintToken();
    const threadId = deriveThreadId(mux.replySubjectFor(token));
    // Root resolution: explicit envelope field > explicit trace > ambient
    // ActiveTrace > new tree. `ambient` survives iff the spawn joins the
    // ambient tree: a forwarded envelope naming the ambient root keeps its
    // edge, a foreign root records none, and trace is the explicit
    // manual-mode opt-out.
    let ambient = activeTrace();
    let rootId: string;
    if (explicitRoot !== undefined) {
      rootId = explicitRoot;
      if (ambient !== undefined && ambient.rootId !== explicitRoot) ambient = undefined;
    } else if (opts.trace !== undefined) {
      rootId = opts.trace.rootId;
      ambient = undefined;
    } else if (ambient !== undefined) {
      rootId = ambient.rootId;
    } else {
      rootId = threadId; // this prompt starts a new tree
    }
    const envelope: RequestEnvelope = { ...base, rootId };
    // §5.4: local validation happens synchronously BEFORE any wire I/O. The
    // caller's own broker may enforce a smaller max_payload than the agent
    // advertises — the validator picks the smaller (0/missing = undeclared).
    assertWithinMaxPayload(
      encodedEnvelopeSize(envelope),
      this.promptEndpoint,
      this.#nc.info?.max_payload,
    );
    // Auto-record the ambient spawn edge only after §5.4 validation — a
    // prompt that never publishes must not leave a phantom edge. Recorded
    // before iteration though: a never-iterated stream leaves a stale
    // claim (accepted — edges are fail-open telemetry).
    const spawnMarkerHeaders = ambient?.recordSpawn?.(threadId);
    return new PromptStream({
      nc: this.#nc,
      mux,
      token,
      requestSubject: this.promptEndpoint.subject,
      envelope,
      inactivityTimeoutMs: opts.inactivityTimeoutMs ?? this.#defaultInactivityTimeoutMs,
      maxWaitMs: opts.maxWaitMs ?? DEFAULT_PROMPT_MAX_WAIT_MS,
      signal,
      threadId,
      rootId,
      spawnMarkerHeaders,
    });
  }
}
