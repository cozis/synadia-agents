// Observability tracing — opt-in configuration (design:
// synadia-agent-fabric-docs/docs/observability.md).
//
// Tracing is off unless the caller passes `trace` to `Agents` (or an
// `AgentService` passes its config down). Omission means byte-identical
// protocol-0.3 prompts: no thread IDs minted, no lineage on the wire.

import type { EdgePublisherOptions } from "./publisher.js";

/**
 * Opt-in tracing configuration. Passing this object (even empty, for all
 * defaults) enables tracing on the client; omitting it disables tracing
 * entirely. Delivery-tuning fields land as the feature is built out.
 */
export interface TraceOptions {
  /**
   * Subject edge records are published to. Default `"TRACE.edges"` (the
   * tenant-side short form; the account's import qualifies it). Pass
   * `null` for propagate-only mode: mint IDs and forward lineage, but
   * publish no edge records.
   */
  readonly edgeSubject?: string | null;
  /**
   * Delivery tuning for the background edge publisher — queue capacity,
   * ack timeout, and the retry backoff. Defaults are in
   * `trace/publisher.ts`; a deployment rarely needs to change them.
   */
  readonly delivery?: EdgePublisherOptions;
}
