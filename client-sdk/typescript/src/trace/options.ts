// Observability tracing — opt-in configuration (design:
// synadia-agent-fabric-docs/docs/observability.md).
//
// Tracing is off unless the caller passes `trace` to `Agents` (or an
// `AgentService` passes its config down). Omission means byte-identical
// protocol-0.3 prompts: no thread IDs minted, no lineage on the wire.

/**
 * Opt-in tracing configuration. Passing this object (even empty, for all
 * defaults) enables tracing on the client; omitting it disables tracing
 * entirely. Configuration fields (edge subject, delivery tuning) land as
 * the tracing feature is built out.
 */
export interface TraceOptions {
  /** Reserved — no configuration fields yet. */
  readonly _reserved?: never;
}
