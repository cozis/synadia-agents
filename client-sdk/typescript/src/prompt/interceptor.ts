// Prompt interceptors — the caller-side hook around `Agent.prompt()`.
//
// An interceptor runs before a prompt is published, once per prompt, in
// the order the client lists them. It sees the target agent, the prompt
// text and the opaque `context` the caller passed in `PromptOptions`, and
// may publish messages of its own first — signed with the prompting
// client's identity, through `ctx.identity` — before it returns extra
// envelope fields and extra headers for the prompt to carry, or nothing.
//
// The SDK gives those fields and headers no meaning. §5.6 obliges a
// receiver to tolerate unknown top-level envelope fields; a host built on
// `@synadia-ai/agent-service` reads them back from `RequestEnvelope.extras`
// and the request's headers in its own request interceptors.
//
// When it runs: at publish time — on the stream's first iteration, after
// the prompt's sender identity is resolved and before its `Agent-Sender`
// header is signed over the final envelope — so a prompt that is never
// iterated, or that fails validation, runs no interceptor. It runs in the
// async context `prompt()` was called in, not the one the stream happens
// to be iterated in, so an interceptor that reads an AsyncLocalStorage
// sees the caller's value.

import type { NatsConnection } from "@nats-io/nats-core";
import type { Agent } from "../agent.js";
import type { AgentId } from "../identity/agent-id.js";
import type { SignedPublishOptions } from "../identity/signed-publish.js";

/**
 * Signing with the prompting client's identity — the one that signs the
 * prompt's own `Agent-Sender` header. The same rules as the `Agents`
 * methods of the same names.
 */
export interface PromptSigning {
  /** `true` iff a signer is configured, so {@link publishSigned} can sign. */
  readonly canSign: boolean;
  /** The client's own agent ID (`{account}.{user}`), as `Agents.selfId()`. */
  selfId(): Promise<AgentId>;
  /**
   * Sign and publish one message, as `Agents.publishSigned()`: its
   * `Agent-Sender` header, and `Nats-Msg-Id` set to the nonce. Pass
   * `opts.nonce` for a body that carries its own id.
   */
  publishSigned(
    subject: string,
    payload: Uint8Array | string,
    opts?: SignedPublishOptions,
  ): Promise<void>;
}

/** What a {@link PromptInterceptor} sees. */
export interface PromptInterceptorContext {
  /** The agent the prompt is addressed to. */
  readonly agent: Agent;
  /** The prompt text. */
  readonly prompt: string;
  /** `PromptOptions.context`, verbatim; `{}` when the caller passed none. */
  readonly context: Readonly<Record<string, unknown>>;
  /** The connection the prompt goes out on. */
  readonly connection: NatsConnection;
  /** Signing with the prompting client's identity. */
  readonly identity: PromptSigning;
}

/** What a {@link PromptInterceptor} adds to the prompt. */
export interface PromptExtras {
  /**
   * Extra top-level envelope fields, by wire name. `prompt` and
   * `attachments` belong to the protocol and are refused.
   */
  readonly fields?: Readonly<Record<string, unknown>>;
  /** Extra message headers. `Agent-Sender` belongs to the SDK and is refused. */
  readonly headers?: Readonly<Record<string, string>>;
}

/**
 * A caller-side hook that runs before each prompt is published. A throw
 * fails the prompt: it surfaces from the stream's first iteration, and
 * the prompt is not sent.
 */
export interface PromptInterceptor {
  beforePrompt(ctx: PromptInterceptorContext): PromptExtras | void | Promise<PromptExtras | void>;
}
