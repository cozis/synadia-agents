// Claude Code-local implementation of the discover_agents, prompt_agent, and
// wait_for_reply tools. Kept here deliberately so model-tool policy is not
// part of the public SDK.

import { randomUUID } from "node:crypto";
import {
  bindActiveTrace,
  type Agent,
  type Agents,
  type TraceScope,
} from "@synadia-ai/agents";

const DEFAULT_QUERY_RESPONSE =
  "This caller cannot answer interactive queries; deny or continue without approval.";
const MAX_RETAINED_PROMPTS = 256;

export interface DiscoverAgentsInput {
  readonly agent?: string;
  readonly owner?: string;
  readonly name?: string;
  readonly session?: string;
  readonly timeout_ms?: number;
}

export interface PromptAgentInput {
  readonly instance_id: string;
  readonly prompt: string;
  /** Absolute lifetime of the remote request. Waiting is controlled separately. */
  readonly max_wait_ms?: number;
  readonly query_response?: string;
}

export interface WaitForReplyInput {
  readonly prompt_ids: readonly string[];
  /** Required. Zero performs a non-blocking poll. */
  readonly timeout_ms: number;
}

export interface PromptAgentOptions {
  readonly toolCallId?: string;
  readonly traceScope?: TraceScope;
  /** Called once when the background prompt settles and no waiter consumed it. */
  readonly onSettled?: (event: PromptSettledEvent) => void | Promise<void>;
}

type PromptState = "pending" | "completed" | "error";
type AgentDescription = ReturnType<typeof describeAgent>;

export interface PromptSettledEvent {
  readonly prompt_id: string;
  readonly state: Exclude<PromptState, "pending">;
}

type ManagedPrompt = {
  readonly promptId: string;
  readonly agent: AgentDescription;
  readonly createdAt: number;
  readonly controller: AbortController;
  readonly completion: Promise<void>;
  readonly finish: () => void;
  readonly statuses: string[];
  readonly queries: Array<{ id: string; prompt: string; response: string }>;
  readonly attachments: Array<{ filename: string; content_base64: string }>;
  readonly onSettled?: (event: PromptSettledEvent) => void | Promise<void>;
  state: PromptState;
  response: string;
  waiterCount: number;
  notificationDeferred: boolean;
  notifyOnSettle: boolean;
  error?: string;
  completedAt?: number;
};

export async function discoverAgents(
  client: Pick<Agents, "discover">,
  input: DiscoverAgentsInput = {},
) {
  const filter = {
    ...(input.agent !== undefined ? { agent: input.agent } : {}),
    ...(input.owner !== undefined ? { owner: input.owner } : {}),
    ...(input.name !== undefined ? { name: input.name } : {}),
    ...(input.session !== undefined ? { session: input.session } : {}),
  };
  const found = await client.discover({
    ...(input.timeout_ms !== undefined ? { timeoutMs: input.timeout_ms } : {}),
    ...(Object.keys(filter).length > 0 ? { filter } : {}),
  });
  return found.map(describeAgent);
}

export class AsyncPromptManager {
  readonly #prompts = new Map<string, ManagedPrompt>();

  async promptAgent(
    client: Pick<Agents, "lookupInstance">,
    input: PromptAgentInput,
    options: PromptAgentOptions = {},
  ) {
    if (options.traceScope !== undefined) {
      const { traceScope, ...promptOptions } = options;
      return bindActiveTrace(traceScope, () =>
        this.#promptAgentUnbound(client, input, promptOptions),
      );
    }
    return this.#promptAgentUnbound(client, input, options);
  }

  async waitForReply(input: WaitForReplyInput) {
    if (!Number.isInteger(input.timeout_ms) || input.timeout_ms < 0) {
      throw new Error("timeout_ms must be a non-negative integer");
    }
    if (!Array.isArray(input.prompt_ids) || input.prompt_ids.length === 0) {
      throw new Error("prompt_ids must contain at least one prompt id");
    }
    const uniqueIds = new Set(input.prompt_ids);
    if (uniqueIds.size !== input.prompt_ids.length) {
      throw new Error("prompt_ids must not contain duplicates");
    }

    const prompts = input.prompt_ids.map((id) => {
      const prompt = this.#prompts.get(id);
      if (!prompt) {
        throw new Error(`pending prompt ${JSON.stringify(id)} was not found`);
      }
      return prompt;
    });
    const completed = prompts.find((prompt) => prompt.state !== "pending");
    if (completed) {
      return { timed_out: false, ...snapshotPrompt(completed) };
    }
    if (input.timeout_ms === 0) return { timed_out: true };

    for (const prompt of prompts) prompt.waiterCount += 1;
    let settled: ManagedPrompt | undefined;
    try {
      settled = await waitUntilOneSettles(prompts, input.timeout_ms);
    } finally {
      for (const prompt of prompts) prompt.waiterCount -= 1;
      for (const prompt of prompts) {
        if (!prompt.notificationDeferred || prompt.waiterCount > 0) continue;
        if (prompt === settled) prompt.notificationDeferred = false;
        else this.#notifySettled(prompt);
      }
    }
    return settled
      ? { timed_out: false, ...snapshotPrompt(settled) }
      : { timed_out: true };
  }

  cancelAll(reason = "NATS channel shutting down"): void {
    for (const prompt of this.#prompts.values()) {
      if (prompt.state !== "pending") continue;
      prompt.state = "error";
      prompt.error = reason;
      prompt.completedAt = Date.now();
      prompt.notifyOnSettle = false;
      prompt.controller.abort(new Error(reason));
      prompt.finish();
    }
  }

  async #promptAgentUnbound(
    client: Pick<Agents, "lookupInstance">,
    input: PromptAgentInput,
    options: Omit<PromptAgentOptions, "traceScope">,
  ) {
    this.#makeRoom();
    const agent = await client.lookupInstance(input.instance_id);
    if (!agent) {
      throw new Error(
        `agent instance ${JSON.stringify(input.instance_id)} was not found`,
      );
    }

    const prompt = createManagedPrompt(agent, options.onSettled);
    const stream = await agent.prompt(input.prompt, {
      ...(input.max_wait_ms !== undefined
        ? { maxWaitMs: input.max_wait_ms }
        : {}),
      signal: prompt.controller.signal,
      ...(options.toolCallId !== undefined
        ? { toolCallId: options.toolCallId }
        : {}),
    });
    this.#prompts.set(prompt.promptId, prompt);

    const queryResponse = input.query_response ?? DEFAULT_QUERY_RESPONSE;
    void this.#collect(prompt, stream, queryResponse);
    return snapshotPrompt(prompt);
  }

  async #collect(
    prompt: ManagedPrompt,
    stream: Awaited<ReturnType<Agent["prompt"]>>,
    queryResponse: string,
  ): Promise<void> {
    try {
      for await (const message of stream) {
        if (prompt.state !== "pending") break;
        switch (message.type) {
          case "response":
            prompt.response += message.text;
            for (const attachment of message.attachments ?? []) {
              prompt.attachments.push({
                filename: attachment.filename,
                content_base64: attachment.content,
              });
            }
            break;
          case "status":
            prompt.statuses.push(message.status);
            break;
          case "query":
            await message.reply(queryResponse);
            prompt.queries.push({
              id: message.id,
              prompt: message.prompt,
              response: queryResponse,
            });
            break;
        }
      }
      if (prompt.state === "pending") prompt.state = "completed";
    } catch (error) {
      if (prompt.state === "pending") {
        prompt.state = "error";
        prompt.error = errorMessage(error);
      }
    } finally {
      if (prompt.completedAt === undefined) prompt.completedAt = Date.now();
      prompt.finish();
      if (prompt.notifyOnSettle && prompt.onSettled) {
        if (prompt.waiterCount > 0) prompt.notificationDeferred = true;
        else this.#notifySettled(prompt);
      }
    }
  }

  #notifySettled(prompt: ManagedPrompt): void {
    if (!prompt.onSettled || !prompt.notifyOnSettle) return;
    prompt.notificationDeferred = false;
    prompt.notifyOnSettle = false;
    const event: PromptSettledEvent = {
      prompt_id: prompt.promptId,
      state: prompt.state === "completed" ? "completed" : "error",
    };
    try {
      void Promise.resolve(prompt.onSettled(event)).catch(() => undefined);
    } catch {
      // A host notification is best-effort and must never change the prompt result.
    }
  }

  #makeRoom(): void {
    if (this.#prompts.size < MAX_RETAINED_PROMPTS) return;
    for (const [id, prompt] of this.#prompts) {
      if (prompt.state === "pending") continue;
      this.#prompts.delete(id);
      if (this.#prompts.size < MAX_RETAINED_PROMPTS) return;
    }
    throw new Error(`too many active prompts (maximum ${MAX_RETAINED_PROMPTS})`);
  }
}

function createManagedPrompt(
  agent: Agent,
  onSettled?: (event: PromptSettledEvent) => void | Promise<void>,
): ManagedPrompt {
  let finish!: () => void;
  const completion = new Promise<void>((resolve) => {
    finish = resolve;
  });
  return {
    promptId: randomUUID(),
    agent: describeAgent(agent),
    createdAt: Date.now(),
    controller: new AbortController(),
    completion,
    finish,
    state: "pending",
    response: "",
    statuses: [],
    queries: [],
    attachments: [],
    ...(onSettled ? { onSettled } : {}),
    waiterCount: 0,
    notificationDeferred: false,
    notifyOnSettle: true,
  };
}

async function waitUntilOneSettles(
  prompts: readonly ManagedPrompt[],
  timeoutMs: number,
): Promise<ManagedPrompt | undefined> {
  return new Promise<ManagedPrompt | undefined>((resolve) => {
    let settled = false;
    const finish = (completed?: ManagedPrompt) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(completed);
    };
    const timer = setTimeout(() => finish(), timeoutMs);
    void Promise.race(
      prompts.map((prompt) => prompt.completion.then(() => prompt)),
    ).then(finish);
  });
}

function snapshotPrompt(prompt: ManagedPrompt) {
  return {
    prompt_id: prompt.promptId,
    state: prompt.state,
    agent: prompt.agent,
    response: prompt.response,
    statuses: [...prompt.statuses],
    queries: [...prompt.queries],
    attachments: [...prompt.attachments],
    created_at: prompt.createdAt,
    ...(prompt.completedAt !== undefined
      ? { completed_at: prompt.completedAt }
      : {}),
    ...(prompt.error !== undefined ? { error: prompt.error } : {}),
  };
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function describeAgent(agent: Agent) {
  return {
    instance_id: agent.instanceId,
    agent: agent.agent,
    owner: agent.owner,
    name: agent.name,
    ...(agent.session !== undefined ? { session: agent.session } : {}),
    description: agent.description,
    version: agent.version,
    protocol_version: agent.protocolVersion,
    prompt_subject: agent.promptSubject,
    ...(agent.promptEndpoint.attachmentsOk !== undefined
      ? { attachments_ok: agent.promptEndpoint.attachmentsOk }
      : {}),
    ...(agent.minSenderTrust !== undefined
      ? { min_sender_trust: agent.minSenderTrust }
      : {}),
    ...(agent.identity !== undefined ? { identity: agent.identity } : {}),
    identity_verified: agent.idSigVerified,
  };
}
