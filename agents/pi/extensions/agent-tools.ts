// PI-local implementation of the discover_agents, prompt_agent,
// list_pending_prompts, wait_for_prompt, and cancel_prompts tools. Model-tool
// policy deliberately stays out of the public SDK.

import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, extname, join } from "node:path";
import {
  StreamMaxWaitExceededError,
  bindActiveTrace,
  decodeStrictBase64,
  type Agent,
  type Agents,
  type AttachmentInput,
  type StreamMessage,
  type TraceScope,
} from "@synadia-ai/agents";

const DEFAULT_QUERY_RESPONSE =
  "This caller cannot answer interactive queries; deny or continue without approval.";
export const DEFAULT_MAX_TRACKED_PROMPTS = 256;

export interface DiscoverAgentsInput {
  readonly agent?: string;
  readonly owner?: string;
  readonly name?: string;
  readonly session?: string;
  readonly timeout_ms?: number;
}

export interface PromptAttachmentInput {
  readonly path: string;
  readonly filename?: string;
}

export interface PromptAgentInput {
  readonly instance_id: string;
  readonly label: string;
  readonly text: string;
  readonly attachments?: readonly PromptAttachmentInput[];
  /** Absolute lifetime of the remote request. Waiting is controlled separately. */
  readonly max_runtime_ms?: number;
  readonly query_response?: string;
}

export interface WaitForPromptInput {
  readonly prompt_ids: readonly string[];
  /** Required. Zero performs a non-blocking poll. */
  readonly timeout_ms: number;
}

export interface CancelPromptsInput {
  readonly prompt_ids: readonly string[];
}

export interface PromptAgentOptions {
  readonly toolCallId?: string;
  readonly traceScope?: TraceScope;
  /** Called once when background work settles and no active waiter receives it. */
  readonly onSettled?: (event: PromptSettledEvent) => void | Promise<void>;
}

export interface AsyncPromptManagerOptions {
  readonly maxTrackedPrompts?: number;
}

export type PromptState =
  "pending" | "completed" | "failed" | "cancelled" | "expired";

export interface PromptSettledEvent {
  readonly event: "agent_prompt_finished";
  readonly prompt_id: string;
  readonly state: Exclude<PromptState, "pending">;
}

export class PromptToolError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "PromptToolError";
  }

  toJSON() {
    return { code: this.code, message: this.message };
  }
}

type StoredAttachment = {
  readonly filename: string;
  readonly path: string;
  readonly size_bytes: number;
};

type PromptError = { readonly code: string; readonly message: string };

type ManagedPrompt = {
  readonly promptId: string;
  readonly label: string;
  readonly targetInstanceId: string;
  readonly createdAt: number;
  readonly controller: AbortController;
  readonly completion: Promise<void>;
  readonly finish: () => void;
  readonly attachments: StoredAttachment[];
  readonly onSettled?: (event: PromptSettledEvent) => void | Promise<void>;
  state: PromptState;
  responseText: string;
  waiterCount: number;
  notificationDeferred: boolean;
  notifyOnSettle: boolean;
  error?: PromptError;
  finishedAt?: number;
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
  readonly #startingControllers = new Set<AbortController>();
  readonly #maxTrackedPrompts: number;
  #nextPromptNumber = 1;
  #startingPrompts = 0;
  #generation = 0;
  #attachmentRoot: string | undefined;

  constructor(options: AsyncPromptManagerOptions = {}) {
    const maximum = options.maxTrackedPrompts ?? DEFAULT_MAX_TRACKED_PROMPTS;
    if (!Number.isInteger(maximum) || maximum < 1) {
      throw new TypeError("maxTrackedPrompts must be a positive integer");
    }
    this.#maxTrackedPrompts = maximum;
  }

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

  listPendingPrompts() {
    return [...this.#prompts.values()]
      .filter((prompt) => prompt.state === "pending")
      .sort((left, right) => left.createdAt - right.createdAt)
      .map(promptDescriptor);
  }

  async waitForPrompt(input: WaitForPromptInput) {
    validatePromptIds(input.prompt_ids);
    if (!Number.isInteger(input.timeout_ms) || input.timeout_ms < 0) {
      throw new PromptToolError(
        "invalid_argument",
        "timeout_ms must be a non-negative integer",
      );
    }

    const prompts = input.prompt_ids.map((id) => this.#requirePrompt(id));
    const alreadyFinished = oldestTerminal(prompts);
    if (alreadyFinished) {
      return promptResult(alreadyFinished, input.prompt_ids);
    }
    if (input.timeout_ms === 0) {
      return {
        type: "timeout" as const,
        pending_prompt_ids: [...input.prompt_ids],
      };
    }

    for (const prompt of prompts) prompt.waiterCount += 1;
    let returned: ManagedPrompt | undefined;
    try {
      await waitUntilOneSettles(prompts, input.timeout_ms);
      returned = oldestTerminal(prompts);
    } finally {
      for (const prompt of prompts) prompt.waiterCount -= 1;
      for (const prompt of prompts) {
        if (!prompt.notificationDeferred || prompt.waiterCount > 0) continue;
        if (prompt === returned) prompt.notificationDeferred = false;
        else this.#notifySettled(prompt);
      }
    }

    return returned
      ? promptResult(returned, input.prompt_ids)
      : {
          type: "timeout" as const,
          pending_prompt_ids: [...input.prompt_ids],
        };
  }

  cancelPrompts(input: CancelPromptsInput) {
    validatePromptIds(input.prompt_ids);
    return input.prompt_ids.map((id) => {
      const prompt = this.#prompts.get(id);
      if (!prompt) return { prompt_id: id, outcome: "not_found" as const };
      if (prompt.state !== "pending") {
        return {
          prompt_id: id,
          outcome: "already_terminal" as const,
          state: prompt.state,
        };
      }

      prompt.notifyOnSettle = false;
      prompt.state = "cancelled";
      prompt.finishedAt = Date.now();
      prompt.controller.abort(new Error("Prompt cancelled"));
      prompt.finish();
      return { prompt_id: id, outcome: "cancelled" as const };
    });
  }

  cancelAll(): void {
    this.#generation += 1;
    for (const controller of this.#startingControllers) {
      controller.abort(new Error("NATS channel shutting down"));
    }
    for (const prompt of this.#prompts.values()) {
      prompt.notifyOnSettle = false;
      if (prompt.state === "pending") {
        prompt.state = "cancelled";
        prompt.finishedAt = Date.now();
        prompt.controller.abort(new Error("NATS channel shutting down"));
        prompt.finish();
      }
    }
    this.#prompts.clear();
    if (this.#attachmentRoot) {
      rmSync(this.#attachmentRoot, { recursive: true, force: true });
      this.#attachmentRoot = undefined;
    }
  }

  async #promptAgentUnbound(
    client: Pick<Agents, "lookupInstance">,
    input: PromptAgentInput,
    options: Omit<PromptAgentOptions, "traceScope">,
  ) {
    validatePromptInput(input);
    this.#reserveSlot();
    const generation = this.#generation;
    let reserved = true;
    let prompt: ManagedPrompt | undefined;
    let controller: AbortController | undefined;
    try {
      const agent = await client.lookupInstance(input.instance_id);
      if (!agent) {
        throw new PromptToolError(
          "agent_not_found",
          `agent instance ${JSON.stringify(input.instance_id)} was not found`,
        );
      }

      const attachmentInputs = loadInputAttachments(input.attachments);
      controller = new AbortController();
      this.#startingControllers.add(controller);
      const stream = await agent.prompt(input.text, {
        ...(attachmentInputs ? { attachments: attachmentInputs } : {}),
        ...(input.max_runtime_ms !== undefined
          ? { maxWaitMs: input.max_runtime_ms }
          : {}),
        signal: controller.signal,
        ...(options.toolCallId !== undefined
          ? { toolCallId: options.toolCallId }
          : {}),
      });

      prompt = createManagedPrompt(
        `p${this.#nextPromptNumber++}`,
        input,
        controller,
        options.onSettled,
      );
      const queryResponse = input.query_response ?? DEFAULT_QUERY_RESPONSE;
      const iterator = stream[Symbol.asyncIterator]();

      // The mandatory leading ack proves that the target accepted the prompt.
      // Do not hand the model a handle for a request rejected before that point.
      const first = await iterator.next();
      if (generation !== this.#generation) {
        throw new PromptToolError(
          "prompt_cancelled",
          "The agent session ended before the target accepted the prompt",
        );
      }
      if (first.done) this.#settle(prompt, "completed");
      else await this.#handleMessage(prompt, first.value, queryResponse);

      this.#startingControllers.delete(controller);
      this.#startingPrompts -= 1;
      reserved = false;
      this.#prompts.set(prompt.promptId, prompt);
      if (prompt.state === "pending") {
        void this.#collect(prompt, iterator, queryResponse);
      }
      return promptDescriptor(prompt);
    } catch (error) {
      if (controller) this.#startingControllers.delete(controller);
      if (prompt) {
        this.#removePromptFiles(prompt.promptId);
        prompt.controller.abort(error);
      }
      throw error;
    } finally {
      if (reserved) this.#startingPrompts -= 1;
    }
  }

  async #collect(
    prompt: ManagedPrompt,
    iterator: AsyncIterator<StreamMessage>,
    queryResponse: string,
  ): Promise<void> {
    try {
      for (;;) {
        const next = await iterator.next();
        if (next.done || prompt.state !== "pending") break;
        await this.#handleMessage(prompt, next.value, queryResponse);
      }
      if (prompt.state === "pending") this.#settle(prompt, "completed");
    } catch (error) {
      if (prompt.state !== "pending") return;
      if (error instanceof StreamMaxWaitExceededError) {
        this.#settle(prompt, "expired", {
          code: "deadline_exceeded",
          message: "The prompt exceeded its maximum runtime",
        });
      } else {
        this.#settle(prompt, "failed", {
          code: "remote_error",
          message: errorMessage(error),
        });
      }
    }
  }

  async #handleMessage(
    prompt: ManagedPrompt,
    message: StreamMessage,
    queryResponse: string,
  ): Promise<void> {
    switch (message.type) {
      case "response":
        prompt.responseText += message.text;
        for (const attachment of message.attachments ?? []) {
          prompt.attachments.push(
            this.#storeAttachment(
              prompt.promptId,
              attachment.filename,
              attachment.content,
              prompt.attachments.length,
            ),
          );
        }
        break;
      case "query":
        await message.reply(queryResponse);
        break;
      case "status":
        break;
    }
  }

  #settle(
    prompt: ManagedPrompt,
    state: Exclude<PromptState, "pending">,
    error?: PromptError,
  ): void {
    if (prompt.state !== "pending") return;
    prompt.state = state;
    prompt.finishedAt = Date.now();
    if (error) prompt.error = error;
    prompt.finish();
    if (!prompt.notifyOnSettle || !prompt.onSettled) return;
    if (prompt.waiterCount > 0) prompt.notificationDeferred = true;
    else this.#notifySettled(prompt);
  }

  #notifySettled(prompt: ManagedPrompt): void {
    if (
      !prompt.onSettled ||
      !prompt.notifyOnSettle ||
      prompt.state === "pending"
    ) {
      return;
    }
    prompt.notificationDeferred = false;
    prompt.notifyOnSettle = false;
    const event: PromptSettledEvent = {
      event: "agent_prompt_finished",
      prompt_id: prompt.promptId,
      state: prompt.state,
    };
    try {
      void Promise.resolve(prompt.onSettled(event)).catch(() => undefined);
    } catch {
      // Host notifications are best-effort and never alter the stored result.
    }
  }

  #reserveSlot(): void {
    while (
      this.#prompts.size + this.#startingPrompts >=
      this.#maxTrackedPrompts
    ) {
      const oldest = oldestTerminal([...this.#prompts.values()]);
      if (!oldest) {
        throw new PromptToolError(
          "prompt_limit_reached",
          `The session is already tracking ${this.#maxTrackedPrompts} pending prompts`,
        );
      }
      this.#prompts.delete(oldest.promptId);
      this.#removePromptFiles(oldest.promptId);
    }
    this.#startingPrompts += 1;
  }

  #requirePrompt(id: string): ManagedPrompt {
    const prompt = this.#prompts.get(id);
    if (!prompt) {
      throw new PromptToolError(
        "prompt_not_found",
        `Prompt ${id} was not found; it may have been evicted`,
      );
    }
    return prompt;
  }

  #storeAttachment(
    promptId: string,
    remoteFilename: string,
    base64: string,
    index: number,
  ): StoredAttachment {
    const root = (this.#attachmentRoot ??= mkdtempSync(
      join(tmpdir(), "synadia-agent-prompts-"),
    ));
    const directory = join(root, promptId);
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    const filename = uniqueFilename(
      directory,
      sanitizeFilename(remoteFilename, index),
    );
    const bytes = decodeStrictBase64(base64);
    const path = join(directory, filename);
    writeFileSync(path, bytes, { flag: "wx", mode: 0o600 });
    return { filename, path, size_bytes: bytes.byteLength };
  }

  #removePromptFiles(promptId: string): void {
    if (!this.#attachmentRoot) return;
    rmSync(join(this.#attachmentRoot, promptId), {
      recursive: true,
      force: true,
    });
  }
}

function createManagedPrompt(
  promptId: string,
  input: PromptAgentInput,
  controller: AbortController,
  onSettled?: (event: PromptSettledEvent) => void | Promise<void>,
): ManagedPrompt {
  let finish!: () => void;
  const completion = new Promise<void>((resolve) => {
    finish = resolve;
  });
  return {
    promptId,
    label: input.label,
    targetInstanceId: input.instance_id,
    createdAt: Date.now(),
    controller,
    completion,
    finish,
    state: "pending",
    responseText: "",
    attachments: [],
    ...(onSettled ? { onSettled } : {}),
    waiterCount: 0,
    notificationDeferred: false,
    notifyOnSettle: true,
  };
}

function promptDescriptor(prompt: ManagedPrompt) {
  return {
    prompt_id: prompt.promptId,
    label: prompt.label,
    state: prompt.state,
    target_instance_id: prompt.targetInstanceId,
    created_at_ms: prompt.createdAt,
  };
}

function promptResult(prompt: ManagedPrompt, requestedIds: readonly string[]) {
  return {
    type: "prompt_result" as const,
    prompt_id: prompt.promptId,
    label: prompt.label,
    state: prompt.state,
    target_instance_id: prompt.targetInstanceId,
    response_text: prompt.responseText,
    attachments: [...prompt.attachments],
    created_at_ms: prompt.createdAt,
    finished_at_ms: prompt.finishedAt!,
    remaining_prompt_ids: requestedIds.filter((id) => id !== prompt.promptId),
    ...(prompt.error ? { error: prompt.error } : {}),
  };
}

function validatePromptInput(input: PromptAgentInput): void {
  if (typeof input.instance_id !== "string" || input.instance_id.length === 0) {
    throw new PromptToolError(
      "invalid_argument",
      "instance_id must be a non-empty string",
    );
  }
  if (typeof input.label !== "string" || input.label.length === 0) {
    throw new PromptToolError(
      "invalid_argument",
      "label must be a non-empty string",
    );
  }
  if (typeof input.text !== "string" || input.text.length === 0) {
    throw new PromptToolError(
      "invalid_argument",
      "text must be a non-empty string",
    );
  }
  if (
    input.max_runtime_ms !== undefined &&
    (!Number.isInteger(input.max_runtime_ms) || input.max_runtime_ms < 1)
  ) {
    throw new PromptToolError(
      "invalid_argument",
      "max_runtime_ms must be a positive integer",
    );
  }
  if (input.attachments !== undefined && !Array.isArray(input.attachments)) {
    throw new PromptToolError(
      "invalid_argument",
      "attachments must be an array",
    );
  }
}

function validatePromptIds(ids: readonly string[]): void {
  if (!Array.isArray(ids) || ids.length === 0) {
    throw new PromptToolError(
      "invalid_argument",
      "prompt_ids must contain at least one prompt id",
    );
  }
  if (ids.some((id) => typeof id !== "string" || id.length === 0)) {
    throw new PromptToolError(
      "invalid_argument",
      "prompt_ids must contain non-empty strings",
    );
  }
  if (new Set(ids).size !== ids.length) {
    throw new PromptToolError(
      "invalid_argument",
      "prompt_ids must not contain duplicates",
    );
  }
}

function loadInputAttachments(
  inputs: readonly PromptAttachmentInput[] | undefined,
): readonly AttachmentInput[] | undefined {
  if (!inputs || inputs.length === 0) return undefined;
  return inputs.map((input, index) => {
    if (!input || typeof input.path !== "string" || input.path.length === 0) {
      throw new PromptToolError(
        "attachment_error",
        `attachment #${index + 1} must have a non-empty path`,
      );
    }
    try {
      if (!statSync(input.path).isFile()) {
        throw new Error("path is not a regular file");
      }
      const filename = input.filename ?? basename(input.path);
      assertSafeInputFilename(filename, index);
      const buffer = readFileSync(input.path);
      return {
        filename,
        content: new Uint8Array(
          buffer.buffer,
          buffer.byteOffset,
          buffer.byteLength,
        ),
      };
    } catch (error) {
      if (error instanceof PromptToolError) throw error;
      throw new PromptToolError(
        "attachment_error",
        `cannot read attachment ${JSON.stringify(input.path)}: ${errorMessage(error)}`,
      );
    }
  });
}

function assertSafeInputFilename(filename: string, index: number): void {
  if (
    filename.length === 0 ||
    filename === "." ||
    filename === ".." ||
    filename.includes("/") ||
    filename.includes("\\") ||
    filename.includes("\0")
  ) {
    throw new PromptToolError(
      "attachment_error",
      `attachment #${index + 1} has an unsafe filename`,
    );
  }
}

function sanitizeFilename(filename: string, index: number): string {
  let safe = basename(filename.replaceAll("\0", ""))
    .replace(/[^A-Za-z0-9._-]/g, "_")
    .replace(/^\.+/, "_");
  if (!safe || safe === "." || safe === "..") safe = `attachment-${index + 1}`;
  return safe;
}

function uniqueFilename(directory: string, requested: string): string {
  let candidate = requested;
  let suffix = 2;
  const extension = extname(requested);
  const stem = requested.slice(0, requested.length - extension.length);
  for (;;) {
    try {
      statSync(join(directory, candidate));
      candidate = `${stem}-${suffix++}${extension}`;
    } catch {
      return candidate;
    }
  }
}

function oldestTerminal(
  prompts: readonly ManagedPrompt[],
): ManagedPrompt | undefined {
  return prompts
    .filter((prompt) => prompt.state !== "pending")
    .sort((left, right) => {
      const finished = left.finishedAt! - right.finishedAt!;
      if (finished !== 0) return finished;
      const created = left.createdAt - right.createdAt;
      if (created !== 0) return created;
      return left.promptId.localeCompare(right.promptId);
    })[0];
}

async function waitUntilOneSettles(
  prompts: readonly ManagedPrompt[],
  timeoutMs: number,
): Promise<void> {
  await new Promise<void>((resolve) => {
    let settled = false;
    const finish = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(finish, timeoutMs);
    timer.unref?.();
    void Promise.race(prompts.map((prompt) => prompt.completion)).then(finish);
  });
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
