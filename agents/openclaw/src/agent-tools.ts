// OpenClaw-local implementation of the discover_agents, prompt_agent,
// list_pending_prompts, wait_for_prompt, answer_agent, and cancel_prompts
// tools. Model-tool
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
import { basename, extname, join, resolve } from "node:path";
import {
  StreamMaxWaitExceededError,
  bindActiveTrace,
  decodeStrictBase64,
  parseAgentSubject,
  type Agent,
  type Agents,
  type RequestAttachment,
  type StreamMessage,
  type TraceScope,
} from "@synadia-ai/agents";

export const DEFAULT_MAX_TRACKED_PROMPTS = 256;
export const DEFAULT_DISCOVERY_CACHE_TTL_MS = 5_000;

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
  readonly prompt_endpoint: string;
  readonly label: string;
  readonly text: string;
  readonly attachments?: readonly PromptAttachmentInput[];
  /** Absolute lifetime of the remote request. Waiting is controlled separately. */
  readonly max_runtime_ms?: number;
}

export interface WaitForPromptInput {
  readonly prompt_ids: readonly string[];
  /** Required. Zero performs a non-blocking poll. */
  readonly timeout_ms: number;
}

export interface AnswerAgentInput {
  readonly prompt_id: string;
  readonly text: string;
  readonly attachments?: readonly PromptAttachmentInput[];
}

export interface CancelPromptsInput {
  readonly prompt_ids: readonly string[];
}

export interface PromptAgentOptions {
  readonly toolCallId?: string;
  readonly traceScope?: TraceScope;
  /** Called when background work needs input or settles without an active waiter. */
  readonly onStateChanged?:
    | ((event: PromptStateEvent) => void | Promise<void>)
    | undefined;
}

export interface AsyncPromptManagerOptions {
  readonly maxTrackedPrompts?: number;
  readonly discoveryCacheTtlMs?: number;
  /** Harness-specific base directory for transient response attachments. */
  readonly attachmentTempDir?: string;
}

export type PromptState =
  | "pending"
  | "input_required"
  | "completed"
  | "failed"
  | "cancelled"
  | "expired";

export interface PromptStateEvent {
  readonly event: "agent_prompt_input_required" | "agent_prompt_finished";
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
  readonly path: string;
  readonly size_bytes: number;
};

type PromptError = { readonly code: string; readonly message: string };

type ChangeSignal = {
  readonly promise: Promise<void>;
  readonly resolve: () => void;
};

type ActiveQuestion = {
  readonly text: string;
  readonly attachments: StoredAttachment[];
  readonly reply: Extract<StreamMessage, { type: "query" }>["reply"];
  answering: boolean;
};

type ManagedPrompt = {
  readonly promptId: string;
  readonly label: string;
  readonly promptEndpoint: string;
  readonly createdAt: number;
  readonly controller: AbortController;
  readonly attachments: StoredAttachment[];
  readonly onStateChanged?:
    | ((event: PromptStateEvent) => void | Promise<void>)
    | undefined;
  state: PromptState;
  change: ChangeSignal;
  responseText: string;
  waiterCount: number;
  notificationDeferred: boolean;
  notificationsEnabled: boolean;
  question?: ActiveQuestion;
  stateChangedAt?: number;
  error?: PromptError;
  finishedAt?: number;
};

export class AsyncPromptManager {
  readonly #prompts = new Map<string, ManagedPrompt>();
  readonly #agentsByEndpoint = new Map<string, Agent>();
  readonly #startingControllers = new Set<AbortController>();
  readonly #maxTrackedPrompts: number;
  readonly #discoveryCacheTtlMs: number;
  readonly #attachmentTempDir: string;
  #lastDiscovery:
    | {
        readonly filterKey: string;
        readonly discoveredAt: number;
        readonly agents: readonly Agent[];
      }
    | undefined;
  #nextPromptNumber = 1;
  #startingPrompts = 0;
  #generation = 0;
  #discoveryGeneration = 0;
  #attachmentRoot: string | undefined;

  constructor(options: AsyncPromptManagerOptions = {}) {
    const maximum = options.maxTrackedPrompts ?? DEFAULT_MAX_TRACKED_PROMPTS;
    if (!Number.isInteger(maximum) || maximum < 1) {
      throw new TypeError("maxTrackedPrompts must be a positive integer");
    }
    const discoveryCacheTtlMs =
      options.discoveryCacheTtlMs ?? DEFAULT_DISCOVERY_CACHE_TTL_MS;
    if (!Number.isInteger(discoveryCacheTtlMs) || discoveryCacheTtlMs < 0) {
      throw new TypeError(
        "discoveryCacheTtlMs must be a non-negative integer",
      );
    }
    this.#maxTrackedPrompts = maximum;
    this.#discoveryCacheTtlMs = discoveryCacheTtlMs;
    this.#attachmentTempDir = resolve(options.attachmentTempDir ?? tmpdir());
  }

  async discoverAgents(
    client: Pick<Agents, "discover">,
    input: DiscoverAgentsInput = {},
  ) {
    const filter = {
      ...(input.agent !== undefined ? { agent: input.agent } : {}),
      ...(input.owner !== undefined ? { owner: input.owner } : {}),
      ...(input.name !== undefined ? { name: input.name } : {}),
      ...(input.session !== undefined ? { session: input.session } : {}),
    };
    const filterKey = JSON.stringify(filter);
    const now = Date.now();
    if (
      this.#lastDiscovery?.filterKey === filterKey &&
      now - this.#lastDiscovery.discoveredAt < this.#discoveryCacheTtlMs
    ) {
      return this.#lastDiscovery.agents.map(describeAgent);
    }

    // Only the most recent query is cached. A changed filter or an expired
    // entry replaces both the returned discovery set and the prompt handles.
    this.#lastDiscovery = undefined;
    this.#agentsByEndpoint.clear();
    const discoveryGeneration = ++this.#discoveryGeneration;
    const found = await client.discover({
      ...(input.timeout_ms !== undefined
        ? { timeoutMs: input.timeout_ms }
        : {}),
      ...(Object.keys(filter).length > 0 ? { filter } : {}),
    });
    const agents = [...found];
    if (discoveryGeneration === this.#discoveryGeneration) {
      for (const agent of agents) {
        this.#agentsByEndpoint.set(agent.promptSubject, agent);
      }
      this.#lastDiscovery = {
        filterKey,
        discoveredAt: Date.now(),
        agents,
      };
    }
    return agents.map(describeAgent);
  }

  async promptAgent(
    input: PromptAgentInput,
    options: PromptAgentOptions = {},
  ) {
    if (options.traceScope !== undefined) {
      const { traceScope, ...promptOptions } = options;
      return bindActiveTrace(traceScope, () =>
        this.#promptAgentUnbound(input, promptOptions),
      );
    }
    return this.#promptAgentUnbound(input, options);
  }

  listPendingPrompts() {
    return [...this.#prompts.values()]
      .filter(isActive)
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
    const alreadyReady = oldestReportable(prompts);
    if (alreadyReady) {
      return promptResult(alreadyReady, input.prompt_ids);
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
      await waitUntilOneIsReportable(prompts, input.timeout_ms);
      returned = oldestReportable(prompts);
    } finally {
      for (const prompt of prompts) prompt.waiterCount -= 1;
      for (const prompt of prompts) {
        if (!prompt.notificationDeferred || prompt.waiterCount > 0) continue;
        if (prompt === returned) prompt.notificationDeferred = false;
        else this.#notifyState(prompt);
      }
    }

    return returned
      ? promptResult(returned, input.prompt_ids)
      : {
          type: "timeout" as const,
          pending_prompt_ids: [...input.prompt_ids],
        };
  }

  async answerAgent(input: AnswerAgentInput) {
    validateAnswerInput(input);
    const prompt = this.#requirePrompt(input.prompt_id);
    const question = prompt.question;
    if (prompt.state !== "input_required" || !question) {
      throw new PromptToolError(
        "input_not_required",
        `Prompt ${input.prompt_id} is not waiting for input`,
      );
    }
    if (question.answering) {
      throw new PromptToolError(
        "answer_in_progress",
        `Prompt ${input.prompt_id} is already being answered`,
      );
    }

    const attachments = loadInputAttachments(input.attachments);
    question.answering = true;
    try {
      await question.reply(
        attachments ? { prompt: input.text, attachments } : input.text,
      );
    } catch (error) {
      question.answering = false;
      this.#settle(prompt, "failed", {
        code: "answer_failed",
        message: errorMessage(error),
      });
      throw new PromptToolError(
        "answer_failed",
        `Could not answer prompt ${input.prompt_id}: ${errorMessage(error)}`,
      );
    }

    if (prompt.state === "input_required" && prompt.question === question) {
      delete prompt.question;
      prompt.state = "pending";
      delete prompt.stateChangedAt;
      prompt.notificationDeferred = false;
      prompt.change = createChangeSignal();
    }
    return promptDescriptor(prompt);
  }

  cancelPrompts(input: CancelPromptsInput) {
    validatePromptIds(input.prompt_ids);
    return input.prompt_ids.map((id) => {
      const prompt = this.#prompts.get(id);
      if (!prompt) return { prompt_id: id, outcome: "not_found" as const };
      if (!isActive(prompt)) {
        return {
          prompt_id: id,
          outcome: "already_terminal" as const,
          state: prompt.state,
        };
      }

      prompt.notificationsEnabled = false;
      prompt.state = "cancelled";
      prompt.finishedAt = Date.now();
      prompt.stateChangedAt = prompt.finishedAt;
      prompt.controller.abort(new Error("Prompt cancelled"));
      delete prompt.question;
      prompt.change.resolve();
      return { prompt_id: id, outcome: "cancelled" as const };
    });
  }

  cancelAll(): void {
    this.#generation += 1;
    this.#discoveryGeneration += 1;
    for (const controller of this.#startingControllers) {
      controller.abort(new Error("NATS channel shutting down"));
    }
    for (const prompt of this.#prompts.values()) {
        prompt.notificationsEnabled = false;
        if (isActive(prompt)) {
          prompt.state = "cancelled";
          prompt.finishedAt = Date.now();
          prompt.stateChangedAt = prompt.finishedAt;
          prompt.controller.abort(new Error("NATS channel shutting down"));
        delete prompt.question;
        prompt.change.resolve();
      }
    }
    this.#prompts.clear();
    this.#agentsByEndpoint.clear();
    this.#lastDiscovery = undefined;
    if (this.#attachmentRoot) {
      rmSync(this.#attachmentRoot, { recursive: true, force: true });
      this.#attachmentRoot = undefined;
    }
  }

  async #promptAgentUnbound(
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
      const agent = this.#agentsByEndpoint.get(input.prompt_endpoint);
      if (!agent) {
        throw new PromptToolError(
          "agent_not_found",
          `prompt endpoint ${JSON.stringify(input.prompt_endpoint)} was not found in this session; call discover_agents first`,
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
        options.onStateChanged,
      );
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
      else await this.#handleMessage(prompt, first.value);

      this.#startingControllers.delete(controller);
      this.#startingPrompts -= 1;
      reserved = false;
      this.#prompts.set(prompt.promptId, prompt);
      if (isActive(prompt)) {
        void this.#collect(prompt, iterator);
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
  ): Promise<void> {
    try {
      for (;;) {
        const next = await iterator.next();
        if (next.done || !isActive(prompt)) break;
        await this.#handleMessage(prompt, next.value);
      }
      if (isActive(prompt)) this.#settle(prompt, "completed");
    } catch (error) {
      if (!isActive(prompt)) return;
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
        if (prompt.question) {
          await message.reply(
            "This caller is already answering an earlier question; continue without this input.",
          );
          break;
        }
        prompt.question = {
          text: message.prompt,
          attachments: (message.attachments ?? []).map((attachment, index) =>
            this.#storeAttachment(
              prompt.promptId,
              attachment.filename,
              attachment.content,
              prompt.attachments.length + index,
            ),
          ),
          reply: message.reply,
          answering: false,
        };
        prompt.state = "input_required";
        prompt.stateChangedAt = Date.now();
        prompt.change.resolve();
        this.#announceState(prompt);
        break;
      case "status":
        break;
    }
  }

  #settle(
    prompt: ManagedPrompt,
    state: Exclude<PromptState, "pending" | "input_required">,
    error?: PromptError,
  ): void {
    if (!isActive(prompt)) return;
    prompt.state = state;
    prompt.finishedAt = Date.now();
    prompt.stateChangedAt = prompt.finishedAt;
    delete prompt.question;
    if (error) prompt.error = error;
    prompt.change.resolve();
    this.#announceState(prompt);
  }

  #announceState(prompt: ManagedPrompt): void {
    if (!prompt.notificationsEnabled || !prompt.onStateChanged) return;
    if (prompt.waiterCount > 0) prompt.notificationDeferred = true;
    else this.#notifyState(prompt);
  }

  #notifyState(prompt: ManagedPrompt): void {
    if (
      !prompt.onStateChanged ||
      !prompt.notificationsEnabled ||
      prompt.state === "pending"
    ) {
      return;
    }
    prompt.notificationDeferred = false;
    const event: PromptStateEvent = {
      event:
        prompt.state === "input_required"
          ? "agent_prompt_input_required"
          : "agent_prompt_finished",
      prompt_id: prompt.promptId,
      state: prompt.state,
    };
    try {
      void Promise.resolve(prompt.onStateChanged(event)).catch(() => undefined);
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
          `The session is already tracking ${this.#maxTrackedPrompts} active prompts`,
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
    mkdirSync(this.#attachmentTempDir, { recursive: true, mode: 0o700 });
    const root = (this.#attachmentRoot ??= mkdtempSync(
      join(this.#attachmentTempDir, "synadia-agent-prompts-"),
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
    return { path, size_bytes: bytes.byteLength };
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
  onStateChanged?: (event: PromptStateEvent) => void | Promise<void>,
): ManagedPrompt {
  return {
    promptId,
    label: input.label,
    promptEndpoint: input.prompt_endpoint,
    createdAt: Date.now(),
    controller,
    change: createChangeSignal(),
    state: "pending",
    responseText: "",
    attachments: [],
    ...(onStateChanged ? { onStateChanged } : {}),
    waiterCount: 0,
    notificationDeferred: false,
    notificationsEnabled: true,
  };
}

function promptDescriptor(prompt: ManagedPrompt) {
  return {
    prompt_id: prompt.promptId,
    label: prompt.label,
    state: prompt.state,
    prompt_endpoint: prompt.promptEndpoint,
    created_at_ms: prompt.createdAt,
  };
}

function promptResult(prompt: ManagedPrompt, requestedIds: readonly string[]) {
  return {
    type: "prompt_result" as const,
    prompt_id: prompt.promptId,
    label: prompt.label,
    state: prompt.state,
    prompt_endpoint: prompt.promptEndpoint,
    response_text: prompt.responseText,
    attachments: [...prompt.attachments],
    created_at_ms: prompt.createdAt,
    remaining_prompt_ids: requestedIds.filter((id) => id !== prompt.promptId),
    ...(prompt.state === "input_required" && prompt.question
      ? {
          question: {
            text: prompt.question.text,
            attachments: [...prompt.question.attachments],
          },
        }
      : {}),
    ...(prompt.finishedAt !== undefined
      ? { finished_at_ms: prompt.finishedAt }
      : {}),
    ...(prompt.error ? { error: prompt.error } : {}),
  };
}

function validateAnswerInput(input: AnswerAgentInput): void {
  if (typeof input.prompt_id !== "string" || input.prompt_id.length === 0) {
    throw new PromptToolError(
      "invalid_argument",
      "prompt_id must be a non-empty string",
    );
  }
  if (typeof input.text !== "string" || input.text.length === 0) {
    throw new PromptToolError(
      "invalid_argument",
      "text must be a non-empty string",
    );
  }
  if (input.attachments !== undefined && !Array.isArray(input.attachments)) {
    throw new PromptToolError(
      "invalid_argument",
      "attachments must be an array",
    );
  }
}

function validatePromptInput(input: PromptAgentInput): void {
  if (
    typeof input.prompt_endpoint !== "string" ||
    input.prompt_endpoint.length === 0
  ) {
    throw new PromptToolError(
      "invalid_argument",
      "prompt_endpoint must be a non-empty string",
    );
  }
  if (!parseAgentSubject(input.prompt_endpoint)) {
    throw new PromptToolError(
      "invalid_argument",
      "prompt_endpoint must be an agents.prompt.<agent>.<owner>.<name> subject",
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
): readonly RequestAttachment[] | undefined {
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
    .filter((prompt) => !isActive(prompt))
    .sort((left, right) => {
      const finished = left.finishedAt! - right.finishedAt!;
      if (finished !== 0) return finished;
      const created = left.createdAt - right.createdAt;
      if (created !== 0) return created;
      return left.promptId.localeCompare(right.promptId);
    })[0];
}

function oldestReportable(
  prompts: readonly ManagedPrompt[],
): ManagedPrompt | undefined {
  return prompts
    .filter((prompt) => prompt.state !== "pending")
    .sort((left, right) => {
      const changed = left.stateChangedAt! - right.stateChangedAt!;
      if (changed !== 0) return changed;
      return left.promptId.localeCompare(right.promptId);
    })[0];
}

function isActive(prompt: ManagedPrompt): boolean {
  return prompt.state === "pending" || prompt.state === "input_required";
}

function createChangeSignal(): ChangeSignal {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function waitUntilOneIsReportable(
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
    void Promise.race(prompts.map((prompt) => prompt.change.promise)).then(
      finish,
    );
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
    prompt_endpoint: agent.promptSubject,
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
