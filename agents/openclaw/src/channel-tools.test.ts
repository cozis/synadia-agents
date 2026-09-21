import type { OpenClawPluginToolContext } from "openclaw/plugin-sdk/core";
import { describe, expect, it, vi } from "vitest";
import {
  createNatsAgentTools,
  notifyPromptCompletion,
} from "./channel.js";
import type { getNatsRuntime } from "./runtime.js";

describe("OpenClaw prompt completion notifications", () => {
  it("queues and wakes the exact session that started the prompt", () => {
    const enqueueSystemEvent = vi.fn(() => true);
    const requestHeartbeat = vi.fn();
    const runtime = {
      system: { enqueueSystemEvent, requestHeartbeat },
    } as unknown as Pick<ReturnType<typeof getNatsRuntime>, "system">;
    const context = {
      sessionKey: "agent:main:conversation-1",
      agentId: "main",
      deliveryContext: { channel: "nats" },
    } as unknown as OpenClawPluginToolContext;

    notifyPromptCompletion(runtime, context, {
      event: "agent_prompt_finished",
      prompt_id: "prompt-1",
      state: "completed",
    });

    expect(enqueueSystemEvent).toHaveBeenCalledWith(
      expect.stringContaining(
        "Agent prompt prompt-1 finished with state completed",
      ),
      {
        sessionKey: "agent:main:conversation-1",
        contextKey: "nats-prompt:prompt-1",
        deliveryContext: { channel: "nats" },
      },
    );
    expect(requestHeartbeat).toHaveBeenCalledWith({
      source: "background-task",
      intent: "event",
      reason: "nats-prompt-completed",
      sessionKey: "agent:main:conversation-1",
      agentId: "main",
    });
  });

  it("does not wake when OpenClaw declines the queued system event", () => {
    const requestHeartbeat = vi.fn();
    const runtime = {
      system: {
        enqueueSystemEvent: vi.fn(() => false),
        requestHeartbeat,
      },
    } as unknown as Pick<ReturnType<typeof getNatsRuntime>, "system">;

    notifyPromptCompletion(
      runtime,
      { sessionKey: "agent:main:conversation-1" } as OpenClawPluginToolContext,
      {
        event: "agent_prompt_finished",
        prompt_id: "prompt-2",
        state: "failed",
      },
    );

    expect(requestHeartbeat).not.toHaveBeenCalled();
  });
});

describe("OpenClaw agent tool registration", () => {
  it("exposes the complete non-SDK prompt tool contract", () => {
    const tools = createNatsAgentTools({
      sessionKey: "agent:main:conversation-1",
    } as OpenClawPluginToolContext);

    expect(tools.map((tool) => tool.name)).toEqual([
      "discover_agents",
      "prompt_agent",
      "list_pending_prompts",
      "wait_for_prompt",
      "cancel_prompts",
    ]);
  });
});
