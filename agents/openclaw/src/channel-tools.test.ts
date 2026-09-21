import type { OpenClawPluginToolContext } from "openclaw/plugin-sdk/core";
import { describe, expect, it, vi } from "vitest";
import { notifyPromptCompletion } from "./channel.js";
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
      prompt_id: "prompt-1",
      state: "completed",
    });

    expect(enqueueSystemEvent).toHaveBeenCalledWith(
      expect.stringContaining("Agent prompt prompt-1 completed"),
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
      { prompt_id: "prompt-2", state: "error" },
    );

    expect(requestHeartbeat).not.toHaveBeenCalled();
  });
});
