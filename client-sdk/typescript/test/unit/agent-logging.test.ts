import { describe, expect, it, vi } from "vitest";
import type { NatsConnection } from "@nats-io/nats-core";
import { Agent, type Logger } from "../../src/index.js";
import { buildAgentInfo } from "../../src/discovery/agent-info.js";

const info = buildAgentInfo({
  name: "agents",
  id: "logging-test",
  version: "0.1.0",
  description: "",
  metadata: { agent: "a", owner: "o", session: "s", protocol_version: "0.3" },
  endpoints: [{ name: "prompt", subject: "agents.prompt.a.o.s", queue_group: "agents" }],
})!;

describe("Agent logging", () => {
  it("routes the unsigned tracing warning through the configured logger once", async () => {
    const nc = {} as unknown as NatsConnection;
    const warnings: Array<{
      message: string;
      context: Record<string, unknown> | undefined;
    }> = [];
    const logger: Logger = {
      debug: () => undefined,
      info: () => undefined,
      warn: (message, context) => warnings.push({ message, context }),
      error: () => undefined,
    };
    const consoleWarn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const agent = new Agent(nc, info, 60_000, undefined, undefined, {}, logger);

    await agent.prompt("first");
    await agent.prompt("second");

    expect(warnings).toHaveLength(1);
    expect(warnings[0]?.message).toContain("no identity signer is configured");
    expect(consoleWarn).not.toHaveBeenCalled();
    consoleWarn.mockRestore();
  });
});
