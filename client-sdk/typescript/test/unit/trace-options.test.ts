import type { NatsConnection } from "@nats-io/nats-core";
import { describe, expect, it } from "vitest";
import { Agent } from "../../src/agent.js";
import { Agents } from "../../src/agents.js";
import { buildAgentInfo, type RawServiceInfo } from "../../src/discovery/agent-info.js";
import type { TraceOptions } from "../../src/trace/options.js";

// Tracker/resolver only store the connection at construction time, so a
// bare stub is enough for option-plumbing tests.
const nc = {} as NatsConnection;

function info(): RawServiceInfo {
  return {
    name: "agents",
    id: "VMKS6MHK71PCPWGY38A7N5",
    version: "1.0.0",
    description: "test agent",
    metadata: {
      agent: "echo",
      owner: "test",
      session: "main",
      protocol_version: "0.3",
    },
    endpoints: [
      {
        name: "prompt",
        subject: "agents.prompt.echo.test.main",
        queue_group: "agents",
        metadata: { max_payload: "1MB", attachments_ok: "true" },
      },
    ],
  };
}

describe("tracing opt-in switch", () => {
  it("is off when the trace option is omitted", () => {
    const agents = new Agents({ nc });
    expect(agents.trace).toBeUndefined();
  });

  it("is on when a trace option is passed, empty object included", () => {
    const trace: TraceOptions = {};
    const agents = new Agents({ nc, trace });
    expect(agents.trace).toBe(trace);
  });

  it("reaches Agent handles that were constructed with it", () => {
    const parsed = buildAgentInfo(info());
    expect(parsed).toBeDefined();
    const off = new Agent(nc, parsed!, 60_000);
    expect(off.tracingEnabled).toBe(false);
    const on = new Agent(nc, parsed!, 60_000, undefined, undefined, {});
    expect(on.tracingEnabled).toBe(true);
  });
});
