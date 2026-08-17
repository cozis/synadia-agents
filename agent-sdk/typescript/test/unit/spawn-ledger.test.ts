// Spawn-ledger lifecycle (stream finish semantics), broker-less. Once the
// request completes the completion-report channel closes: late spawns stop
// accreting into the undrainable pending set and deliver via the
// spawn-time marker only. Mirrors Python's test_trace_lifecycle.py.

import { describe, expect, it } from "vitest";
import type { NatsConnection } from "@nats-io/nats-core";
import type { ServiceMsg } from "@nats-io/services";
import { PromptResponse } from "../../src/service.js";

function makeResponse(): PromptResponse {
  const msg = { reply: "_INBOX.agents.mux.token" } as unknown as ServiceMsg;
  return new PromptResponse(msg, {} as NatsConnection);
}

describe("spawn-ledger lifecycle", () => {
  it("records on both channels while the stream is live", () => {
    const response = makeResponse();
    const marker = response.recordSpawn("cafebabe00000000");
    expect(marker["x-synadia-event"]).toBe("spawn");
    expect(response.traceHeaders()["x-synadia-spawned"]).toBe("cafebabe00000000::programmatic");
  });

  it("finish() closes the completion-report channel; the marker still delivers", () => {
    const response = makeResponse();
    response.finish();
    const marker = response.recordSpawn("cafebabe00000000");
    expect(marker["x-synadia-spawned"]).toBe("cafebabe00000000::programmatic");
    expect(response.traceHeaders()["x-synadia-spawned"]).toBeUndefined();
  });

  it("the ambient recorder respects finish()", () => {
    const response = makeResponse();
    const recorder = response.asActiveTrace().recordSpawn!;
    response.finish();
    const marker = recorder("cafebabe00000000");
    expect(marker["x-synadia-event"]).toBe("spawn");
    expect(response.traceHeaders()["x-synadia-spawned"]).toBeUndefined();
  });

  it("an empty tool id counts as none", () => {
    const marker = makeResponse().recordSpawn("cafebabe00000000", { toolCallId: "" });
    expect(marker["x-synadia-spawned"]).toBe("cafebabe00000000::programmatic");
  });

  it("entries recorded before finish() still drain", () => {
    const response = makeResponse();
    response.recordSpawn("cafebabe00000000");
    response.finish();
    expect(response.traceHeaders()["x-synadia-spawned"]).toBe("cafebabe00000000::programmatic");
  });
});
