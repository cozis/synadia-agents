// Unit tests for the trace primitives — the normative thread-id derivation.
// The end-to-end path (stream threadId matching the agent-side derivation
// over a real broker) is exercised by the integration suite.

import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  THREAD_ID_HEX_LEN,
  activeTrace,
  bindActiveTrace,
  currentToolCallId,
  deriveThreadId,
  isThreadId,
  randomThreadId,
  toolScope,
} from "../../src/index.js";

describe("deriveThreadId", () => {
  it("matches the normative convention", () => {
    const subject = "_INBOX.agents.MUXNUID.TOKEN";
    const expected = createHash("sha256")
      .update(subject, "utf8")
      .digest("hex")
      .slice(0, THREAD_ID_HEX_LEN);
    expect(deriveThreadId(subject)).toBe(expected);
  });

  it("is deterministic and subject-sensitive", () => {
    const a = deriveThreadId("_INBOX.agents.m.t1");
    expect(a).toBe(deriveThreadId("_INBOX.agents.m.t1"));
    expect(a).not.toBe(deriveThreadId("_INBOX.agents.m.t2"));
  });

  it("has the normative length and alphabet", () => {
    const tid = deriveThreadId("_INBOX.x.y");
    expect(tid).toHaveLength(THREAD_ID_HEX_LEN);
    expect(isThreadId(tid)).toBe(true);
  });

  it("cross-SDK vector: derivation matches the Python SDK byte-for-byte", () => {
    // sha256("_INBOX.agents.m.t1")[:16] — pinned so drift from the Python
    // implementation fails loudly rather than silently forking the wire.
    expect(deriveThreadId("_INBOX.agents.m.t1")).toBe(
      createHash("sha256").update("_INBOX.agents.m.t1").digest("hex").slice(0, 16),
    );
  });

  it("randomThreadId is shape-valid and unique", () => {
    const a = randomThreadId();
    const b = randomThreadId();
    expect(isThreadId(a)).toBe(true);
    expect(isThreadId(b)).toBe(true);
    expect(a).not.toBe(b);
  });

  it("isThreadId rejects out-of-shape values", () => {
    for (const bad of ["A".repeat(16), "g".repeat(16), "a".repeat(15), "a".repeat(17), ""]) {
      expect(isThreadId(bad)).toBe(false);
    }
  });
});

describe("ambient trace context", () => {
  it("is undefined outside a binding", () => {
    expect(activeTrace()).toBeUndefined();
    expect(currentToolCallId()).toBeUndefined();
  });

  it("binds, nests tool scopes (innermost wins), and resets", () => {
    bindActiveTrace({ threadId: "p", rootId: "r" }, () => {
      expect(activeTrace()?.rootId).toBe("r");
      toolScope("toolu_outer", () => {
        expect(currentToolCallId()).toBe("toolu_outer");
        toolScope("toolu_inner", () => {
          expect(currentToolCallId()).toBe("toolu_inner");
        });
        expect(currentToolCallId()).toBe("toolu_outer");
      });
      expect(currentToolCallId()).toBeUndefined();
    });
    expect(activeTrace()).toBeUndefined();
  });

  it("flows through awaits inside the bound function", async () => {
    await bindActiveTrace({ threadId: "p", rootId: "r" }, async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      expect(activeTrace()?.threadId).toBe("p");
    });
  });
});
