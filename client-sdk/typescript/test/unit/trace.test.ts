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
  IDENTITY_PATH_MARKER,
  deriveThreadId,
  formatSpawnEntry,
  identityPath,
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

describe("formatSpawnEntry — the edge policy", () => {
  it("uses an explicit tool id", () => {
    expect(formatSpawnEntry("c1", "toolu_x")).toBe("c1:toolu_x:tool_call");
  });

  it("defaults the tool id from the ambient toolScope", () => {
    toolScope("toolu_ambient", () => {
      expect(formatSpawnEntry("c1")).toBe("c1:toolu_ambient:tool_call");
    });
  });

  it("is honestly programmatic without a tool", () => {
    expect(formatSpawnEntry("c1")).toBe("c1::programmatic");
  });

  it("lets an explicit edge type win", () => {
    expect(formatSpawnEntry("c1", undefined, "handoff")).toBe("c1::handoff");
  });

  it("counts an empty tool id as no tool", () => {
    expect(formatSpawnEntry("c1", "")).toBe("c1::programmatic");
    toolScope("", () => {
      expect(formatSpawnEntry("c1")).toBe("c1::programmatic");
    });
  });

  it("percent-encodes reserved characters (cross-SDK vector)", () => {
    // ':' and ',' would corrupt the entry / comma-joined report. Pinned to
    // the Python SDK's quote(safe="") output byte-for-byte.
    expect(formatSpawnEntry("c1", "a:b,c")).toBe("c1:a%3Ab%2Cc:tool_call");
    expect(formatSpawnEntry("c1", "a!b'c(d)e*f")).toBe("c1:a%21b%27c%28d%29e%2Af:tool_call");
  });

  it("passes plain provider ids through unchanged", () => {
    expect(formatSpawnEntry("c1", "toolu_01AbC")).toBe("c1:toolu_01AbC:tool_call");
  });
});

describe("identityPath — §3.2 attribution as a base-URL path prefix", () => {
  it("composes the full identity", () => {
    expect(
      identityPath({ agent: "openclaw", owner: "acme", name: "default", instanceId: "svc01" }),
    ).toBe("synadia/openclaw/acme/default/svc01");
  });

  it("fills an absent instance slot with '-' (fixed arity)", () => {
    expect(identityPath({ agent: "openclaw", owner: "acme", name: "default" })).toBe(
      "synadia/openclaw/acme/default/-",
    );
  });

  it("leads with the reserved marker", () => {
    const path = identityPath({ agent: "a", owner: "o", name: "s", instanceId: "i" });
    expect(path.split("/")[0]).toBe(IDENTITY_PATH_MARKER);
  });
});
