import { describe, expect, it, vi } from "vitest";
import { resolveOpenClawSessionId } from "./session-id.js";

const route = { agentId: "main", sessionKey: "agent:main:nats:direct:remote" };
const storePath = "/tmp/openclaw/sessions.json";

describe("resolveOpenClawSessionId", () => {
  it("reads the entry through agent.session.getSessionEntry on 2026.8 and later", () => {
    const getSessionEntry = vi.fn().mockReturnValue({ sessionId: "sess-1" });
    const loadSessionStore = vi.fn();
    expect(
      resolveOpenClawSessionId(
        {
          agent: { session: { getSessionEntry } },
          // 2026.8+ keeps only route bookkeeping under channel.session.
          channel: { session: { loadSessionStore } },
        },
        route,
        storePath,
      ),
    ).toBe("sess-1");
    expect(getSessionEntry).toHaveBeenCalledWith({
      agentId: "main",
      sessionKey: route.sessionKey,
      storePath,
    });
    expect(loadSessionStore).not.toHaveBeenCalled();
  });

  it("accepts getSessionEntry under channel.session too", () => {
    const getSessionEntry = vi.fn().mockReturnValue({ sessionId: "sess-c" });
    expect(
      resolveOpenClawSessionId(
        { channel: { session: { getSessionEntry } } },
        route,
        storePath,
      ),
    ).toBe("sess-c");
  });

  it("falls back to channel.session.loadSessionStore on 2026.5.4", () => {
    const loadSessionStore = vi.fn().mockReturnValue({
      [route.sessionKey]: { sessionId: "sess-2" },
      other: { sessionId: "x" },
    });
    expect(
      resolveOpenClawSessionId(
        { agent: {}, channel: { session: { loadSessionStore } } },
        route,
        storePath,
      ),
    ).toBe("sess-2");
    expect(loadSessionStore).toHaveBeenCalledWith(storePath);
  });

  it("is undefined without a session reader, a missing entry, or a blank id", () => {
    expect(resolveOpenClawSessionId({}, route, storePath)).toBeUndefined();
    expect(
      resolveOpenClawSessionId({ agent: {}, channel: {} }, route, storePath),
    ).toBeUndefined();
    expect(
      resolveOpenClawSessionId(
        { agent: { session: {} }, channel: { session: {} } },
        route,
        storePath,
      ),
    ).toBeUndefined();
    expect(
      resolveOpenClawSessionId(
        { agent: { session: { getSessionEntry: () => undefined } } },
        route,
        storePath,
      ),
    ).toBeUndefined();
    expect(
      resolveOpenClawSessionId(
        { agent: { session: { getSessionEntry: () => ({ sessionId: "" }) } } },
        route,
        storePath,
      ),
    ).toBeUndefined();
    expect(
      resolveOpenClawSessionId(
        { channel: { session: { loadSessionStore: () => ({}) } } },
        route,
        storePath,
      ),
    ).toBeUndefined();
  });

  it("lets a throwing reader propagate so the caller can log it", () => {
    expect(() =>
      resolveOpenClawSessionId(
        {
          agent: {
            session: {
              getSessionEntry: () => {
                throw new Error("store locked");
              },
            },
          },
        },
        route,
        storePath,
      ),
    ).toThrow("store locked");
  });
});
