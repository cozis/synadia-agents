import { describe, expect, it, vi } from "vitest";
import { resolveOpenClawSessionId } from "./session-id.js";

const route = { agentId: "main", sessionKey: "agent:main:nats:direct:remote" };
const storePath = "/tmp/openclaw/sessions.json";

describe("resolveOpenClawSessionId", () => {
  it("reads the entry through getSessionEntry on runtimes that have it", () => {
    const getSessionEntry = vi.fn().mockReturnValue({ sessionId: "sess-1" });
    const loadSessionStore = vi.fn();
    expect(
      resolveOpenClawSessionId(
        { channel: { session: { getSessionEntry, loadSessionStore } } },
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

  it("falls back to loadSessionStore on older runtimes", () => {
    const loadSessionStore = vi.fn().mockReturnValue({
      [route.sessionKey]: { sessionId: "sess-2" },
      other: { sessionId: "x" },
    });
    expect(
      resolveOpenClawSessionId(
        { channel: { session: { loadSessionStore } } },
        route,
        storePath,
      ),
    ).toBe("sess-2");
    expect(loadSessionStore).toHaveBeenCalledWith(storePath);
  });

  it("is undefined without a session reader, a missing entry, or a blank id", () => {
    expect(resolveOpenClawSessionId({}, route, storePath)).toBeUndefined();
    expect(
      resolveOpenClawSessionId({ channel: {} }, route, storePath),
    ).toBeUndefined();
    expect(
      resolveOpenClawSessionId({ channel: { session: {} } }, route, storePath),
    ).toBeUndefined();
    expect(
      resolveOpenClawSessionId(
        { channel: { session: { getSessionEntry: () => undefined } } },
        route,
        storePath,
      ),
    ).toBeUndefined();
    expect(
      resolveOpenClawSessionId(
        { channel: { session: { getSessionEntry: () => ({ sessionId: "" }) } } },
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
          channel: {
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
