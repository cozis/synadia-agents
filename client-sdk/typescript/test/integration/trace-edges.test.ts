// Edge records over the real account boundary (observability.md,
// "Crossing the Account Boundary"): a tenant publishes the short form
// `TRACE.edges`, the fabric's stream stores `TRACE.APP.edges`, and the
// record verifies in stored mode once the inserted account token is
// removed by position — with that token cross-checked against the
// signed header's `account`. Retries are idempotent through the stream's
// duplicate window (`Nats-Msg-Id` = `record_id`), and a tenant cannot
// read back what it wrote.

import { readFile } from "node:fs/promises";
import { jetstream, jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import { Empty, nkeyAuthenticator, type NatsConnection } from "@nats-io/nats-core";
import { connect } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Agent } from "../../src/agent.js";
import { buildAgentInfo, type RawServiceInfo } from "../../src/discovery/agent-info.js";
import { IdentityContext } from "../../src/identity/context.js";
import {
  NATS_MSG_ID_HEADER,
  newAgentId,
  signerFromSeed,
  verifySender,
  type SenderInfo,
} from "../../src/index.js";
import { findNatsServerBinary, identityFixture, NatsServerProcess } from "../harness/nats-server.js";

interface KeysFile {
  readonly users: Record<string, { readonly public: string; readonly seed: string }>;
}

const bin = await findNatsServerBinary();
const keys = JSON.parse(await readFile(identityFixture("keys.json"), "utf8")) as KeysFile;
const enc = new TextEncoder();
const FABRIC_USER = keys.users["alice"]!; // FABRIC account
const APP_USER = keys.users["bob"]!; // APP account (the tenant)

const PROMPT_SUBJECT = "agents.prompt.echo.test.main";

function connectAs(url: string, seed: string): Promise<NatsConnection> {
  return connect({
    servers: url,
    authenticator: nkeyAuthenticator(enc.encode(seed)),
    reconnect: false,
  });
}

function agentInfo(): RawServiceInfo {
  return {
    name: "agents",
    id: "VMKS6MHK71PCPWGY38A7N5",
    version: "1.0.0",
    description: "echo",
    metadata: { agent: "echo", owner: "test", session: "main", protocol_version: "0.3" },
    endpoints: [
      {
        name: "prompt",
        subject: PROMPT_SUBJECT,
        queue_group: "agents",
        metadata: { max_payload: "1MB", attachments_ok: "true" },
      },
    ],
  };
}

interface StoredEdge {
  readonly subject: string;
  readonly record: Record<string, unknown>;
  readonly msgId: string | undefined;
  readonly sender: SenderInfo | undefined;
  readonly error: unknown;
}

describe.skipIf(!bin)("observability — edge records across the account boundary", () => {
  const server = new NatsServerProcess();
  let fabric: NatsConnection;
  let app: NatsConnection;
  let jsm: JetStreamManager;
  let agent: Agent;

  /** Read the stream, verifying each record the way the console would. */
  async function readEdges(): Promise<StoredEdge[]> {
    const info = await jsm.streams.info("TRACE");
    const out: StoredEdge[] = [];
    const consumer = await jetstream(fabric).consumers.get("TRACE");
    for (let i = 0; i < info.state.messages; i++) {
      const m = await consumer.next({ expires: 2_000 });
      if (!m) break;
      let sender: SenderInfo | undefined;
      let error: unknown;
      try {
        // The export inserts the caller's account at token 2; the
        // verifier removes it before comparing and checks it against the
        // header's `account`.
        sender = await verifySender(m, "stored", { accountTokenPosition: 2 });
      } catch (err) {
        error = err;
      }
      out.push({
        subject: m.subject,
        record: JSON.parse(new TextDecoder().decode(m.data)) as Record<string, unknown>,
        msgId: m.headers?.get(NATS_MSG_ID_HEADER) || undefined,
        sender,
        error,
      });
      m.ack();
    }
    return out;
  }

  beforeAll(async () => {
    await server.start({ configPath: identityFixture("trace-atp.conf"), jetstream: true });
    fabric = await connectAs(server.url, FABRIC_USER.seed);
    jsm = await jetstreamManager(fabric);
    await jsm.streams.add({ name: "TRACE", subjects: ["TRACE.>"] });

    app = await connectAs(server.url, APP_USER.seed);
    // A minimal responder so `prompt()` completes: the §6.5 terminator.
    const sub = app.subscribe(PROMPT_SUBJECT);
    void (async () => {
      for await (const m of sub) m.respond(Empty);
    })();
    await app.flush();

    agent = new Agent(
      app,
      buildAgentInfo(agentInfo())!,
      2_000,
      undefined,
      new IdentityContext(app, { signer: signerFromSeed(APP_USER.seed), name: "tenant" }),
      {},
    );
  });

  afterAll(async () => {
    await app?.close();
    await fabric?.close();
    await server.stop();
  });

  it("stores a tenant's TRACE.edges publish as TRACE.APP.edges, verifiable in stored mode", async () => {
    for await (const _ of await agent.prompt("hello", { tool: "call_9xJ2" })) {
      // drain
    }
    // The edge rides a background drain; wait for the stream to see it.
    const deadline = Date.now() + 5_000;
    let edges: StoredEdge[] = [];
    while (Date.now() < deadline) {
      edges = await readEdges();
      if (edges.length > 0) break;
      await new Promise((r) => setTimeout(r, 50));
    }

    expect(edges).toHaveLength(1);
    const edge = edges[0]!;
    // The tenant published `TRACE.edges`; the import inserted its account.
    expect(edge.subject).toBe("TRACE.APP.edges");
    expect(edge.error).toBeUndefined();
    expect(edge.sender).toMatchObject({
      trust: "verified",
      id: newAgentId("APP", APP_USER.public),
    });
    expect(edge.record).toMatchObject({
      version: 1,
      parent_id: null, // a root: no ambient trace on this handle
      tool_call_id: "call_9xJ2",
    });
    expect(edge.record["thread_id"]).toBe(edge.record["root_id"]);
    // `Nats-Msg-Id` is the record id, so a retry de-duplicates.
    expect(edge.msgId).toBe(edge.record["record_id"]);
  });

  it("gives the tenant no way to read what the fabric stored", async () => {
    // A service import grants publishing toward the exporter and nothing
    // else: the tenant sees only its own local echo, never the stream.
    const seen: string[] = [];
    const sub = app.subscribe("TRACE.>");
    void (async () => {
      for await (const m of sub) seen.push(m.subject);
    })();
    await app.flush();

    for await (const _ of await agent.prompt("second")) {
      // drain
    }
    await new Promise((r) => setTimeout(r, 300));
    sub.unsubscribe();

    // Whatever it saw is its own publish on the short form — never the
    // account-qualified subject the fabric stores.
    expect(seen.every((s) => s === "TRACE.edges")).toBe(true);
  });
});
