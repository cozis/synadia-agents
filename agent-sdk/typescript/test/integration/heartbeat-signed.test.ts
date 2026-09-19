// Signed heartbeats through `AgentService`: with a live-bound signer every
// heartbeat the service publishes carries a verifying `Agent-Sender` —
// `sub` the heartbeat subject, `ts` the frame's own, the nonce fresh per
// beat — and the status reply carries none. Without a signer, whether host
// identity is omitted or registered unsigned, the frame goes out bare, as
// plain protocol 0.3.

import { readFile } from "node:fs/promises";
import { nkeyAuthenticator, type Msg, type NatsConnection } from "@nats-io/nats-core";
import { connect } from "@nats-io/transport-node";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  decodeHeartbeatPayload,
  newAgentId,
  readSenderHeaderValue,
  signerFromSeed,
  verifySender,
} from "@synadia-ai/agents";
import { AgentService, type AgentServiceOptions } from "../../src/service.js";
import {
  findNatsServerBinary,
  identityFixture,
  NatsServerProcess,
} from "../harness/nats-server.js";

interface KeysFile {
  readonly users: Record<string, { readonly public: string; readonly seed: string }>;
}

const bin = await findNatsServerBinary();
const keys = JSON.parse(await readFile(identityFixture("keys.json"), "utf8")) as KeysFile;
const enc = new TextEncoder();
const dec = new TextDecoder();
const ALICE = keys.users["alice"]!;

function connectAs(url: string, seed: string): Promise<NatsConnection> {
  return connect({
    servers: url,
    authenticator: nkeyAuthenticator(enc.encode(seed)),
    reconnect: false,
  });
}

describe.skipIf(!bin)("AgentService — signed heartbeats (nkey user, $G)", () => {
  const server = new NatsServerProcess();
  let hostNc: NatsConnection;
  let probeNc: NatsConnection;

  beforeAll(async () => {
    await server.start({ configPath: identityFixture("nkey-noaccounts.conf") });
    hostNc = await connectAs(server.url, ALICE.seed);
    probeNc = await connectAs(server.url, ALICE.seed);
  });

  afterAll(async () => {
    await probeNc.close();
    await hostNc.close();
    await server.stop();
  });

  /** Start a service, collect `count` beats and one status reply, stop it. */
  async function observe(
    name: string,
    options: Partial<AgentServiceOptions>,
    count: number,
  ): Promise<{ readonly beats: Msg[]; readonly status: Msg; readonly service: AgentService }> {
    const service = new AgentService({
      nc: hostNc,
      agent: "hb-signed",
      owner: "alice",
      name,
      heartbeatIntervalS: 1,
      keepaliveIntervalS: null,
      ...options,
    });
    service.onPrompt(async (_envelope, response) => {
      await response.send("ok");
    });
    const sub = probeNc.subscribe(service.subject.heartbeat);
    await probeNc.flush();
    await service.start();
    try {
      const beats: Msg[] = [];
      for await (const m of sub) {
        beats.push(m);
        if (beats.length === count) break;
      }
      const status = await probeNc.request(service.subject.status, new Uint8Array(0), {
        timeout: 2_000,
      });
      return { beats, status, service };
    } finally {
      sub.unsubscribe();
      await service.stop();
    }
  }

  it("signs every beat with the id_sig signer: sub the subject, ts the frame's, a fresh nonce", async () => {
    const { beats, status, service } = await observe(
      "signed",
      { identity: { signer: signerFromSeed(ALICE.seed) } },
      2,
    );
    expect(service.identity).toBe(newAgentId("$G", ALICE.public));
    const seen = new Set<string>();
    for (const m of beats) {
      const frame = decodeHeartbeatPayload(JSON.parse(dec.decode(m.data)));
      if (!frame) throw new Error("malformed heartbeat frame");
      // The SDK's own verifier, live mode, over the bytes as they arrived.
      const verified = await verifySender(m, "live", {
        nonceSeen: (user, nonce) => seen.has(`${user}.${nonce}`),
      });
      expect(verified?.trust).toBe("verified");
      if (verified?.trust !== "verified") throw new Error("unreachable");
      expect(verified.id).toBe(service.identity);
      expect(verified.header.sub).toBe(m.subject);
      expect(verified.header.sub).toBe(service.subject.heartbeat);
      expect(verified.header.ts).toBe(frame.ts);
      expect(verified.header.name).toBeUndefined();
      seen.add(`${verified.header.user}.${verified.header.nonce}`);
    }
    expect(seen.size).toBe(beats.length);
    // The status reply builds the same frame but is not a heartbeat: no header.
    expect(readSenderHeaderValue(status.headers)).toBeUndefined();
    expect(decodeHeartbeatPayload(JSON.parse(dec.decode(status.data)))).toBeDefined();
  });

  it("beats bare with an unsigned registration", async () => {
    const { beats, status, service } = await observe("claimed", { identity: {} }, 1);
    expect(service.identity).toBe(newAgentId("$G", ALICE.public));
    for (const m of [...beats, status]) {
      expect(readSenderHeaderValue(m.headers)).toBeUndefined();
      expect(await verifySender(m, "live")).toBeUndefined();
    }
  });

  it("beats bare without host identity", async () => {
    const { beats, status, service } = await observe("plain", {}, 1);
    expect(service.identity).toBeUndefined();
    for (const m of [...beats, status]) {
      expect(readSenderHeaderValue(m.headers)).toBeUndefined();
    }
  });
});
