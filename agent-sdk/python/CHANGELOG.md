# Changelog

All notable changes to `synadia-ai-agent-service` are documented here.

Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html);
the 0.x line is explicitly unstable per protocol spec §11.2.

## [Unreleased]

### Added

- **Trace propagation** — `PromptStream` now exposes
  `thread_id` (derived from the request's reply subject), `root_id` /
  `is_root` (from the envelope's optional `root_id` field),
  `trace_headers()` for outbound model requests,
  and `record_spawn()` / `child_trace()` for reporting sub-agent
  spawn edges. Requires `synadia-ai-agents` with `derive_thread_id` /
  `TraceContext`.
- **Ambient trace binding** — `AgentService` binds the client-sdk's
  `ActiveTrace` (contextvars) around each prompt-handler invocation:
  `Agent.prompt()` calls inside a handler join the thread's tree and
  auto-record spawn edges with no explicit plumbing;
  `trace_headers()` defaults its tool id from the ambient
  `tool_scope()`.
- **Agent attribution via the base URL** — new
  `AgentService.identity_path` property (available after `start()`,
  when the §8.3 instance id exists): harnesses compose their
  provider-client base URL as `f"{proxy_root}/{service.identity_path}"`
  and every request through that client is attributed to the agent by
  the observing proxy — no per-request identity headers. Values mirror
  the §3.2 registration metadata exactly, so proxy node labels match
  discovery and heartbeats. Thread + root ids travel packed as the
  `x-synadia-trace: <root_id>:<thread_id>` header. Requires
  `synadia-ai-agents` with `identity_path`.
- **Malformed `root_id` rejected at the boundary** — via the shared
  envelope codec, a request whose `root_id` is not 16 lowercase hex
  chars now gets the §9 `400` error (like any malformed envelope)
  before the handler runs, so caller-controlled bytes can never reach
  the agent's outbound `x-synadia-trace` header.
- **Empty tool ids count as none** — `record_spawn` with an empty id
  produces an honest `programmatic` edge (via the shared
  `format_spawn_entry` policy, whose tool slot is now percent-encoded).
  Tool ids have no header of their own: `trace_headers()` takes no
  `tool_call_id` argument — tool attribution exists only as spawn-edge
  labels, defaulted from the ambient `tool_scope()`.
- **Spawn-ledger lifecycle** — when a request completes (just before
  the §6.5 terminator), the stream's completion-report channel closes:
  spawns recorded afterwards by handler-spawned tasks that outlive the
  request (via their inherited ambient trace) no longer accrete
  undrainable entries in the finished stream's pending set. Late
  spawns still join the tree and deliver their edge via the spawn-time
  marker — now the documented single channel for post-completion
  spawns. `record_spawn()` / `trace_headers()` docstrings state the
  lifecycle rule.
- **Reply-less prompts get random thread ids** — a fire-and-forget
  request (raw-NATS publish with no reply subject) previously hashed
  the empty string, collapsing every such request on every agent onto
  the constant thread id `e3b0c44298fc1c14`; each now gets a distinct
  random id with the normative shape (`random_thread_id()` from
  `synadia-ai-agents`), a provisional root of its own tree.

- **Agent-ladder examples** (`examples/01-echo.py` … `05-tools.py`,
  plus the shared `examples/llm.py` base) — the Python mirror of
  `agent-sdk/typescript/examples/`: echo, Ollama, OpenRouter, a
  combined auto-selecting agent, and a tool-calling agent backed by a
  NATS microservice. Identity and heartbeat are flags that default to
  env vars (env-first, flag-overridable); connection uses the shared
  `_connect_cli.py` resolver. Identity follows the same `SYNADIA_*`
  ladder the TypeScript agents use, first non-empty wins:
  `SYNADIA_<AGENT>_OWNER` (per-agent) > `SYNADIA_OWNER` (fleet-wide) >
  `NATS_AGENT_OWNER` (legacy alias) > `$USER` > `anon`, and the `NAME`
  analogue (`SYNADIA_<AGENT>_NAME` > `SYNADIA_NAME` > `NATS_AGENT_NAME`
  > the `--session-name` fallback) for the 5th subject token.
  `<AGENT>` is the example's registered subject token uppercased with
  hyphens turned into underscores (e.g. `echo` → `SYNADIA_ECHO_OWNER`).
  Heartbeat cadence stays config, not identity — it keeps its single
  `NATS_AGENT_HEARTBEAT_INTERVAL` var. The `_reference_agent.py` flags
  default through the fleet-wide + legacy + fallback rungs only (no
  per-agent var — its `agent` token is a runtime CLI flag). Non-breaking
  — explicit flags still win, and the legacy `NATS_AGENT_*` vars keep
  working as lowest-priority aliases.
- **`examples` extra** — `httpx`, used by the LLM/tool example scripts
  (`uv sync --extra examples`). Not part of the published SDK surface.

### Changed

- **Dependency floor `synadia-ai-agents>=0.8`** (was `>=0.7`) — this
  package now imports the trace surface (`ActiveTrace`,
  `derive_thread_id`, `identity_path`, `bind_active_trace`, …),
  which first ships in client-sdk 0.8.0; resolving 0.7.x would
  `ImportError` on import of `synadia_ai.agent_service`. Release-ladder
  consequence: client-sdk 0.8.0 must be published to PyPI before the
  next release of this package.

## [0.4.1] - 2026-05-12

### Changed

- **Protocol name** — package metadata, module docstring, and README
  updated to "Synadia Agent Protocol for NATS" (was: "NATS Agent
  Protocol"). Renamed in PR #103. No wire format, public identifier,
  or behavior change — protocol version stays `"0.3"`, leading-ack
  semantics from 0.4.0 unchanged.

## [0.4.0] - 2026-05-11

### Changed

- **Leading `status=ack` chunk is now emitted unconditionally (§6.4).**
  Spec §6.4 was sharpened to require that every prompt handler emit
  exactly one `{"type":"status","data":"ack"}` chunk as the **first**
  message on the reply subject, **before** any work that introduces
  observable latency. `AgentService._on_prompt_request` now publishes
  the ack after a successful envelope decode and before invoking the
  user-supplied handler — so every Python agent in the repo
  (reference agent, `demo_echo`, in-tree test handlers) becomes
  spec-compliant on upgrade with no code change. The ack is emitted
  regardless of `keepalive_interval_s`; passing `None` only disables
  the periodic keep-alive cadence, not the leading ack. A malformed
  envelope still produces `error(400) → terminator` with no
  spurious ack — the ack lives after decode validation.

## [0.3.0] - 2026-05-04

Restores wire-shape parity with the spec and TS SDK after the
2026-04-28 session-name collapse mistakenly dropped `session` from
service metadata and §8.3 / §8.7 payloads. Reported as
[issue #73](https://github.com/synadia-ai/synadia-agents/issues/73).

### Fixed

- **`metadata.session` (§3.2)** — `AgentService.start()` now advertises
  `metadata.session = session_name` alongside the existing
  `{agent, owner, protocol_version}` triple. Per §3.2 a session-less
  harness MAY omit the field OR set it to `"default"`; since the
  Python constructor takes a required `session_name` (callers pass
  `"default"` when session-less), we always emit it for a uniform
  shape across both styles.
- **`HeartbeatPayload.session` (§8.3)** — `build_heartbeat_payload`
  now populates `session=subject.session_name`, so periodic
  heartbeats published on `agents.hb.{a}.{o}.{session_name}` mirror
  `metadata.session` per §8.3 / appendix B.11.
- **Status reply `session` (§8.7)** — the same builder feeds the
  `agents.status.{a}.{o}.{session_name}` request/reply endpoint, so
  a §8.7 status reply carries `session` matching the heartbeat. §8.7
  + appendix B.11a explicitly require the same §8.3 schema.

### Changed

- **Dependency floor bumped to `synadia-ai-agents>=0.7`** so the
  shared `HeartbeatPayload` model has the `session` field — needed
  for the publisher and status handler to populate it.

## [0.2.0] - 2026-05-03

### Changed

- **Dependency floor bumped to `synadia-ai-agents>=0.6`** in lockstep
  with the client-sdk's prompt-stream catch-up to the TS SDK's PR #66
  (`requestMany` + sentinel) — see
  [`client-sdk/python/CHANGELOG.md`](../../client-sdk/python/CHANGELOG.md)
  `[0.6.0]` for the substance of that change. **No agent-side
  code changes:** PR #66 was confirmed to touch only
  `client-sdk/typescript/` (`gh pr view 66 --json files`); the
  agent-host wire is identical pre/post. Agents still publish
  individual chunks to `msg.reply` with the §6.5 zero-byte
  terminator — whether the client subscribed per-stream or via a
  shared mux is invisible from the agent's POV. The bump exists
  purely to keep the published metapackage coherent so a user
  installing `synadia-ai-agent-service` via PyPI pulls a client-sdk
  that exposes the new `Agent.prompt(max_wait_s=...)` /
  `StreamMaxWaitExceededError` / `StreamStalledError` surface shared
  between both packages.

## [0.1.0] - 2026-04-30

Initial release. **Carved out of `synadia-ai-agents` at the 0.5.0
cut**: through 0.4.x the agent-host surface lived inside
`synadia-ai-agents`; the 0.5.0 release removed it there and shipped
it here as 0.1.0. Both packages were cut together —
`synadia-ai-agents@0.5.0` is the first PyPI version that no longer
carries this surface. Harness authors get a focused dependency;
callers install just the client SDK.

### Added

- `synadia_ai.agent_service.AgentService` — service registration,
  prompt endpoint, status endpoint, heartbeat publisher loop, and
  mid-stream `ask` per the §12 implementation checklist. Sourced
  from `synadia-ai-agents`'s pre-0.5.0 `service.py` (the file moved
  here at the split — `synadia-ai-agents@0.5.0` no longer carries
  it; the lineage includes the post-0.3.0 server-`max_payload`
  clamp and the v0.3 verb-first wire); rewired to import shared
  wire types (`Envelope`, `HeartbeatPayload`, `AgentSubject`, error
  classes, discovery constants) from `synadia_ai.agents`.
- `synadia_ai.agent_service.PromptStream` — emit response chunks /
  ask mid-stream queries / observe terminator semantics.
- `synadia_ai.agent_service.PromptHandler` — handler-callable type
  alias.
- `DEFAULT_MAX_PAYLOAD`, `DEFAULT_KEEPALIVE_INTERVAL_S`,
  `DEFAULT_ATTACHMENTS_OK` — agent-side defaults; previously
  exported from `synadia_ai.agents`.
- Heartbeat publisher helpers `build_heartbeat_payload`,
  `run_publisher`, `publish_one` in
  `synadia_ai.agent_service.heartbeat`. Imports
  `HeartbeatPayload`, `now_iso`, `AgentSubject` from
  `synadia_ai.agents`.
- `examples/_reference_agent.py` — spec-compliant runnable echo
  agent, used by both this package's tests and the client-sdk's
  numbered demos.
- `scripts/demo_echo.py` — one-shot dev-diagnostic echo agent for
  manual `nats` CLI poking. Moved from
  `client-sdk/python/scripts/`.

### Removed

- Dropped the unused `utf8_byte_length` helper from the private
  `synadia_ai.agent_service._bytes` module. It was copied wholesale
  from the client-sdk during the 0.1.0 extraction but has no caller
  in the agent-sdk — it's a caller-side pre-publish size check used
  inside `synadia-ai-agents` only.

### Fixed

- `run_publisher` no longer propagates publish exceptions out of the
  heartbeat task. A failed publish (e.g. `ConnectionClosedError`
  after a broker restart) is logged and the publisher exits cleanly
  so `AgentService.stop()` teardown stays deterministic instead of
  re-raising mid-cleanup. `AgentService.stop()` now suppresses
  `Exception` alongside `CancelledError` when awaiting the publisher
  task as a belt-and-braces guard against unforeseen errors that
  predate the catch in `run_publisher`. Surfaced by the Claude
  reviewer bot on PR #45.
- `test_run_publisher_emits_immediate_then_periodic` no longer
  asserts a lower bound on heartbeat inter-arrival times.
  `asyncio.wait_for(stop.wait(), ...)` can return slightly early on
  a loaded event loop, and a tight lower bound flaked on contended
  CI runners without protecting any caller-visible invariant; the
  upper bound is the load-bearing liveness check.

### Wire compatibility

Same protocol version as the client-sdk:
`metadata.protocol_version = "0.3"`. Wire-version history is shared
with `synadia-ai-agents` — see its
[CHANGELOG](../../client-sdk/python/CHANGELOG.md) for protocol
milestones (v0.1 alignment, v0.2 service-name + queue-group, v0.3
verb-first subjects + status endpoint, the 2026-04-28 session-name
collapse).

### Migration from `synadia-ai-agents`

For agent harness code that imported the host surface directly:

```diff
- from synadia_ai.agents import AgentService, PromptStream, PromptHandler
+ from synadia_ai.agent_service import AgentService, PromptStream, PromptHandler
+ # Envelope / Attachment / HeartbeatPayload / errors continue to import
+ # from synadia_ai.agents.
```

The constructor signature, behavior, and wire emission are
unchanged.

### CI

- The "Install nats-server" steps in
  `client-sdk-python-agent-service.yml` and
  `release-python-agent-service.yml` now extract the tarball into
  `${{ runner.temp }}` instead of inheriting
  `defaults.run.working-directory: agent-sdk/python`. Stops every
  run from leaving an empty `nats-server-v*-linux-amd64/` parent
  dir in `agent-sdk/python/` after the binary is `mv`'d to
  `/usr/local/bin/`. Cosmetic only — no behavior change.
