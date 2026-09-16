# Changelog

All notable changes to the Claude Code NATS channel are documented here.

## Unreleased

### Added

- Optional connection-bound signed host identity through `senderIdentity` and independent inbound
  sender policy through `minSenderTrust`.
- Safe, explicit `request_info` inspection for the classified sender of an active request.
- Opt-in tracing through `tracing: "on"` / `NATS_TRACING=on` /
  `/nats-channel:configure tracing on`: the channel adopts a traced caller's thread, or mints
  one for a prompt that carries none, and publishes two signed `served` records per prompt on
  `TRACE.edges`, binding the thread to `claude:<session id>` with the outcome (`ok`, `error`,
  `timeout`). Requires `senderIdentity: "signed"`; without it nothing is published, startup
  warns, and the records owed count as dropped on the heartbeat.
- A `SessionStart` hook (`hooks/hooks.json` → `hooks/session-start.ts`) records the current
  Claude Code session id under `<state dir>/sessions/<Claude Code pid>` so the binding follows
  `/clear`; the server falls back to `CLAUDE_CODE_SESSION_ID` and removes the file on shutdown.

### Changed

- Migrated service registration, prompt admission, status classification, replay protection,
  acknowledgements, heartbeats, errors, and stream termination to `AgentService`.
- Permission queries now use `PromptResponse.ask()` and pending requests settle on completion,
  expiry, or shutdown.
- The marketplace plugin runs a committed, deterministic, self-contained bundle and no longer
  installs mutable dependencies whenever its MCP server starts.
- Synchronized the existing package and Claude plugin descriptor version at `0.5.1`.
