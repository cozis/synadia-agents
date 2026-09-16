import { readFileSync } from 'node:fs'
import { join } from 'node:path'

/**
 * How the channel learns the Claude Code session id — the value Claude
 * Code sends to the model API on every request, which a model proxy files
 * the session's calls under as `claude:<session id>`.
 *
 * Claude Code puts the id in `CLAUDE_CODE_SESSION_ID` when it spawns the
 * MCP server, so the environment is the baseline. A session can change
 * under a running server, though: `/clear` starts a new session with a
 * new id and keeps the server. The plugin's SessionStart hook therefore
 * records the current id in the state directory, keyed by the Claude Code
 * process id, and the server reads that file before falling back to its
 * environment. Both are children of the same Claude Code process, which
 * the hook sees as `CLAUDE_PID` and the server as its parent pid.
 */

// Header-safe by construction: the id ends up in a JSON field a reader
// matches against a request header the harness sent, so anything a header
// value could never hold — empty, whitespace, control characters — is
// refused rather than recorded. Claude Code's ids are UUIDs.
const SESSION_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/

/** `true` iff `value` is shaped like a session id Claude Code would send. */
export function isClaudeSessionId(value: unknown): value is string {
  return typeof value === 'string' && SESSION_ID_RE.test(value)
}

/** Directory of the per-Claude-process session files under the state dir. */
export function sessionsDir(stateDir: string): string {
  return join(stateDir, 'sessions')
}

/** The session file the hook writes for Claude Code process `pid`. */
export function sessionFilePath(stateDir: string, pid: number | string): string {
  return join(sessionsDir(stateDir), String(pid))
}

export type SessionIdSource = {
  /** The MCP server's environment. */
  env: NodeJS.ProcessEnv
  /** The channel's state directory (`NATS_STATE_DIR`). */
  stateDir: string
  /** The Claude Code process that launched the server (`process.ppid`). */
  parentPid: number
}

/**
 * The current session id, or `undefined` when neither the hook file nor
 * the environment names one. The file wins when present and well-formed:
 * it is newer than the environment by construction. A missing or
 * unreadable file is not an error; a malformed one is ignored.
 */
export function resolveClaudeSessionId(source: SessionIdSource): string | undefined {
  const fromHook = readSessionFile(sessionFilePath(source.stateDir, source.parentPid))
  if (fromHook !== undefined) return fromHook
  const fromEnv = source.env.CLAUDE_CODE_SESSION_ID
  return isClaudeSessionId(fromEnv) ? fromEnv : undefined
}

function readSessionFile(path: string): string | undefined {
  let text: string
  try {
    text = readFileSync(path, 'utf8')
  } catch {
    return undefined
  }
  const value = text.trim()
  return isClaudeSessionId(value) ? value : undefined
}
