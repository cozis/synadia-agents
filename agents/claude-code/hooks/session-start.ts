#!/usr/bin/env bun
/**
 * SessionStart hook: record the session id Claude Code is now using so the
 * channel's MCP server, which keeps running across `/clear`, files each
 * prompt under the right session.
 *
 * Reads the hook payload from stdin, writes the session id to
 * `<state dir>/sessions/<CLAUDE_PID>` atomically, and exits 0 whatever
 * happens — a hook must never interrupt a session, and anything printed
 * to stdout would land in the model's context. It depends on nothing
 * outside the plugin directory, so it runs from source.
 */

import { mkdirSync, renameSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { isClaudeSessionId, sessionFilePath, sessionsDir } from '../src/session-id.js'

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of process.stdin) chunks.push(Buffer.from(chunk))
  return Buffer.concat(chunks).toString('utf8')
}

try {
  const pid = process.env.CLAUDE_PID
  if (pid !== undefined && /^\d+$/.test(pid)) {
    const payload = JSON.parse(await readStdin()) as { session_id?: unknown }
    if (isClaudeSessionId(payload.session_id)) {
      const stateDir = process.env.NATS_STATE_DIR ?? join(homedir(), '.claude', 'channels', 'nats')
      mkdirSync(sessionsDir(stateDir), { recursive: true })
      const target = sessionFilePath(stateDir, pid)
      const staging = `${target}.${process.pid}.tmp`
      writeFileSync(staging, `${payload.session_id}\n`)
      renameSync(staging, target)
    }
  }
} catch {
  // Best effort: the server falls back to the session id in its environment.
}
process.exit(0)
