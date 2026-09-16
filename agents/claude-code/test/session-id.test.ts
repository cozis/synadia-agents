import { describe, expect, test } from 'bun:test'
import { spawnSync } from 'node:child_process'
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  hooksActive,
  isClaudeSessionId,
  readTurnStop,
  resolveClaudeSessionId,
  sessionFilePath,
  sessionsDir,
  stopFilePath,
} from '../src/session-id.js'

const SESSION = '317f624b-c7c6-4b27-936b-c0de80892e6d'
const NEWER = '9d0b2c4e-1111-4222-8333-444455556666'
const HOOK = join(dirname(fileURLToPath(import.meta.url)), '..', 'hooks', 'session-event.ts')

function withStateDir(run: (stateDir: string) => void): void {
  const stateDir = mkdtempSync(join(tmpdir(), 'claude-channel-session-'))
  try {
    run(stateDir)
  } finally {
    rmSync(stateDir, { recursive: true, force: true })
  }
}

describe('isClaudeSessionId', () => {
  test('accepts a UUID and other header-safe ids, rejects the rest', () => {
    expect(isClaudeSessionId(SESSION)).toBe(true)
    expect(isClaudeSessionId('abc_123.x:y')).toBe(true)
    expect(isClaudeSessionId(undefined)).toBe(false)
    expect(isClaudeSessionId(42)).toBe(false)
    expect(isClaudeSessionId('')).toBe(false)
    expect(isClaudeSessionId('-leading')).toBe(false)
    expect(isClaudeSessionId('has space')).toBe(false)
    expect(isClaudeSessionId('a\nb')).toBe(false)
    expect(isClaudeSessionId('a'.repeat(128))).toBe(true)
    expect(isClaudeSessionId('a'.repeat(129))).toBe(false)
  })
})

describe('resolveClaudeSessionId', () => {
  test('falls back to the environment when no hook file exists', () => {
    withStateDir(stateDir => {
      expect(resolveClaudeSessionId({
        env: { CLAUDE_CODE_SESSION_ID: SESSION },
        stateDir,
        parentPid: 4242,
      })).toBe(SESSION)
      expect(resolveClaudeSessionId({ env: {}, stateDir, parentPid: 4242 })).toBeUndefined()
      expect(resolveClaudeSessionId({
        env: { CLAUDE_CODE_SESSION_ID: 'not a session' },
        stateDir,
        parentPid: 4242,
      })).toBeUndefined()
    })
  })

  test('the hook file for the parent pid wins over the environment', () => {
    withStateDir(stateDir => {
      mkdirSync(sessionsDir(stateDir), { recursive: true })
      writeFileSync(sessionFilePath(stateDir, 4242), `${NEWER}\n`)
      const env = { CLAUDE_CODE_SESSION_ID: SESSION }
      expect(resolveClaudeSessionId({ env, stateDir, parentPid: 4242 })).toBe(NEWER)
      // Another Claude Code process's file is not this server's.
      expect(resolveClaudeSessionId({ env, stateDir, parentPid: 4243 })).toBe(SESSION)
    })
  })

  test('a malformed hook file is ignored, not an error', () => {
    withStateDir(stateDir => {
      mkdirSync(sessionsDir(stateDir), { recursive: true })
      writeFileSync(sessionFilePath(stateDir, 4242), 'garbage id\n')
      expect(resolveClaudeSessionId({
        env: { CLAUDE_CODE_SESSION_ID: SESSION },
        stateDir,
        parentPid: 4242,
      })).toBe(SESSION)
    })
  })
})

describe('readTurnStop and hooksActive', () => {
  test('hooks are active once the SessionStart file exists', () => {
    withStateDir(stateDir => {
      const source = { env: {}, stateDir, parentPid: 4242 }
      expect(hooksActive(source)).toBe(false)
      mkdirSync(sessionsDir(stateDir), { recursive: true })
      writeFileSync(sessionFilePath(stateDir, 4242), `${SESSION}\n`)
      expect(hooksActive(source)).toBe(true)
    })
  })

  test('reads the Stop file as unix seconds with its mtime, ignores garbage', () => {
    withStateDir(stateDir => {
      const source = { env: {}, stateDir, parentPid: 4242 }
      expect(readTurnStop(source)).toBeUndefined()
      mkdirSync(sessionsDir(stateDir), { recursive: true })
      writeFileSync(stopFilePath(stateDir, 4242), '1700000000\n')
      const stop = readTurnStop(source)
      expect(stop?.ts).toBe(1_700_000_000)
      expect(stop?.atMs).toBeGreaterThan(Date.now() - 10_000)
      writeFileSync(stopFilePath(stateDir, 4242), '-5\n')
      expect(readTurnStop(source)).toBeUndefined()
      writeFileSync(stopFilePath(stateDir, 4242), 'later\n')
      expect(readTurnStop(source)).toBeUndefined()
    })
  })
})

describe('the hook script', () => {
  function runHook(input: string, env: Record<string, string>) {
    return spawnSync('bun', [HOOK], {
      input,
      env: { ...process.env, ...env },
      encoding: 'utf8',
    })
  }

  test('records the session id for the Claude Code process, silently', () => {
    withStateDir(stateDir => {
      const result = runHook(
        JSON.stringify({ session_id: NEWER, hook_event_name: 'SessionStart', source: 'clear' }),
        { CLAUDE_PID: '4242', NATS_STATE_DIR: stateDir },
      )
      expect(result.status).toBe(0)
      // Anything on stdout would enter the model's context.
      expect(result.stdout).toBe('')
      expect(readFileSync(sessionFilePath(stateDir, 4242), 'utf8')).toBe(`${NEWER}\n`)
      expect(resolveClaudeSessionId({ env: {}, stateDir, parentPid: 4242 })).toBe(NEWER)
    })
  })

  test('records a turn end on Stop, keyed by the Claude Code process', () => {
    withStateDir(stateDir => {
      const before = Math.floor(Date.now() / 1000)
      const result = runHook(
        JSON.stringify({ session_id: NEWER, hook_event_name: 'Stop', stop_hook_active: false }),
        { CLAUDE_PID: '4242', NATS_STATE_DIR: stateDir },
      )
      expect(result.status).toBe(0)
      expect(result.stdout).toBe('')
      const stop = readTurnStop({ env: {}, stateDir, parentPid: 4242 })
      expect(stop?.ts).toBeGreaterThanOrEqual(before)
      // A Stop names no session: the session file is the SessionStart hook's.
      expect(existsSync(sessionFilePath(stateDir, 4242))).toBe(false)
    })
  })

  test('exits 0 and writes nothing on a malformed payload or without CLAUDE_PID', () => {
    withStateDir(stateDir => {
      const bad = runHook('not json', { CLAUDE_PID: '4242', NATS_STATE_DIR: stateDir })
      expect(bad.status).toBe(0)
      expect(bad.stdout).toBe('')
      expect(existsSync(sessionFilePath(stateDir, 4242))).toBe(false)

      const unshaped = runHook(
        JSON.stringify({ session_id: 'not a session' }),
        { CLAUDE_PID: '4242', NATS_STATE_DIR: stateDir },
      )
      expect(unshaped.status).toBe(0)
      expect(existsSync(sessionFilePath(stateDir, 4242))).toBe(false)

      const env: Record<string, string> = { NATS_STATE_DIR: stateDir }
      const orphan = spawnSync('bun', [HOOK], {
        input: JSON.stringify({ session_id: NEWER }),
        env: { ...Object.fromEntries(Object.entries(process.env).filter(([k]) => k !== 'CLAUDE_PID')), ...env },
        encoding: 'utf8',
      })
      expect(orphan.status).toBe(0)
      expect(existsSync(sessionsDir(stateDir))).toBe(false)
    })
  })
})
