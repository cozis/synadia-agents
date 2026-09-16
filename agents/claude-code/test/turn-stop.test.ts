import { describe, expect, test } from 'bun:test'
import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { sessionFilePath, sessionsDir, stopFilePath, type SessionIdSource } from '../src/session-id.js'
import { turnStopWaiter } from '../src/turn-stop.js'

const PID = 4242
const SESSION = '317f624b-c7c6-4b27-936b-c0de80892e6d'

function withSource(run: (source: SessionIdSource) => Promise<void>): Promise<void> {
  const stateDir = mkdtempSync(join(tmpdir(), 'claude-channel-stop-'))
  return run({ env: {}, stateDir, parentPid: PID }).finally(() =>
    rmSync(stateDir, { recursive: true, force: true }),
  )
}

function activateHooks(source: SessionIdSource): void {
  mkdirSync(sessionsDir(source.stateDir), { recursive: true })
  writeFileSync(sessionFilePath(source.stateDir, PID), `${SESSION}\n`)
}

function recordStop(source: SessionIdSource, ts: number, atMs: number = Date.now()): void {
  const path = stopFilePath(source.stateDir, PID)
  writeFileSync(path, `${ts}\n`)
  utimesSync(path, atMs / 1000, atMs / 1000)
}

describe('turnStopWaiter', () => {
  test('without active hooks the reply is the turn end: resolves at once', () =>
    withSource(async source => {
      const started = Date.now()
      expect(await turnStopWaiter(source, Date.now(), 5_000, 10).wait()).toBeUndefined()
      expect(Date.now() - started).toBeLessThan(500)
    }))

  test('resolves with the Stop recorded after the reply', () =>
    withSource(async source => {
      activateHooks(source)
      const waiter = turnStopWaiter(source, Date.now(), 5_000, 10)
      const waiting = waiter.wait()
      await Bun.sleep(50)
      recordStop(source, 1_700_000_123, Date.now() + 5)
      expect(await waiting).toBe(1_700_000_123)
    }))

  test('a Stop older than the reply belongs to an earlier turn and is ignored', () =>
    withSource(async source => {
      activateHooks(source)
      recordStop(source, 1_700_000_000, Date.now() - 10_000)
      const waiter = turnStopWaiter(source, Date.now(), 200, 10)
      expect(await waiter.wait()).toBeUndefined()
    }))

  test('gives up at the limit', () =>
    withSource(async source => {
      activateHooks(source)
      const started = Date.now()
      expect(await turnStopWaiter(source, Date.now(), 150, 10).wait()).toBeUndefined()
      const elapsed = Date.now() - started
      expect(elapsed).toBeGreaterThanOrEqual(140)
      expect(elapsed).toBeLessThan(2_000)
    }))

  test('cancel ends the wait now', () =>
    withSource(async source => {
      activateHooks(source)
      const waiter = turnStopWaiter(source, Date.now(), 60_000, 10)
      const waiting = waiter.wait()
      await Bun.sleep(30)
      waiter.cancel()
      expect(await waiting).toBeUndefined()
    }))

  test('a malformed Stop file is ignored', () =>
    withSource(async source => {
      activateHooks(source)
      writeFileSync(stopFilePath(source.stateDir, PID), 'soon\n')
      expect(await turnStopWaiter(source, Date.now() - 1_000, 100, 10).wait()).toBeUndefined()
    }))
})
