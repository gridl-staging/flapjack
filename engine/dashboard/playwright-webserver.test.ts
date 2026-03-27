/* @vitest-environment node */
import { describe, expect, it, vi } from 'vitest'

import {
  ensureDashboardServer,
  resolveWaitForPortFreeTarget,
  waitForPortFree,
} from './scripts/playwright-webserver.mjs'

function createFakeChild() {
  return {
    once: vi.fn(),
    on: vi.fn(),
    kill: vi.fn(),
    stdout: null,
    stderr: null,
  }
}

describe('ensureDashboardServer', () => {
  it('reuses an already healthy dashboard server without spawning', async () => {
    const spawnServer = vi.fn()
    const probeUrl = vi.fn().mockResolvedValue(true)
    const probePort = vi.fn()

    const result = await ensureDashboardServer({
      url: 'http://127.0.0.1:5177',
      host: '127.0.0.1',
      port: 5177,
      probeUrl,
      probePort,
      spawnServer,
      sleep: vi.fn(),
      timeoutMs: 100,
      pollIntervalMs: 1,
    })

    expect(result.mode).toBe('reuse')
    expect(spawnServer).not.toHaveBeenCalled()
    expect(probePort).not.toHaveBeenCalled()
  })

  it('waits for an in-progress startup when the port is already bound', async () => {
    const spawnServer = vi.fn()
    const probeUrl = vi.fn()
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true)
    const probePort = vi.fn().mockResolvedValue(true)
    const sleep = vi.fn().mockResolvedValue(undefined)

    const acquireStartupLease = vi.fn().mockResolvedValue(vi.fn())

    const result = await ensureDashboardServer({
      url: 'http://127.0.0.1:5177',
      host: '127.0.0.1',
      port: 5177,
      probeUrl,
      probePort,
      spawnServer,
      sleep,
      acquireStartupLease,
      timeoutMs: 100,
      pollIntervalMs: 1,
    })

    expect(result.mode).toBe('wait')
    expect(spawnServer).not.toHaveBeenCalled()
    expect(probePort).toHaveBeenCalledWith('127.0.0.1', 5177)
    expect(sleep).toHaveBeenCalled()
  })

  it('waits for another wrapper process that already owns startup', async () => {
    const spawnServer = vi.fn()
    const probeUrl = vi.fn()
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true)
    const sleep = vi.fn().mockResolvedValue(undefined)
    const acquireStartupLease = vi.fn().mockResolvedValue(null)

    const result = await ensureDashboardServer({
      url: 'http://127.0.0.1:5177',
      host: '127.0.0.1',
      port: 5177,
      probeUrl,
      probePort: vi.fn().mockResolvedValue(false),
      spawnServer,
      sleep,
      acquireStartupLease,
      timeoutMs: 100,
      pollIntervalMs: 1,
    })

    expect(result.mode).toBe('wait')
    expect(acquireStartupLease).toHaveBeenCalledTimes(1)
    expect(spawnServer).not.toHaveBeenCalled()
    expect(sleep).toHaveBeenCalled()
  })

  it('spawns a new dashboard server when the port is free', async () => {
    const child = createFakeChild()
    const spawnServer = vi.fn().mockReturnValue(child)
    const probeUrl = vi.fn()
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true)
    const probePort = vi.fn().mockResolvedValue(false)

    const acquireStartupLease = vi.fn().mockResolvedValue(vi.fn())

    const result = await ensureDashboardServer({
      url: 'http://127.0.0.1:5177',
      host: '127.0.0.1',
      port: 5177,
      probeUrl,
      probePort,
      spawnServer,
      sleep: vi.fn().mockResolvedValue(undefined),
      acquireStartupLease,
      timeoutMs: 100,
      pollIntervalMs: 1,
    })

    expect(result).toEqual({
      mode: 'spawn',
      child,
    })
    expect(spawnServer).toHaveBeenCalledTimes(1)
  })
})

describe('waitForPortFree', () => {
  it('returns immediately when the port is already free', async () => {
    const probePort = vi.fn().mockResolvedValue(false)
    const sleep = vi.fn()

    await waitForPortFree('127.0.0.1', 5177, {
      probePort,
      sleep,
      timeoutMs: 100,
      pollIntervalMs: 1,
    })

    expect(probePort).toHaveBeenCalledTimes(1)
    expect(probePort).toHaveBeenCalledWith('127.0.0.1', 5177)
    expect(sleep).not.toHaveBeenCalled()
  })

  it('polls until the port becomes free', async () => {
    const probePort = vi.fn()
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(false)
    const sleep = vi.fn().mockResolvedValue(undefined)

    await waitForPortFree('127.0.0.1', 5177, {
      probePort,
      sleep,
      timeoutMs: 100,
      pollIntervalMs: 1,
    })

    expect(probePort).toHaveBeenCalledTimes(3)
    expect(sleep).toHaveBeenCalledTimes(2)
  })

  it('throws after timeout when the port never becomes free', async () => {
    const probePort = vi.fn().mockResolvedValue(true)
    const sleep = vi.fn().mockResolvedValue(undefined)

    await expect(waitForPortFree('127.0.0.1', 5177, {
      probePort,
      sleep,
      timeoutMs: 10,
      pollIntervalMs: 1,
    })).rejects.toThrow('Timed out waiting for 127.0.0.1:5177 to become free')
  })
})

describe('resolveWaitForPortFreeTarget', () => {
  it('prefers the Playwright URL target over the raw bind host', () => {
    expect(resolveWaitForPortFreeTarget({
      url: 'http://127.0.0.1:5177',
      host: '0.0.0.0',
      port: 5177,
    })).toEqual({
      host: '127.0.0.1',
      port: 5177,
    })
  })

  it('normalizes bracketed IPv6 URL hosts for node socket probes', () => {
    expect(resolveWaitForPortFreeTarget({
      url: 'http://[::1]:5177',
      host: '0.0.0.0',
      port: 5177,
    })).toEqual({
      host: '::1',
      port: 5177,
    })
  })

  it('falls back to the explicit host and port when the URL is missing', () => {
    expect(resolveWaitForPortFreeTarget({
      url: undefined,
      host: '127.0.0.1',
      port: 5177,
    })).toEqual({
      host: '127.0.0.1',
      port: 5177,
    })
  })
})
