import fs from 'node:fs/promises';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const DEFAULT_TIMEOUT_MS = 120_000;
const DEFAULT_POLL_INTERVAL_MS = 250;
const PORT_PROBE_TIMEOUT_MS = 500;

function parseInteger(value, fallback) {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function probeUrl(url, fetchImpl = fetch) {
  try {
    const response = await fetchImpl(url, { method: 'GET' });
    return response.status < 500;
  } catch {
    return false;
  }
}

export function probePort(host, port) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host, port });
    let settled = false;

    const finish = (value) => {
      if (settled) {
        return;
      }
      settled = true;
      socket.destroy();
      resolve(value);
    };

    socket.setTimeout(PORT_PROBE_TIMEOUT_MS);
    socket.once('connect', () => finish(true));
    socket.once('timeout', () => finish(false));
    socket.once('error', () => finish(false));
  });
}

function buildStartupLeasePath(port) {
  return path.join(os.tmpdir(), `flapjack-playwright-webserver-${port}.lock`);
}

export async function acquireStartupLease(lockPath) {
  try {
    const handle = await fs.open(lockPath, 'wx');
    let released = false;

    return async () => {
      if (released) {
        return;
      }
      released = true;
      await handle.close().catch(() => {});
      await fs.unlink(lockPath).catch(() => {});
    };
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'EEXIST') {
      return null;
    }
    throw error;
  }
}

function readWebServerEnv() {
  return {
    url: process.env.PLAYWRIGHT_WEBSERVER_URL,
    host: process.env.PLAYWRIGHT_WEBSERVER_HOST,
    port: parseInteger(process.env.PLAYWRIGHT_WEBSERVER_PORT, 0),
    allowReuse: process.env.PLAYWRIGHT_WEBSERVER_REUSE !== '0',
    timeoutMs: parseInteger(process.env.PLAYWRIGHT_WEBSERVER_TIMEOUT_MS, DEFAULT_TIMEOUT_MS),
    pollIntervalMs: parseInteger(
      process.env.PLAYWRIGHT_WEBSERVER_POLL_INTERVAL_MS,
      DEFAULT_POLL_INTERVAL_MS,
    ),
  };
}

async function waitForUrlReady({
  url,
  timeoutMs,
  pollIntervalMs,
  probeUrl: probeUrlImpl,
  sleep,
}) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() <= deadline) {
    if (await probeUrlImpl(url)) {
      return;
    }
    await sleep(pollIntervalMs);
  }

  throw new Error(`Timed out waiting for dashboard server at ${url}`);
}

export function resolveWaitForPortFreeTarget({ url, host, port }) {
  if (url) {
    try {
      const parsed = new URL(url);
      const parsedPort = parseInteger(parsed.port, parsed.protocol === 'https:' ? 443 : 80);
      const parsedHost = parsed.hostname.replace(/^\[(.*)\]$/, '$1');

      if (parsedHost && parsedPort) {
        return {
          host: parsedHost,
          port: parsedPort,
        };
      }
    } catch {
      // Fall back to the explicit bind target when the URL is malformed.
    }
  }

  return { host, port };
}

export async function waitForPortFree(host, port, {
  probePort: probePortImpl = probePort,
  sleep = delay,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
} = {}) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() <= deadline) {
    if (!await probePortImpl(host, port)) {
      return;
    }
    await sleep(pollIntervalMs);
  }

  throw new Error(`Timed out waiting for ${host}:${port} to become free`);
}

export async function ensureDashboardServer({
  url,
  host,
  port,
  allowReuse = true,
  timeoutMs = DEFAULT_TIMEOUT_MS,
  pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
  probeUrl: probeUrlImpl = probeUrl,
  probePort: probePortImpl = probePort,
  sleep = delay,
  acquireStartupLease: acquireStartupLeaseImpl = () => acquireStartupLease(buildStartupLeasePath(port)),
  spawnServer = () =>
    spawn('npm', ['run', 'dev'], {
      stdio: 'inherit',
      env: process.env,
    }),
}) {
  const waitForReady = () =>
    waitForUrlReady({
      url,
      timeoutMs,
      pollIntervalMs,
      probeUrl: probeUrlImpl,
      sleep,
    });

  if (allowReuse && await probeUrlImpl(url)) {
    return { mode: 'reuse' };
  }

  let releaseStartupLease = null;

  if (allowReuse) {
    releaseStartupLease = await acquireStartupLeaseImpl();
    if (!releaseStartupLease) {
      await waitForReady();
      return { mode: 'wait' };
    }
  }

  const releaseLease = async () => {
    if (releaseStartupLease) {
      const release = releaseStartupLease;
      releaseStartupLease = null;
      await release();
    }
  };

  if (allowReuse && await probePortImpl(host, port)) {
    try {
      await waitForReady();
      return { mode: 'wait' };
    } finally {
      await releaseLease();
    }
  }

  try {
    const child = spawnServer();
    const childExit = new Promise((_, reject) => {
      child.once('error', reject);
      child.once('exit', (code, signal) => {
        reject(
          new Error(
            `Dashboard dev server exited before becoming ready (code=${code ?? 'null'}, signal=${signal ?? 'null'})`,
          ),
        );
      });
    });

    await Promise.race([
      waitForReady(),
      childExit,
    ]);

    await releaseLease();
    return { mode: 'spawn', child };
  } catch (error) {
    await releaseLease();
    throw error;
  }
}

function waitForShutdownSignal() {
  return new Promise((resolve) => {
    let settled = false;
    const keepAlive = setInterval(() => {}, 1_000);

    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      clearInterval(keepAlive);
      resolve();
    };

    process.once('SIGINT', finish);
    process.once('SIGTERM', finish);
  });
}

async function runWaitForPortFree() {
  const {
    url,
    host,
    port,
    timeoutMs,
    pollIntervalMs,
  } = readWebServerEnv();
  const probeTarget = resolveWaitForPortFreeTarget({ url, host, port });

  if (!probeTarget.host || !probeTarget.port) {
    throw new Error('PLAYWRIGHT_WEBSERVER_HOST and PLAYWRIGHT_WEBSERVER_PORT are required');
  }

  await waitForPortFree(probeTarget.host, probeTarget.port, {
    timeoutMs,
    pollIntervalMs,
  });
}

async function run() {
  if (process.argv.includes('--wait-port-free')) {
    await runWaitForPortFree();
    return;
  }

  const {
    url,
    host,
    port,
    allowReuse,
    timeoutMs,
    pollIntervalMs,
  } = readWebServerEnv();

  if (!url || !host || !port) {
    throw new Error(
      'PLAYWRIGHT_WEBSERVER_URL, PLAYWRIGHT_WEBSERVER_HOST, and PLAYWRIGHT_WEBSERVER_PORT are required',
    );
  }

  const result = await ensureDashboardServer({
    url,
    host,
    port,
    allowReuse,
    timeoutMs,
    pollIntervalMs,
  });

  if (result.mode === 'spawn') {
    const child = result.child;
    const forwardSignal = (signal) => {
      if (!child.killed) {
        child.kill(signal);
      }
    };

    process.once('SIGINT', () => forwardSignal('SIGINT'));
    process.once('SIGTERM', () => forwardSignal('SIGTERM'));

    await new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', (code, signal) => {
        if (signal || code === 0) {
          resolve();
          return;
        }
        reject(new Error(`Dashboard dev server exited with code ${code}`));
      });
    });
    return;
  }

  await waitForShutdownSignal();
}

const invokedAsScript =
  process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;

if (invokedAsScript) {
  run().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exit(1);
  });
}
