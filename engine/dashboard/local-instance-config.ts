/**
 * Local instance configuration for development and testing.
 * Reads port/host settings from flapjack.local.conf for per-clone isolation.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const REPO_ROOT = path.resolve(__dirname, '..', '..');
const LOCAL_CONFIG_PATH = path.join(REPO_ROOT, 'flapjack.local.conf');
const MULTI_INSTANCE_STATE_DIR = path.join(process.env.TMPDIR || '/tmp', 'flapjack-multi-instance');
const DEFAULT_BACKEND_DATA_DIR = path.join(REPO_ROOT, 'engine', 'data');

const DEFAULTS = {
  host: '127.0.0.1',
  backendPort: 7700,
  dashboardPort: 5177,
  adminKey: 'fj_devtestadminkey000000',
} as const;

const LOOPBACK_HOSTS = new Set(['127.0.0.1', 'localhost', '::1', '[::1]']);

export interface LocalInstanceConfig {
  host: string;
  backendPort: number;
  dashboardPort: number;
  adminKey: string;
  backendBaseUrl: string;
  backendDataDir: string;
  dashboardBaseUrl: string;
  configPath: string;
  loadedFromFile: boolean;
}

/** Parses a shell-style config file into key-value pairs. */
export function parseLocalConfigFile(contents: string): Record<string, string> {
  const parsed: Record<string, string> = {};
  for (const rawLine of contents.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) {
      continue;
    }
    const assignment = line.startsWith('export ') ? line.slice('export '.length).trim() : line;
    const equalsAt = assignment.indexOf('=');
    if (equalsAt <= 0) {
      continue;
    }
    const key = assignment.slice(0, equalsAt).trim();
    let value = assignment.slice(equalsAt + 1).trim();
    if (!key) {
      continue;
    }
    value = parseShellAssignmentValue(value);
    parsed[key] = value;
  }
  return parsed;
}

/** Strips quotes and inline comments from a shell assignment right-hand side. */
function parseShellAssignmentValue(value: string): string {
  let parsedValue = '';
  let activeQuote: '"' | "'" | null = null;
  let trailingUnquotedWhitespace = 0;

  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];

    if (activeQuote === "'") {
      if (char === "'") {
        activeQuote = null;
      } else {
        parsedValue += char;
      }
      continue;
    }

    if (activeQuote === '"') {
      if (char === '"' && !hasOddBackslashRunBeforeIndex(value, index)) {
        activeQuote = null;
      } else {
        parsedValue += char;
      }
      continue;
    }

    if (char === '#' && /\s/.test(value[index - 1] ?? '')) {
      return stripTrailingUnquotedWhitespace(parsedValue, trailingUnquotedWhitespace);
    }

    if (char === '"' || char === "'") {
      activeQuote = char;
      trailingUnquotedWhitespace = 0;
      continue;
    }

    parsedValue += char;
    if (/\s/.test(char)) {
      trailingUnquotedWhitespace += 1;
      continue;
    }

    trailingUnquotedWhitespace = 0;
  }

  if (activeQuote) {
    // Keep malformed quoted assignments untouched instead of guessing where they end.
    return value;
  }

  return stripTrailingUnquotedWhitespace(parsedValue, trailingUnquotedWhitespace);
}

function hasOddBackslashRunBeforeIndex(value: string, index: number): boolean {
  let backslashCount = 0;
  for (let cursor = index - 1; cursor >= 0 && value[cursor] === '\\'; cursor -= 1) {
    backslashCount += 1;
  }

  return backslashCount % 2 === 1;
}

function stripTrailingUnquotedWhitespace(value: string, whitespaceCount: number): string {
  if (whitespaceCount === 0) {
    return value;
  }

  return value.slice(0, value.length - whitespaceCount);
}

function parsePort(raw: string | undefined, fallback: number): number {
  if (!raw) {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65535) {
    return fallback;
  }
  return parsed;
}

function resolveConfiguredPort(fallback: number, ...values: Array<string | undefined>): number {
  return parsePort(pickConfiguredValue(...values), fallback);
}

function parseHttpOrigin(raw: string | undefined): string | null {
  if (!raw) {
    return null;
  }
  try {
    const parsed = new URL(raw);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      return null;
    }
    return parsed.origin;
  } catch {
    return null;
  }
}

/** Builds an `http://host:port` origin string, with optional browser-safe bind-address substitution. */
function formatHttpOrigin(
  host: string,
  port: number,
  options: { browserSafe?: boolean } = {},
): string {
  let urlHost = host;

  // Browsers cannot navigate to unspecified bind addresses, so expose a loopback URL instead.
  if (options.browserSafe) {
    if (host === '0.0.0.0') {
      urlHost = '127.0.0.1';
    } else if (host === '::') {
      urlHost = '[::1]';
    }
  }

  if (urlHost.includes(':') && !urlHost.startsWith('[') && !urlHost.endsWith(']')) {
    urlHost = `[${urlHost}]`;
  }

  return `http://${urlHost}:${port}`;
}

function pickConfiguredValue(...values: Array<string | undefined>): string | undefined {
  for (const value of values) {
    if (value) {
      return value;
    }
  }

  return undefined;
}

/** Reads and parses `flapjack.local.conf` from disk, returning key-value pairs. */
function readLocalConfigValues(
  configPath: string = LOCAL_CONFIG_PATH,
): { fileValues: Record<string, string>; loadedFromFile: boolean } {
  if (!fs.existsSync(configPath)) {
    return { fileValues: {}, loadedFromFile: false };
  }

  try {
    const contents = fs.readFileSync(configPath, 'utf8');
    return {
      fileValues: parseLocalConfigFile(contents),
      loadedFromFile: true,
    };
  } catch {
    return { fileValues: {}, loadedFromFile: false };
  }
}

function currentUid(): number | null {
  return typeof process.getuid === 'function' ? process.getuid() : null;
}

function hasUnsafeWritePermissions(mode: number): boolean {
  return (mode & 0o022) !== 0;
}

function isSecureOwnedPath(stats: fs.Stats, expectedUid: number | null): boolean {
  if (expectedUid !== null && typeof stats.uid === 'number' && stats.uid !== expectedUid) {
    return false;
  }

  if (process.platform !== 'win32' && hasUnsafeWritePermissions(stats.mode)) {
    return false;
  }

  return true;
}

/** Return paths to .meta files in the state directory, filtering out symlinks and insecurely-owned entries. */
function secureTrackedMetaFiles(stateDir: string): string[] {
  const expectedUid = currentUid();

  try {
    const stateDirStats = fs.lstatSync(stateDir);
    if (!stateDirStats.isDirectory() || stateDirStats.isSymbolicLink()) {
      return [];
    }
    if (!isSecureOwnedPath(stateDirStats, expectedUid)) {
      return [];
    }

    const realStateDir = fs.realpathSync(stateDir);
    return fs.readdirSync(realStateDir, { withFileTypes: true })
      .filter((entry) => entry.isFile() && entry.name.endsWith('.meta'))
      .map((entry) => path.join(realStateDir, entry.name))
      .filter((entryPath) => {
        try {
          const entryStats = fs.lstatSync(entryPath);
          if (!entryStats.isFile() || entryStats.isSymbolicLink()) {
            return false;
          }
          return isSecureOwnedPath(entryStats, expectedUid);
        } catch {
          return false;
        }
      });
  } catch {
    return [];
  }
}

/** Resolve and validate a raw data directory path, returning null if invalid or non-existent. */
function normalizeTrackedDataDir(rawPath: string | undefined): string | null {
  if (!rawPath || !path.isAbsolute(rawPath)) {
    return null;
  }

  try {
    const resolved = fs.realpathSync(rawPath);
    const stats = fs.statSync(resolved);
    if (!stats.isDirectory()) {
      return null;
    }
    return resolved;
  } catch {
    return null;
  }
}

export function findTrackedBackendDataDir(
  backendBaseUrl: string,
  stateDir: string = MULTI_INSTANCE_STATE_DIR,
): string | null {
  let backendHost: string;
  try {
    backendHost = new URL(backendBaseUrl).host;
  } catch {
    return null;
  }

  try {
    for (const metaPath of secureTrackedMetaFiles(stateDir)) {
      const contents = fs.readFileSync(metaPath, 'utf8');
      const values = parseLocalConfigFile(contents);
      if (values.bind_addr !== backendHost) {
        continue;
      }

      const trackedDataDir = normalizeTrackedDataDir(values.data_dir);
      if (trackedDataDir) {
        return trackedDataDir;
      }
    }
  } catch {
    return null;
  }

  return null;
}

/** Resolves the backend data directory from env vars, config file, multi-instance state, or the default path. */
export function resolveBackendDataDir(
  fileValues: Record<string, string>,
  backendBaseUrl: string,
  stateDir: string = MULTI_INSTANCE_STATE_DIR,
): string {
  return (
    pickConfiguredValue(
      process.env.FLAPJACK_DATA_DIR,
      process.env.FJ_DATA_DIR,
      fileValues.FLAPJACK_DATA_DIR,
      fileValues.FJ_DATA_DIR,
    )
    || findTrackedBackendDataDir(backendBaseUrl, stateDir)
    || DEFAULT_BACKEND_DATA_DIR
  );
}

/** Returns the configured admin key, falling back to the dev default for loopback hosts. */
export function resolveAdminKey(
  configuredAdminKey: string | undefined,
  backendBaseUrl: string,
): string {
  if (configuredAdminKey) {
    return configuredAdminKey;
  }

  let hostname: string;
  try {
    hostname = new URL(backendBaseUrl).hostname;
  } catch {
    return DEFAULTS.adminKey;
  }

  if (LOOPBACK_HOSTS.has(hostname)) {
    return DEFAULTS.adminKey;
  }

  throw new Error(
    `FJ_TEST_ADMIN_KEY must be set when using a non-loopback backend URL: ${backendBaseUrl}`,
  );
}

export function getLocalInstanceConfig(): LocalInstanceConfig {
  const { fileValues, loadedFromFile } = readLocalConfigValues();
  const host = pickConfiguredValue(fileValues.FJ_HOST, process.env.FJ_HOST) || DEFAULTS.host;
  const backendPort = resolveConfiguredPort(
    DEFAULTS.backendPort,
    fileValues.FJ_BACKEND_PORT,
    process.env.FJ_BACKEND_PORT,
  );
  const dashboardPort = resolveConfiguredPort(
    DEFAULTS.dashboardPort,
    fileValues.FJ_DASHBOARD_PORT,
    process.env.FJ_DASHBOARD_PORT,
    process.env.FLAPJACK_DASHBOARD_PORT,
  );
  const backendBaseUrl =
    parseHttpOrigin(process.env.FLAPJACK_BACKEND_URL) || formatHttpOrigin(host, backendPort);
  const adminKey = resolveAdminKey(
    pickConfiguredValue(fileValues.FJ_TEST_ADMIN_KEY, process.env.FJ_TEST_ADMIN_KEY),
    backendBaseUrl,
  );
  const backendDataDir = resolveBackendDataDir(fileValues, backendBaseUrl);
  const dashboardBaseUrl = formatHttpOrigin(host, dashboardPort, { browserSafe: true });

  return {
    host,
    backendPort,
    dashboardPort,
    adminKey,
    backendBaseUrl,
    backendDataDir,
    dashboardBaseUrl,
    configPath: LOCAL_CONFIG_PATH,
    loadedFromFile,
  };
}
