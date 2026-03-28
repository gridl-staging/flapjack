const DEFAULT_CONFIG = {
  baseUrl: "http://127.0.0.1:7700",
  readIndexName: "loadtest_read",
  writeIndexName: "loadtest_write",
  benchmarkIndexName: "benchmark_100k",
  appId: "flapjack",
  apiKey: "",
  soakDuration: "4h",
  taskPollMaxAttempts: 20000,
  taskPollIntervalSeconds: 0.01,
};

function readEnvValue(name, fallback) {
  if (typeof __ENV !== "undefined" && Object.prototype.hasOwnProperty.call(__ENV, name)) {
    return __ENV[name];
  }
  if (
    typeof process !== "undefined" &&
    process.env &&
    Object.prototype.hasOwnProperty.call(process.env, name)
  ) {
    return process.env[name];
  }
  return fallback;
}

function readPositiveNumber(name, fallback) {
  const rawValue = readEnvValue(name, String(fallback));
  const numericValue = Number(rawValue);
  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return fallback;
  }
  return numericValue;
}

export const sharedLoadtestConfig = Object.freeze({
  baseUrl: readEnvValue("FLAPJACK_LOADTEST_BASE_URL", DEFAULT_CONFIG.baseUrl),
  readIndexName: readEnvValue("FLAPJACK_LOADTEST_READ_INDEX", DEFAULT_CONFIG.readIndexName),
  writeIndexName: readEnvValue("FLAPJACK_LOADTEST_WRITE_INDEX", DEFAULT_CONFIG.writeIndexName),
  benchmarkIndexName: readEnvValue("FLAPJACK_LOADTEST_BENCHMARK_INDEX", DEFAULT_CONFIG.benchmarkIndexName),
  appId: readEnvValue("FLAPJACK_LOADTEST_APP_ID", DEFAULT_CONFIG.appId),
  apiKey: readEnvValue("FLAPJACK_LOADTEST_API_KEY", DEFAULT_CONFIG.apiKey),
  soakDuration: readEnvValue("FLAPJACK_LOADTEST_SOAK_DURATION", DEFAULT_CONFIG.soakDuration),
  taskPollMaxAttempts: Math.floor(
    readPositiveNumber("FLAPJACK_LOADTEST_TASK_MAX_ATTEMPTS", DEFAULT_CONFIG.taskPollMaxAttempts),
  ),
  taskPollIntervalSeconds: readPositiveNumber(
    "FLAPJACK_LOADTEST_TASK_POLL_INTERVAL_SECONDS",
    DEFAULT_CONFIG.taskPollIntervalSeconds,
  ),
});
