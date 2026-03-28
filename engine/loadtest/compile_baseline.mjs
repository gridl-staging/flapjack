#!/usr/bin/env node

import { execSync } from "node:child_process";
import { constants, createReadStream } from "node:fs";
import { access, readFile } from "node:fs/promises";
import path from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { summarizeBatchLatencies } from "./import_benchmark.mjs";

const REQUIRED_K6_SCENARIOS = Object.freeze([
  "smoke",
  "search-throughput",
  "write-throughput",
  "mixed-workload",
  "spike",
  "memory-pressure",
]);
const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(MODULE_DIR, "..", "..");

function formatNumber(value, digits = 1) {
  if (!Number.isFinite(value)) {
    return "n/a";
  }
  return Number(value).toFixed(digits);
}

function markdownValue(value) {
  return value == null || value === "" ? "n/a" : value;
}

function safeShell(command, fallback = "unknown") {
  try {
    const output = execSync(command, { stdio: ["ignore", "pipe", "ignore"] }).toString().trim();
    return output.length > 0 ? output : fallback;
  } catch {
    return fallback;
  }
}

function normalizePath(value) {
  if (!value) {
    return "";
  }
  const absolutePath = path.resolve(value);
  const relativePath = path.relative(REPO_ROOT, absolutePath);
  if (relativePath && !relativePath.startsWith("..") && !path.isAbsolute(relativePath)) {
    return relativePath.split(path.sep).join("/");
  }
  return absolutePath;
}

async function fileExists(filePath) {
  if (!filePath) {
    return false;
  }
  try {
    await access(filePath, constants.F_OK);
    return true;
  } catch {
    return false;
  }
}

async function readTextFileIfExists(filePath) {
  if (!(await fileExists(filePath))) {
    return "";
  }
  return readFile(filePath, "utf8");
}

async function readJsonFile(filePath, missingMessage) {
  if (!(await fileExists(filePath))) {
    throw new Error(missingMessage ?? `missing file: ${filePath}`);
  }
  const contents = await readFile(filePath, "utf8");
  return JSON.parse(contents);
}

function emptyLatencySummary() {
  return { p95: null, p99: null };
}

function parseDurationToMs(token) {
  if (!token) {
    return null;
  }
  const matched = String(token).trim().match(/^([0-9]*\.?[0-9]+)(ns|us|ms|s)$/i);
  if (!matched) {
    return null;
  }
  const value = Number(matched[1]);
  const unit = matched[2].toLowerCase();
  if (unit === "ns") return value / 1_000_000;
  if (unit === "us") return value / 1_000;
  if (unit === "ms") return value;
  if (unit === "s") return value * 1_000;
  return null;
}

function parseStdoutLatencySummary(stdoutText) {
  if (!stdoutText) {
    return emptyLatencySummary();
  }
  const lineMatch = stdoutText.match(/^.*http_req_duration.*$/im);
  if (!lineMatch) {
    return emptyLatencySummary();
  }
  const summaryLine = lineMatch[0];
  const p95Token = summaryLine.match(/p\(95\)=([0-9.]+(?:ns|us|ms|s))/i)?.[1] ?? null;
  const p99Token = summaryLine.match(/p\(99\)=([0-9.]+(?:ns|us|ms|s))/i)?.[1] ?? null;
  return {
    p95: parseDurationToMs(p95Token),
    p99: parseDurationToMs(p99Token),
  };
}

function parseStdoutScenarioStatus(stdoutText) {
  if (!stdoutText) {
    return "UNKNOWN";
  }
  if (/thresholds?.*(crossed|failed|on metrics)/i.test(stdoutText)) {
    return "FAIL";
  }
  return "PASS";
}

async function parseK6JsonArtifacts(jsonPath) {
  if (!(await fileExists(jsonPath))) {
    return { latenciesMs: [], hasThresholdFailure: false };
  }

  const latenciesMs = [];
  let hasThresholdFailure = false;
  const lineReader = createInterface({
    input: createReadStream(jsonPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const rawLine of lineReader) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }
    let entry;
    try {
      entry = JSON.parse(line);
    } catch {
      continue;
    }

    if (entry?.type === "Point" && entry?.metric === "http_req_duration") {
      const value = Number(entry?.data?.value);
      if (Number.isFinite(value) && value >= 0) {
        latenciesMs.push(value);
      }
    }

    if (entry?.type === "Metric" && entry?.data?.thresholds && typeof entry.data.thresholds === "object") {
      for (const threshold of Object.values(entry.data.thresholds)) {
        if (threshold && typeof threshold === "object" && threshold.ok === false) {
          hasThresholdFailure = true;
        }
      }
    }
  }

  return { latenciesMs, hasThresholdFailure };
}

async function summarizeK6Scenario({ scenarioName, jsonPath, stdoutPath }) {
  const [parsedJson, stdoutText] = await Promise.all([
    parseK6JsonArtifacts(jsonPath),
    readTextFileIfExists(stdoutPath),
  ]);

  const jsonLatencySummary = summarizeBatchLatencies(parsedJson.latenciesMs);
  const fallbackStdoutLatency = parseStdoutLatencySummary(stdoutText);

  const statusFromStdout = parseStdoutScenarioStatus(stdoutText);
  const status = parsedJson.hasThresholdFailure || statusFromStdout === "FAIL" ? "FAIL" : statusFromStdout;
  const p95 = jsonLatencySummary.count > 0 ? jsonLatencySummary.p95 : fallbackStdoutLatency.p95;
  const p99 = jsonLatencySummary.count > 0 ? jsonLatencySummary.p99 : fallbackStdoutLatency.p99;

  return {
    scenarioName,
    status,
    count: jsonLatencySummary.count,
    p95: Number.isFinite(p95) ? p95 : null,
    p99: Number.isFinite(p99) ? p99 : null,
    jsonPath: normalizePath(jsonPath),
    stdoutPath: normalizePath(stdoutPath),
  };
}

function unavailableDashboardStatus(reportPath) {
  return {
    collected: false,
    message: "not available",
    reportPath: reportPath || "",
  };
}

function collectHardwareAndOs() {
  const cpuModel = safeShell("sysctl -n machdep.cpu.brand_string", safeShell("sysctl -n hw.model", "unknown"));
  const ramBytesRaw = safeShell("sysctl -n hw.memsize", "0");
  const ramBytes = Number(ramBytesRaw);
  const ramGiB = Number.isFinite(ramBytes) && ramBytes > 0 ? `${(ramBytes / (1024 ** 3)).toFixed(2)} GiB` : "unknown";

  const osName = safeShell("sw_vers -productName", "unknown");
  const osVersion = safeShell("sw_vers -productVersion", "unknown");
  const osBuild = safeShell("sw_vers -buildVersion", "unknown");

  return {
    cpuModel,
    ram: ramGiB,
    osVersion: `${osName} ${osVersion} (Build ${osBuild})`,
    kernel: safeShell("uname -srmo", "unknown"),
  };
}

function firstFiniteNumber(...values) {
  for (const value of values) {
    const numericValue = Number(value);
    if (Number.isFinite(numericValue)) {
      return numericValue;
    }
  }
  return null;
}

async function collectDashboardStatus(reportPath) {
  if (!(await fileExists(reportPath))) {
    return unavailableDashboardStatus(reportPath);
  }

  let reportJson;
  try {
    reportJson = await readJsonFile(reportPath);
  } catch {
    return unavailableDashboardStatus(reportPath);
  }

  const durationMs = firstFiniteNumber(
    reportJson?.stats?.duration,
    reportJson?.duration,
    reportJson?.summary?.duration,
  );
  const expectedCount = firstFiniteNumber(reportJson?.stats?.expected, reportJson?.expected);
  const unexpectedCount = firstFiniteNumber(reportJson?.stats?.unexpected, reportJson?.unexpected);

  return {
    collected: true,
    reportPath: reportPath || "",
    timings: {
      totalDurationMs: durationMs,
      expectedTests: expectedCount,
      unexpectedTests: unexpectedCount,
    },
  };
}

function inferDatasetSize(importBenchmark, searchBenchmark) {
  const importDocs = Number(importBenchmark?.totalDocs) || 0;
  const searchDocs = Number(searchBenchmark?.docCount) || 0;
  return Math.max(importDocs, searchDocs, 0);
}

function inferImportBatchSize(importBenchmark) {
  const totalDocs = Number(importBenchmark?.totalDocs) || 0;
  const batchCount = Number(importBenchmark?.batchCount) || 0;
  if (totalDocs > 0 && batchCount > 0) {
    return Math.round(totalDocs / batchCount);
  }
  return 0;
}

function validateRequiredK6Artifacts(options) {
  const missingScenarios = REQUIRED_K6_SCENARIOS.filter((scenarioName) => {
    return !options.k6JsonByScenario?.[scenarioName] && !options.k6StdoutByScenario?.[scenarioName];
  });
  if (missingScenarios.length > 0) {
    throw new Error(
      `missing k6 artifacts for scenarios: ${missingScenarios.join(", ")} ` +
      "(provide --k6-json or --k6-stdout for each required scenario)",
    );
  }
}

function renderMetricTable(rows) {
  return [
    "| Metric | Value |",
    "| --- | --- |",
    ...rows.map(([label, value]) => `| ${label} | ${markdownValue(value)} |`),
  ].join("\n");
}

function renderK6ScenarioRows(k6ConcurrentLoad) {
  return REQUIRED_K6_SCENARIOS.map((scenarioName) => {
    const scenario = k6ConcurrentLoad?.[scenarioName] ?? {};
    return `| ${scenarioName} | ${scenario.status ?? "UNKNOWN"} | ${formatNumber(scenario.p95, 2)} | ${formatNumber(scenario.p99, 2)} |`;
  }).join("\n");
}

function renderK6EvidenceLines(k6ConcurrentLoad) {
  return REQUIRED_K6_SCENARIOS.map((scenarioName) => {
    const scenario = k6ConcurrentLoad?.[scenarioName] ?? {};
    const jsonPart = scenario.jsonPath ? `json=${scenario.jsonPath}` : "json=n/a";
    const stdoutPart = scenario.stdoutPath ? `stdout=${scenario.stdoutPath}` : "stdout=n/a";
    return `- k6 ${scenarioName}: ${jsonPart}; ${stdoutPart}`;
  });
}

export function renderLargeDatasetBaselineSection(input) {
  const dashboardSection = input.dashboard?.collected
    ? renderMetricTable([
        ["Total duration (ms)", formatNumber(input.dashboard?.timings?.totalDurationMs, 0)],
        ["Expected tests", formatNumber(input.dashboard?.timings?.expectedTests, 0)],
        ["Unexpected tests", formatNumber(input.dashboard?.timings?.unexpectedTests, 0)],
      ])
    : "- not available";

  return [
    "# Large-Dataset Baseline",
    "",
    "## Run Metadata",
    `- Baseline generated at (UTC): ${input.generationTimestamp}`,
    `- Import benchmark timestamp: ${input.importBenchmark?.timestamp ?? "n/a"}`,
    `- Search benchmark timestamp: ${input.searchBenchmark?.timestamp ?? "n/a"}`,
    "",
    "## Hardware and OS",
    `- CPU: ${input.hardwareAndOs?.cpuModel ?? "unknown"}`,
    `- RAM: ${input.hardwareAndOs?.ram ?? "unknown"}`,
    `- OS: ${input.hardwareAndOs?.osVersion ?? "unknown"}`,
    `- Kernel: ${input.hardwareAndOs?.kernel ?? "unknown"}`,
    "",
    "## Import Throughput",
    renderMetricTable([
      ["Index", input.importBenchmark?.indexName],
      ["Total docs", input.importBenchmark?.totalDocs],
      ["Batches", input.importBenchmark?.batchCount],
      ["Wall clock (ms)", input.importBenchmark?.wallClockMs],
      ["Avg batch latency (ms)", input.importBenchmark?.latency?.avg],
      ["P95 batch latency (ms)", input.importBenchmark?.latency?.p95],
      ["P99 batch latency (ms)", input.importBenchmark?.latency?.p99],
    ]),
    "",
    "## Search Latency",
    renderMetricTable([
      ["Index", input.searchBenchmark?.indexName],
      ["Doc count", input.searchBenchmark?.docCount],
      ["Wall clock (ms)", input.searchBenchmark?.wallClockMs],
      ["Overall avg (ms)", input.searchBenchmark?.overall?.avg],
      ["Overall p95 (ms)", input.searchBenchmark?.overall?.p95],
      ["Overall p99 (ms)", input.searchBenchmark?.overall?.p99],
    ]),
    "",
    "## k6 Concurrent Load",
    "| Scenario | Status | p95 (ms) | p99 (ms) |",
    "| --- | --- | ---: | ---: |",
    renderK6ScenarioRows(input.k6ConcurrentLoad),
    "",
    "## Dashboard Timings",
    dashboardSection,
    "",
    "## Reproduction",
    `- Dataset size: ${input.reproducibility?.datasetSize ?? "n/a"}`,
    `- Import batch size: ${input.reproducibility?.importBatchSize ?? "n/a"}`,
    `- k6 search concurrency (VUs): ${input.reproducibility?.k6SearchVus ?? "n/a"}`,
    `- Build mode: ${input.reproducibility?.buildMode ?? "n/a"}`,
    `- Import command: \`${input.reproducibility?.importEntrypoint ?? "n/a"}\``,
    `- Search command: \`${input.reproducibility?.searchEntrypoint ?? "n/a"}\``,
    `- k6 command: \`${input.reproducibility?.k6Entrypoint ?? "n/a"}\``,
    "",
    "## Evidence Sources",
    `- import artifact: ${input.evidence?.importArtifact ?? "n/a"}`,
    `- search artifact: ${input.evidence?.searchArtifact ?? "n/a"}`,
    `- dashboard report: ${input.evidence?.dashboardReport ?? "not available"}`,
    ...renderK6EvidenceLines(input.k6ConcurrentLoad),
  ].join("\n");
}

function parseScenarioMappingToken(token) {
  const separatorIndex = token.indexOf("=");
  if (separatorIndex <= 0 || separatorIndex === token.length - 1) {
    return null;
  }

  return {
    scenarioName: token.slice(0, separatorIndex),
    scenarioPath: token.slice(separatorIndex + 1),
  };
}

function parseCliArgs(argv) {
  const options = {
    importArtifact: "",
    searchArtifact: "",
    dashboardReport: "",
    k6JsonByScenario: {},
    k6StdoutByScenario: {},
    k6SearchVus: null,
    buildMode: "unspecified",
    importCommand: "bash engine/loadtest/import_benchmark.sh",
    searchCommand: "bash engine/loadtest/search_benchmark.sh",
    k6Command: "bash engine/loadtest/run.sh",
  };

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--import-artifact") {
      options.importArtifact = argv[index + 1] ?? "";
      index += 1;
      continue;
    }
    if (argument === "--search-artifact") {
      options.searchArtifact = argv[index + 1] ?? "";
      index += 1;
      continue;
    }
    if (argument === "--dashboard-report") {
      options.dashboardReport = argv[index + 1] ?? "";
      index += 1;
      continue;
    }
    if (argument === "--k6-json") {
      const scenarioMapping = parseScenarioMappingToken(argv[index + 1] ?? "");
      if (scenarioMapping) {
        const { scenarioName, scenarioPath } = scenarioMapping;
        options.k6JsonByScenario[scenarioName] = scenarioPath;
      }
      index += 1;
      continue;
    }
    if (argument === "--k6-stdout") {
      const scenarioMapping = parseScenarioMappingToken(argv[index + 1] ?? "");
      if (scenarioMapping) {
        const { scenarioName, scenarioPath } = scenarioMapping;
        options.k6StdoutByScenario[scenarioName] = scenarioPath;
      }
      index += 1;
      continue;
    }
    if (argument === "--k6-search-vus") {
      const value = Number(argv[index + 1]);
      if (Number.isFinite(value) && value > 0) {
        options.k6SearchVus = Math.round(value);
      }
      index += 1;
      continue;
    }
    if (argument === "--build-mode") {
      options.buildMode = argv[index + 1] ?? options.buildMode;
      index += 1;
      continue;
    }
    if (argument === "--import-command") {
      options.importCommand = argv[index + 1] ?? options.importCommand;
      index += 1;
      continue;
    }
    if (argument === "--search-command") {
      options.searchCommand = argv[index + 1] ?? options.searchCommand;
      index += 1;
      continue;
    }
    if (argument === "--k6-command") {
      options.k6Command = argv[index + 1] ?? options.k6Command;
      index += 1;
      continue;
    }
    throw new Error(`Unknown argument: ${argument}`);
  }

  return options;
}

export async function buildLargeDatasetBaselineInput(options) {
  validateRequiredK6Artifacts(options);

  const importArtifact = await readJsonFile(
    options.importArtifact,
    `missing import benchmark artifact: ${options.importArtifact}`,
  );
  const searchArtifact = await readJsonFile(
    options.searchArtifact,
    `missing search benchmark artifact: ${options.searchArtifact}`,
  );
  const dashboardStatus = await collectDashboardStatus(options.dashboardReport);

  const k6ConcurrentLoad = {};
  for (const scenarioName of REQUIRED_K6_SCENARIOS) {
    k6ConcurrentLoad[scenarioName] = await summarizeK6Scenario({
      scenarioName,
      jsonPath: options.k6JsonByScenario[scenarioName] ?? "",
      stdoutPath: options.k6StdoutByScenario[scenarioName] ?? "",
    });
  }

  const datasetSize = inferDatasetSize(importArtifact, searchArtifact);
  const importBatchSize = inferImportBatchSize(importArtifact);
  const derivedK6Vus = Number.isFinite(options.k6SearchVus) && options.k6SearchVus > 0
    ? options.k6SearchVus
    : "n/a";

  return {
    generationTimestamp: new Date().toISOString(),
    importBenchmark: importArtifact,
    searchBenchmark: searchArtifact,
    k6ConcurrentLoad,
    dashboard: dashboardStatus,
    hardwareAndOs: collectHardwareAndOs(),
    reproducibility: {
      datasetSize,
      importBatchSize,
      k6SearchVus: derivedK6Vus,
      buildMode: options.buildMode,
      importEntrypoint: options.importCommand,
      searchEntrypoint: options.searchCommand,
      k6Entrypoint: options.k6Command,
    },
    evidence: {
      importArtifact: normalizePath(options.importArtifact),
      searchArtifact: normalizePath(options.searchArtifact),
      dashboardReport: dashboardStatus.reportPath ? normalizePath(dashboardStatus.reportPath) : "not available",
    },
  };
}

async function runCli() {
  const options = parseCliArgs(process.argv.slice(2));
  if (!options.importArtifact || !options.searchArtifact) {
    throw new Error(
      "Usage: compile_baseline.mjs --import-artifact <path> --search-artifact <path> " +
      "[--k6-json scenario=path] [--k6-stdout scenario=path] [--dashboard-report <path>]",
    );
  }

  const input = await buildLargeDatasetBaselineInput(options);
  const markdown = renderLargeDatasetBaselineSection(input);
  process.stdout.write(`${markdown}\n`);
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runCli().catch((error) => {
    console.error(`FAIL: ${error.message}`);
    process.exit(1);
  });
}
