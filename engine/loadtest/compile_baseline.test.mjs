import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

import { buildLargeDatasetBaselineInput, renderLargeDatasetBaselineSection } from "./compile_baseline.mjs";

const REQUIRED_K6_SCENARIOS = [
  "smoke",
  "search-throughput",
  "write-throughput",
  "mixed-workload",
  "spike",
  "memory-pressure",
];

function buildFixtureInput({ dashboardCollected }) {
  return {
    generationTimestamp: "2026-03-22T18:00:00Z",
    importBenchmark: {
      timestamp: "2026-03-22T16:02:00Z",
      indexName: "benchmark_100k",
      totalDocs: 100000,
      batchCount: 100,
      wallClockMs: 54429,
      latency: { avg: 464.9, p95: 660, p99: 877 },
    },
    searchBenchmark: {
      timestamp: "2026-03-22T16:30:00Z",
      indexName: "benchmark_100k",
      docCount: 100000,
      wallClockMs: 1180,
      overall: { avg: 12.2, p95: 26.8, p99: 35.1 },
    },
    k6ConcurrentLoad: {
      smoke: { status: "PASS", p95: 20.1, p99: 28.3 },
      "search-throughput": { status: "PASS", p95: 22.4, p99: 30.8 },
      "write-throughput": { status: "FAIL", p95: 185.2, p99: 241.3 },
      "mixed-workload": { status: "PASS", p95: 47.9, p99: 61.7 },
      spike: { status: "PASS", p95: 35.2, p99: 49.5 },
      "memory-pressure": { status: "PASS", p95: 63.7, p99: 91.4 },
    },
    dashboard: dashboardCollected
      ? {
          collected: true,
          timings: {
            totalDurationMs: 1320,
            expectedTests: 5,
            unexpectedTests: 0,
          },
        }
      : {
          collected: false,
          message: "not available",
        },
    hardwareAndOs: {
      cpuModel: "Apple M4 Max",
      ram: "36.00 GiB",
      osVersion: "macOS 26.0.1",
      kernel: "Darwin 25.0.0 arm64",
    },
    reproducibility: {
      datasetSize: 100000,
      importBatchSize: 1000,
      k6SearchVus: 64,
      buildMode: "release",
      importEntrypoint: "bash engine/loadtest/import_benchmark.sh",
      searchEntrypoint: "bash engine/loadtest/search_benchmark.sh",
      k6Entrypoint: "bash engine/loadtest/run.sh",
    },
  };
}

test("renderLargeDatasetBaselineSection includes required sections and reproducibility fields", () => {
  const markdown = renderLargeDatasetBaselineSection(buildFixtureInput({ dashboardCollected: true }));

  const requiredSections = [
    "# Large-Dataset Baseline",
    "## Run Metadata",
    "## Hardware and OS",
    "## Import Throughput",
    "## Search Latency",
    "## k6 Concurrent Load",
    "## Dashboard Timings",
    "## Reproduction",
  ];

  for (const section of requiredSections) {
    assert.ok(markdown.includes(section), `missing section: ${section}`);
  }
  assert.ok(markdown.includes("| memory-pressure | PASS | 63.70 | 91.40 |"), "memory-pressure row should be rendered");

  assert.ok(!markdown.includes("2026-03-21"), "new baseline section must not include 2026-03-21 timestamp");

  const requiredReproFields = [
    "Dataset size: 100000",
    "Import batch size: 1000",
    "k6 search concurrency (VUs): 64",
    "Build mode: release",
    "Import command: `bash engine/loadtest/import_benchmark.sh`",
    "Search command: `bash engine/loadtest/search_benchmark.sh`",
    "k6 command: `bash engine/loadtest/run.sh`",
  ];

  for (const field of requiredReproFields) {
    assert.ok(markdown.includes(field), `missing reproducibility field: ${field}`);
  }
  assert.ok(!markdown.includes("Stage 4 command"), "legacy stage 4 command label should not be rendered");

  // Verify dashboard timing values are rendered (not "n/a") when collected
  assert.ok(markdown.includes("| Total duration (ms) | 1320 |"), "dashboard total duration should be rendered");
  assert.ok(markdown.includes("| Expected tests | 5 |"), "dashboard expected tests should be rendered");
  assert.ok(markdown.includes("| Unexpected tests | 0 |"), "dashboard unexpected tests should be rendered");
});

test("renderLargeDatasetBaselineSection marks dashboard timings as not available when report is absent", () => {
  const markdown = renderLargeDatasetBaselineSection(buildFixtureInput({ dashboardCollected: false }));
  assert.ok(markdown.includes("## Dashboard Timings"), "dashboard section should still be present");
  assert.ok(markdown.toLowerCase().includes("not available"), "dashboard section should report not available");
});

test("buildLargeDatasetBaselineInput renders repo-relative evidence paths", async () => {
  const fixtureDir = await mkdtemp(path.join(process.cwd(), ".tmp-compile-baseline-"));
  const repoRoot = path.resolve(process.cwd(), "..", "..");
  const importArtifact = path.join(fixtureDir, "import_benchmark.json");
  const searchArtifact = path.join(fixtureDir, "search_benchmark.json");
  const smokeJson = path.join(fixtureDir, "smoke.json");
  const smokeStdout = path.join(fixtureDir, "smoke.stdout.txt");
  const k6StdoutByScenario = {};

  try {
    const writes = [
      writeFile(importArtifact, JSON.stringify({
        timestamp: "2026-03-22T16:02:00Z",
        indexName: "benchmark_100k",
        totalDocs: 100000,
        batchCount: 100,
        wallClockMs: 54429,
        latency: { avg: 464.9, p95: 660, p99: 877 },
      })),
      writeFile(searchArtifact, JSON.stringify({
        timestamp: "2026-03-22T16:30:00Z",
        indexName: "benchmark_100k",
        docCount: 100000,
        wallClockMs: 1180,
        overall: { avg: 12.2, p95: 26.8, p99: 35.1 },
      })),
      writeFile(smokeJson, "{}\n"),
      writeFile(smokeStdout, "http_req_duration......: avg=1ms min=1ms med=1ms max=2ms p(95)=1ms p(99)=2ms\n"),
    ];
    for (const scenarioName of REQUIRED_K6_SCENARIOS) {
      const stdoutPath = path.join(fixtureDir, `${scenarioName}.stdout.txt`);
      k6StdoutByScenario[scenarioName] = stdoutPath;
      if (scenarioName !== "smoke") {
        writes.push(writeFile(stdoutPath, "http_req_duration......: avg=1ms min=1ms med=1ms max=2ms p(95)=1ms p(99)=2ms\n"));
      }
    }
    await Promise.all(writes);

    const input = await buildLargeDatasetBaselineInput({
      importArtifact,
      searchArtifact,
      dashboardReport: path.join(fixtureDir, "missing-dashboard.json"),
      k6JsonByScenario: { smoke: smokeJson },
      k6StdoutByScenario,
      k6SearchVus: 20,
      buildMode: "release",
      importCommand: "bash engine/loadtest/import_benchmark.sh",
      searchCommand: "bash engine/loadtest/search_benchmark.sh",
      k6Command: "bash engine/loadtest/run.sh",
    });
    const markdown = renderLargeDatasetBaselineSection(input);
    const expectedImportPath = path.relative(repoRoot, importArtifact).split(path.sep).join("/");
    const expectedSmokeJsonPath = path.relative(repoRoot, smokeJson).split(path.sep).join("/");
    const expectedSmokeStdoutPath = path.relative(repoRoot, smokeStdout).split(path.sep).join("/");

    assert.equal(input.evidence.importArtifact, expectedImportPath);
    assert.equal(input.k6ConcurrentLoad.smoke.jsonPath, expectedSmokeJsonPath);
    assert.equal(input.k6ConcurrentLoad.smoke.stdoutPath, expectedSmokeStdoutPath);
    assert.ok(markdown.includes(`- import artifact: ${expectedImportPath}`), "markdown should render repo-relative import path");
    assert.ok(markdown.includes(`- k6 smoke: json=${expectedSmokeJsonPath}; stdout=${expectedSmokeStdoutPath}`), "markdown should render repo-relative k6 paths");
    assert.ok(!markdown.includes(importArtifact), "markdown should not render absolute import path");
  } finally {
    await rm(fixtureDir, { recursive: true, force: true });
  }
});

test("buildLargeDatasetBaselineInput rejects missing required k6 scenarios", async () => {
  const fixtureDir = await mkdtemp(path.join(process.cwd(), ".tmp-compile-baseline-"));
  const importArtifact = path.join(fixtureDir, "import_benchmark.json");
  const searchArtifact = path.join(fixtureDir, "search_benchmark.json");

  try {
    await Promise.all([
      writeFile(importArtifact, JSON.stringify({
        timestamp: "2026-03-22T16:02:00Z",
        indexName: "benchmark_100k",
        totalDocs: 100000,
        batchCount: 100,
        wallClockMs: 54429,
        latency: { avg: 464.9, p95: 660, p99: 877 },
      })),
      writeFile(searchArtifact, JSON.stringify({
        timestamp: "2026-03-22T16:30:00Z",
        indexName: "benchmark_100k",
        docCount: 100000,
        wallClockMs: 1180,
        overall: { avg: 12.2, p95: 26.8, p99: 35.1 },
      })),
    ]);

    await assert.rejects(
      buildLargeDatasetBaselineInput({
        importArtifact,
        searchArtifact,
        dashboardReport: path.join(fixtureDir, "missing-dashboard.json"),
        k6JsonByScenario: {},
        k6StdoutByScenario: { smoke: path.join(fixtureDir, "smoke.stdout.txt") },
        k6SearchVus: 20,
        buildMode: "release",
        importCommand: "bash engine/loadtest/import_benchmark.sh",
        searchCommand: "bash engine/loadtest/search_benchmark.sh",
        k6Command: "bash engine/loadtest/run.sh",
      }),
      /missing k6 artifacts for scenarios: search-throughput, write-throughput, mixed-workload, spike, memory-pressure/,
    );
  } finally {
    await rm(fixtureDir, { recursive: true, force: true });
  }
});
