# Load Test Runner

## Prerequisites

- k6
- Rust toolchain (cargo)
- curl
- jq
- node

## Configuration Contract

All load-test scripts use `engine/loadtest/lib/config.js::sharedLoadtestConfig` as the only environment contract.

Set these variables when overriding defaults:

- `FLAPJACK_LOADTEST_BASE_URL`
- `FLAPJACK_LOADTEST_APP_ID`
- `FLAPJACK_LOADTEST_API_KEY`
- `FLAPJACK_LOADTEST_READ_INDEX`
- `FLAPJACK_LOADTEST_WRITE_INDEX`
- `FLAPJACK_LOADTEST_TASK_MAX_ATTEMPTS`
- `FLAPJACK_LOADTEST_TASK_POLL_INTERVAL_SECONDS`

## Full Suite Orchestration

From `engine/loadtest`:

```bash
./run.sh
```

The runner executes one normal pass and one pressure pass, and stores artifacts under `engine/loadtest/results/<timestamp>/`.

## Scenario Commands

Run from `engine/loadtest`:

```bash
k6 run scenarios/smoke.js
k6 run scenarios/search-throughput.js
k6 run scenarios/write-throughput.js
k6 run scenarios/mixed-workload.js
k6 run scenarios/spike.js
k6 run scenarios/mixed-soak.js
k6 run scenarios/write-soak.js
k6 inspect scenarios/memory-pressure.js
```

These direct `k6 run` commands assume `FLAPJACK_LOADTEST_BASE_URL` already points at a running, seeded server. Use `./seed-loadtest-data.sh` after starting the server to populate the read index and reset the write index before direct scenario runs. Use `./run.sh` when you want the fully managed normal pass, isolated write-index resets, and the pressure-pass restart flow.

## Scenario Intent

- `smoke.js`: validates health, read query, and write task publication.
- `search-throughput.js`: exercises read-index query throughput.
- `write-throughput.js`: exercises write-index batch throughput.
- `mixed-workload.js`: runs concurrent read and write pressure.
- `spike.js`: applies short burst traffic and recovery.
- `mixed-soak.js`: 4-hour steady mixed read/write soak profile for longer confidence runs.
- `write-soak.js`: 4-hour write-heavy overload profile that should observe intentional `429` backpressure.
- `memory-pressure.js`: validates behavior when the runner restarts with alternate memory settings.

## Results and Interpretation

For each scenario, the runner writes:

- k6 stdout summary: `<scenario>.stdout.txt`
- k6 JSON output: `<scenario>.json`

Use stdout summaries for quick pass/fail checks and JSON outputs for detailed trend analysis and tooling.

## Current Overload Contract

The canonical write-overload contract lives in
`engine/loadtest/lib/throughput.js::WRITE_THRESHOLDS`.

For the single-node loadtest scenarios, sustained write-side `429 Too Many Requests`
responses are expected and acceptable under deliberate overload. They are treated as
intentional backpressure, not as a correctness failure by themselves.

The current write-path pass criteria are:

- keep write latency bounded (`p95<1000ms`, `p99<2000ms`)
- keep unexpected non-`429` client errors rare (`write_http_unexpected_4xx_rate<0.005`)
- keep server errors rare (`write_http_5xx_rate<0.005`)
- preserve forward progress instead of total saturation (`write returns 200` checks `rate>0.01`)

This suite is a short-run baseline, not a multi-hour soak test. For the current
evidence summary and its limits, see [BENCHMARKS.md](BENCHMARKS.md).

## Soak Scenario Designs

These scenarios are checked in for Stage 3 confidence-completeness work but are
not part of the default `./run.sh` baseline because they are intentionally long-running.

Suggested commands:

```bash
k6 run scenarios/mixed-soak.js
k6 run scenarios/write-soak.js
```

Suggested evidence to capture alongside those runs:

- k6 stdout + JSON summaries for latency drift over time
- periodic server RSS or equivalent memory sampling from the host
- one server restart after the soak to confirm clean recovery
- one post-soak search consistency check on both the read and write indices

## Memory-Pressure Restart Contract

`run.sh` owns the pressure-mode restart flow. It stops the normal server, applies pressure-mode `FLAPJACK_MEMORY_LIMIT_MB`, `FLAPJACK_MEMORY_HIGH_WATERMARK`, and `FLAPJACK_MEMORY_CRITICAL` via one helper, waits for `/health`, reseeds data with `./seed-loadtest-data.sh`, verifies `/health` again, then runs only `scenarios/memory-pressure.js`.
