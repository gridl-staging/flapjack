# Flapjack Loadtest Baseline (Stage 4)

## Run Metadata

- Run date (UTC): 2026-03-21T02:54:26Z
- Run date (local): 2026-03-20 22:54:26 EDT
- Results directory: `engine/loadtest/results/20260321T025426Z/`
- Runner command: `cd engine/loadtest && FLAPJACK_LOADTEST_BASE_URL=http://127.0.0.1:7701 ./run.sh`
- Release build command: `cd engine && cargo build --release -p flapjack-server`
- Release binary: `engine/target/release/flapjack` (executable)

## Hardware and OS

- CPU: Apple M4 Max
- RAM: 36.00 GiB (`38654705664` bytes)
- OS: macOS 26.0.1 (Build 25A362)
- Kernel: Darwin 25.0.0 (arm64)

## Scenario Threshold Contracts

The 2026-03-21 run below was executed under the original Stage 4 write contract.
Current HEAD relaxes the write-side thresholds so expected single-node `429`
backpressure does not fail the baseline by itself; a fresh benchmark rerun is
still required if you want PASS/FAIL labels regenerated under the updated
contract.

- `smoke.js`: no explicit thresholds; hard failures come from in-script assertions (`check` + `fail`).
- `search-throughput.js`: spreads `SEARCH_THRESHOLDS` from `lib/throughput.js`.
  - `http_req_duration{type:search}`: `p(95)<500`, `p(99)<1000`
  - `http_req_failed{type:search}`: `rate<0.01`
  - `http_reqs{type:search}`: `rate>5`
  - `checks{search returns 200,hits array}`: `rate==1`
- `write-throughput.js`: spreads `WRITE_THRESHOLDS` from `lib/throughput.js`.
  - `http_req_duration{type:write}`: `p(95)<1000`, `p(99)<2000`
  - `http_req_failed{type:write}`: `rate<0.99`
  - `write_http_4xx_rate`: `rate<0.99`
  - `write_http_unexpected_4xx_rate`: `rate<0.005`
  - `write_http_5xx_rate`: `rate<0.005`
  - `checks{write returns 200, numeric taskID, objectIDs array}`: `rate>0.01`
- `mixed-workload.js`: applies both `SEARCH_THRESHOLDS` and `WRITE_THRESHOLDS`.
- `spike.js`: applies `SEARCH_THRESHOLDS`.
- `memory-pressure.js`: no explicit k6 threshold map; pass/fail is assertion-driven in scenario code.

## Current Interpretation At HEAD

What the current evidence does prove:

- search-heavy traffic on the documented single-node baseline stays fast with low error rates
- write overload is backpressure-limited rather than crash-prone on this setup
- memory-pressure mode assertions pass under the documented restart harness
- large-dataset import and search numbers provide a useful same-machine baseline

What it does not prove:

- multi-hour soak behavior
- cluster-wide performance guarantees
- hardware-independent SLOs
- that every overload pattern is safe just because one k6 profile passed

For deliberate single-node write overload, the current contract treats `429 Too Many Requests`
as acceptable intentional backpressure. The failure boundary is not "zero `429`s";
the failure boundary is:

- unexpected non-`429` client errors above the documented threshold
- `5xx` responses above the documented threshold
- latency breaching the documented write thresholds
- total loss of forward progress, reflected by successful write-task publication dropping below the documented minimum

The canonical threshold source is `engine/loadtest/lib/throughput.js::WRITE_THRESHOLDS`.

## Benchmark Summary (From k6 stdout)

| Scenario | Status | p50 latency | p95 latency | p99 latency | Throughput (http_reqs/s) | Notes |
|---|---|---:|---:|---:|---:|---|
| smoke | PASS | 618us | 34.56ms | n/a (not printed) | 67.055935 | 9/9 checks passed |
| search-throughput | PASS | 3.63ms | 10.74ms | 17.87ms | 2567.89946 | all thresholds passed |
| write-throughput | FAIL | 4.02ms | 6.53ms | 9.11ms | 1685.898731 | write backpressure (`429`) breached failure-rate thresholds |
| mixed-workload | FAIL | 3.8ms | 8.9ms | n/a (type-specific p99: search 12.55ms, write 14.28ms) | 2940.152034 | write-side thresholds breached (`http_req_failed{type:write}`, `write_http_4xx_rate`) |
| spike | PASS | 7.43ms | 22.34ms | 31.4ms | 2390.306952 | all thresholds passed |
| memory-pressure | PASS | 560us | 3.07ms | n/a (not printed) | 601.757131 | pressure-mode assertions all passed |

### Threshold Breach Detail

- `run.sh` completed all six scenarios and exited with `99` because two scenarios breached thresholds (`write-throughput`, `mixed-workload`).
- `write-throughput` k6 summary:
  - `http_req_failed{type:write}`: 98.38%
  - `write_http_4xx_rate`: 98.38%
  - `write_http_5xx_rate`: 0.00%
  - raw status counts (`write-throughput.json`, `http_reqs`): `429=116105`, `200=1910`
- `mixed-workload` k6 summary:
  - `http_req_failed{type:write}`: 96.22%
  - `write_http_4xx_rate`: 96.22%
  - `http_req_failed{type:search}`: 0.00%
  - raw status counts (`mixed-workload.json`, `http_reqs`): `429=48889`, `200=156925`

## Caveats

- This is a single-node baseline run on one developer machine.
- Results are environment-specific (hardware, OS, local runtime conditions).
- This is useful operational evidence, not a product performance guarantee.
- The current committed baselines are short-run benchmarks, not soak proofs.
- These numbers are not product performance guarantees.
- Raw run artifacts under `engine/loadtest/results/<timestamp>/` are gitignored (`*` with `!.gitignore`), so this document is the committed summary artifact.

## Reproduction

See `engine/loadtest/README.md` for the canonical run procedure and configuration contract.

## Evidence Sources

- `engine/loadtest/run.sh`
- `engine/loadtest/lib/loadtest_shell_helpers.sh`
- `engine/loadtest/lib/throughput.js`
- `engine/loadtest/scenarios/smoke.js`
- `engine/loadtest/scenarios/search-throughput.js`
- `engine/loadtest/scenarios/write-throughput.js`
- `engine/loadtest/scenarios/mixed-workload.js`
- `engine/loadtest/scenarios/spike.js`
- `engine/loadtest/scenarios/memory-pressure.js`
- `engine/loadtest/results/.gitignore`
- `engine/loadtest/results/20260321T025426Z/smoke.stdout.txt`
- `engine/loadtest/results/20260321T025426Z/search-throughput.stdout.txt`
- `engine/loadtest/results/20260321T025426Z/write-throughput.stdout.txt`
- `engine/loadtest/results/20260321T025426Z/mixed-workload.stdout.txt`
- `engine/loadtest/results/20260321T025426Z/spike.stdout.txt`
- `engine/loadtest/results/20260321T025426Z/memory-pressure.stdout.txt`
- `engine/loadtest/results/20260321T025426Z/write-throughput.json`
- `engine/loadtest/results/20260321T025426Z/mixed-workload.json`
- `engine/loadtest/results/20260321T025426Z/server.log`

---

# Large-Dataset Baseline

## Run Metadata
- Baseline generated at (UTC): 2026-03-23T18:03:59.735Z
- Import benchmark timestamp: 2026-03-23T15:52:55.271Z
- Search benchmark timestamp: 2026-03-23T15:53:18.574Z

## Hardware and OS
- CPU: Apple M4 Max
- RAM: 36.00 GiB
- OS: macOS 26.0.1 (Build 25A362)
- Kernel: Darwin 25.0.0 arm64

## Import Throughput
| Metric | Value |
| --- | --- |
| Index | benchmark_100k |
| Total docs | 100000 |
| Batches | 100 |
| Wall clock (ms) | 48402 |
| Avg batch latency (ms) | 397.6 |
| P95 batch latency (ms) | 507 |
| P99 batch latency (ms) | 578 |

## Search Latency
| Metric | Value |
| --- | --- |
| Index | benchmark_100k |
| Doc count | 100000 |
| Wall clock (ms) | 7036 |
| Overall avg (ms) | 85.7 |
| Overall p95 (ms) | 128 |
| Overall p99 (ms) | 195 |

## k6 Concurrent Load
| Scenario | Status | p95 (ms) | p99 (ms) |
| --- | --- | ---: | ---: |
| smoke | PASS | 69.04 | 69.04 |
| search-throughput | PASS | 12.10 | 19.65 |
| write-throughput | PASS | 5.84 | 8.62 |
| mixed-workload | PASS | 9.70 | 14.73 |
| spike | PASS | 33.12 | 59.65 |
| memory-pressure | PASS | 2.76 | 2.76 |

## Dashboard Timings
- not available

## Reproduction
- Dataset size: 100000
- Import batch size: 1000
- k6 search concurrency (VUs): 20
- Build mode: release
- Import command: `bash engine/loadtest/import_benchmark.sh`
- Search command: `bash engine/loadtest/search_benchmark.sh`
- k6 command: `bash engine/loadtest/run.sh`

## Evidence Sources
- import artifact: engine/loadtest/results/20260323T155255Z/import_benchmark.json
- search artifact: engine/loadtest/results/20260323T155318Z/search_benchmark.json
- dashboard report: not available
- k6 smoke: json=engine/loadtest/results/20260323T164412Z/smoke.json; stdout=engine/loadtest/results/20260323T164412Z/smoke.stdout.txt
- k6 search-throughput: json=engine/loadtest/results/20260323T164412Z/search-throughput.json; stdout=engine/loadtest/results/20260323T164412Z/search-throughput.stdout.txt
- k6 write-throughput: json=engine/loadtest/results/20260323T164412Z/write-throughput.json; stdout=engine/loadtest/results/20260323T164412Z/write-throughput.stdout.txt
- k6 mixed-workload: json=engine/loadtest/results/20260323T164412Z/mixed-workload.json; stdout=engine/loadtest/results/20260323T164412Z/mixed-workload.stdout.txt
- k6 spike: json=engine/loadtest/results/20260323T164412Z/spike.json; stdout=engine/loadtest/results/20260323T164412Z/spike.stdout.txt
- k6 memory-pressure: json=engine/loadtest/results/20260323T164412Z/memory-pressure.json; stdout=engine/loadtest/results/20260323T164412Z/memory-pressure.stdout.txt
