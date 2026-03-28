// Write-heavy soak scenario — sustained overload profile that is expected to observe
// intentional `429` backpressure while still requiring bounded latency and forward progress.
// This is intentionally separate from run.sh because it is for longer operational proof runs.
//
// Uses SOAK_WRITE_THRESHOLDS (not WRITE_THRESHOLDS) because multi-hour sustained
// overload with 12 VUs will push >99% of writes into 429 backpressure. That is
// correct engine behavior — the soak contract validates bounded degradation, not
// absence of overload. See lib/throughput.js for the threshold rationale.

import { check } from "k6";
import exec from "k6/execution";
import { sharedLoadtestConfig } from "../lib/config.js";
import { batchWrite } from "../lib/http.js";
import {
  buildWriteBatchPayload,
  recordWriteHttpStatusCode,
  WRITE_RESPONSE_CHECKS,
  SOAK_WRITE_THRESHOLDS,
} from "../lib/throughput.js";

export const options = {
  scenarios: {
    write_soak: {
      executor: "constant-vus",
      vus: 12,
      duration: sharedLoadtestConfig.soakDuration,
      tags: { type: "write" },
    },
  },
  thresholds: {
    ...SOAK_WRITE_THRESHOLDS,
  },
};

export default function () {
  const vuId = __VU;
  const iterationIndex = exec.scenario.iterationInTest;
  const batchPayload = buildWriteBatchPayload(vuId, iterationIndex);

  const response = batchWrite(sharedLoadtestConfig.writeIndexName, batchPayload);
  recordWriteHttpStatusCode(response.status);

  check(response, WRITE_RESPONSE_CHECKS);
}
