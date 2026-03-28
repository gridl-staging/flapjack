// Write throughput scenario — ramps VUs posting batch writes to the disposable write
// index. Every successful batch is checked for a numeric taskID. Thresholds track
// task-creation success rate and error rates in the options export.
// Critical write-response checks are enforced by thresholds so failed assertions
// fail the run instead of only surfacing in the summary output.
// No scenario-side reset/reseed — that belongs in the Stage 4 runner.

import { check } from "k6";
import exec from "k6/execution";
import { sharedLoadtestConfig } from "../lib/config.js";
import { batchWrite } from "../lib/http.js";
import {
  buildWriteBatchPayload,
  recordWriteHttpStatusCode,
  WRITE_RESPONSE_CHECKS,
  WRITE_THRESHOLDS,
} from "../lib/throughput.js";

export const options = {
  scenarios: {
    write_ramp: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "10s", target: 3 },
        { duration: "30s", target: 10 },
        { duration: "20s", target: 10 },
        { duration: "10s", target: 0 },
      ],
      tags: { type: "write" },
    },
  },
  thresholds: {
    ...WRITE_THRESHOLDS,
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
