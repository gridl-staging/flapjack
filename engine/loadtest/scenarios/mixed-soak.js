// Mixed soak scenario — steady-state read/write traffic for multi-hour confidence runs.
// This is intentionally separate from run.sh because it is designed for longer manual
// evidence-gathering sessions, not the short baseline pass.
//
// Uses SOAK_WRITE_THRESHOLDS for the write side because multi-hour mixed traffic
// will push writes into sustained 429 backpressure. Search thresholds remain
// unchanged — reads should stay fast even under write overload.
// See lib/throughput.js for the threshold rationale.

import { check } from "k6";
import exec from "k6/execution";
import { sharedLoadtestConfig } from "../lib/config.js";
import { searchPost, batchWrite } from "../lib/http.js";
import {
  buildSearchRequest,
  buildWriteBatchPayload,
  recordWriteHttpStatusCode,
  SEARCH_RESPONSE_CHECKS,
  SEARCH_THRESHOLDS,
  WRITE_RESPONSE_CHECKS,
  SOAK_WRITE_THRESHOLDS,
} from "../lib/throughput.js";

export const options = {
  scenarios: {
    read_traffic: {
      executor: "constant-vus",
      exec: "readScenario",
      vus: 15,
      duration: sharedLoadtestConfig.soakDuration,
      tags: { type: "search" },
    },
    write_traffic: {
      executor: "constant-vus",
      exec: "writeScenario",
      vus: 4,
      duration: sharedLoadtestConfig.soakDuration,
      tags: { type: "write" },
    },
  },
  thresholds: {
    ...SEARCH_THRESHOLDS,
    ...SOAK_WRITE_THRESHOLDS,
  },
};

export function readScenario() {
  const iterationIndex = exec.scenario.iterationInTest;
  const searchRequest = buildSearchRequest(iterationIndex);
  const response = searchPost(sharedLoadtestConfig.readIndexName, searchRequest);

  check(response, SEARCH_RESPONSE_CHECKS);
}

export function writeScenario() {
  const vuId = __VU;
  const iterationIndex = exec.scenario.iterationInTest;
  const batchPayload = buildWriteBatchPayload(vuId, iterationIndex);
  const response = batchWrite(sharedLoadtestConfig.writeIndexName, batchPayload);
  recordWriteHttpStatusCode(response.status);

  check(response, WRITE_RESPONSE_CHECKS);
}
