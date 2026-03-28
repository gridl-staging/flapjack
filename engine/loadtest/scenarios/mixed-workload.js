// Mixed workload scenario — runs concurrent search on the read index and writes on the
// write index as separate k6 scenarios with independent tagged metrics. Reuses shared
// query mixes and payload builders from lib/throughput.js.

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
  WRITE_THRESHOLDS,
} from "../lib/throughput.js";

// Separate read and write scenarios with independent tags so k6 evaluates
// their thresholds against distinct metric streams.
export const options = {
  scenarios: {
    read_traffic: {
      executor: "ramping-vus",
      exec: "readScenario",
      startVUs: 1,
      stages: [
        { duration: "10s", target: 5 },
        { duration: "30s", target: 15 },
        { duration: "20s", target: 15 },
        { duration: "10s", target: 0 },
      ],
      tags: { type: "search" },
    },
    write_traffic: {
      executor: "ramping-vus",
      exec: "writeScenario",
      startVUs: 1,
      stages: [
        { duration: "10s", target: 2 },
        { duration: "30s", target: 5 },
        { duration: "20s", target: 5 },
        { duration: "10s", target: 0 },
      ],
      tags: { type: "write" },
    },
  },
  thresholds: {
    ...SEARCH_THRESHOLDS,
    ...WRITE_THRESHOLDS,
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
