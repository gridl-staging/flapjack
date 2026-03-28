// Search throughput scenario — ramps VUs against the read index with a deterministic
// query mix modeled on engine/_dev/s/bench_latency.sh (text, text+facets, filter,
// short, long, empty+facets). Thresholds are defined in-file via the options export
// and enforce the critical response checks as hard pass/fail conditions.

import { check } from "k6";
import exec from "k6/execution";
import { sharedLoadtestConfig } from "../lib/config.js";
import { searchPost } from "../lib/http.js";
import {
  buildSearchRequest,
  SEARCH_RESPONSE_CHECKS,
  SEARCH_THRESHOLDS,
} from "../lib/throughput.js";

export const options = {
  scenarios: {
    search_ramp: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "10s", target: 5 },
        { duration: "30s", target: 20 },
        { duration: "20s", target: 20 },
        { duration: "10s", target: 0 },
      ],
      tags: { type: "search" },
    },
  },
  thresholds: {
    ...SEARCH_THRESHOLDS,
  },
};

export default function () {
  const iterationIndex = exec.scenario.iterationInTest;
  const searchRequest = buildSearchRequest(iterationIndex);
  const response = searchPost(sharedLoadtestConfig.readIndexName, searchRequest);

  check(response, SEARCH_RESPONSE_CHECKS);
}
