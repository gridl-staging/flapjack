// Spike stress scenario — read-only traffic against the seeded read index.
// Reuses the Stage 2 deterministic search request builder.
import { check } from "k6";
import exec from "k6/execution";
import { sharedLoadtestConfig } from "../lib/config.js";
import { searchPost } from "../lib/http.js";
import { buildSearchRequest, SEARCH_THRESHOLDS } from "../lib/throughput.js";

const WARMUP_STAGE = { duration: "15s", target: 4 };
const SPIKE_STAGE = { duration: "10s", target: 40 };
const HOLD_STAGE = { duration: "20s", target: 40 };
const RECOVERY_STAGE = { duration: "15s", target: 1 };

export const options = {
  scenarios: {
    spike_load: {
      executor: "ramping-vus",
      startVUs: 1,
      // warmup -> spike -> hold -> recovery
      stages: [WARMUP_STAGE, SPIKE_STAGE, HOLD_STAGE, RECOVERY_STAGE],
      tags: { type: "search" },
    },
  },
  thresholds: {
    ...SEARCH_THRESHOLDS,
  },
};

export default function () {
  const iterationIndex = exec.scenario.iterationInTest;
  const requestBody = buildSearchRequest(iterationIndex);
  const response = searchPost(sharedLoadtestConfig.readIndexName, requestBody);

  check(response, {
    "search returns 200": (r) => r.status === 200,
    "search returns hits array": (r) => {
      try {
        return Array.isArray(r.json("hits"));
      } catch (_) {
        return false;
      }
    },
  });
}
