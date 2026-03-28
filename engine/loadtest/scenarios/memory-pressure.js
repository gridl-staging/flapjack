// Memory-pressure scenario — observes middleware behavior via /health and /internal/status.
// This scenario does not mutate memory thresholds and instead asserts the live pressure level.
import { check, fail } from "k6";
import exec from "k6/execution";
import { sharedLoadtestConfig } from "../lib/config.js";
import {
  batchWrite,
  getHealth,
  getInternalStatus,
  searchGet,
  searchPost,
} from "../lib/http.js";
import { buildSearchRequest, buildWriteBatchPayload } from "../lib/throughput.js";

function retryAfterIsFive(response) {
  return response.headers["Retry-After"] === "5";
}

function assertChecks(response, label, assertions) {
  if (!check(response, assertions)) {
    fail(`${label} assertions failed status=${response.status} body=${response.body}`);
  }
}

function runNormalAssertions(indexName, searchRequest, writePayload, internalStatusResponse) {
  const getSearchResponse = searchGet(indexName, {
    query: searchRequest.query ?? "",
    hitsPerPage: searchRequest.hitsPerPage ?? 20,
  });
  const postSearchResponse = searchPost(indexName, searchRequest);
  const writeResponse = batchWrite(sharedLoadtestConfig.writeIndexName, writePayload);

  assertChecks(internalStatusResponse, "normal internal-status", {
    "normal pressure internal status returns 200": (r) => r.status === 200,
  });
  assertChecks(getSearchResponse, "normal get-search", {
    "normal pressure get search returns 200": (r) => r.status === 200,
  });
  assertChecks(postSearchResponse, "normal post-search", {
    "normal pressure post search returns 200": (r) => r.status === 200,
  });
  assertChecks(writeResponse, "normal write", {
    "normal pressure write returns 200": (r) => r.status === 200,
  });
}

function runElevatedAssertions(indexName, searchRequest, writePayload, internalStatusResponse) {
  const getSearchResponse = searchGet(indexName, {
    query: searchRequest.query ?? "",
    hitsPerPage: searchRequest.hitsPerPage ?? 20,
  });
  const postSearchResponse = searchPost(indexName, searchRequest);
  const writeResponse = batchWrite(sharedLoadtestConfig.writeIndexName, writePayload);

  assertChecks(internalStatusResponse, "elevated internal-status", {
    "elevated pressure internal status returns 200": (r) => r.status === 200,
  });
  assertChecks(getSearchResponse, "elevated get-search", {
    "elevated pressure get search returns 200": (r) => r.status === 200,
  });
  assertChecks(postSearchResponse, "elevated post-search", {
    "elevated pressure post search returns 503": (r) => r.status === 503,
    "elevated pressure post search retry-after is 5": retryAfterIsFive,
  });
  assertChecks(writeResponse, "elevated write", {
    "elevated pressure write returns 503": (r) => r.status === 503,
    "elevated pressure write retry-after is 5": retryAfterIsFive,
  });
}

function runCriticalAssertions(indexName, searchRequest, writePayload, internalStatusResponse) {
  const getSearchResponse = searchGet(indexName, {
    query: searchRequest.query ?? "",
    hitsPerPage: searchRequest.hitsPerPage ?? 20,
  });
  const postSearchResponse = searchPost(indexName, searchRequest);
  const writeResponse = batchWrite(sharedLoadtestConfig.writeIndexName, writePayload);

  assertChecks(internalStatusResponse, "critical internal-status", {
    "critical pressure internal status returns 200": (r) => r.status === 200,
  });
  assertChecks(getSearchResponse, "critical get-search", {
    "critical pressure get search returns 503": (r) => r.status === 503,
    "critical pressure get search retry-after is 5": retryAfterIsFive,
  });
  assertChecks(postSearchResponse, "critical post-search", {
    "critical pressure post search returns 503": (r) => r.status === 503,
    "critical pressure post search retry-after is 5": retryAfterIsFive,
  });
  assertChecks(writeResponse, "critical write", {
    "critical pressure write returns 503": (r) => r.status === 503,
    "critical pressure write retry-after is 5": retryAfterIsFive,
  });
}

export const options = {
  vus: 1,
  iterations: 1,
};

export default function () {
  const healthResponse = getHealth();
  assertChecks(healthResponse, "health", {
    "health returns 200": (r) => r.status === 200,
    "health includes pressure level": (r) => {
      const level = r.json("pressure_level");
      return level === "normal" || level === "elevated" || level === "critical";
    },
  });

  const pressureLevel = healthResponse.json("pressure_level");
  if (typeof pressureLevel !== "string") {
    fail(`health pressure_level missing or not a string: ${healthResponse.body}`);
  }

  const internalStatusResponse = getInternalStatus();
  const iterationIndex = exec.scenario.iterationInTest;
  const searchRequest = buildSearchRequest(iterationIndex);
  const writePayload = buildWriteBatchPayload(__VU, iterationIndex);

  if (pressureLevel === "normal") {
    runNormalAssertions(
      sharedLoadtestConfig.readIndexName,
      searchRequest,
      writePayload,
      internalStatusResponse,
    );
    return;
  }

  if (pressureLevel === "elevated") {
    runElevatedAssertions(
      sharedLoadtestConfig.readIndexName,
      searchRequest,
      writePayload,
      internalStatusResponse,
    );
    return;
  }

  if (pressureLevel === "critical") {
    runCriticalAssertions(
      sharedLoadtestConfig.readIndexName,
      searchRequest,
      writePayload,
      internalStatusResponse,
    );
    return;
  }

  fail(`unsupported pressure_level ${pressureLevel}`);
}
