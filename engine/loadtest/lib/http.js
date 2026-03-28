import http from "k6/http";
import { fail, sleep } from "k6";
import { sharedLoadtestConfig } from "./config.js";

function withLeadingSlash(path) {
  return path.startsWith("/") ? path : `/${path}`;
}

function toQueryString(queryParams = {}) {
  const entries = Object.entries(queryParams).filter(([, value]) => value !== undefined && value !== null);
  if (entries.length === 0) {
    return "";
  }
  const encoded = entries.map(([key, value]) => {
    return `${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`;
  });
  return `?${encoded.join("&")}`;
}

function parseJsonResponse(response, label) {
  try {
    return response.json();
  } catch (error) {
    fail(`${label} returned non-JSON response status=${response.status} body=${response.body}`);
  }
}

export function buildHeaders({ json = false, extraHeaders = {} } = {}) {
  const headers = { ...extraHeaders };
  if (json) {
    headers["Content-Type"] = "application/json";
  }
  if (sharedLoadtestConfig.apiKey) {
    headers["x-algolia-api-key"] = sharedLoadtestConfig.apiKey;
    headers["x-algolia-application-id"] = sharedLoadtestConfig.appId;
  }
  return headers;
}

function sendRequest(method, path, body = null, { json = false, extraHeaders = {} } = {}) {
  const requestUrl = `${sharedLoadtestConfig.baseUrl}${withLeadingSlash(path)}`;
  const headers = buildHeaders({ json, extraHeaders });
  const payload = json && body !== null ? JSON.stringify(body) : body;
  return http.request(method, requestUrl, payload, { headers });
}

export function getHealth() {
  return sendRequest("GET", "/health");
}

export function getMetrics() {
  return sendRequest("GET", "/metrics");
}

export function getInternalStatus() {
  return sendRequest("GET", "/internal/status");
}

export function searchPost(indexName, searchRequest) {
  return sendRequest("POST", `/1/indexes/${encodeURIComponent(indexName)}/query`, searchRequest, {
    json: true,
  });
}

export function searchGet(indexName, queryParams = {}) {
  const suffix = toQueryString(queryParams);
  return sendRequest("GET", `/1/indexes/${encodeURIComponent(indexName)}/query${suffix}`);
}

export function updateSettings(indexName, settingsPayload) {
  return sendRequest("PUT", `/1/indexes/${encodeURIComponent(indexName)}/settings`, settingsPayload, {
    json: true,
  });
}

export function batchWrite(indexName, batchPayload) {
  return sendRequest("POST", `/1/indexes/${encodeURIComponent(indexName)}/batch`, batchPayload, {
    json: true,
  });
}

export function waitForTaskPublished(taskId, {
  maxAttempts = sharedLoadtestConfig.taskPollMaxAttempts,
  intervalSeconds = sharedLoadtestConfig.taskPollIntervalSeconds,
} = {}) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const taskResponse = sendRequest("GET", `/1/tasks/${taskId}`);
    if (taskResponse.status !== 200) {
      fail(`task ${taskId} polling returned HTTP ${taskResponse.status}: ${taskResponse.body}`);
    }
    const taskJson = parseJsonResponse(taskResponse, `task ${taskId}`);

    const status = taskJson.status;
    const pendingTask = taskJson.pendingTask;
    if (status === "published" && pendingTask === false) {
      return taskResponse;
    }

    if (attempt === maxAttempts) {
      fail(`task ${taskId} did not reach published in ${maxAttempts} attempts: ${taskResponse.body}`);
    }

    sleep(intervalSeconds);
  }

  fail(`task polling exhausted unexpectedly for task ${taskId}`);
}
