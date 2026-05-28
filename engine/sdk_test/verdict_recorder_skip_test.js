import assert from "node:assert/strict";

import { createVerdictRecorder } from "./lib/test-helpers.js";

async function main() {
  const verdict = createVerdictRecorder();

  await verdict.runStep("pass-row", async () => "ok");
  const failed = await verdict.runStep("fail-row", async () => {
    throw new Error("expected-failure");
  });
  assert.equal(failed, false, "runStep should return false for failing operations");

  verdict.runSkip("skip-row", "confirmed external product gap");

  const summary = verdict.summarize();
  assert.equal(summary.passCount, 1, "summary.passCount mismatch");
  assert.equal(summary.failCount, 1, "summary.failCount mismatch");
  assert.equal(summary.skipCount, 1, "summary.skipCount mismatch");
  assert.equal(summary.totalCount, 3, "summary.totalCount mismatch");

  const firstFailure = verdict.getFirstFailureError();
  assert.ok(firstFailure instanceof Error, "expected first failure error to be captured");
  assert.equal(firstFailure.message, "expected-failure");

  console.log("PASS verdict recorder supports PASS/FAIL/SKIP summary accounting");
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
