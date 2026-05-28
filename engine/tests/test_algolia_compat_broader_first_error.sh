#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENGINE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SDK_DIR="$ENGINE_DIR/sdk_test"
BROADER_HARNESS="$SDK_DIR/algolia_compat_broader.js"

if ! grep -Fq verdict.getFirstFailureError "$BROADER_HARNESS"; then
  echo "Expected broader harness to rethrow verdict.getFirstFailureError()"
  exit 1
fi

(cd "$SDK_DIR" && node --input-type=module <<NODE)
import assert from "node:assert/strict";
import { createVerdictRecorder } from "./lib/test-helpers.js";

const recorder = createVerdictRecorder();
await recorder.runStep("keys.create", async () => "ok");
await recorder.runStep("keys.update", async () => {
  throw new Error("UNIMPLEMENTED case.id=keys.update");
});

const summary = recorder.summarize();
assert.equal(summary.failCount, 1, "expected one failing row");
assert.equal(typeof recorder.getFirstFailureError, "function", "missing getFirstFailureError seam");

const firstError = recorder.getFirstFailureError();
assert.ok(firstError instanceof Error, "first failure should be captured as Error");
assert.equal(firstError.message, "UNIMPLEMENTED case.id=keys.update");

console.log("PASS: verdict recorder preserves first failing error");
NODE
