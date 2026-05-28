import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { assertDispatchCoverage, findMissingDispatchCaseIds } from "./algolia_compat_broader.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const MANIFEST_PATH = join(__dirname, "..", "docs2", "algolia_parity_cases.json");

async function loadManifestCaseIds() {
  const raw = await readFile(MANIFEST_PATH, "utf8");
  const parsed = JSON.parse(raw);
  return parsed.map((row) => row.id);
}

async function main() {
  const caseIds = await loadManifestCaseIds();
  const missing = findMissingDispatchCaseIds(caseIds);
  assert.equal(
    missing.length,
    0,
    `expected full dispatch coverage for exported Algolia parity IDs; missing=${missing.join(", ")}`
  );
  assert.doesNotThrow(() => assertDispatchCoverage(caseIds));
  console.log(`PASS dispatch coverage complete (${caseIds.length} cases)`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
