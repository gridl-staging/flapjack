import assert from "node:assert/strict";

import { createFlapjackClient } from "./lib/flapjack-client.js";
import { bindClientHelpers, createVerdictRecorder } from "./lib/test-helpers.js";

const client = createFlapjackClient();
const helpers = bindClientHelpers(client);
const verdict = createVerdictRecorder();
const TEST_INDEX = `stage5_algolia_compat_${Date.now()}`;

const documents = [
  { objectID: "1", title: "pancake recipes", tags: "breakfast" },
  { objectID: "2", title: "waffle guide", tags: "breakfast" },
  { objectID: "3", title: "omelette handbook", tags: "breakfast" }
];

async function main() {
  await helpers.cleanupIndexes([TEST_INDEX]);

  await verdict.runStep("saveObjects", async () => {
    const responses = await client.saveObjects({
      indexName: TEST_INDEX,
      objects: documents
    });

    assert.ok(Array.isArray(responses), "saveObjects should return a BatchResponse[]");

    const objectIDs = responses.flatMap((response) =>
      Array.isArray(response.objectIDs) ? response.objectIDs : []
    );

    assert.ok(
      responses.every((response) => typeof response.taskID === "number"),
      "saveObjects response missing numeric taskID"
    );
    assert.equal(
      objectIDs.length,
      documents.length,
      `expected ${documents.length} objectIDs across saveObjects batch responses`
    );
    assert.deepEqual(
      objectIDs,
      documents.map((document) => document.objectID),
      "saveObjects returned unexpected objectIDs"
    );
    return `batches=${responses.length} objectIDs=${objectIDs.join(",")}`;
  });

  await verdict.runStep("searchable_visibility_polling", async () => {
    const searchReady = await helpers.waitForSearch(
      TEST_INDEX,
      { query: "pancake" },
      (result) => result?.nbHits === 1,
      10000
    );
    assert.ok(searchReady, "timed out waiting for searchable pancake hit");
    return `nbHits=${searchReady.nbHits}`;
  });

  await verdict.runStep("simple_search", async () => {
    const result = await helpers.searchIndex(TEST_INDEX, { query: "pancake" });
    assert.equal(result.nbHits, 1, `expected pancake nbHits=1, got ${result.nbHits}`);
    assert.equal(result.hits[0]?.objectID, "1", "expected pancake hit objectID=1");
    return `nbHits=${result.nbHits}`;
  });

  await verdict.runStep("typo_search", async () => {
    const result = await helpers.searchIndex(TEST_INDEX, { query: "pacake" });
    assert.ok(result.nbHits >= 1, `expected typo search nbHits>=1, got ${result.nbHits}`);
    return `nbHits=${result.nbHits}`;
  });

  await verdict.runStep("setSettings_plus_facets", async () => {
    const settingsResponse = await client.setSettings({
      indexName: TEST_INDEX,
      indexSettings: { attributesForFaceting: ["tags"] }
    });
    assert.equal(
      typeof settingsResponse.taskID,
      "number",
      "setSettings missing numeric taskID"
    );

    const facetsReady = await helpers.waitForSearch(
      TEST_INDEX,
      { query: "", facets: ["tags"] },
      (result) => result?.facets?.tags?.breakfast === 3,
      10000
    );
    assert.ok(facetsReady, "timed out waiting for facets.tags.breakfast=3");
    assert.equal(
      facetsReady.facets.tags.breakfast,
      3,
      `expected facets.tags.breakfast=3, got ${facetsReady.facets.tags.breakfast}`
    );
    return `facets.tags.breakfast=${facetsReady.facets.tags.breakfast}`;
  });

  await verdict.runStep("deleteObject_visibility", async () => {
    const response = await client.deleteObject({ indexName: TEST_INDEX, objectID: "1" });
    assert.equal(typeof response.taskID, "number", "deleteObject missing numeric taskID");

    const deleted = await helpers.waitForSearch(
      TEST_INDEX,
      { query: "pancake" },
      (result) => result?.nbHits === 0,
      10000
    );
    assert.ok(deleted, "timed out waiting for pancake deletion visibility");
    assert.equal(deleted.nbHits, 0, `expected post-delete nbHits=0, got ${deleted.nbHits}`);
    return `nbHits=${deleted.nbHits}`;
  });

  const { failCount, totalCount } = verdict.summarize();

  await helpers.cleanupIndexes([TEST_INDEX]);

  if (failCount > 0) {
    throw new Error(`Stage 5 compatibility matrix failed (${failCount}/${totalCount} failing rows)`);
  }
}

main().catch(async (error) => {
  try {
    await helpers.cleanupIndexes([TEST_INDEX]);
  } catch {
    // Cleanup failures are secondary to the compatibility verdict.
  }
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
