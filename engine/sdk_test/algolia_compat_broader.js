import assert from "node:assert/strict";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { createFlapjackClient } from "./lib/flapjack-client.js";
import { createVerdictRecorder } from "./lib/test-helpers.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const MANIFEST_PATH = join(__dirname, "..", "docs2", "algolia_parity_cases.json");

const client = createFlapjackClient();
const verdict = createVerdictRecorder();

class ConfirmedGapError extends Error {
  constructor(caseId, reason) {
    super(reason);
    this.name = "ConfirmedGapError";
    this.caseId = caseId;
  }
}

async function loadManifest() {
  const raw = await readFile(MANIFEST_PATH, "utf8");
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed) || parsed.length === 0) {
    throw new Error(`Manifest at ${MANIFEST_PATH} is missing or empty`);
  }

  const caseIds = parsed.map((item, index) => {
    if (!item || typeof item.id !== "string" || item.id.length === 0) {
      throw new Error(`Manifest entry ${index} is missing a non-empty id`);
    }
    return item.id;
  });

  return caseIds;
}

function confirmedGap(caseId, reason) {
  return new ConfirmedGapError(caseId, reason);
}

function assertNonEmptyString(value, message) {
  assert.equal(typeof value, "string", message);
  assert.ok(value.length > 0, message);
}

function assertNumeric(value, message) {
  assert.equal(typeof value, "number", message);
}

function requireMethod(owner, methodName, caseId) {
  const candidate = owner?.[methodName];
  if (typeof candidate !== "function") {
    throw confirmedGap(
      caseId,
      `algoliasearch@5 SDK surface missing method ${methodName} required for ${caseId}`
    );
  }
  return candidate;
}

function createHarnessContext() {
  const runId = Date.now();
  let sequence = 0;
  let cachedAbtestingClient = null;

  function nextToken(label) {
    sequence += 1;
    return `${label}_${runId}_${sequence}`;
  }

  function nextIndexName(label) {
    return `stage4_broader_${nextToken(label)}`;
  }

  function nextKeyDescription(label) {
    return `stage4-broader-${nextToken(label)}`;
  }

  async function callSearchClient(caseId, methodName, payload) {
    const method = requireMethod(client, methodName, caseId);
    return method.call(client, payload);
  }

  function getAbtestingClient(caseId) {
    if (!cachedAbtestingClient) {
      const initAbtesting = requireMethod(client, "initAbtesting", caseId);
      cachedAbtestingClient = initAbtesting.call(client, {
        region: "us",
        options: {
          requester: client.transporter.requester
        }
      });
    }
    return cachedAbtestingClient;
  }

  async function callAbtestingClient(caseId, methodName, payload) {
    const abtestingClient = getAbtestingClient(caseId);
    const method = requireMethod(abtestingClient, methodName, caseId);
    return method.call(abtestingClient, payload);
  }

  async function createKeyForCase(caseId, label) {
    const response = await callSearchClient(caseId, "addApiKey", {
      acl: ["search"],
      description: nextKeyDescription(label),
      validity: 120
    });
    assertNonEmptyString(response.key, `${caseId} create-key setup missing key`);
    return response.key;
  }

  async function seedObjectForCase(caseId, indexName, objectID) {
    const response = await callSearchClient(caseId, "saveObject", {
      indexName,
      body: {
        objectID,
        title: `Seed ${objectID}`
      }
    });
    assertNonEmptyString(response.objectID, `${caseId} seedObject missing objectID`);
    assertNumeric(response.taskID, `${caseId} seedObject missing numeric taskID`);
  }

  async function createAbTestForCase(caseId, label) {
    const controlIndexName = nextIndexName(`${label}_control`);
    const variantIndexName = nextIndexName(`${label}_variant`);
    const response = await callAbtestingClient(caseId, "addABTests", {
      name: `stage4 broader ${nextToken("abtest")}`,
      variants: [
        { index: controlIndexName, trafficPercentage: 50 },
        { index: variantIndexName, trafficPercentage: 50 }
      ],
      endAt: new Date(Date.now() + 3600_000).toISOString()
    });
    assertNumeric(response.abTestID, `${caseId} create AB test missing numeric abTestID`);
    assertNumeric(response.taskID, `${caseId} create AB test missing numeric taskID`);
    return response.abTestID;
  }

  async function startAbTestForStopCase(caseId, abTestID) {
    await callSearchClient(caseId, "customPost", {
      path: `2/abtests/${abTestID}/start`
    });
  }

  async function ensureIndexExistsForDelete(caseId, indexName) {
    await callSearchClient(caseId, "customPost", {
      path: "1/indexes",
      body: { uid: indexName }
    });
  }

  return {
    nextToken,
    nextIndexName,
    nextKeyDescription,
    callSearchClient,
    callAbtestingClient,
    createKeyForCase,
    seedObjectForCase,
    createAbTestForCase,
    startAbTestForStopCase,
    ensureIndexExistsForDelete
  };
}

async function runKeysCreateCase(context) {
  const response = await context.callSearchClient("keys.create", "addApiKey", {
    acl: ["search"],
    description: context.nextKeyDescription("keys_create"),
    validity: 60
  });
  assertNonEmptyString(response.key, "keys.create missing key in response");
  return `created_key_prefix=${response.key.slice(0, 8)}`;
}

async function runKeysUpdateCase(context) {
  const key = await context.createKeyForCase("keys.update", "keys_update_target");
  const response = await context.callSearchClient("keys.update", "updateApiKey", {
    key,
    apiKey: {
      acl: ["search", "browse"],
      description: context.nextKeyDescription("keys_update")
    }
  });
  assert.equal(response.key, key, "keys.update returned an unexpected key");
  assertNonEmptyString(response.updatedAt, "keys.update missing updatedAt");
  return `updated_key_prefix=${response.key.slice(0, 8)}`;
}

async function runKeysDeleteCase(context) {
  const key = await context.createKeyForCase("keys.delete", "keys_delete_target");
  const response = await context.callSearchClient("keys.delete", "deleteApiKey", { key });
  assertNonEmptyString(response.deletedAt, "keys.delete missing deletedAt");
  return `deleted_key_prefix=${key.slice(0, 8)}`;
}

async function runKeysRestoreCase(context) {
  const key = await context.createKeyForCase("keys.restore", "keys_restore_target");
  await context.callSearchClient("keys.restore", "deleteApiKey", { key });
  const response = await context.callSearchClient("keys.restore", "restoreApiKey", { key });
  assert.equal(response.key, key, "keys.restore returned an unexpected key");
  assertNonEmptyString(response.createdAt, "keys.restore missing createdAt");
  return `restored_key_prefix=${key.slice(0, 8)}`;
}

async function runAbtestsCreateCase(context) {
  const response = await context.callAbtestingClient("abtests.create", "addABTests", {
    name: `stage4 broader create ${context.nextToken("abtest_create")}`,
    variants: [
      { index: context.nextIndexName("abtests_create_control"), trafficPercentage: 50 },
      { index: context.nextIndexName("abtests_create_variant"), trafficPercentage: 50 }
    ],
    endAt: new Date(Date.now() + 3600_000).toISOString()
  });
  assertNumeric(response.abTestID, "abtests.create missing numeric abTestID");
  assertNumeric(response.taskID, "abtests.create missing numeric taskID");
  return `abTestID=${response.abTestID}`;
}

async function runAbtestsDeleteCase(context) {
  const abTestID = await context.createAbTestForCase("abtests.delete", "abtests_delete");
  const response = await context.callAbtestingClient("abtests.delete", "deleteABTest", {
    id: abTestID
  });
  assert.equal(response.abTestID, abTestID, "abtests.delete returned unexpected abTestID");
  assertNumeric(response.taskID, "abtests.delete missing numeric taskID");
  return `abTestID=${response.abTestID}`;
}

async function runAbtestsStopCase(context) {
  const abTestID = await context.createAbTestForCase("abtests.stop", "abtests_stop");
  await context.startAbTestForStopCase("abtests.stop", abTestID);
  const response = await context.callAbtestingClient("abtests.stop", "stopABTest", {
    id: abTestID
  });
  assert.equal(response.abTestID, abTestID, "abtests.stop returned unexpected abTestID");
  assertNumeric(response.taskID, "abtests.stop missing numeric taskID");
  return `abTestID=${response.abTestID}`;
}

async function runIndexesDeleteCase(context) {
  const indexName = context.nextIndexName("indexes_delete");
  await context.ensureIndexExistsForDelete("indexes.delete", indexName);
  const response = await context.callSearchClient("indexes.delete", "deleteIndex", { indexName });
  assertNonEmptyString(response.deletedAt, "indexes.delete missing deletedAt");
  return `index=${indexName}`;
}

async function runObjectsSaveAutoIdCase(context) {
  const indexName = context.nextIndexName("objects_save_auto_id");
  const response = await context.callSearchClient("objects.save_auto_id", "saveObject", {
    indexName,
    body: {
      title: `Auto ${context.nextToken("save_auto_id")}`
    }
  });
  assertNonEmptyString(response.objectID, "objects.save_auto_id missing objectID");
  assertNumeric(response.taskID, "objects.save_auto_id missing numeric taskID");
  return `objectID=${response.objectID}`;
}

async function runObjectsBatchCase(context) {
  const indexName = context.nextIndexName("objects_batch");
  const response = await context.callSearchClient("objects.batch", "batch", {
    indexName,
    batchWriteParams: {
      requests: [
        { action: "addObject", body: { objectID: context.nextToken("one"), title: "One" } },
        { action: "addObject", body: { objectID: context.nextToken("two"), title: "Two" } }
      ]
    }
  });
  assertNumeric(response.taskID, "objects.batch missing numeric taskID");
  assert.ok(Array.isArray(response.objectIDs), "objects.batch missing objectIDs array");
  assert.equal(response.objectIDs.length, 2, "objects.batch expected two objectIDs");
  return `taskID=${response.taskID} objectIDs=${response.objectIDs.join(",")}`;
}

async function runObjectsDeleteCase(context) {
  const indexName = context.nextIndexName("objects_delete");
  const objectID = context.nextToken("objects_delete_target");
  await context.seedObjectForCase("objects.delete", indexName, objectID);
  const response = await context.callSearchClient("objects.delete", "deleteObject", {
    indexName,
    objectID
  });
  assertNonEmptyString(response.deletedAt, "objects.delete missing deletedAt");
  return `objectID=${objectID}`;
}

async function runObjectsPartialCase(context) {
  const indexName = context.nextIndexName("objects_partial");
  const objectID = context.nextToken("objects_partial_target");
  await context.seedObjectForCase("objects.partial", indexName, objectID);
  const response = await context.callSearchClient("objects.partial", "partialUpdateObject", {
    indexName,
    objectID,
    attributesToUpdate: {
      title: `Partial ${context.nextToken("objects_partial_update")}`
    }
  });
  assert.equal(response.objectID, objectID, "objects.partial returned unexpected objectID");
  assertNumeric(response.taskID, "objects.partial missing numeric taskID");
  assertNonEmptyString(response.updatedAt, "objects.partial missing updatedAt");
  return `objectID=${response.objectID}`;
}

const CASE_DISPATCH = {
  "keys.create": runKeysCreateCase,
  "keys.update": runKeysUpdateCase,
  "keys.delete": runKeysDeleteCase,
  "keys.restore": runKeysRestoreCase,
  "abtests.create": runAbtestsCreateCase,
  "abtests.delete": runAbtestsDeleteCase,
  "abtests.stop": runAbtestsStopCase,
  "indexes.delete": runIndexesDeleteCase,
  "objects.save_auto_id": runObjectsSaveAutoIdCase,
  "objects.batch": runObjectsBatchCase,
  "objects.delete": runObjectsDeleteCase,
  "objects.partial": runObjectsPartialCase
};

export function findMissingDispatchCaseIds(caseIds, dispatchTable = CASE_DISPATCH) {
  return caseIds.filter((caseId) => typeof dispatchTable[caseId] !== "function");
}

export function assertDispatchCoverage(caseIds, dispatchTable = CASE_DISPATCH) {
  const missingCaseIds = findMissingDispatchCaseIds(caseIds, dispatchTable);
  if (missingCaseIds.length > 0) {
    throw new Error(
      `Missing broader dispatch handlers for exported case.id values: ${missingCaseIds.join(", ")}`
    );
  }
}

async function dispatchCase(caseId, context) {
  return CASE_DISPATCH[caseId](context);
}

function sanitizeCaseId(caseId) {
  return caseId.replace(/[^A-Za-z0-9]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase();
}

async function writeFollowupStub(caseId, reason) {
  const fileName = `may23_pm_11_followup_${sanitizeCaseId(caseId)}.md`;
  const relativePath = join("chats", "icg", fileName).replaceAll("\\", "/");
  const absolutePath = join(__dirname, "..", relativePath);
  await mkdir(dirname(absolutePath), { recursive: true });

  const body = [
    `# Follow-up: ${caseId}`,
    "",
    "- Created by Stage 4 broader Algolia compatibility SKIP policy.",
    `- Case ID: \`${caseId}\``,
    `- Confirmed gap: ${reason}`,
    "- Next step: close the product/SDK parity gap and convert this SKIP to PASS."
  ].join("\n");

  try {
    await writeFile(absolutePath, `${body}\n`, { flag: "wx" });
  } catch (error) {
    if (!(error && typeof error === "object" && error.code === "EEXIST")) {
      throw error;
    }
  }

  return relativePath;
}

async function main() {
  const caseIds = await loadManifest();
  assertDispatchCoverage(caseIds);

  const context = createHarnessContext();

  for (const caseId of caseIds) {
    try {
      const detail = await dispatchCase(caseId, context);
      await verdict.runStep(caseId, async () => detail);
    } catch (error) {
      if (error instanceof ConfirmedGapError) {
        const followupPath = await writeFollowupStub(error.caseId, error.message);
        verdict.runSkip(caseId, `${error.message} followup=${followupPath}`);
        continue;
      }
      await verdict.runStep(caseId, async () => {
        throw error;
      });
    }
  }

  const { failCount, totalCount } = verdict.summarize();
  if (totalCount !== caseIds.length) {
    throw new Error(
      `Broader Algolia compatibility harness accounted for ${totalCount}/${caseIds.length} rows`
    );
  }

  if (failCount > 0) {
    const firstFailureError = verdict.getFirstFailureError();
    if (firstFailureError) {
      throw firstFailureError;
    }
    throw new Error(
      `Broader Algolia compatibility harness failed (${failCount}/${totalCount} failing rows)`
    );
  }
}

if (process.argv[1] && resolve(process.argv[1]) === __filename) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
