import assert from "node:assert/strict";
import { mkdtemp, readdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  generateDataset,
  parseCliArgs,
  validateGeneratedBatches,
} from "./generate_dataset.mjs";

async function withTempDir(prefix, fn) {
  const tempDir = await mkdtemp(path.join(os.tmpdir(), prefix));
  try {
    await fn(tempDir);
  } finally {
    await rm(tempDir, { recursive: true, force: true });
  }
}

async function readBatchFiles(outputDir) {
  const entries = await readdir(outputDir);
  const batchFiles = entries.filter((entry) => /^batch_\d+\.json$/.test(entry)).sort();
  const batches = [];
  for (const filename of batchFiles) {
    const filePath = path.join(outputDir, filename);
    const content = await readFile(filePath, "utf8");
    batches.push({
      filename,
      content,
      payload: JSON.parse(content),
    });
  }
  return batches;
}

test("parseCliArgs applies defaults and supports --validate", () => {
  const defaults = parseCliArgs([]);
  assert.equal(defaults.count, 100000);
  assert.equal(defaults.batchSize, 1000);
  assert.equal(defaults.validate, false);

  const custom = parseCliArgs(["--count", "250", "--batch-size", "80", "--validate"]);
  assert.equal(custom.count, 250);
  assert.equal(custom.batchSize, 80);
  assert.equal(custom.validate, true);
});

test("generateDataset writes deterministic Algolia batch payloads", async () => {
  await withTempDir("flapjack-generate-a-", async (dirA) => {
    await withTempDir("flapjack-generate-b-", async (dirB) => {
      const args = { count: 250, batchSize: 80, outputDir: dirA, printSummary: false };
      const summaryA = await generateDataset(args);
      const summaryB = await generateDataset({ ...args, outputDir: dirB });

      assert.equal(summaryA.totalDocs, 250);
      assert.equal(summaryA.batchCount, 4);
      assert.equal(summaryA.batchCount, summaryB.batchCount);

      const batchesA = await readBatchFiles(dirA);
      const batchesB = await readBatchFiles(dirB);
      assert.deepEqual(
        batchesA.map((batch) => batch.filename),
        batchesB.map((batch) => batch.filename),
      );

      const seenObjectIds = new Set();
      for (let index = 0; index < batchesA.length; index += 1) {
        const payloadA = batchesA[index].payload;
        const payloadB = batchesB[index].payload;
        assert.deepEqual(payloadA, payloadB);
        assert.ok(Array.isArray(payloadA.requests));

        for (const request of payloadA.requests) {
          assert.equal(request.action, "addObject");
          assert.ok(request.body);
          assert.ok(request.body.objectID.startsWith("bench-"));
          assert.ok(!Object.hasOwn(request.body, "_geo"));
          assert.ok(Object.hasOwn(request.body, "_geoloc"));
          assert.equal(typeof request.body._geoloc.lat, "number");
          assert.equal(typeof request.body._geoloc.lng, "number");
          seenObjectIds.add(request.body.objectID);
        }
      }
      assert.equal(seenObjectIds.size, 250);
    });
  });
});

test("validateGeneratedBatches checks required fields and uniqueness", async () => {
  await withTempDir("flapjack-validate-good-", async (goodDir) => {
    await generateDataset({ count: 180, batchSize: 50, outputDir: goodDir, printSummary: false });
    const report = await validateGeneratedBatches({
      outputDir: goodDir,
      expectedCount: 180,
    });
    assert.equal(report.isValid, true);
    assert.equal(report.totalDocs, 180);
    assert.equal(report.errors.length, 0);
  });

  await withTempDir("flapjack-validate-bad-", async (badDir) => {
    const invalidPayload = {
      requests: [
        {
          action: "addObject",
          body: {
            objectID: "bench-000001",
            name: "Broken Product",
            description: "Missing _geoloc and duplicated objectID",
            brand: "Broken",
            category: "Broken",
            subcategory: "Broken",
            price: 10,
            rating: 4,
            reviewCount: 1,
            inStock: true,
            tags: ["broken"],
            color: "Black",
            releaseYear: 2020,
            _geo: { lat: 0, lng: 0 },
          },
        },
        {
          action: "addObject",
          body: {
            objectID: "bench-000001",
            name: "Broken Product 2",
            description: "Duplicate ID",
            brand: "Broken",
            category: "Broken",
            subcategory: "Broken",
            price: 10,
            rating: 4,
            reviewCount: 1,
            inStock: true,
            tags: ["broken"],
            color: "Black",
            releaseYear: 2020,
            _geo: { lat: 0, lng: 0 },
          },
        },
      ],
    };
    await writeFile(path.join(badDir, "batch_001.json"), `${JSON.stringify(invalidPayload, null, 2)}\n`);
    const report = await validateGeneratedBatches({
      outputDir: badDir,
      expectedCount: 2,
    });
    assert.equal(report.isValid, false);
    assert.ok(report.errors.some((error) => error.includes("Duplicate objectID")));
    assert.ok(report.errors.some((error) => error.includes("missing _geoloc")));
    assert.ok(report.errors.some((error) => error.includes("unexpected _geo")));
  });
});

test("validateGeneratedBatches rejects malformed operation bodies and wrong field types", async () => {
  await withTempDir("flapjack-validate-types-", async (outputDir) => {
    const invalidPayload = {
      requests: [
        {
          action: "addObject",
          body: null,
        },
        {
          action: "addObject",
          body: {
            objectID: "bench-000002",
            name: "",
            description: 42,
            brand: "Broken",
            category: "Broken",
            subcategory: "Broken",
            price: "10.50",
            rating: Number.NaN,
            reviewCount: "100",
            inStock: "yes",
            tags: ["valid", 7],
            color: "Black",
            releaseYear: "2024",
            _geoloc: null,
          },
        },
      ],
    };

    await writeFile(path.join(outputDir, "batch_001.json"), `${JSON.stringify(invalidPayload, null, 2)}\n`);
    const report = await validateGeneratedBatches({
      outputDir,
      expectedCount: 1,
    });

    assert.equal(report.isValid, false);
    assert.ok(report.errors.some((error) => error.includes("request 0 is not a valid addObject operation")));
    assert.ok(report.errors.some((error) => error.includes("field name must be a non-empty string")));
    assert.ok(report.errors.some((error) => error.includes("field description must be a non-empty string")));
    assert.ok(report.errors.some((error) => error.includes("field price must be a finite number")));
    assert.ok(report.errors.some((error) => error.includes("field rating must be a finite number")));
    assert.ok(report.errors.some((error) => error.includes("field reviewCount must be a finite number")));
    assert.ok(report.errors.some((error) => error.includes("field inStock must be a boolean")));
    assert.ok(report.errors.some((error) => error.includes("tags must contain only non-empty strings")));
    assert.ok(report.errors.some((error) => error.includes("_geoloc must be an object")));
  });
});

test("validateGeneratedBatches rejects symlinked batch files", async () => {
  await withTempDir("flapjack-validate-symlink-", async (outputDir) => {
    const outsideFile = path.join(outputDir, "outside.json");
    const symlinkedBatch = path.join(outputDir, "batch_001.json");
    await writeFile(outsideFile, JSON.stringify({ requests: [] }));
    await symlink(outsideFile, symlinkedBatch);

    const report = await validateGeneratedBatches({
      outputDir,
      expectedCount: 0,
    });

    assert.equal(report.isValid, false);
    assert.ok(report.errors.some((error) => error.includes("refusing symlinked batch file")));
  });
});
