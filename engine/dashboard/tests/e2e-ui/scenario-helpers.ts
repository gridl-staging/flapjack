/**
 */
import { expect, type APIRequestContext } from '@playwright/test';
import { addDocuments, createIndex, deleteIndex, searchIndex } from '../fixtures/api-helpers';

export interface IsolatedMerchandisingScenario {
  indexName: string;
  searchQuery: string;
}

export interface IsolatedMerchandisingComparisonScenario {
  indexName: string;
  firstQuery: string;
  secondQuery: string;
  firstObjectId: string;
  secondObjectId: string;
}

export interface IsolatedMerchandisingLifecycleScenario {
  indexName: string;
  searchQuery: string;
  expectedObjectIDs: string[];
}

function buildUniqueSuffix(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function slugifyIndexComponent(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 24);
}

export async function createIsolatedMerchandisingScenario(
  request: APIRequestContext,
  scenarioName: string,
): Promise<IsolatedMerchandisingScenario> {
  const suffix = buildUniqueSuffix();
  const indexName = `e2e-merch-${slugifyIndexComponent(scenarioName)}-${suffix}`;
  const searchQuery = `merch-fixture-${suffix}`;

  await deleteIndex(request, indexName);
  await createIndex(request, indexName);
  await addDocuments(request, indexName, [
    {
      objectID: `merch-${suffix}`,
      name: `Merch Fixture ${searchQuery}`,
      brand: `MerchBrand-${suffix}`,
      category: 'Merchandising Fixtures',
    },
  ]);

  await expect(async () => {
    const response = await searchIndex(request, indexName, searchQuery);
    expect(response.nbHits ?? 0).toBeGreaterThan(0);
  }).toPass({ timeout: 15_000 });

  return { indexName, searchQuery };
}

export async function createIsolatedMerchandisingComparisonScenario(
  request: APIRequestContext,
  scenarioName: string,
): Promise<IsolatedMerchandisingComparisonScenario> {
  const suffix = buildUniqueSuffix();
  const indexName = `e2e-merch-compare-${slugifyIndexComponent(scenarioName)}-${suffix}`;
  const firstQuery = `tablet-fixture-${suffix}`;
  const secondQuery = `monitor-fixture-${suffix}`;
  const firstObjectId = `p${Date.now()}11`;
  const secondObjectId = `p${Date.now()}22`;

  await deleteIndex(request, indexName);
  await createIndex(request, indexName);
  await addDocuments(request, indexName, [
    {
      objectID: firstObjectId,
      name: `Fixture ${firstQuery}`,
      brand: `MerchBrand-${suffix}`,
      category: 'Merchandising Fixtures',
      description: 'Deterministic tablet fixture for merchandising comparison',
    },
    {
      objectID: secondObjectId,
      name: `Fixture ${secondQuery}`,
      brand: `MerchBrand-${suffix}`,
      category: 'Merchandising Fixtures',
      description: 'Deterministic monitor fixture for merchandising comparison',
    },
  ]);

  await expect(async () => {
    const firstResponse = await searchIndex(request, indexName, firstQuery);
    const secondResponse = await searchIndex(request, indexName, secondQuery);
    const firstHits = Array.isArray(firstResponse.hits) ? firstResponse.hits : [];
    const secondHits = Array.isArray(secondResponse.hits) ? secondResponse.hits : [];
    expect(firstHits.length).toBeGreaterThan(0);
    expect(secondHits.length).toBeGreaterThan(0);
  }).toPass({ timeout: 15_000 });

  return {
    indexName,
    firstQuery,
    secondQuery,
    firstObjectId,
    secondObjectId,
  };
}

/**
 * Builds an isolated index fixture for deterministic merchandising lifecycle assertions.
 *
 * The fixture contains four query-matching documents with predictable objectID values
 * so tests can pin/hide/reorder cards and assert persisted rule payloads.
 */
export async function createIsolatedMerchandisingLifecycleScenario(
  request: APIRequestContext,
  scenarioName: string,
): Promise<IsolatedMerchandisingLifecycleScenario> {
  const suffix = buildUniqueSuffix();
  const indexName = `e2e-merch-life-${slugifyIndexComponent(scenarioName)}-${suffix}`;
  const objectIdSeed = Date.now();
  const searchQuery = `merchlifecycle${objectIdSeed}`;
  const expectedObjectIDs = [
    `p${objectIdSeed}01`,
    `p${objectIdSeed}02`,
    `p${objectIdSeed}03`,
    `p${objectIdSeed}04`,
  ];

  await deleteIndex(request, indexName);
  await createIndex(request, indexName);
  await addDocuments(request, indexName, [
    {
      objectID: expectedObjectIDs[0],
      name: `Lifecycle Alpha ${searchQuery}`,
      brand: `MerchBrand-${suffix}`,
      category: 'Merchandising Fixtures',
      description: `Deterministic merchandising fixture A for ${searchQuery}`,
    },
    {
      objectID: expectedObjectIDs[1],
      name: `Lifecycle Beta ${searchQuery}`,
      brand: `MerchBrand-${suffix}`,
      category: 'Merchandising Fixtures',
      description: `Deterministic merchandising fixture B for ${searchQuery}`,
    },
    {
      objectID: expectedObjectIDs[2],
      name: `Lifecycle Gamma ${searchQuery}`,
      brand: `MerchBrand-${suffix}`,
      category: 'Merchandising Fixtures',
      description: `Deterministic merchandising fixture C for ${searchQuery}`,
    },
    {
      objectID: expectedObjectIDs[3],
      name: `Lifecycle Delta ${searchQuery}`,
      brand: `MerchBrand-${suffix}`,
      category: 'Merchandising Fixtures',
      description: `Deterministic merchandising fixture D for ${searchQuery}`,
    },
  ]);

  await expect(async () => {
    const response = await searchIndex(request, indexName, searchQuery);
    const hits = Array.isArray(response.hits) ? response.hits : [];
    const hitObjectIDs = hits
      .map((hit) => (typeof hit?.objectID === 'string' ? hit.objectID : ''))
      .filter((value): value is string => Boolean(value));

    expect(hitObjectIDs).toEqual(expect.arrayContaining(expectedObjectIDs));
  }).toPass({ timeout: 15_000 });

  return {
    indexName,
    searchQuery,
    expectedObjectIDs,
  };
}
