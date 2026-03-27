import { createFlapjackClient } from './lib/flapjack-client.js';
import { assert, bindClientHelpers } from './lib/test-helpers.js';

const client = createFlapjackClient();
const { waitForMinHits, cleanupIndexes } = bindClientHelpers(client);

async function run() {
  const testIndex = `test_exhaustive_${Date.now()}`;

  await cleanupIndexes([testIndex]);

  try {
    const settingsResponse = await client.setSettings({
      indexName: testIndex,
      indexSettings: { attributesForFaceting: ['category'] }
    });
    assert(
      settingsResponse && typeof settingsResponse.taskID === 'number',
      `Expected numeric taskID from setSettings, got ${JSON.stringify(settingsResponse)}`
    );
    await client.waitForTask({ indexName: testIndex, taskID: settingsResponse.taskID });

    const saveResponse = await client.saveObjects({
      indexName: testIndex,
      objects: [
        { objectID: '1', name: 'Product A', category: 'electronics' },
        { objectID: '2', name: 'Product B', category: 'books' }
      ]
    });
    const firstTask = Array.isArray(saveResponse) ? saveResponse[0] : saveResponse;
    assert(firstTask && typeof firstTask.taskID === 'number', `Expected numeric taskID from saveObjects, got ${JSON.stringify(saveResponse)}`);
    await client.waitForTask({ indexName: testIndex, taskID: firstTask.taskID });

    await waitForMinHits(testIndex, 2, 5000);

    const withoutFacetsResponse = await client.search({
      requests: [{ indexName: testIndex, query: '' }]
    });
    const withoutFacets = withoutFacetsResponse.results[0];

    assert(withoutFacets.exhaustive && typeof withoutFacets.exhaustive === 'object', 'Expected exhaustive object without facets');
    assert(typeof withoutFacets.exhaustive.nbHits === 'boolean', 'Expected exhaustive.nbHits boolean without facets');
    assert(typeof withoutFacets.exhaustive.typo === 'boolean', 'Expected exhaustive.typo boolean without facets');
    assert(typeof withoutFacets.exhaustive.facetValues === 'boolean', 'Expected exhaustive.facetValues boolean without facets');
    assert(typeof withoutFacets.exhaustive.rulesMatch === 'boolean', 'Expected exhaustive.rulesMatch boolean without facets');
    assert(!('exhaustiveFacetsCount' in withoutFacets), 'Did not expect exhaustiveFacetsCount without facets request');
    assert(!('facets' in withoutFacets), 'Did not expect facets field without facets request');

    const withFacetsResponse = await client.search({
      requests: [{ indexName: testIndex, query: '', facets: ['category'] }]
    });
    const withFacets = withFacetsResponse.results[0];

    assert(typeof withFacets.exhaustiveFacetsCount === 'boolean', 'Expected exhaustiveFacetsCount boolean with facets request');
    assert(withFacets.exhaustive && typeof withFacets.exhaustive === 'object', 'Expected exhaustive object with facets');
    assert(typeof withFacets.exhaustive.facetsCount === 'boolean', 'Expected exhaustive.facetsCount boolean with facets');
    assert(withFacets.facets && withFacets.facets.category, 'Expected facets.category with facets request');
    assert(withFacets.facets.category.electronics === 1, `Expected electronics facet count=1, got ${JSON.stringify(withFacets.facets.category)}`);
    assert(withFacets.facets.category.books === 1, `Expected books facet count=1, got ${JSON.stringify(withFacets.facets.category)}`);

    const noHitFacetsResponse = await client.search({
      requests: [{ indexName: testIndex, query: 'nonexistent', facets: ['category'] }]
    });
    const noHitFacets = noHitFacetsResponse.results[0];

    assert(noHitFacets.nbHits === 0, `Expected nbHits=0 for no-hit query, got ${noHitFacets.nbHits}`);
    assert(typeof noHitFacets.exhaustiveFacetsCount === 'boolean', 'Expected exhaustiveFacetsCount on no-hit facets request');
    assert(noHitFacets.facets && Object.keys(noHitFacets.facets).length === 0, `Expected empty facets object for no-hit query, got ${JSON.stringify(noHitFacets.facets)}`);

    console.log('Exhaustive field assertions passed');
  } finally {
    await cleanupIndexes([testIndex]);
  }
}

run().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
