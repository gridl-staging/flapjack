import { createFlapjackClient } from './lib/flapjack-client.js';
import { assert, bindClientHelpers } from './lib/test-helpers.js';

const client = createFlapjackClient();
const { cleanupIndexes } = bindClientHelpers(client);

const INDEX = 'race_' + Date.now();

async function run() {
  await cleanupIndexes([INDEX]);

  try {
    const saveResponse = await client.saveObjects({
      indexName: INDEX,
      objects: [{ objectID: '1', name: 'Test' }]
    });

    const firstTask = Array.isArray(saveResponse) ? saveResponse[0] : saveResponse;
    assert(firstTask && typeof firstTask.taskID === 'number', `Expected taskID from saveObjects, got ${JSON.stringify(saveResponse)}`);

    const immediateSearch = await client.search({
      requests: [{ indexName: INDEX, query: 'test' }]
    });
    const immediateHits = immediateSearch.results[0].nbHits;
    assert(
      Number.isInteger(immediateHits) && immediateHits >= 0 && immediateHits <= 1,
      `Expected immediate search hit count to stay within [0,1], got ${immediateHits}`
    );

    await client.waitForTask({ indexName: INDEX, taskID: firstTask.taskID });

    const eventualSearch = await client.search({
      requests: [{ indexName: INDEX, query: 'test' }]
    });
    const eventualHits = eventualSearch.results[0].nbHits;
    assert(eventualHits === 1, `Expected exactly 1 hit after waitTask, got ${eventualHits}`);
    assert(
      eventualHits >= immediateHits,
      `Expected waitTask visibility to be monotonic, got immediate=${immediateHits} eventual=${eventualHits}`
    );

    console.log(`Immediate hits before waitTask: ${immediateHits}`);
    console.log(`Hits after waitTask: ${eventualHits}`);
  } finally {
    await cleanupIndexes([INDEX]);
  }
}

run().catch((error) => {
  console.error(error?.message || error);
  process.exit(1);
});
