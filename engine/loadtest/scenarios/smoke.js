import { check, fail } from 'k6';
import { sharedLoadtestConfig } from '../lib/config.js';
import {
  batchWrite,
  getHealth,
  searchPost,
  waitForTaskPublished,
} from '../lib/http.js';

export const options = {
  vus: 1,
  iterations: 1,
};

function assertOk(response, label) {
  if (!check(response, { [`${label} should be HTTP 200`]: (r) => r.status === 200 })) {
    fail(`${label} failed with status ${response.status} body=${response.body}`);
  }
}

function assertChecks(response, label, assertions) {
  if (!check(response, assertions)) {
    fail(`${label} assertions failed with status ${response.status} body=${response.body}`);
  }
}

export default function () {
  const healthResponse = getHealth();
  assertOk(healthResponse, 'health');
  assertChecks(healthResponse, 'health payload', {
    'health has status ok': (r) => r.json('status') === 'ok',
    'health has version': (r) => typeof r.json('version') === 'string',
    'health has uptime_secs': (r) => Number.isFinite(r.json('uptime_secs')),
  });

  const seededSearchResponse = searchPost(sharedLoadtestConfig.readIndexName, {
    query: 'MacBook',
    hitsPerPage: 5,
  });
  assertOk(seededSearchResponse, 'seeded search');
  assertChecks(seededSearchResponse, 'seeded search payload', {
    'seeded search has hits': (r) => {
      const hits = r.json('hits');
      return Array.isArray(hits) && hits.length > 0;
    },
  });

  const writeBatchResponse = batchWrite(sharedLoadtestConfig.writeIndexName, {
    requests: [
      {
        action: 'addObject',
        body: {
          objectID: 'smoke-write-doc-0001',
          name: 'Smoke Write Product',
          description: 'Smoke write document for deterministic loadtest validation.',
          brand: 'SmokeBrand',
          category: 'Accessories',
          subcategory: 'Input',
          price: 10.0,
          rating: 4.0,
          reviewCount: 1,
          inStock: true,
          tags: ['smoke', 'write'],
          color: 'Black',
          releaseYear: 2026,
          _geo: { lat: 40.7128, lng: -74.0060 },
        },
      },
    ],
  });
  assertOk(writeBatchResponse, 'write batch');

  const taskId = writeBatchResponse.json('taskID');
  if (!Number.isInteger(taskId)) {
    fail(`write batch response missing numeric taskID: ${writeBatchResponse.body}`);
  }

  const taskResponse = waitForTaskPublished(taskId);
  assertChecks(taskResponse, 'write task payload', {
    'write task is published': (r) => r.json('status') === 'published',
    'write task is not pending': (r) => r.json('pendingTask') === false,
  });
}
