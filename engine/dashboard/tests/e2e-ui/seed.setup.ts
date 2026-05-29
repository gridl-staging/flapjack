/**
 * Playwright setup project — seeds test data into the real Flapjack backend.
 * Runs ONCE before any e2e-ui tests.
 *
 * Requires: Flapjack server running on the repo-local configured backend port.
 */
import { test as setup, expect } from '@playwright/test';
import { TEST_INDEX, PRODUCTS, SYNONYMS, RULES, SETTINGS } from '../fixtures/test-data';
import { API_BASE as API, API_HEADERS as H } from '../fixtures/local-instance';
import { deletePersonalizationStrategy } from '../fixtures/api-helpers';
import {
  readSeedMetricsSnapshot,
  seededIndexMetricsReady,
} from './seed_metrics_readiness';

const INDEX = TEST_INDEX;
setup.setTimeout(120_000);

setup('seed test data', async ({ request }) => {
  // 1. Backend must be running
  const health = await request.get(`${API}/health`);
  expect(
    health.ok(),
    `Flapjack server must be running at ${API}`,
  ).toBeTruthy();

  // Reset the global strategy through the backend so test state does not depend on a local data-dir guess.
  await deletePersonalizationStrategy(request);

  // 2. Clean slate — delete test index if it exists (ignore 404)
  await request.delete(`${API}/1/indexes/${INDEX}`, { headers: H }).catch(() => {});

  // 3. Add documents (creates index implicitly)
  const batchRes = await request.post(`${API}/1/indexes/${INDEX}/batch`, {
    headers: H,
    data: {
      requests: PRODUCTS.map((doc) => ({ action: 'addObject', body: doc })),
    },
  });
  expect(batchRes.ok(), 'Failed to batch-add documents').toBeTruthy();

  // 4. Configure settings
  const settingsRes = await request.put(`${API}/1/indexes/${INDEX}/settings`, {
    headers: H,
    data: SETTINGS,
  });
  expect(settingsRes.ok(), 'Failed to update settings').toBeTruthy();

  // 5. Add synonyms (batch)
  const synRes = await request.post(`${API}/1/indexes/${INDEX}/synonyms/batch`, {
    headers: H,
    data: SYNONYMS,
  });
  expect(synRes.ok(), 'Failed to batch-add synonyms').toBeTruthy();

  // 6. Add rules (batch)
  const rulesRes = await request.post(`${API}/1/indexes/${INDEX}/rules/batch`, {
    headers: H,
    data: RULES,
  });
  expect(rulesRes.ok(), 'Failed to batch-add rules').toBeTruthy();

  // 7. Seed analytics data (7 days of realistic search/click/geo data)
  const analyticsSeedRes = await request.post(`${API}/2/analytics/seed`, {
    headers: H,
    data: { index: INDEX, days: 7 },
  });
  expect(analyticsSeedRes.ok(), 'Failed to seed analytics data').toBeTruthy();

  // 8. Wait for indexing to complete — poll until all documents are searchable
  await expect(async () => {
    const res = await request.post(`${API}/1/indexes/${INDEX}/query`, {
      headers: H,
      data: { query: '' },
    });
    expect(res.ok()).toBeTruthy();
    const body = await res.json();
    expect(body.nbHits).toBeGreaterThanOrEqual(PRODUCTS.length);
  }).toPass({ timeout: 15_000 });

  // 9. Wait for seeded per-index metrics to appear before dashboard specs start.
  // Storage bytes can lag document/search counters until the runtime metrics
  // refresh catches up, so poll the authenticated /metrics contract directly.
  await expect(async () => {
    const metricsResponse = await request.get(`${API}/metrics`, {
      headers: {
        'x-algolia-application-id': H['x-algolia-application-id'],
        'x-algolia-api-key': H['x-algolia-api-key'],
      },
    });
    expect(metricsResponse.ok(), 'Failed to fetch /metrics during seed setup').toBeTruthy();
    const metricsBody = await metricsResponse.text();

    const metricsSnapshot = readSeedMetricsSnapshot(metricsBody, INDEX);
    const docsValue = metricsSnapshot.documents;
    const storageValue = metricsSnapshot.storage;
    const searchesValue = metricsSnapshot.searches;
    const oplogValue = metricsSnapshot.oplog;

    expect(docsValue, `missing flapjack_documents_count for ${INDEX}`).not.toBeNull();
    expect(storageValue, `missing flapjack_storage_bytes for ${INDEX}`).not.toBeNull();
    expect(searchesValue, `missing flapjack_search_requests_total for ${INDEX}`).not.toBeNull();
    expect(oplogValue, `missing flapjack_oplog_current_seq for ${INDEX}`).not.toBeNull();
    expect(docsValue, `stale flapjack_documents_count for ${INDEX}`).toBeGreaterThanOrEqual(PRODUCTS.length);
    expect(searchesValue, `stale flapjack_search_requests_total for ${INDEX}`).toBeGreaterThan(0);
    expect(
      seededIndexMetricsReady(metricsSnapshot, {
        minimumDocuments: PRODUCTS.length,
        minimumSearchRequests: 1,
      }),
      `seeded metrics not ready for ${INDEX}`,
    ).toBeTruthy();
  }).toPass({ timeout: 75_000 });
});
