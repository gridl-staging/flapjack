import { test, expect } from '../fixtures/auth.fixture';
import type { APIRequestContext } from '@playwright/test';
import { API_BASE, API_HEADERS } from '../fixtures/local-instance';

/**
 * Demo Analytics API Tests — PURE API (no browser)
 *
 * Tests the analytics seed/flush/clear API endpoints directly.
 * These tests do NOT open a browser. For browser-based demo analytics verification,
 * see tests/e2e-ui/full/analytics*.spec.ts
 */

async function skipIfNoServer({ request }: { request: APIRequestContext }) {
  try {
    const res = await request.get(`${API_BASE}/health`, { timeout: 3000 });
    if (!res.ok()) test.skip(true, 'Flapjack server not available');
  } catch {
    test.skip(true, 'Flapjack server not reachable');
  }
}

test.describe('Analytics Management API (no browser)', () => {
  test.beforeEach(skipIfNoServer);

  test('flush endpoint triggers immediate analytics update', async ({ request }) => {
    const res = await request.post(`${API_BASE}/2/analytics/flush`, {
      headers: API_HEADERS,
    });
    expect(res.status()).toBe(200);
    const data = await res.json();
    expect(data.status).toBe('ok');
  });

  test('clear endpoint removes all analytics for an index', async ({ request }) => {
    const clearIndex = `clear-test-${Date.now()}`;
    await request.post(`${API_BASE}/2/analytics/seed`, {
      headers: API_HEADERS,
      data: { index: clearIndex, days: 7 },
    });

    const clearRes = await request.delete(`${API_BASE}/2/analytics/clear`, {
      headers: API_HEADERS,
      data: { index: clearIndex },
    });
    expect(clearRes.status()).toBe(200);
    const clearData = await clearRes.json();
    expect(clearData.status).toBe('ok');
    expect(clearData.partitionsRemoved).toBeGreaterThan(0);

    const secondClearRes = await request.delete(`${API_BASE}/2/analytics/clear`, {
      headers: API_HEADERS,
      data: { index: clearIndex },
    });
    expect(secondClearRes.status()).toBe(200);
    const secondClearData = await secondClearRes.json();
    expect(secondClearData.status).toBe('ok');
    expect(secondClearData.partitionsRemoved).toBe(0);
  });

  test('seed endpoint generates analytics data', async ({ request }) => {
    const INDEX_NAME = `seed-api-test-${Date.now()}`;
    const seedRes = await request.post(`${API_BASE}/2/analytics/seed`, {
      headers: API_HEADERS,
      data: { index: INDEX_NAME, days: 30 },
    });
    expect(seedRes.status()).toBe(200);
    const seedData = await seedRes.json();
    expect(seedData.totalSearches).toBeGreaterThan(0);
    expect(seedData.totalClicks).toBeGreaterThan(0);

    const countRes = await request.get(`${API_BASE}/2/searches/count`, {
      params: { index: INDEX_NAME, startDate: '2025-01-01', endDate: '2027-01-01' },
      headers: API_HEADERS,
    });
    expect(countRes.status()).toBe(200);
    const countData = await countRes.json();
    expect(countData.count).toBeGreaterThan(0);

    // Cleanup
    await request.delete(`${API_BASE}/1/indexes/${INDEX_NAME}`, { headers: API_HEADERS });
    await request.delete(`${API_BASE}/2/analytics/clear`, {
      headers: API_HEADERS,
      data: { index: INDEX_NAME },
    });
  });
});
