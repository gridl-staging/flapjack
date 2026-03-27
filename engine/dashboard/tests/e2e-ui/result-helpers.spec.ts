import { test, expect } from '@playwright/test';
import { extractObjectIdFromText, responseMatchesIndexQuery } from './result-helpers';

test.describe('result-helpers extractObjectIdFromText', () => {
  test('reads labeled objectID values', () => {
    expect(extractObjectIdFromText('Object ID: merch-001')).toBe('merch-001');
    expect(extractObjectIdFromText('id # p123')).toBe('p123');
  });

  test('does not treat id-prefixed tokens as labeled IDs without a separator', () => {
    const cardText = '4 results for merchlifecycle1773903800212-idc1fy p177390380021301';
    expect(extractObjectIdFromText(cardText)).toBe('p177390380021301');
  });

  test('returns empty string when no labeled or pNNN objectID is present', () => {
    expect(extractObjectIdFromText('merchlifecycle1773903800212-idc1fy only')).toBe('');
  });

  test('falls back to prod-prefixed objectIDs from seeded cards', () => {
    expect(extractObjectIdFromText('LG UltraFine 5K prod-204')).toBe('prod-204');
  });
});

test.describe('result-helpers responseMatchesIndexQuery', () => {
  const mockResponse = (body: Record<string, unknown>) =>
    ({
      url: () => 'http://127.0.0.1:7700/indexes/e2e-products/query',
      status: () => 200,
      request: () => ({
        method: () => 'POST',
        postData: () => JSON.stringify(body),
      }),
    }) as Parameters<typeof responseMatchesIndexQuery>[0];

  test('accepts wildcard string facets when requireFacets is enabled', () => {
    const response = mockResponse({ query: 'laptop', facets: '*' });
    expect(
      responseMatchesIndexQuery(response, 'e2e-products', 'laptop', { requireFacets: true }),
    ).toBe(true);
  });
});
