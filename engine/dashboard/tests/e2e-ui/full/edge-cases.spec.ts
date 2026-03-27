/**
 * E2E-UI Full Suite — Search & Browse Edge Cases (Real Server)
 *
 * Adversarial coverage for the Search & Browse form-submit contract.
 * Tests hostile/noisy queries against the real Flapjack server, verifying that
 * SearchBox's <form onSubmit> is the only trigger for search, and that
 * ResultsPanel lands in its defensive states rather than crashing.
 *
 * Canonical owner for adversarial scenarios — normal-flow tests live in search.spec.ts.
 */
import type { APIRequestContext, Page, Request } from '@playwright/test';
import { test, expect } from '../../fixtures/auth.fixture';
import { TEST_INDEX, gotoIndexPage } from '../helpers';
import { searchIndex } from '../../fixtures/api-helpers';
import {
  readVisibleObjectId,
  responseMatchesIndexQuery,
} from '../result-helpers';

/**
 * Submit a search query through the SearchBox form via Enter key or
 * the visible Search button. Does NOT wait for a network response —
 * callers decide what page state to assert afterwards.
 */
async function submitSearch(
  page: Page,
  query: string,
  via: 'enter' | 'button',
): Promise<void> {
  const searchInput = page.getByPlaceholder(/search documents/i);
  await searchInput.fill(query);

  if (via === 'enter') {
    await searchInput.press('Enter');
  } else {
    await page.getByRole('button', { name: 'Search' }).click();
  }
}

/**
 * Submit a search query and wait for the matching 200/202 server response.
 * Use for known-good queries where we expect results to render.
 */
async function submitSearchAndWaitForResponse(
  page: Page,
  query: string,
  via: 'enter' | 'button',
  indexName = TEST_INDEX,
): Promise<void> {
  const responsePromise = page.waitForResponse(
    (response) => responseMatchesIndexQuery(response, indexName, query),
    { timeout: 15_000 },
  );
  await submitSearch(page, query, via);
  await responsePromise;
}

function requestMatchesIndexQuery(
  request: Request,
  indexName: string,
  query: string,
): boolean {
  if (request.method() !== 'POST') {
    return false;
  }

  const searchPath = `/indexes/${indexName}/search`;
  const queryPath = `/indexes/${indexName}/query`;
  if (!request.url().includes(searchPath) && !request.url().includes(queryPath)) {
    return false;
  }

  const requestBody = request.postData();
  if (!requestBody) {
    return false;
  }

  try {
    const parsedBody = JSON.parse(requestBody) as Record<string, unknown>;
    const requestQuery = typeof parsedBody.q === 'string'
      ? parsedBody.q
      : typeof parsedBody.query === 'string'
      ? parsedBody.query
      : undefined;
    return requestQuery === query;
  } catch {
    return false;
  }
}

async function expectRecoveredTopResultToMatchBackend(
  page: Page,
  request: APIRequestContext,
  query: string,
): Promise<void> {
  const backendResponse = await searchIndex(request, TEST_INDEX, query);
  const expectedTopObjectId = (backendResponse.hits as Array<{ objectID?: string }>)
    .find((hit) => typeof hit.objectID === 'string')
    ?.objectID ?? '';
  expect(expectedTopObjectId, `Expected backend query "${query}" to return at least one hit`).not.toBe('');

  await submitSearchAndWaitForResponse(page, query, 'enter');

  const firstCard = page.getByTestId('results-panel').getByTestId('document-card').first();
  await expect(firstCard).toBeVisible({ timeout: 10_000 });

  const topResultObjectId = await readVisibleObjectId(firstCard);
  expect(
    topResultObjectId,
    `Recovered top result should match backend top hit for "${query}"`,
  ).toBe(expectedTopObjectId);
}

test.describe('Search & Browse Edge Cases', () => {

  test.beforeEach(async ({ page }) => {
    const initialQueryResponse = page.waitForResponse(
      (response) => responseMatchesIndexQuery(response, TEST_INDEX),
      { timeout: 15_000 },
    );
    await gotoIndexPage(page, TEST_INDEX);
    await initialQueryResponse;
  });

  // ---------------------------------------------------------------------------
  // Baseline: seeded Search & Browse results render before any hostile input
  // ---------------------------------------------------------------------------
  test('seeded results render on initial load', async ({ page }) => {
    const resultsPanel = page.getByTestId('results-panel');
    await expect(resultsPanel).toBeVisible({ timeout: 10_000 });

    const firstCard = resultsPanel.getByTestId('document-card').first();
    await expect(firstCard).toBeVisible({ timeout: 10_000 });

    // Verify actual result count content — not just element existence
    const resultsCount = resultsPanel.getByTestId('results-count');
    await expect(resultsCount).toBeVisible({ timeout: 10_000 });
    const countText = await resultsCount.textContent();
    expect(Number(countText?.replace(/,/g, '') ?? '0')).toBeGreaterThan(0);
  });

  // ---------------------------------------------------------------------------
  // Adversarial case 1: hostile query via Search button → recovery via Enter
  // ---------------------------------------------------------------------------
  test('hostile query via Search button lands in defensive state, then recovers via Enter', async ({
    page,
    request,
  }) => {
    // Submit a quote-heavy HTML-like hostile query via the visible Search button
    await submitSearch(page, '"""<script>alert("xss")</script>"""', 'button');
    await expect(page.getByText(/no results found/i)).toBeVisible({ timeout: 10_000 });

    // Recover with a known-good query via Enter
    const recoveryQuery = 'thinkpad';
    await expectRecoveredTopResultToMatchBackend(page, request, recoveryQuery);
  });

  // ---------------------------------------------------------------------------
  // Adversarial case 2: hostile query via Enter, prove typing alone is inert
  // ---------------------------------------------------------------------------
  test('hostile query via Enter shows defensive state; typing alone does not trigger search', async ({
    page,
    request,
  }) => {
    // Submit a SQL-injection-style hostile query via Enter
    await submitSearch(page, "'; DROP TABLE indexes; -- @@injection@@", 'enter');
    await expect(page.getByText(/no results found/i)).toBeVisible({ timeout: 10_000 });

    const recoveryQuery = 'thinkpad';
    // Type a known-good query WITHOUT submitting the form.
    // Monitor for any matching request — if typing alone triggers search,
    // waitForRequest resolves instead of timing out.
    const searchInput = page.getByPlaceholder(/search documents/i);
    const typingTriggeredSearch = page.waitForRequest(
      (request) => requestMatchesIndexQuery(request, TEST_INDEX, recoveryQuery),
      { timeout: 1_500 },
    ).then(
      () => true, // request matched — typing triggered a search (unexpected)
      () => false, // timeout — no search fired (expected)
    );

    await searchInput.fill(recoveryQuery);
    const searchFired = await typingTriggeredSearch;
    expect(searchFired, 'Typing alone must not trigger a search request').toBe(false);

    // "No results found" should still be visible after typing without submitting
    await expect(page.getByText(/no results found/i)).toBeVisible();

    // Now submit the typed query via Enter and verify recovery
    await expectRecoveredTopResultToMatchBackend(page, request, recoveryQuery);
  });

});
