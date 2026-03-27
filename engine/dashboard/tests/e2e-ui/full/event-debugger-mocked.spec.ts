/**
 * E2E-UI Full Suite -- Event Debugger Mocked Error States (Browser-Mocked)
 *
 * Tests the Event Debugger page with mocked API responses to verify
 * error states and edge cases that are hard to reproduce with a real server.
 * Uses Playwright's page.route() to intercept API calls.
 *
 * Covers:
 * - 500 from debug endpoint → error state
 * - Empty event list → empty state message
 * - Malformed payload handling
 *
 * STANDARDS COMPLIANCE:
 * - page.route() is allowed for browser-mocked tests (intercepts network, not DOM)
 * - Zero page.evaluate() — all assertions via Playwright locators
 * - Zero CSS class selectors — uses data-testid, getByRole, getByText
 */
import { test, expect } from '../../fixtures/auth.fixture';

test.describe('Event Debugger — Mocked Error States', () => {
  test('shows error state when debug endpoint returns 500', async ({ page }) => {
    // Mock the debug endpoint to return 500
    await page.route('**/1/events/debug*', (route) =>
      route.fulfill({
        status: 500,
        contentType: 'application/json',
        body: JSON.stringify({ message: 'Internal server error' }),
      }),
    );

    await page.goto('/events');
    await expect(page.getByRole('heading', { name: 'Event Debugger' })).toBeVisible({ timeout: 15_000 });

    // Error state message should appear
    await expect(
      page.getByText(/unable to load events/i),
    ).toBeVisible({ timeout: 10_000 });
  });

  test('shows empty state when no events exist', async ({ page }) => {
    // Mock the debug endpoint to return empty events
    await page.route('**/1/events/debug*', (route) =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ events: [], count: 0 }),
      }),
    );

    await page.goto('/events');
    await expect(page.getByRole('heading', { name: 'Event Debugger' })).toBeVisible({ timeout: 15_000 });

    // Empty state message should appear
    await expect(
      page.getByText(/no events received yet/i),
    ).toBeVisible({ timeout: 10_000 });
  });

  test('handles events with validation errors', async ({ page }) => {
    // Mock the debug endpoint to return events with validation errors
    await page.route('**/1/events/debug*', (route) =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          events: [
            {
              timestampMs: Date.now(),
              index: 'test-index',
              eventType: 'click',
              eventSubtype: null,
              eventName: 'Bad Click Event',
              userToken: 'user-123',
              objectIds: ['obj-1'],
              httpCode: 422,
              validationErrors: ['positions required for click events'],
            },
          ],
          count: 1,
        }),
      }),
    );

    await page.goto('/events');
    await expect(page.getByRole('heading', { name: 'Event Debugger' })).toBeVisible({ timeout: 15_000 });

    const table = page.getByTestId('event-table');
    await expect(table).toBeVisible({ timeout: 10_000 });

    // The event with errors should show an Error badge
    await expect(table.getByText('Error').first()).toBeVisible();
    await expect(table.getByText('Bad Click Event').first()).toBeVisible();

    // Click the row to see validation errors in the detail panel
    await table.getByTestId('event-row').first().click();
    const detail = page.getByTestId('event-detail');
    await expect(detail).toBeVisible({ timeout: 5_000 });
    await expect(detail.getByText('Validation Errors')).toBeVisible();
    await expect(detail.getByText('positions required for click events', { exact: true })).toBeVisible();
  });
});
