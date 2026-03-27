/**
 * E2E-UI Full Suite -- Analytics Conversions Mocked Error States (Browser-Mocked)
 *
 * Tests the Conversions tab on the Analytics page with mocked API responses to
 * verify graceful degradation when conversion endpoints fail or return empty data.
 *
 * Covers:
 * - Conversions tab renders KPI cards with empty/missing data
 * - 4xx from conversion endpoint shows graceful empty state
 * - Timeout handling for conversion endpoints
 *
 * STANDARDS COMPLIANCE:
 * - page.route() is allowed for browser-mocked tests
 * - Zero page.evaluate()
 * - Zero CSS class selectors
 */
import { test, expect } from '../../fixtures/auth.fixture';

const INDEX = 'e2e-products';
const ANALYTICS_URL = `/index/${INDEX}/analytics`;

test.describe('Analytics Conversions — Mocked Error States', () => {
  test('Conversions tab handles missing conversion data gracefully', async ({ page }) => {
    // Mock conversion endpoints to return empty data
    await page.route('**/2/conversions/addToCartRate*', (route) =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ rate: 0, count: 0, dates: [] }),
      }),
    );
    await page.route('**/2/conversions/purchaseRate*', (route) =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ rate: 0, count: 0, dates: [] }),
      }),
    );
    await page.route('**/2/conversions/revenue*', (route) =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ currencies: {} }),
      }),
    );
    await page.route('**/2/conversions/conversionRate*', (route) =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ rate: 0, count: 0, dates: [] }),
      }),
    );

    await page.goto(ANALYTICS_URL);
    await expect(page.getByTestId('analytics-heading')).toBeVisible({ timeout: 15_000 });

    // Switch to Conversions tab
    await page.getByTestId('tab-conversions').click();

    // KPI cards should still render (with zero/empty values)
    const kpiCards = page.getByTestId('conversion-kpi-cards');
    await expect(kpiCards).toBeVisible({ timeout: 10_000 });
    await expect(page.getByTestId('kpi-conversion-rate')).toBeVisible();
    await expect(page.getByTestId('kpi-add-to-cart-rate')).toBeVisible();
    await expect(page.getByTestId('kpi-purchase-rate')).toBeVisible();
    await expect(page.getByTestId('kpi-revenue')).toBeVisible();
  });

  test('Conversions tab handles 404 from conversion endpoints', async ({ page }) => {
    // Mock conversion endpoints to return 404
    await page.route('**/2/conversions/**', (route) =>
      route.fulfill({
        status: 404,
        contentType: 'application/json',
        body: JSON.stringify({ message: 'Not found' }),
      }),
    );

    await page.goto(ANALYTICS_URL);
    await expect(page.getByTestId('analytics-heading')).toBeVisible({ timeout: 15_000 });

    // Switch to Conversions tab
    await page.getByTestId('tab-conversions').click();

    // KPI cards should still render — they handle errors gracefully
    const kpiCards = page.getByTestId('conversion-kpi-cards');
    await expect(kpiCards).toBeVisible({ timeout: 10_000 });

    // The page should not crash — heading should still be visible
    await expect(page.getByTestId('analytics-heading')).toBeVisible();
  });

  test('Conversions tab handles slow responses without crashing', async ({ page }) => {
    // Mock conversion endpoints with delayed response
    await page.route('**/2/conversions/**', async (route) => {
      // 3-second delay simulating slow response
      await new Promise((r) => setTimeout(r, 3000));
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ rate: 0.05, count: 10, dates: [] }),
      });
    });

    await page.goto(ANALYTICS_URL);
    await expect(page.getByTestId('analytics-heading')).toBeVisible({ timeout: 15_000 });

    // Switch to Conversions tab
    await page.getByTestId('tab-conversions').click();

    // KPI cards should eventually render after the delay
    await expect(page.getByTestId('conversion-kpi-cards')).toBeVisible({ timeout: 15_000 });
    await expect(page.getByTestId('kpi-conversion-rate')).toBeVisible({ timeout: 15_000 });
  });
});
