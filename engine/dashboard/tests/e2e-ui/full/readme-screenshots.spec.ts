import { fileURLToPath } from 'node:url';
import type { Page } from '@playwright/test';
import { PRODUCTS, TEST_INDEX } from '../../fixtures/test-data';
import { test, expect } from '../../fixtures/auth.fixture';
import {
  gotoIndexPage,
  gotoOverviewPage,
  waitForOverviewIndexRow,
  waitForSearchResultsOrEmptyState,
} from '../helpers';

const SCREENSHOT_TIMEOUT_MS = 10_000;
const SEEDED_PRODUCT_NAME = PRODUCTS[0]?.name ?? 'MacBook Pro 16"';

function readmeScreenshotPath(filename: string): string {
  return fileURLToPath(new URL(`../../../img/${filename}`, import.meta.url));
}

async function expectNoOpenDialogs(page: Page): Promise<void> {
  await expect(page.getByRole('dialog')).toHaveCount(0);
}

async function saveReadmeScreenshot(page: Page, filename: string): Promise<void> {
  await expectNoOpenDialogs(page);
  await page.screenshot({
    path: readmeScreenshotPath(filename),
  });
}

test.describe('README screenshots', () => {
  test.skip(!process.env.UPDATE_README_SCREENSHOTS, 'Set UPDATE_README_SCREENSHOTS=1 to refresh tracked README PNGs.');
  test('Overview screenshot refresh uses seeded index readiness contract', async ({ page }) => {
    await gotoOverviewPage(page);
    await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible({
      timeout: SCREENSHOT_TIMEOUT_MS,
    });
    await expect(page.getByTestId('stat-card-indexes')).toBeVisible({
      timeout: SCREENSHOT_TIMEOUT_MS,
    });

    const seededRow = await waitForOverviewIndexRow(page, TEST_INDEX);
    await expect(seededRow).toBeVisible({ timeout: SCREENSHOT_TIMEOUT_MS });
    await saveReadmeScreenshot(page, 'dash_overview.png');
  });

  test('Search screenshot refresh uses seeded results readiness contract', async ({ page }) => {
    await gotoIndexPage(page, TEST_INDEX);
    await waitForSearchResultsOrEmptyState(page);

    await expect(page.getByPlaceholder('Search documents...')).toBeVisible({
      timeout: SCREENSHOT_TIMEOUT_MS,
    });
    await expect(page.getByRole('button', { name: 'Search', exact: true })).toBeVisible({
      timeout: SCREENSHOT_TIMEOUT_MS,
    });

    const resultsPanel = page.getByTestId('results-panel');
    await expect(resultsPanel).toBeVisible({ timeout: SCREENSHOT_TIMEOUT_MS });
    await expect(
      resultsPanel.getByTestId('document-card').filter({ hasText: SEEDED_PRODUCT_NAME }).first(),
    ).toBeVisible({ timeout: 15_000 });
    await saveReadmeScreenshot(page, 'dash_search.png');
  });

  test('Migrate screenshot refresh uses route-level readiness contract', async ({ page }) => {
    await page.goto('/migrate');
    await expect(page).toHaveURL(/\/migrate$/);
    await expect(page.getByRole('heading', { name: /migrate/i }).first()).toBeVisible({
      timeout: SCREENSHOT_TIMEOUT_MS,
    });
    await expect(page.getByRole('main')).toBeVisible({ timeout: SCREENSHOT_TIMEOUT_MS });
    await saveReadmeScreenshot(page, 'dash_migrate_alg.png');
  });
});
