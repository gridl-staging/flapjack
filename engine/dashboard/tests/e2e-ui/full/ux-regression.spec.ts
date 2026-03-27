import { test, expect } from '../../fixtures/auth.fixture';
import type { Page, TestInfo } from '@playwright/test';
import {
  createApiKey,
  createIndex,
  deleteApiKeysByDescriptionPrefix,
  deleteIndex,
} from '../../fixtures/api-helpers';
import { MAX_VISIBLE_INDEXES } from '../../../src/components/layout/sidebar-index-constants';
import { TEST_INDEX } from '../helpers';

const STAGE2_API_KEY_DESCRIPTION_PREFIX = 'E2E UX Stage2';
const STAGE4_INDEX_PREFIX = 'e2e-ux-stage4';
const STAGE4_DISPOSABLE_INDEX_COUNT = 6;
const UI_TIMEOUT_MS = 10_000;
const SYSTEM_UI_TIMEOUT_MS = 15_000;

function apiKeyDescriptionPrefix(testInfo: TestInfo): string {
  return `${STAGE2_API_KEY_DESCRIPTION_PREFIX} ${testInfo.testId}`;
}

function prefixedDescription(testInfo: TestInfo, name: string): string {
  return `${apiKeyDescriptionPrefix(testInfo)} ${name}`;
}

function keyCard(page: Page, description: string) {
  return page.getByTestId('key-card').filter({ hasText: description });
}

test.describe('UX regressions', () => {
  test.afterEach(async ({ request }, testInfo) => {
    await deleteApiKeysByDescriptionPrefix(request, apiKeyDescriptionPrefix(testInfo));
  });

  test('Stage 2 - API Keys page supports orientation, filter scanability, and readable card grouping', async ({ page, request }, testInfo) => {
    const primaryIndex = `e2e-ux-stage2-primary-${Date.now()}`;
    const secondaryIndex = `e2e-ux-stage2-secondary-${Date.now()}`;
    const primaryDescription = prefixedDescription(testInfo, 'Scoped Primary Key');
    const globalDescription = prefixedDescription(testInfo, 'Global Key');
    const secondaryDescription = prefixedDescription(testInfo, 'Scoped Secondary Key');

    await createIndex(request, primaryIndex);
    await createIndex(request, secondaryIndex);

    try {
      await createApiKey(request, {
        description: primaryDescription,
        acl: ['search'],
        indexes: [primaryIndex],
      });
      await createApiKey(request, {
        description: globalDescription,
        acl: ['search'],
      });
      await createApiKey(request, {
        description: secondaryDescription,
        acl: ['search'],
        indexes: [secondaryIndex],
      });

      await page.goto('/keys');
      await expect(page.getByRole('heading', { name: 'API Keys', exact: true })).toBeVisible({ timeout: UI_TIMEOUT_MS });

      await expect(
        page.getByText('Review each key\'s scope, permissions, and lifecycle before sharing it.'),
      ).toBeVisible({ timeout: UI_TIMEOUT_MS });

      const filterBar = page.getByTestId('index-filter-bar');
      await expect(filterBar).toBeVisible({ timeout: UI_TIMEOUT_MS });
      await expect(filterBar.getByText('Filter by Index Access', { exact: true })).toBeVisible({ timeout: UI_TIMEOUT_MS });
      await expect(filterBar.getByText('Viewing keys across all indexes', { exact: true })).toBeVisible({ timeout: UI_TIMEOUT_MS });

      await page.getByTestId(`filter-index-${primaryIndex}`).click();
      await expect(filterBar.getByText(`Viewing keys that can access ${primaryIndex}`, { exact: true })).toBeVisible({ timeout: UI_TIMEOUT_MS });
      await expect(keyCard(page, primaryDescription)).toBeVisible({ timeout: UI_TIMEOUT_MS });
      await expect(keyCard(page, globalDescription)).toBeVisible({ timeout: UI_TIMEOUT_MS });
      await expect(keyCard(page, secondaryDescription)).toHaveCount(0, { timeout: UI_TIMEOUT_MS });

      const primaryCard = keyCard(page, primaryDescription);
      await expect(primaryCard.getByText('Key Value', { exact: true })).toBeVisible({ timeout: UI_TIMEOUT_MS });
      await expect(primaryCard.getByText('Index Scope', { exact: true })).toBeVisible({ timeout: UI_TIMEOUT_MS });
      await expect(primaryCard.getByText('Permissions', { exact: true })).toBeVisible({ timeout: UI_TIMEOUT_MS });
      await expect(primaryCard.getByText('Lifecycle & Limits', { exact: true })).toBeVisible({ timeout: UI_TIMEOUT_MS });
    } finally {
      await deleteIndex(request, primaryIndex);
      await deleteIndex(request, secondaryIndex);
    }
  });

  test('Stage 3 - System page explains healthy vs processing index states', async ({ page }) => {
    await page.goto('/system');
    await expect(page.getByRole('heading', { name: /system/i })).toBeVisible({ timeout: UI_TIMEOUT_MS });

    const healthSummary = page.getByTestId('index-health-summary');
    await expect(healthSummary).toBeVisible({ timeout: SYSTEM_UI_TIMEOUT_MS });
    await expect(healthSummary.getByText('Healthy indexes have no pending tasks.', { exact: true })).toBeVisible({ timeout: SYSTEM_UI_TIMEOUT_MS });
    await expect(
      healthSummary.getByText('Processing indexes still have pending tasks in progress.', { exact: true }),
    ).toBeVisible({ timeout: SYSTEM_UI_TIMEOUT_MS });

    await expect(page.getByTestId(`index-dot-${TEST_INDEX}`)).toBeVisible({ timeout: SYSTEM_UI_TIMEOUT_MS });

    await page.getByRole('tab', { name: /indexes/i }).click();
    const statusCell = page.getByTestId(`index-status-${TEST_INDEX}`);
    await expect(statusCell).toBeVisible({ timeout: SYSTEM_UI_TIMEOUT_MS });
    await expect(statusCell.getByText('Healthy (no pending tasks)', { exact: true })).toBeVisible({ timeout: SYSTEM_UI_TIMEOUT_MS });
  });

  test('Stage 4 - Sidebar communicates hidden indexes before expansion', async ({ page, request }) => {
    const runId = Date.now();
    const stage4IndexIds = Array.from(
      { length: STAGE4_DISPOSABLE_INDEX_COUNT },
      (_, indexOffset) => `${STAGE4_INDEX_PREFIX}-${runId}-${indexOffset + 1}`,
    );

    await Promise.all(stage4IndexIds.map((indexId) => createIndex(request, indexId)));

    try {
      await page.goto('/overview');
      await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible({ timeout: UI_TIMEOUT_MS });

      const sidebarIndexes = page.getByTestId('sidebar-indexes');
      await expect(sidebarIndexes).toBeVisible({ timeout: UI_TIMEOUT_MS });

      const collapsedLinks = sidebarIndexes.getByTestId(/^sidebar-index-/);
      await expect(collapsedLinks).toHaveCount(MAX_VISIBLE_INDEXES, { timeout: UI_TIMEOUT_MS });

      const visibleDisposableIndexCount = (
        await Promise.all(
          stage4IndexIds.map(async (indexId) => {
            const indexLink = sidebarIndexes.getByTestId(`sidebar-index-${indexId}`);
            return (await indexLink.count()) > 0;
          }),
        )
      ).filter(Boolean).length;

      expect(visibleDisposableIndexCount).toBeLessThanOrEqual(MAX_VISIBLE_INDEXES);
      expect(visibleDisposableIndexCount).toBeLessThan(STAGE4_DISPOSABLE_INDEX_COUNT);

      const showAllIndexesButton = sidebarIndexes.getByTestId('sidebar-show-all-indexes');
      await expect(showAllIndexesButton).toBeVisible({ timeout: UI_TIMEOUT_MS });
      const collapsedButtonText = await showAllIndexesButton.innerText();

      await showAllIndexesButton.click();
      const expandedLinksCount = await collapsedLinks.count();
      const hiddenIndexesCount = expandedLinksCount - MAX_VISIBLE_INDEXES;
      expect(hiddenIndexesCount).toBeGreaterThan(0);
      expect(collapsedButtonText).toContain(String(hiddenIndexesCount));

      for (const indexId of stage4IndexIds) {
        await expect(sidebarIndexes.getByTestId(`sidebar-index-${indexId}`)).toBeVisible({ timeout: UI_TIMEOUT_MS });
      }
    } finally {
      await Promise.all(stage4IndexIds.map((indexId) => deleteIndex(request, indexId)));
    }
  });
});
