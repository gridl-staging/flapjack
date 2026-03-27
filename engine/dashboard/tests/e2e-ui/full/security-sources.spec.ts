import { test, expect } from '../../fixtures/auth.fixture';
import { appendSecuritySource, getSecuritySources } from '../../fixtures/api-helpers';
import {
  SECURITY_SOURCES_BASELINE,
  clearSecuritySources,
  resetSecuritySources,
} from '../tenant-admin-helpers';
import { formatSecuritySourceDescription } from '../../../src/pages/security-sources/shared';
import type { Page, APIRequestContext } from '@playwright/test';

const SECURITY_SOURCES_URL = '/security-sources';

async function appendSecuritySourceAndReload(
  page: Page,
  request: APIRequestContext,
  entry: { source: string; description: string },
) {
  await appendSecuritySource(request, entry);
  await expect
    .poll(
      async () =>
        (await getSecuritySources(request)).some(
          ({ source, description }) => source === entry.source && description === entry.description,
        ),
      { timeout: 10_000 },
    )
    .toBe(true);
  await page.reload();
  await expect(page.getByRole('heading', { name: 'Security Sources' })).toBeVisible({ timeout: 15_000 });
}

test.describe('Security Sources', () => {
  // Tests modify shared tenant-level allowlist state — must run serially.
  test.describe.configure({ mode: 'serial' });

  test.beforeEach(async ({ page, request }) => {
    await resetSecuritySources(request);
    await page.goto(SECURITY_SOURCES_URL);
    await expect(page.getByRole('heading', { name: 'Security Sources' })).toBeVisible({ timeout: 15_000 });
  });

  test.afterEach(async ({ request }) => {
    await clearSecuritySources(request);
  });

  test('loads seeded security source in the page list', async ({ page, request }) => {
    await appendSecuritySourceAndReload(page, request, {
      source: '192.168.1.0/24',
      description: 'office network',
    });

    const sourcesList = page.getByTestId('security-sources-list');
    await expect(sourcesList).toBeVisible({ timeout: 10_000 });
    await expect(sourcesList.getByText('192.168.1.0/24')).toBeVisible({ timeout: 10_000 });
    await expect(sourcesList.getByText('office network')).toBeVisible({ timeout: 10_000 });
  });

  test('create-delete lifecycle preserves badge and row counts', async ({ page }) => {
    // Baseline: 1 seeded loopback entry
    const baselineCount = SECURITY_SOURCES_BASELINE.length;
    const sourcesList = page.getByTestId('security-sources-list');
    await expect(sourcesList).toBeVisible({ timeout: 10_000 });

    const baselineRows = sourcesList.getByTestId('security-source-row');
    await expect(baselineRows).toHaveCount(baselineCount, { timeout: 10_000 });
    await expect(page.getByText(`${baselineCount} entries`)).toBeVisible({ timeout: 10_000 });

    // Add a source via dialog
    await page.getByTestId('add-security-source-btn').click();
    const dialog = page.getByRole('dialog');
    await expect(dialog).toBeVisible({ timeout: 10_000 });
    await dialog.getByLabel('Source').fill('10.10.10.0/24');
    await dialog.getByLabel('Description').fill('vpn subnet');
    await dialog.getByRole('button', { name: 'Add Source' }).click();

    // Verify count increased by exactly one
    await expect(sourcesList.getByTestId('security-source-row')).toHaveCount(baselineCount + 1, { timeout: 10_000 });
    await expect(page.getByText(`${baselineCount + 1} entries`)).toBeVisible({ timeout: 10_000 });
    await expect(sourcesList.getByText('10.10.10.0/24')).toBeVisible({ timeout: 10_000 });

    // Delete the newly added row
    const addedRow = sourcesList.getByTestId('security-source-row').filter({ hasText: '10.10.10.0/24' });
    await addedRow.getByRole('button', { name: /delete/i }).click();

    // Verify counts return to baseline
    await expect(sourcesList.getByTestId('security-source-row')).toHaveCount(baselineCount, { timeout: 10_000 });
    await expect(page.getByText(`${baselineCount} entries`)).toBeVisible({ timeout: 10_000 });
  });

  test('adds a security source through the add dialog', async ({ page }) => {
    await page.getByTestId('add-security-source-btn').click();

    const dialog = page.getByRole('dialog');
    await expect(dialog).toBeVisible({ timeout: 10_000 });

    await dialog.getByLabel('Source').fill('10.10.10.0/24');
    await dialog.getByLabel('Description').fill('vpn subnet');
    await dialog.getByRole('button', { name: 'Add Source' }).click();

    const sourcesList = page.getByTestId('security-sources-list');
    await expect(sourcesList.getByText('10.10.10.0/24')).toBeVisible({ timeout: 10_000 });
    await expect(sourcesList.getByText('vpn subnet')).toBeVisible({ timeout: 10_000 });
  });

  test('blank description renders fallback and malformed CIDR keeps dialog open', async ({ page }) => {
    const sourcesList = page.getByTestId('security-sources-list');
    await expect(sourcesList).toBeVisible({ timeout: 10_000 });
    const baselineRowCount = await sourcesList.getByTestId('security-source-row').count();

    // Submit a valid source with blank description
    await page.getByTestId('add-security-source-btn').click();
    const dialog = page.getByRole('dialog');
    await expect(dialog).toBeVisible({ timeout: 10_000 });
    await dialog.getByLabel('Source').fill('172.16.0.0/16');
    // Leave description empty — formatSecuritySourceDescription returns "No description"
    await dialog.getByRole('button', { name: 'Add Source' }).click();

    // Dialog should close, row should render fallback via shared formatter
    await expect(dialog).not.toBeVisible({ timeout: 10_000 });
    await expect(sourcesList.getByText('172.16.0.0/16')).toBeVisible({ timeout: 10_000 });
    await expect(sourcesList.getByText(formatSecuritySourceDescription(''))).toBeVisible({ timeout: 10_000 });
    await expect(sourcesList.getByTestId('security-source-row')).toHaveCount(baselineRowCount + 1, { timeout: 10_000 });

    // Now try a malformed CIDR — dialog should stay open, row count unchanged
    await page.getByTestId('add-security-source-btn').click();
    const malformedDialog = page.getByRole('dialog');
    await expect(malformedDialog).toBeVisible({ timeout: 10_000 });
    await malformedDialog.getByLabel('Source').fill('not-a-cidr');
    await malformedDialog.getByLabel('Description').fill('invalid');
    await malformedDialog.getByRole('button', { name: 'Add Source' }).click();

    // Dialog remains open with server validation error
    await expect(page.getByText(/Invalid source CIDR/i).first()).toBeVisible({ timeout: 10_000 });
    await expect(malformedDialog).toBeVisible();

    // Close dialog to verify row count was not affected by the rejected submission
    await malformedDialog.getByRole('button', { name: 'Cancel' }).click();
    await expect(sourcesList.getByTestId('security-source-row')).toHaveCount(baselineRowCount + 1, { timeout: 10_000 });
  });

  test('blocks blank source submission locally', async ({ page }) => {
    await page.getByTestId('add-security-source-btn').click();

    const dialog = page.getByRole('dialog');
    await expect(dialog).toBeVisible({ timeout: 10_000 });

    await dialog.getByLabel('Source').fill('   ');
    await dialog.getByRole('button', { name: 'Add Source' }).click();

    await expect(dialog.getByText('Source is required.')).toBeVisible({ timeout: 10_000 });
  });

  test('surfaces malformed source error returned by the server', async ({ page }) => {
    await page.getByTestId('add-security-source-btn').click();

    const dialog = page.getByRole('dialog');
    await expect(dialog).toBeVisible({ timeout: 10_000 });

    await dialog.getByLabel('Source').fill('not-a-cidr');
    await dialog.getByLabel('Description').fill('invalid');
    await dialog.getByRole('button', { name: 'Add Source' }).click();

    await expect(page.getByText(/Invalid source CIDR/i).first()).toBeVisible({ timeout: 10_000 });
  });

  test('deletes a seeded source through the UI', async ({ page, request }) => {
    await appendSecuritySourceAndReload(page, request, {
      source: '203.0.113.0/24',
      description: 'temporary partner',
    });

    const sourcesList = page.getByTestId('security-sources-list');
    const sourceRow = sourcesList.getByTestId('security-source-row').filter({ hasText: '203.0.113.0/24' });
    await expect(sourceRow).toBeVisible({ timeout: 10_000 });

    await sourceRow.getByRole('button', { name: /delete/i }).click();

    await expect(sourcesList.getByText('203.0.113.0/24')).not.toBeVisible({ timeout: 10_000 });
  });

  test('shows empty state when there are no security sources', async ({ page, request }) => {
    await clearSecuritySources(request);
    await page.reload();

    await expect(page.getByText('No security sources configured yet.')).toBeVisible({ timeout: 10_000 });
  });
});
