/**
 * Smoke — Sidebar Sections (Structural Chrome Only)
 *
 * Verifies that sidebar section headings, link containers, and the seeded
 * index are rendered. Does NOT click through routes — navigation.spec.ts
 * is the canonical owner of route-click assertions.
 */
import { test, expect, TEST_INDEX, getSidebar, gotoOverviewPage } from '../helpers';

test.describe('Sidebar grouped sections smoke', () => {
  test('renders all four section headings', async ({ page }) => {
    await gotoOverviewPage(page);

    const sidebar = getSidebar(page);

    // All four section headings present via data-testid
    await expect(sidebar.getByTestId('sidebar-section-heading-indexes')).toBeVisible();
    await expect(sidebar.getByTestId('sidebar-section-heading-intelligence')).toBeVisible();
    await expect(sidebar.getByTestId('sidebar-section-heading-developer')).toBeVisible();
    await expect(sidebar.getByTestId('sidebar-section-heading-system')).toBeVisible();
  });

  test('renders link containers for each section', async ({ page }) => {
    await gotoOverviewPage(page);

    const sidebar = getSidebar(page);

    await expect(sidebar.getByTestId('sidebar-section-links-intelligence')).toBeVisible();
    await expect(sidebar.getByTestId('sidebar-section-links-developer')).toBeVisible();
    await expect(sidebar.getByTestId('sidebar-section-links-system')).toBeVisible();
  });

  test('seeded index appears in the indexes section', async ({ page }) => {
    await gotoOverviewPage(page);

    const sidebar = getSidebar(page);
    await expect(sidebar.getByTestId('sidebar-indexes')).toBeVisible();
    await expect(sidebar.getByTestId(`sidebar-index-${TEST_INDEX}`)).toBeVisible({ timeout: 10_000 });
  });

  test('spot-checks one link per section exists without clicking', async ({ page }) => {
    await gotoOverviewPage(page);

    const sidebar = getSidebar(page);

    // Intelligence: Query Suggestions link exists
    await expect(sidebar.getByRole('link', { name: 'Query Suggestions' })).toBeVisible();
    // Developer: API Keys link exists
    await expect(sidebar.getByRole('link', { name: 'API Keys' })).toBeVisible();
    // System: System link exists
    await expect(sidebar.getByRole('link', { name: 'System', exact: true })).toBeVisible();
  });
});
