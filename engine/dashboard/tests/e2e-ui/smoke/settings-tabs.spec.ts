import { test, expect } from '../../fixtures/auth.fixture';
import { SETTINGS_TAB_ASSERTIONS, TEST_INDEX } from '../helpers';

test.describe('Settings tabs smoke', () => {
  test('loads tabbed settings and navigates all six tabs', async ({ page }) => {
    await page.goto(`/index/${TEST_INDEX}/settings`);
    await expect(page.getByRole('heading', { name: /settings/i })).toBeVisible({ timeout: 10_000 });

    for (const { tabLabel } of SETTINGS_TAB_ASSERTIONS) {
      await expect(page.getByRole('tab', { name: tabLabel })).toBeVisible();
    }

    for (const { tabLabel, panelAssertion } of SETTINGS_TAB_ASSERTIONS) {
      const tab = page.getByRole('tab', { name: tabLabel });
      await tab.click();
      await expect(tab).toHaveAttribute('aria-selected', 'true');
      await expect(panelAssertion(page.getByRole('tabpanel', { name: tabLabel }))).toBeVisible({ timeout: 10_000 });
    }
  });
});
