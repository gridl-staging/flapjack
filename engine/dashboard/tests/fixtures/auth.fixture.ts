import { test as base } from '@playwright/test';
import { TEST_ADMIN_KEY } from './local-instance';

/**
 * Custom test fixture that pre-seeds localStorage with Flapjack auth credentials.
 * Import { test, expect } from this module instead of '@playwright/test' to get
 * an authenticated page automatically.
 */
export const test = base.extend<{ readClipboard: () => Promise<string> }>({
  page: async ({ page }, use) => {
    await page.addInitScript((apiKey: string) => {
      localStorage.setItem('flapjack-api-key', apiKey);
      localStorage.setItem('flapjack-app-id', 'flapjack');
      // Seed the Zustand persist store so useAuth().apiKey is populated on hydration
      localStorage.setItem('flapjack-auth', JSON.stringify({
        state: { apiKey, appId: 'flapjack' },
        version: 0,
      }));
    }, TEST_ADMIN_KEY);
    await use(page);
  },

  readClipboard: async ({ context }, use) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);
    await use(async () => {
      const page = context.pages()[0];
      return page.evaluate(() => navigator.clipboard.readText());
    });
  },
});

export { expect } from '@playwright/test';
