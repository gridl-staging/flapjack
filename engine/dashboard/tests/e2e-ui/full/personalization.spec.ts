import { test, expect } from '../../fixtures/auth.fixture';
import { TEST_INDEX } from '../helpers';
import {
  findFirstObjectIdByQuery,
  getPersonalizationStrategy,
  setPersonalizationStrategy,
  sendEvents,
  waitForPersonalizationProfile,
} from '../../fixtures/api-helpers';

const PERSONALIZATION_URL = '/personalization';

test.describe('Personalization', () => {
  test.describe.configure({ mode: 'serial' });

  test('shows setup state when strategy is not configured', async ({ page }) => {
    await page.goto(PERSONALIZATION_URL);

    await expect(page.getByRole('heading', { name: 'Personalization', level: 2 })).toBeVisible({ timeout: 15_000 });
    await expect(page.getByText('Personalization is not configured yet.')).toBeVisible();
    await expect(page.getByRole('button', { name: 'Use starter strategy' })).toBeVisible();
  });

  test('uses starter strategy defaults, persists event and facet edits, and unlocks profile lookup after save', async ({
    page,
    request,
  }) => {
    const strategyBeforeSave = await getPersonalizationStrategy(request);
    expect(strategyBeforeSave).toBeNull();

    await page.goto(PERSONALIZATION_URL);
    await page.getByRole('button', { name: 'Use starter strategy' }).click();

    const impactInput = page.getByTestId('personalization-impact-input');
    await expect(impactInput).toHaveValue('60');

    const starterEventRow = page.getByTestId('event-row-0');
    await expect(starterEventRow.getByLabel('Event name')).toHaveValue('Product Viewed');
    await expect(starterEventRow.getByLabel('Event type')).toHaveValue('view');
    await expect(starterEventRow.getByLabel('Event score')).toHaveValue('20');

    const facetsList = page.getByTestId('facets-scoring-list');
    await expect(facetsList).toBeVisible();
    const starterFacetRow = page.getByTestId('facet-row-0');
    await expect(starterFacetRow.getByLabel('Facet name')).toHaveValue('brand');
    await expect(starterFacetRow.getByLabel('Facet score')).toHaveValue('70');

    await expect(page.getByTestId('profile-lookup-input')).toHaveCount(0);

    await starterEventRow.getByLabel('Event name').fill('Product Purchased');
    await starterEventRow.getByLabel('Event type').selectOption('conversion');
    await starterEventRow.getByLabel('Event score').fill('55');

    await page.getByRole('button', { name: 'Add event' }).click();
    const secondEventRow = page.getByTestId('event-row-1');
    await secondEventRow.getByLabel('Event name').fill('Product Added To Cart');
    await secondEventRow.getByLabel('Event type').selectOption('click');
    await secondEventRow.getByLabel('Event score').fill('32');

    await starterFacetRow.getByLabel('Facet score').fill('75');
    await page.getByTestId('add-facet-btn').click();
    const secondFacetRow = page.getByTestId('facet-row-1');
    await secondFacetRow.getByLabel('Facet name').fill('category');
    await secondFacetRow.getByLabel('Facet score').fill('35');

    await page.getByTestId('save-strategy-btn').click();
    await expect(page.getByText('Save the strategy to enable profile lookup.')).toHaveCount(0);

    await page.reload();

    await expect(page.getByTestId('personalization-impact-input')).toHaveValue('60');
    await expect(page.getByTestId('event-row-0').getByLabel('Event name')).toHaveValue('Product Purchased');
    await expect(page.getByTestId('event-row-0').getByLabel('Event type')).toHaveValue('conversion');
    await expect(page.getByTestId('event-row-0').getByLabel('Event score')).toHaveValue('55');
    await expect(page.getByTestId('event-row-1').getByLabel('Event name')).toHaveValue('Product Added To Cart');
    await expect(page.getByTestId('event-row-1').getByLabel('Event type')).toHaveValue('click');
    await expect(page.getByTestId('event-row-1').getByLabel('Event score')).toHaveValue('32');
    await expect(page.getByTestId('facet-row-0').getByLabel('Facet name')).toHaveValue('brand');
    await expect(page.getByTestId('facet-row-0').getByLabel('Facet score')).toHaveValue('75');
    await expect(page.getByTestId('facet-row-1').getByLabel('Facet name')).toHaveValue('category');
    await expect(page.getByTestId('facet-row-1').getByLabel('Facet score')).toHaveValue('35');
    await expect(page.getByTestId('profile-lookup-input')).toBeVisible();
  });

  test('profile lookup shows known user profile and unknown-user empty state', async ({ page, request }) => {
    test.setTimeout(90_000);

    const knownUserToken = `e2e-personalization-known-${Date.now()}`;
    const unknownUserToken = `e2e-personalization-unknown-${Date.now()}`;

    await setPersonalizationStrategy(request, {
      eventsScoring: [
        { eventName: 'Product Viewed', eventType: 'view', score: 100 },
      ],
      facetsScoring: [
        { facetName: 'brand', score: 100 },
        { facetName: 'category', score: 50 },
      ],
      personalizationImpact: 80,
    });

    const knownUserObjectIDs = await Promise.all([
      findFirstObjectIdByQuery(request, TEST_INDEX, 'MacBook'),
      findFirstObjectIdByQuery(request, TEST_INDEX, 'iPad'),
    ]);

    await sendEvents(request, [
      {
        eventType: 'view',
        eventName: 'Product Viewed',
        index: TEST_INDEX,
        userToken: knownUserToken,
        objectIDs: knownUserObjectIDs,
      },
    ]);

    await waitForPersonalizationProfile(request, knownUserToken);

    await page.goto(PERSONALIZATION_URL);

    await page.getByTestId('profile-lookup-input').fill(knownUserToken);
    await page.getByTestId('profile-lookup-btn').click();

    const profileResults = page.getByTestId('profile-results');
    await expect(profileResults).toBeVisible({ timeout: 10_000 });
    await expect(profileResults.getByText('brand')).toBeVisible();
    await expect(profileResults.getByText('Apple')).toBeVisible();

    await page.getByTestId('profile-lookup-input').fill(unknownUserToken);
    await page.getByTestId('profile-lookup-btn').click();

    await expect(page.getByText('No profile found')).toBeVisible({ timeout: 10_000 });
  });
});
