import { describe, expect, it, vi } from 'vitest';
import type { Experiment } from '@/lib/types';
import {
  normalizeExperimentListResponse,
  normalizeExperimentRecord,
  normalizeExperimentResultsResponse,
} from './experiment-normalization';

function expectExperimentShape(experiment: Experiment) {
  expect(experiment).toMatchObject({
    id: expect.any(String),
    name: expect.any(String),
    indexName: expect.any(String),
    status: expect.any(String),
    control: expect.any(Object),
    variant: expect.any(Object),
  });
}

describe('experiment-normalization', () => {
  it('normalizes list responses from the Algolia abtests envelope', () => {
    const experiments = normalizeExperimentListResponse({
      abtests: [
        {
          abTestID: 42,
          name: 'Ranking experiment',
          variants: [
            { index: 'products', description: 'control', trafficPercentage: 50 },
            { index: 'products_variant', description: 'variant', trafficPercentage: 50 },
          ],
          status: 'active',
          startAt: '2026-04-05T12:00:00.000Z',
          createdAt: '2026-04-01T12:00:00.000Z',
          metrics: [{ name: 'revenue' }],
        },
      ],
    });

    expect(experiments).toHaveLength(1);
    expectExperimentShape(experiments[0]);
    expect(experiments[0]).toMatchObject({
      id: '42',
      indexName: 'products',
      status: 'running',
      primaryMetric: 'revenuePerSearch',
      trafficSplit: 0.5,
      control: { name: 'control' },
      variant: { name: 'variant', indexName: 'products_variant' },
    });
  });

  it('prefers explicit traffic split and query override fields on raw experiment records', () => {
    const experiment = normalizeExperimentRecord({
      id: 'exp-1',
      name: 'Explicit fields',
      indexName: 'products',
      status: 'draft',
      trafficSplit: 0.25,
      primaryMetric: 'clickThroughRate',
      control: {
        name: 'baseline',
        queryOverrides: { filters: 'brand:Apple' },
      },
      variant: {
        name: 'challenger',
        queryOverrides: { filters: 'brand:Samsung' },
        indexName: 'products_variant',
      },
      createdAt: '2026-04-01T00:00:00.000Z',
      minimumDays: 21,
    });

    expectExperimentShape(experiment);
    expect(experiment).toMatchObject({
      id: 'exp-1',
      status: 'draft',
      trafficSplit: 0.25,
      primaryMetric: 'ctr',
      minimumDays: 21,
      control: {
        name: 'baseline',
        queryOverrides: { filters: 'brand:Apple' },
      },
      variant: {
        name: 'challenger',
        queryOverrides: { filters: 'brand:Samsung' },
        indexName: 'products_variant',
      },
    });
  });

  it('falls back to sane defaults for malformed payloads', () => {
    const nowSpy = vi.spyOn(Date, 'now').mockReturnValue(1_700_000_000_000);

    try {
      const experiment = normalizeExperimentRecord({
        variants: [{}, { trafficPercentage: 'bogus' }],
        primaryMetric: 'unexpected',
        status: 'active',
      });

      expectExperimentShape(experiment);
      expect(experiment).toMatchObject({
        id: '',
        name: '',
        indexName: '',
        status: 'draft',
        trafficSplit: 0.5,
        primaryMetric: 'ctr',
        minimumDays: 14,
        createdAt: 1_700_000_000_000,
      });
    } finally {
      nowSpy.mockRestore();
    }
  });

  it('normalizes experiment results when the backend sends deltaAb instead of deltaAB', () => {
    const results = normalizeExperimentResultsResponse({
      experimentID: 'exp-1',
      name: 'Experiment',
      status: 'running',
      indexName: 'products',
      startDate: '2026-04-01T00:00:00.000Z',
      endedAt: null,
      conclusion: null,
      trafficSplit: 0.5,
      primaryMetric: 'ctr',
      gate: {
        minimumNReached: false,
        minimumDaysReached: false,
        readyToRead: false,
        requiredSearchesPerArm: 1000,
        currentSearchesPerArm: 100,
        progressPct: 10,
        estimatedDaysRemaining: 7,
      },
      control: {
        name: 'control',
        searches: 10,
        users: 10,
        clicks: 5,
        conversions: 1,
        revenue: 1,
        ctr: 0.5,
        conversionRate: 0.1,
        revenuePerSearch: 0.1,
        zeroResultRate: 0,
        abandonmentRate: 0,
        meanClickRank: 1.2,
      },
      variant: {
        name: 'variant',
        searches: 10,
        users: 10,
        clicks: 4,
        conversions: 2,
        revenue: 2,
        ctr: 0.4,
        conversionRate: 0.2,
        revenuePerSearch: 0.2,
        zeroResultRate: 0,
        abandonmentRate: 0,
        meanClickRank: 1.1,
      },
      significance: null,
      bayesian: null,
      sampleRatioMismatch: false,
      cupedApplied: false,
      guardRailAlerts: [],
      outlierUsersExcluded: 0,
      noStableIdQueries: 0,
      recommendation: null,
      interleaving: {
        deltaAb: 0.42,
        winsControl: 1,
        winsVariant: 2,
        ties: 3,
        pValue: 0.1,
        significant: false,
        totalQueries: 6,
        dataQualityOk: true,
      },
    });

    expect(results.interleaving?.deltaAB).toBe(0.42);
  });
});
