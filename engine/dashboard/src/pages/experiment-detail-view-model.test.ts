import { describe, expect, it } from 'vitest';
import type { ExperimentResultsResponse } from '@/hooks/useExperiments';
import { buildExperimentDetailViewModel } from './experiment-detail-view-model';

function createBaseResults(
  overrides: Partial<ExperimentResultsResponse> = {},
): ExperimentResultsResponse {
  return {
    experimentID: 'exp-1',
    name: 'Ranking test',
    status: 'running',
    indexName: 'products',
    startDate: '2026-02-01T00:00:00Z',
    endedAt: null,
    conclusion: null,
    trafficSplit: 0.5,
    primaryMetric: 'ctr',
    gate: {
      minimumNReached: true,
      minimumDaysReached: false,
      readyToRead: false,
      requiredSearchesPerArm: 60000,
      currentSearchesPerArm: 41200,
      progressPct: 68.7,
      estimatedDaysRemaining: 12.3,
    },
    control: {
      name: 'control',
      searches: 41200,
      users: 8500,
      clicks: 5068,
      conversions: 1854,
      revenue: 45200,
      ctr: 0.123,
      conversionRate: 0.045,
      revenuePerSearch: 1.1,
      zeroResultRate: 0.032,
      abandonmentRate: 0.15,
      meanClickRank: 3.5,
    },
    variant: {
      name: 'variant',
      searches: 41200,
      users: 8400,
      clicks: 5397,
      conversions: 2142,
      revenue: 49800,
      ctr: 0.131,
      conversionRate: 0.052,
      revenuePerSearch: 1.21,
      zeroResultRate: 0.028,
      abandonmentRate: 0.12,
      meanClickRank: 2.1,
    },
    significance: null,
    bayesian: { probVariantBetter: 0.78 },
    sampleRatioMismatch: false,
    cupedApplied: false,
    guardRailAlerts: [],
    outlierUsersExcluded: 0,
    noStableIdQueries: 0,
    recommendation: null,
    interleaving: null,
    ...overrides,
  };
}

describe('buildExperimentDetailViewModel', () => {
  it('computes declare-winner and warning state for running experiments', () => {
    const results = createBaseResults({
      noStableIdQueries: 7000,
      guardRailAlerts: undefined as unknown as [],
      outlierUsersExcluded: undefined as unknown as number,
    });

    const viewModel = buildExperimentDetailViewModel(results);

    expect(viewModel.canDeclareWinner).toBe(true);
    expect(viewModel.needsDaysGateWarning).toBe(true);
    expect(viewModel.hasConclusion).toBe(false);
    expect(viewModel.guardRailAlerts).toEqual([]);
    expect(viewModel.outlierUsersExcluded).toBe(0);
    expect(viewModel.noStableIdQueries).toBe(7000);
    expect(viewModel.unstableIdFraction).toBeCloseTo(7000 / 82400, 8);
    expect(viewModel.conclusionWinnerLabel).toBe('No winner (inconclusive)');
    expect(viewModel.conclusionMetricLabel).toBe('CTR');
  });

  it('maps concluded winner and metric label using canonical aliases', () => {
    const results = createBaseResults({
      status: 'concluded',
      primaryMetric: 'conversion_rate',
      gate: {
        minimumNReached: true,
        minimumDaysReached: true,
        readyToRead: true,
        requiredSearchesPerArm: 60000,
        currentSearchesPerArm: 62000,
        progressPct: 100,
        estimatedDaysRemaining: null,
      },
      conclusion: {
        winner: 'variant',
        reason: 'Variant won.',
        controlMetric: 0.045,
        variantMetric: 0.052,
        confidence: 0.973,
        significant: true,
        promoted: false,
      },
    });

    const viewModel = buildExperimentDetailViewModel(results);

    expect(viewModel.canDeclareWinner).toBe(false);
    expect(viewModel.needsDaysGateWarning).toBe(false);
    expect(viewModel.hasConclusion).toBe(true);
    expect(viewModel.conclusionWinnerLabel).toBe('Variant');
    expect(viewModel.conclusionMetricLabel).toBe('Conversion Rate');
  });
});
