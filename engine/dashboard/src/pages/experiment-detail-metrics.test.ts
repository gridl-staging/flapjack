import { describe, expect, it, vi } from 'vitest';
import type { ArmResultsResponse } from '@/hooks/useExperiments';
import {
  formatExperimentNumber,
  formatExperimentPercentage,
  formatExperimentPrimaryMetricValue,
  getExperimentPrimaryMetricValue,
} from './experiment-detail-metrics';

const exampleArm: ArmResultsResponse = {
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
};

describe('experiment-detail-metrics', () => {
  it('maps camelCase and snake_case metric aliases to the same arm value', () => {
    expect(getExperimentPrimaryMetricValue(exampleArm, 'conversionRate')).toBe(0.052);
    expect(getExperimentPrimaryMetricValue(exampleArm, 'conversion_rate')).toBe(0.052);
    expect(getExperimentPrimaryMetricValue(exampleArm, 'revenuePerSearch')).toBe(1.21);
    expect(getExperimentPrimaryMetricValue(exampleArm, 'revenue_per_search')).toBe(1.21);
  });

  it('formats percentage and currency metrics consistently across aliases', () => {
    expect(formatExperimentPrimaryMetricValue('conversionRate', 0.052)).toBe('5.2%');
    expect(formatExperimentPrimaryMetricValue('conversion_rate', 0.052)).toBe('5.2%');
    expect(formatExperimentPrimaryMetricValue('revenuePerSearch', 1.21)).toBe('$1.21');
    expect(formatExperimentPrimaryMetricValue('revenue_per_search', 1.21)).toBe('$1.21');
  });

  it('uses a stable locale for shared numeric formatting in ExperimentDetail sections', () => {
    expect(formatExperimentPercentage(0.052)).toBe('5.2%');

    const toLocaleStringSpy = vi
      .spyOn(Number.prototype, 'toLocaleString')
      .mockReturnValue('5,397');

    try {
      expect(formatExperimentNumber(5397)).toBe('5,397');
      expect(toLocaleStringSpy).toHaveBeenCalledWith('en-US');
    } finally {
      toLocaleStringSpy.mockRestore();
    }
  });

  it('preserves the existing fallback behavior for unknown metrics', () => {
    expect(getExperimentPrimaryMetricValue(exampleArm, 'unexpected_metric')).toBe(0.131);
    expect(formatExperimentPrimaryMetricValue('unexpected_metric', 42)).toBe('42');
  });

  it('uses a numeric arm field when the backend sends a newer metric key', () => {
    const armWithNewMetric = {
      ...exampleArm,
      unexpected_metric: 7.5,
    } as ArmResultsResponse & Record<string, number>;

    expect(getExperimentPrimaryMetricValue(armWithNewMetric, 'unexpected_metric')).toBe(7.5);
  });
});
