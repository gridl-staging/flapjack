import type { ArmResultsResponse } from '@/hooks/useExperiments';
import { formatCurrency } from '@/lib/analytics-utils';

const EXPERIMENT_NUMBER_FORMAT_LOCALE = 'en-US';

type CanonicalExperimentPrimaryMetric =
  | 'ctr'
  | 'conversionRate'
  | 'revenuePerSearch'
  | 'zeroResultRate'
  | 'abandonmentRate';

const EXPERIMENT_PRIMARY_METRIC_ALIASES: Record<string, CanonicalExperimentPrimaryMetric> = {
  ctr: 'ctr',
  conversionRate: 'conversionRate',
  conversion_rate: 'conversionRate',
  revenuePerSearch: 'revenuePerSearch',
  revenue_per_search: 'revenuePerSearch',
  zeroResultRate: 'zeroResultRate',
  zero_result_rate: 'zeroResultRate',
  abandonmentRate: 'abandonmentRate',
  abandonment_rate: 'abandonmentRate',
};

const EXPERIMENT_PRIMARY_METRIC_VALUE_SELECTORS: Record<
  CanonicalExperimentPrimaryMetric,
  (arm: ArmResultsResponse) => number
> = {
  ctr: (arm) => arm.ctr,
  conversionRate: (arm) => arm.conversionRate,
  revenuePerSearch: (arm) => arm.revenuePerSearch,
  zeroResultRate: (arm) => arm.zeroResultRate,
  abandonmentRate: (arm) => arm.abandonmentRate,
};

const EXPERIMENT_PERCENTAGE_METRICS = new Set<CanonicalExperimentPrimaryMetric>([
  'ctr',
  'conversionRate',
  'zeroResultRate',
  'abandonmentRate',
]);

function resolveExperimentPrimaryMetric(metric: string): CanonicalExperimentPrimaryMetric | null {
  return EXPERIMENT_PRIMARY_METRIC_ALIASES[metric] ?? null;
}

function readUnknownExperimentMetricValue(
  arm: ArmResultsResponse,
  metric: string,
): number | null {
  const rawValue = (arm as unknown as Record<string, unknown>)[metric];
  return typeof rawValue === 'number' ? rawValue : null;
}

export function formatExperimentPercentage(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

export function formatExperimentNumber(value: number): string {
  return value.toLocaleString(EXPERIMENT_NUMBER_FORMAT_LOCALE);
}

export function getExperimentPrimaryMetricValue(arm: ArmResultsResponse, metric: string): number {
  const resolvedMetric = resolveExperimentPrimaryMetric(metric);
  if (resolvedMetric) {
    return EXPERIMENT_PRIMARY_METRIC_VALUE_SELECTORS[resolvedMetric](arm);
  }

  return readUnknownExperimentMetricValue(arm, metric) ?? arm.ctr;
}

export function formatExperimentPrimaryMetricValue(metric: string, value: number): string {
  const resolvedMetric = resolveExperimentPrimaryMetric(metric);

  if (resolvedMetric === 'revenuePerSearch') {
    return formatCurrency(value);
  }

  if (resolvedMetric && EXPERIMENT_PERCENTAGE_METRICS.has(resolvedMetric)) {
    return formatExperimentPercentage(value);
  }

  return formatExperimentNumber(value);
}
