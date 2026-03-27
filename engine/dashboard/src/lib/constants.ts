export const METRIC_LABELS: Record<string, string> = {
  ctr: 'CTR',
  conversionRate: 'Conversion Rate',
  revenuePerSearch: 'Revenue / Search',
  zeroResultRate: 'Zero Result Rate',
  abandonmentRate: 'Abandonment Rate',
  conversion_rate: 'Conversion Rate',
  revenue_per_search: 'Revenue / Search',
  zero_result_rate: 'Zero Result Rate',
  abandonment_rate: 'Abandonment Rate',
}

export function formatMetricLabel(metric: string): string {
  return METRIC_LABELS[metric] || metric
}

const DEFAULT_EXPERIMENT_STATUS_BADGE_CLASS =
  'bg-slate-100 text-slate-700 border-slate-300'

export const EXPERIMENT_STATUS_BADGE_CLASSES: Record<string, string> = {
  running: 'bg-emerald-100 text-emerald-800 border-emerald-200 animate-pulse',
  draft: DEFAULT_EXPERIMENT_STATUS_BADGE_CLASS,
  stopped: 'bg-orange-100 text-orange-800 border-orange-200',
  expired: 'bg-orange-100 text-orange-800 border-orange-200',
  concluded: 'bg-blue-100 text-blue-800 border-blue-200',
}

export function formatExperimentStatusBadgeClass(status: string): string {
  return EXPERIMENT_STATUS_BADGE_CLASSES[status] || DEFAULT_EXPERIMENT_STATUS_BADGE_CLASS
}
