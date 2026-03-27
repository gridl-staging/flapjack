import { describe, expect, it } from 'vitest'
import {
  METRIC_LABELS,
  formatMetricLabel,
  formatExperimentStatusBadgeClass,
} from './constants'

describe('METRIC_LABELS', () => {
  it('contains canonical labels for supported metrics', () => {
    expect(METRIC_LABELS.ctr).toBe('CTR')
    expect(METRIC_LABELS.conversionRate).toBe('Conversion Rate')
    expect(METRIC_LABELS.conversion_rate).toBe('Conversion Rate')
    expect(METRIC_LABELS.revenuePerSearch).toBe('Revenue / Search')
    expect(METRIC_LABELS.revenue_per_search).toBe('Revenue / Search')
  })
})

describe('formatMetricLabel', () => {
  it('formats camelCase keys', () => {
    expect(formatMetricLabel('conversionRate')).toBe('Conversion Rate')
    expect(formatMetricLabel('zeroResultRate')).toBe('Zero Result Rate')
  })

  it('formats snake_case keys', () => {
    expect(formatMetricLabel('conversion_rate')).toBe('Conversion Rate')
    expect(formatMetricLabel('zero_result_rate')).toBe('Zero Result Rate')
  })

  it('falls back to the raw key when no label exists', () => {
    expect(formatMetricLabel('custom_metric_key')).toBe('custom_metric_key')
  })
})

describe('formatExperimentStatusBadgeClass', () => {
  it('returns canonical status badge classes for known experiment statuses', () => {
    expect(formatExperimentStatusBadgeClass('running')).toContain('bg-emerald-100')
    expect(formatExperimentStatusBadgeClass('draft')).toContain('bg-slate-100')
    expect(formatExperimentStatusBadgeClass('stopped')).toContain('bg-orange-100')
    expect(formatExperimentStatusBadgeClass('concluded')).toContain('bg-blue-100')
    expect(formatExperimentStatusBadgeClass('expired')).toContain('bg-orange-100')
  })

  it('falls back to neutral badge styling for unknown statuses', () => {
    expect(formatExperimentStatusBadgeClass('paused')).toContain('bg-slate-100')
  })
})
