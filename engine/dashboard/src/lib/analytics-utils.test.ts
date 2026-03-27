import { describe, expect, it } from 'vitest'
import {
  RANGE_OPTIONS,
  formatCurrency,
  formatDateLong,
  formatDateShort,
  formatPercentAxisTick,
} from './analytics-utils'

describe('RANGE_OPTIONS', () => {
  it('defines canonical analytics range options', () => {
    expect(RANGE_OPTIONS).toEqual([
      { label: '7d', days: 7 },
      { label: '30d', days: 30 },
      { label: '90d', days: 90 },
    ])
  })
})

describe('formatDateShort', () => {
  it('formats ISO dates for compact chart labels', () => {
    expect(formatDateShort('2026-02-18')).toBe('Feb 18')
    expect(formatDateShort('2026-12-01')).toBe('Dec 1')
  })
})

describe('formatDateLong', () => {
  it('formats ISO dates for tooltip labels', () => {
    expect(formatDateLong('2026-02-18')).toBe('Wed, February 18, 2026')
  })
})

describe('formatCurrency', () => {
  it('formats with an explicit currency', () => {
    expect(formatCurrency(1234.5, 'USD')).toBe('$1,234.50')
  })

  it('defaults to USD when currency is omitted', () => {
    expect(formatCurrency(1234.5)).toBe('$1,234.50')
  })

  it('falls back to plain number + currency when Intl rejects the currency code', () => {
    expect(formatCurrency(1234.5, 'INVALID')).toBe('1,234.5 INVALID')
  })
})

describe('formatPercentAxisTick', () => {
  it('formats ratio values as whole-number percentages for chart axes', () => {
    expect(formatPercentAxisTick(0)).toBe('0%')
    expect(formatPercentAxisTick(0.126)).toBe('13%')
    expect(formatPercentAxisTick(1.2)).toBe('120%')
  })
})
