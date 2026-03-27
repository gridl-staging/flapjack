const ANALYTICS_FORMAT_LOCALE = 'en-US'

export const RANGE_OPTIONS = [
  { label: '7d', days: 7 },
  { label: '30d', days: 30 },
  { label: '90d', days: 90 },
]

export function formatDateShort(dateString: string): string {
  const date = new Date(`${dateString}T00:00:00`)
  return date.toLocaleDateString(ANALYTICS_FORMAT_LOCALE, { month: 'short', day: 'numeric' })
}

export function formatDateLong(dateString: string): string {
  const date = new Date(`${dateString}T00:00:00`)
  return date.toLocaleDateString(ANALYTICS_FORMAT_LOCALE, {
    weekday: 'short',
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  })
}

export function formatCurrency(amount: number, currency = 'USD'): string {
  try {
    return new Intl.NumberFormat(ANALYTICS_FORMAT_LOCALE, { style: 'currency', currency }).format(amount)
  } catch {
    return `${amount.toLocaleString(ANALYTICS_FORMAT_LOCALE)} ${currency}`
  }
}

export function formatPercentAxisTick(value: number): string {
  return `${(value * 100).toFixed(0)}%`
}
