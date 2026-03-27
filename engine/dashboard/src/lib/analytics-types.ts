import type { DateRange } from '@/hooks/useAnalytics'

export interface TabProps {
  index: string
  range: DateRange
  prevRange?: DateRange
}
