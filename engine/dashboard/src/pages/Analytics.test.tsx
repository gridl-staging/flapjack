import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { TEST_ROUTER_FUTURE } from '@/test/routerFuture'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { IndexLayout } from '@/components/layout/IndexLayout'
import { Analytics } from './Analytics'

// Mock all analytics hooks — tests focus on the page shell, not chart data
vi.mock('@/hooks/useAnalytics', () => ({
  useSearchCount: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useUsersCount: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useNoResultRate: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useTopSearches: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useNoResults: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useTopFilters: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useFilterValues: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useFiltersNoResults: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useDeviceBreakdown: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useGeoBreakdown: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useGeoTopSearches: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useGeoRegions: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useAddToCartRate: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  usePurchaseRate: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useRevenue: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useCountries: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  useConversionRate: vi.fn().mockReturnValue({ data: undefined, isLoading: false }),
  defaultRange: vi.fn((days: number) => {
    const end = new Date('2026-02-18T00:00:00.000Z')
    const start = new Date(end)
    start.setUTCDate(start.getUTCDate() - days)
    return { startDate: start.toISOString().slice(0, 10), endDate: end.toISOString().slice(0, 10) }
  }),
  previousRange: vi.fn((range: { startDate: string; endDate: string }) => {
    const start = new Date(`${range.startDate}T00:00:00.000Z`)
    const end = new Date(`${range.endDate}T00:00:00.000Z`)
    const days = Math.round((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24))
    const prevEnd = new Date(start)
    prevEnd.setUTCDate(prevEnd.getUTCDate() - 1)
    const prevStart = new Date(prevEnd)
    prevStart.setUTCDate(prevStart.getUTCDate() - days)
    return { startDate: prevStart.toISOString().slice(0, 10), endDate: prevEnd.toISOString().slice(0, 10) }
  }),
}))

vi.mock('@/hooks/useIndexes', () => ({
  useIndexes: vi.fn(),
}))

vi.mock('@/hooks/useSettings', () => ({
  useSettings: vi.fn(),
}))

// Recharts uses ResizeObserver which isn't in jsdom
vi.mock('recharts', () => ({
  AreaChart: ({ children }: any) => <div data-testid="area-chart">{children}</div>,
  Area: () => null,
  XAxis: () => null,
  YAxis: () => null,
  CartesianGrid: () => null,
  Tooltip: () => null,
  ResponsiveContainer: ({ children }: any) => <div>{children}</div>,
}))

import { useIndexes } from '@/hooks/useIndexes'
import { useSettings } from '@/hooks/useSettings'

function makeWrapper(path: string, nonIndexRoute?: string) {
  return function wrapper({ children }: { children: React.ReactNode }) {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    return (
      <QueryClientProvider client={qc}>
        <MemoryRouter future={TEST_ROUTER_FUTURE} initialEntries={[path]}>
          <Routes>
            <Route path="/index/:indexName" element={<IndexLayout />}>
              <Route path="analytics" element={children} />
            </Route>
            {nonIndexRoute ? <Route path={nonIndexRoute} element={children} /> : null}
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    )
  }
}

const withIndex = makeWrapper('/index/products/analytics')
const withoutIndex = makeWrapper('/analytics', '/analytics')

describe('Analytics', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(useSettings).mockReturnValue({ data: { mode: 'keywordSearch' }, isLoading: false } as any)
  })

  it('shows "No Indexes Found" when no indexes exist and no indexName in URL', () => {
    vi.mocked(useIndexes).mockReturnValue({ data: [], isLoading: false } as any)
    render(<Analytics />, { wrapper: withoutIndex })
    expect(screen.getByText('No Indexes Found')).toBeInTheDocument()
  })

  it('renders the Analytics heading with BETA badge', () => {
    vi.mocked(useIndexes).mockReturnValue({ data: [{ uid: 'products', entries: 100, dataSize: 0 }], isLoading: false } as any)
    render(<Analytics />, { wrapper: withIndex })
    expect(screen.getByTestId('analytics-heading')).toHaveTextContent('Analytics')
    expect(screen.getByText('BETA')).toBeInTheDocument()
  })

  it('renders the breadcrumb when indexName is in the URL', () => {
    vi.mocked(useIndexes).mockReturnValue({ data: [{ uid: 'products', entries: 100, dataSize: 0 }], isLoading: false } as any)
    render(<Analytics />, { wrapper: withIndex })
    expect(screen.getByTestId('analytics-breadcrumb')).toBeInTheDocument()
    // Breadcrumb should mention the index name
    expect(screen.getByTestId('analytics-breadcrumb')).toHaveTextContent('products')
  })

  it('renders date range toggle buttons', () => {
    vi.mocked(useIndexes).mockReturnValue({ data: [{ uid: 'products', entries: 100, dataSize: 0 }], isLoading: false } as any)
    render(<Analytics />, { wrapper: withIndex })
    expect(screen.getByTestId('analytics-date-range')).toBeInTheDocument()
    expect(screen.getByTestId('range-7d')).toBeInTheDocument()
    expect(screen.getByTestId('range-30d')).toBeInTheDocument()
    expect(screen.getByTestId('range-90d')).toBeInTheDocument()
  })

  it('switches active range when a date range button is clicked', async () => {
    const user = userEvent.setup()
    vi.mocked(useIndexes).mockReturnValue({ data: [{ uid: 'products', entries: 100, dataSize: 0 }], isLoading: false } as any)
    render(<Analytics />, { wrapper: withIndex })

    const btn30d = screen.getByTestId('range-30d')
    await user.click(btn30d)
    // After clicking, 30d button should gain primary styling (bg-primary)
    expect(btn30d.className).toMatch(/bg-primary/)
  })

  it('shows analytics tabs when an index is available', () => {
    vi.mocked(useIndexes).mockReturnValue({ data: [{ uid: 'products', entries: 100, dataSize: 0 }], isLoading: false } as any)
    render(<Analytics />, { wrapper: withIndex })
    expect(screen.getByTestId('analytics-tabs')).toBeInTheDocument()
    expect(screen.getByTestId('tab-overview')).toBeInTheDocument()
    expect(screen.getByTestId('tab-searches')).toBeInTheDocument()
    expect(screen.getByTestId('tab-no-results')).toBeInTheDocument()
  })

  it('renders the Conversions tab trigger', () => {
    vi.mocked(useIndexes).mockReturnValue({ data: [{ uid: 'products', entries: 100, dataSize: 0 }], isLoading: false } as any)
    render(<Analytics />, { wrapper: withIndex })
    expect(screen.getByTestId('tab-conversions')).toBeInTheDocument()
  })

  it('wires the extracted overview, conversions, and geography tabs into the page shell', async () => {
    const user = userEvent.setup()
    vi.mocked(useIndexes).mockReturnValue({ data: [{ uid: 'products', entries: 100, dataSize: 0 }], isLoading: false } as any)

    render(<Analytics />, { wrapper: withIndex })

    expect(screen.getByTestId('tab-overview')).toHaveAttribute('data-state', 'active')
    expect(screen.getByTestId('search-volume-chart')).toBeInTheDocument()

    await user.click(screen.getByTestId('tab-conversions'))

    expect(screen.getByTestId('tab-conversions')).toHaveAttribute('data-state', 'active')
    expect(screen.getByTestId('conversion-country-filter')).toBeInTheDocument()

    await user.click(screen.getByTestId('tab-geography'))

    expect(screen.getByTestId('tab-geography')).toHaveAttribute('data-state', 'active')
    expect(screen.getByText('No geographic data')).toBeInTheDocument()
  })
})
