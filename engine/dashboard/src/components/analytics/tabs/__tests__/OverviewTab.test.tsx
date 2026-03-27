import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

// Recharts mock pattern from Stage 2's AnalyticsShared.test.tsx
vi.mock('recharts', () => ({
  ResponsiveContainer: ({ children }: { children: any }) => <div>{children}</div>,
  AreaChart: ({ children }: { children: any }) => (
    <svg data-testid="area-chart" viewBox="0 0 100 100">
      {children}
    </svg>
  ),
  Area: () => null,
  CartesianGrid: () => null,
  Tooltip: () => null,
  XAxis: () => null,
  YAxis: () => null,
}));

vi.mock('@/hooks/useAnalytics', () => ({
  useSearchCount: vi.fn(),
  useUsersCount: vi.fn(),
  useNoResultRate: vi.fn(),
  useTopSearches: vi.fn(),
}));

import {
  useNoResultRate,
  useSearchCount,
  useTopSearches,
  useUsersCount,
} from '@/hooks/useAnalytics';
import { OverviewTab } from '@/components/analytics/tabs/OverviewTab';

const mockUseNoResultRate = vi.mocked(useNoResultRate);
const mockUseSearchCount = vi.mocked(useSearchCount);
const mockUseTopSearches = vi.mocked(useTopSearches);
const mockUseUsersCount = vi.mocked(useUsersCount);

const range = { startDate: '2026-02-01', endDate: '2026-02-07' };
const prevRange = { startDate: '2026-01-25', endDate: '2026-01-31' };

describe('OverviewTab', () => {
  it('renders KPI cards, trend charts, and top-searches card when data exists', () => {
    mockUseSearchCount
      .mockReturnValueOnce({
        data: {
          count: 1234,
          dates: [
            { date: '2026-02-01', count: 600 },
            { date: '2026-02-02', count: 634 },
          ],
        },
        isLoading: false,
      } as any)
      .mockReturnValueOnce({ data: { count: 1100 }, isLoading: false } as any);

    mockUseUsersCount
      .mockReturnValueOnce({ data: { count: 987 }, isLoading: false } as any)
      .mockReturnValueOnce({ data: { count: 950 }, isLoading: false } as any);

    mockUseNoResultRate
      .mockReturnValueOnce({
        data: {
          rate: 0.12,
          dates: [
            { date: '2026-02-01', rate: 0.1 },
            { date: '2026-02-02', rate: 0.12 },
          ],
        },
        isLoading: false,
      } as any)
      .mockReturnValueOnce({ data: { rate: 0.14 }, isLoading: false } as any);

    mockUseTopSearches.mockReturnValue({
      data: {
        searches: [
          { search: 'matrix', count: 42 },
          { search: 'inception', count: 31 },
        ],
      },
      isLoading: false,
    } as any);

    render(<OverviewTab index="movies" range={range} prevRange={prevRange} />);

    expect(screen.getByTestId('kpi-total-searches')).toBeInTheDocument();
    expect(screen.getByTestId('kpi-unique-users')).toBeInTheDocument();
    expect(screen.getByTestId('kpi-no-result-rate')).toBeInTheDocument();
    expect(screen.getByTestId('search-volume-chart')).toBeInTheDocument();
    expect(screen.getByTestId('no-result-rate-chart')).toBeInTheDocument();
    expect(screen.getByTestId('top-searches-overview')).toBeInTheDocument();
    expect(screen.getByText('matrix')).toBeInTheDocument();
  });

  it('renders loading states when hooks are loading', () => {
    mockUseSearchCount
      .mockReturnValueOnce({ data: undefined, isLoading: true } as any)
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any);
    mockUseUsersCount
      .mockReturnValueOnce({ data: undefined, isLoading: true } as any)
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any);
    mockUseNoResultRate
      .mockReturnValueOnce({ data: undefined, isLoading: true } as any)
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any);
    mockUseTopSearches.mockReturnValue({ data: undefined, isLoading: true } as any);

    render(<OverviewTab index="movies" range={range} prevRange={prevRange} />);

    expect(screen.getByTestId('search-volume-chart').querySelector('.animate-pulse')).toBeInTheDocument();
    expect(screen.getByTestId('no-result-rate-chart').querySelector('.animate-pulse')).toBeInTheDocument();
    expect(screen.getByTestId('table-skeleton')).toBeInTheDocument();
  });

  it('renders empty states when no overview data is available', () => {
    mockUseSearchCount
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any)
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any);
    mockUseUsersCount
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any)
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any);
    mockUseNoResultRate
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any)
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any);
    mockUseTopSearches.mockReturnValue({ data: undefined, isLoading: false } as any);

    render(<OverviewTab index="movies" range={range} prevRange={prevRange} />);

    expect(screen.getAllByText('No data')).toHaveLength(3);
    expect(screen.getAllByText('No search data yet')).toHaveLength(2);
    expect(screen.getByText('No data available')).toBeInTheDocument();
  });

  it('renders an error state when an overview hook fails', () => {
    mockUseSearchCount
      .mockReturnValueOnce({ data: undefined, isLoading: false, error: new Error('Search count failed') } as any)
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any);
    mockUseUsersCount
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any)
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any);
    mockUseNoResultRate
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any)
      .mockReturnValueOnce({ data: undefined, isLoading: false } as any);
    mockUseTopSearches.mockReturnValue({ data: undefined, isLoading: false } as any);

    render(<OverviewTab index="movies" range={range} prevRange={prevRange} />);

    expect(screen.getByTestId('error-state')).toHaveTextContent('Unable to load analytics data. Try again.');
  });
});
