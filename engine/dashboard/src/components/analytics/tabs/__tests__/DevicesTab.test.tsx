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
  useDeviceBreakdown: vi.fn(),
}));

import { useDeviceBreakdown } from '@/hooks/useAnalytics';
import { DevicesTab } from '@/components/analytics/tabs/DevicesTab';

const mockUseDeviceBreakdown = vi.mocked(useDeviceBreakdown);

const defaultRange = { startDate: '2026-01-01', endDate: '2026-01-07' };

describe('DevicesTab', () => {
  it('renders device platform grid and trend chart when data is present', () => {
    mockUseDeviceBreakdown.mockReturnValue({
      data: {
        platforms: [
          { platform: 'desktop', count: 500 },
          { platform: 'mobile', count: 300 },
          { platform: 'tablet', count: 200 },
        ],
        dates: [
          { date: '2026-01-01', platform: 'desktop', count: 100 },
          { date: '2026-01-01', platform: 'mobile', count: 60 },
        ],
      },
      isLoading: false,
      error: null,
    } as any);

    render(<DevicesTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('device-desktop')).toBeInTheDocument();
    expect(screen.getByTestId('device-mobile')).toBeInTheDocument();
    expect(screen.getByTestId('device-tablet')).toBeInTheDocument();
    expect(screen.getByTestId('device-chart')).toBeInTheDocument();
  });

  it('shows loading skeleton', () => {
    mockUseDeviceBreakdown.mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as any);

    render(<DevicesTab index="movies" range={defaultRange} />);

    expect(screen.getAllByTestId('table-skeleton')).toHaveLength(3);
  });

  it('renders empty state when no device data exists', () => {
    mockUseDeviceBreakdown.mockReturnValue({
      data: { platforms: [], dates: [] },
      isLoading: false,
      error: null,
    } as any);

    render(<DevicesTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('empty-state')).toBeInTheDocument();
    expect(screen.getByText(/No device data/)).toBeInTheDocument();
  });
});
