import { render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';

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
  useAddToCartRate: vi.fn(),
  useConversionRate: vi.fn(),
  useCountries: vi.fn(),
  usePurchaseRate: vi.fn(),
  useRevenue: vi.fn(),
}));

import {
  useAddToCartRate,
  useConversionRate,
  useCountries,
  usePurchaseRate,
  useRevenue,
} from '@/hooks/useAnalytics';
import { ConversionsTab } from '@/components/analytics/tabs/ConversionsTab';

const mockUseAddToCartRate = vi.mocked(useAddToCartRate);
const mockUseConversionRate = vi.mocked(useConversionRate);
const mockUseCountries = vi.mocked(useCountries);
const mockUsePurchaseRate = vi.mocked(usePurchaseRate);
const mockUseRevenue = vi.mocked(useRevenue);

const range = { startDate: '2026-02-11', endDate: '2026-02-18' };
const prevRange = { startDate: '2026-02-03', endDate: '2026-02-10' };
const nextRange = { startDate: '2026-01-19', endDate: '2026-02-18' };
const nextPrevRange = { startDate: '2025-12-20', endDate: '2026-01-18' };

function buildConversionSeries(rate: number) {
  return [{ date: '2026-02-11', rate }];
}

function configureConversionMocks(options?: {
  conversionRate?: number;
  previousConversionRate?: number;
  currencies?: Record<string, { currency: string; revenue: number }>;
}) {
  const currentConversionRate = options?.conversionRate ?? 0.15;
  const previousConversionRate = options?.previousConversionRate ?? 0.1;

  mockUseCountries.mockReturnValue({
    data: {
      countries: [
        { country: 'US', count: 150 },
        { country: 'DE', count: 40 },
      ],
    },
    isLoading: false,
  } as any);

  mockUseConversionRate.mockImplementation((_, hookRange) => ({
    data: {
      rate: hookRange.startDate === range.startDate ? currentConversionRate : previousConversionRate,
      dates: buildConversionSeries(hookRange.startDate === range.startDate ? currentConversionRate : previousConversionRate),
    },
    isLoading: false,
  }) as any);

  mockUseAddToCartRate.mockImplementation((_, hookRange) => ({
    data: {
      rate: hookRange.startDate === range.startDate ? 0.25 : 0.2,
      dates: buildConversionSeries(hookRange.startDate === range.startDate ? 0.25 : 0.2),
    },
    isLoading: false,
  }) as any);

  mockUsePurchaseRate.mockImplementation((_, hookRange) => ({
    data: {
      rate: hookRange.startDate === range.startDate ? 0.1 : 0.08,
      dates: buildConversionSeries(hookRange.startDate === range.startDate ? 0.1 : 0.08),
    },
    isLoading: false,
  }) as any);

  mockUseRevenue.mockImplementation((_, _hookRange, country) => ({
    data: {
      currencies: options?.currencies ?? {
        USD: { currency: 'USD', revenue: country ? 1500.25 : 1999.99 },
      },
      dates: buildConversionSeries(0.05),
    },
    isLoading: false,
  }) as any);
}

describe('ConversionsTab', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('shows conversion KPI cards', () => {
    configureConversionMocks();

    render(<ConversionsTab index="movies" range={range} prevRange={prevRange} />);

    expect(screen.getByTestId('conversion-kpi-cards')).toBeInTheDocument();
    expect(screen.getByTestId('kpi-conversion-rate')).toBeInTheDocument();
    expect(screen.getByTestId('kpi-add-to-cart-rate')).toBeInTheDocument();
    expect(screen.getByTestId('kpi-purchase-rate')).toBeInTheDocument();
    expect(screen.getByTestId('kpi-revenue')).toBeInTheDocument();
    expect(screen.getByTestId('conversion-rate-chart')).toBeInTheDocument();
    expect(screen.getByTestId('atc-rate-chart')).toBeInTheDocument();
    expect(screen.getByTestId('purchase-rate-chart')).toBeInTheDocument();
  });

  it('shows empty state in conversion tab when no data', () => {
    mockUseCountries.mockReturnValue({ data: { countries: [] }, isLoading: false } as any);
    mockUseConversionRate.mockReturnValue({ data: undefined, isLoading: false } as any);
    mockUseAddToCartRate.mockReturnValue({ data: undefined, isLoading: false } as any);
    mockUsePurchaseRate.mockReturnValue({ data: undefined, isLoading: false } as any);
    mockUseRevenue.mockReturnValue({ data: undefined, isLoading: false } as any);

    render(<ConversionsTab index="movies" range={range} prevRange={prevRange} />);

    expect(screen.getAllByText('No data')).toHaveLength(4);
    expect(screen.getByText('No conversion data yet')).toBeInTheDocument();
    expect(screen.getAllByText('No data available')).toHaveLength(2);
  });

  it('renders revenue with currency label', () => {
    configureConversionMocks();

    render(<ConversionsTab index="movies" range={range} prevRange={prevRange} />);

    expect(screen.getByTestId('kpi-revenue')).toHaveTextContent('$1,999.99');
  });

  it('shows conversion delta badge when current and previous ranges differ', () => {
    configureConversionMocks({ conversionRate: 0.2, previousConversionRate: 0.1 });

    render(<ConversionsTab index="movies" range={range} prevRange={prevRange} />);

    expect(within(screen.getByTestId('kpi-conversion-rate')).getByTestId('delta-badge')).toHaveTextContent('100.0%');
  });

  it('renders revenue breakdown when multiple currencies are returned', () => {
    configureConversionMocks({
      currencies: {
        USD: { currency: 'USD', revenue: 1999.99 },
        EUR: { currency: 'EUR', revenue: 1450.5 },
      },
    });

    render(<ConversionsTab index="movies" range={range} prevRange={prevRange} />);

    expect(screen.getByTestId('revenue-breakdown')).toBeInTheDocument();
    expect(screen.getByText('USD')).toBeInTheDocument();
    expect(screen.getByText('EUR')).toBeInTheDocument();
  });

  it('filters conversion hooks by selected country', async () => {
    const user = userEvent.setup();
    configureConversionMocks();

    render(<ConversionsTab index="movies" range={range} prevRange={prevRange} />);

    await user.selectOptions(screen.getByTestId('conversion-country-filter'), 'US');

    await waitFor(() => {
      expect(mockUseAddToCartRate).toHaveBeenLastCalledWith('movies', prevRange, 'US');
      expect(mockUsePurchaseRate).toHaveBeenLastCalledWith('movies', prevRange, 'US');
      expect(mockUseRevenue).toHaveBeenLastCalledWith('movies', range, 'US');
    });

    expect(mockUseConversionRate.mock.calls.every((call) => call.length === 2)).toBe(true);
  });

  it('recomputes conversion hook ranges when date range changes', () => {
    configureConversionMocks();

    const { rerender } = render(<ConversionsTab index="movies" range={range} prevRange={prevRange} />);

    expect(mockUseAddToCartRate).toHaveBeenCalledWith('movies', range, undefined);
    expect(mockUseAddToCartRate).toHaveBeenCalledWith('movies', prevRange, undefined);

    rerender(<ConversionsTab index="movies" range={nextRange} prevRange={nextPrevRange} />);

    expect(mockUseAddToCartRate).toHaveBeenCalledWith('movies', nextRange, undefined);
    expect(mockUseAddToCartRate).toHaveBeenCalledWith('movies', nextPrevRange, undefined);
    expect(mockUsePurchaseRate).toHaveBeenCalledWith('movies', nextRange, undefined);
    expect(mockUsePurchaseRate).toHaveBeenCalledWith('movies', nextPrevRange, undefined);
    expect(mockUseRevenue).toHaveBeenCalledWith('movies', nextRange, undefined);
  });

  it('renders an error state when a conversion hook fails', () => {
    mockUseCountries.mockReturnValue({ data: { countries: [] }, isLoading: false } as any);
    mockUseConversionRate.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('Conversions failed'),
    } as any);
    mockUseAddToCartRate.mockReturnValue({ data: undefined, isLoading: false } as any);
    mockUsePurchaseRate.mockReturnValue({ data: undefined, isLoading: false } as any);
    mockUseRevenue.mockReturnValue({ data: undefined, isLoading: false } as any);

    render(<ConversionsTab index="movies" range={range} prevRange={prevRange} />);

    expect(screen.getByTestId('error-state')).toHaveTextContent('Unable to load analytics data. Try again.');
  });
});
