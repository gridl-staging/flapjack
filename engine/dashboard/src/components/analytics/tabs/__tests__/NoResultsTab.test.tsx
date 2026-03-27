import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

vi.mock('@/hooks/useAnalytics', () => ({
  useNoResults: vi.fn(),
  useNoResultRate: vi.fn(),
}));

import { useNoResults, useNoResultRate } from '@/hooks/useAnalytics';
import { NoResultsTab } from '@/components/analytics/tabs/NoResultsTab';

const mockUseNoResults = vi.mocked(useNoResults);
const mockUseNoResultRate = vi.mocked(useNoResultRate);

const defaultRange = { startDate: '2026-01-01', endDate: '2026-01-07' };
const prevRange = { startDate: '2025-12-25', endDate: '2025-12-31' };

function setHookDefaults(overrides?: {
  noResults?: Partial<ReturnType<typeof useNoResults>>;
  noResultRate?: Partial<ReturnType<typeof useNoResultRate>>;
}) {
  mockUseNoResults.mockReturnValue({
    data: undefined,
    isLoading: false,
    error: null,
    ...overrides?.noResults,
  } as any);
  mockUseNoResultRate.mockReturnValue({
    data: undefined,
    isLoading: false,
    error: null,
    ...overrides?.noResultRate,
  } as any);
}

describe('NoResultsTab', () => {
  it('renders no-result rate banner with percentage and delta badge', () => {
    setHookDefaults({
      noResultRate: {
        data: { rate: 0.15, dates: [] },
      },
    });
    // Second call is for prevRange
    mockUseNoResultRate.mockReturnValueOnce({
      data: { rate: 0.15, dates: [] },
      isLoading: false,
      error: null,
    } as any).mockReturnValueOnce({
      data: { rate: 0.2 },
      isLoading: false,
      error: null,
    } as any);

    render(<NoResultsTab index="movies" range={defaultRange} prevRange={prevRange} />);

    expect(screen.getByTestId('no-result-rate-banner')).toBeInTheDocument();
    expect(screen.getByTestId('rate-value')).toHaveTextContent('15.0%');
    expect(screen.getByTestId('delta-badge')).toBeInTheDocument();
  });

  it('renders the no-results search table when data is present', () => {
    mockUseNoResults.mockReturnValue({
      data: { searches: [{ search: 'missing-term', count: 42 }] },
      isLoading: false,
      error: null,
    } as any);
    mockUseNoResultRate.mockReturnValue({
      data: { rate: 0.05 },
      isLoading: false,
      error: null,
    } as any);

    render(<NoResultsTab index="movies" range={defaultRange} prevRange={prevRange} />);

    expect(screen.getByTestId('no-results-table')).toBeInTheDocument();
    expect(screen.getByText('missing-term')).toBeInTheDocument();
    expect(screen.getByText('42')).toBeInTheDocument();
  });

  it('renders EmptyState with positive indicator when rate is zero', () => {
    mockUseNoResults.mockReturnValue({
      data: { searches: [] },
      isLoading: false,
      error: null,
    } as any);
    mockUseNoResultRate.mockReturnValue({
      data: { rate: 0 },
      isLoading: false,
      error: null,
    } as any);

    render(<NoResultsTab index="movies" range={defaultRange} prevRange={prevRange} />);

    const emptyState = screen.getByTestId('empty-state');
    expect(emptyState).toBeInTheDocument();
    expect(emptyState).toHaveTextContent('No zero-result searches');
  });

  it('shows loading skeleton', () => {
    mockUseNoResults.mockReturnValue({
      data: undefined,
      isLoading: true,
      error: null,
    } as any);
    mockUseNoResultRate.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: null,
    } as any);

    render(<NoResultsTab index="movies" range={defaultRange} prevRange={prevRange} />);

    expect(screen.getByTestId('table-skeleton')).toBeInTheDocument();
  });

  it('renders a generic error state when the no-results request fails', () => {
    mockUseNoResults.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('No results failed'),
    } as any);
    mockUseNoResultRate.mockReturnValue({
      data: { rate: 0.1 },
      isLoading: false,
      error: null,
    } as any);

    render(<NoResultsTab index="movies" range={defaultRange} prevRange={prevRange} />);

    expect(screen.getByTestId('error-state')).toHaveTextContent('Unable to load analytics data. Try again.');
  });
});
