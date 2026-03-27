import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

vi.mock('@/hooks/useAnalytics', () => ({
  useTopFilters: vi.fn(),
  useFiltersNoResults: vi.fn(),
  useFilterValues: vi.fn(),
}));

import { useTopFilters, useFiltersNoResults, useFilterValues } from '@/hooks/useAnalytics';
import { FiltersTab } from '@/components/analytics/tabs/FiltersTab';

const mockUseTopFilters = vi.mocked(useTopFilters);
const mockUseFiltersNoResults = vi.mocked(useFiltersNoResults);
const mockUseFilterValues = vi.mocked(useFilterValues);

const defaultRange = { startDate: '2026-01-01', endDate: '2026-01-07' };

function setHookDefaults(overrides?: {
  topFilters?: Partial<ReturnType<typeof useTopFilters>>;
  filtersNoResults?: Partial<ReturnType<typeof useFiltersNoResults>>;
}) {
  mockUseTopFilters.mockReturnValue({
    data: undefined,
    isLoading: false,
    error: null,
    ...overrides?.topFilters,
  } as any);
  mockUseFiltersNoResults.mockReturnValue({
    data: undefined,
    isLoading: false,
    error: null,
    ...overrides?.filtersNoResults,
  } as any);
  mockUseFilterValues.mockReturnValue({
    data: undefined,
    isLoading: false,
    error: null,
  } as any);
}

describe('FiltersTab', () => {
  it('renders the top-filters card and filters-no-results card', () => {
    setHookDefaults({
      topFilters: {
        data: {
          filters: [
            { attribute: 'genre:action', count: 200 },
            { attribute: 'color:red', count: 150 },
          ],
        },
      },
      filtersNoResults: {
        data: {
          filters: [
            { attribute: 'size:xxl', count: 30 },
          ],
        },
      },
    });

    render(<FiltersTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('filters-table')).toBeInTheDocument();
    expect(screen.getByText('genre:action')).toBeInTheDocument();
    expect(screen.getByTestId('filters-no-results')).toBeInTheDocument();
    expect(screen.getByText('size:xxl')).toBeInTheDocument();
  });

  it('renders loading skeleton', () => {
    setHookDefaults({
      topFilters: { isLoading: true },
    });

    render(<FiltersTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('table-skeleton')).toBeInTheDocument();
  });

  it('renders a generic error state when the filters request fails', () => {
    setHookDefaults({
      topFilters: { error: new Error('Backend failed') },
    });

    render(<FiltersTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('error-state')).toHaveTextContent('Unable to load analytics data. Try again.');
  });

  it('handles empty filter data', () => {
    setHookDefaults({
      topFilters: { data: { filters: [] } },
    });

    render(<FiltersTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('empty-state')).toBeInTheDocument();
    expect(screen.getByText(/No filter usage recorded/)).toBeInTheDocument();
  });
});
