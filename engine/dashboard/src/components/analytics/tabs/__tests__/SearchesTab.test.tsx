import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

vi.mock('@/hooks/useAnalytics', () => ({
  useTopSearches: vi.fn(),
  useGeoBreakdown: vi.fn(),
  useDeviceBreakdown: vi.fn(),
}));

import { useTopSearches, useGeoBreakdown, useDeviceBreakdown } from '@/hooks/useAnalytics';
import { SearchesTab } from '@/components/analytics/tabs/SearchesTab';

const mockUseTopSearches = vi.mocked(useTopSearches);
const mockUseGeoBreakdown = vi.mocked(useGeoBreakdown);
const mockUseDeviceBreakdown = vi.mocked(useDeviceBreakdown);

const defaultRange = { startDate: '2026-01-01', endDate: '2026-01-07' };

function setHookDefaults(overrides?: {
  topSearches?: Partial<ReturnType<typeof useTopSearches>>;
  geo?: Partial<ReturnType<typeof useGeoBreakdown>>;
  device?: Partial<ReturnType<typeof useDeviceBreakdown>>;
}) {
  mockUseTopSearches.mockReturnValue({
    data: undefined,
    isLoading: false,
    error: null,
    ...overrides?.topSearches,
  } as any);
  mockUseGeoBreakdown.mockReturnValue({
    data: undefined,
    isLoading: false,
    error: null,
    ...overrides?.geo,
  } as any);
  mockUseDeviceBreakdown.mockReturnValue({
    data: undefined,
    isLoading: false,
    error: null,
    ...overrides?.device,
  } as any);
}

describe('SearchesTab', () => {
  it('renders the search filters toolbar and top-searches table', () => {
    setHookDefaults({
      topSearches: {
        data: {
          searches: [
            { search: 'batman', count: 100, nbHits: 25 },
            { search: 'superman', count: 80, nbHits: 15 },
          ],
        },
      },
    });

    render(<SearchesTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('searches-filter')).toBeInTheDocument();
    expect(screen.getByTestId('top-searches-table')).toBeInTheDocument();
    expect(screen.getByText('batman')).toBeInTheDocument();
    expect(screen.getByText('superman')).toBeInTheDocument();
  });

  it('renders loading skeleton', () => {
    setHookDefaults({
      topSearches: { isLoading: true },
    });

    render(<SearchesTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('table-skeleton')).toBeInTheDocument();
  });

  it('renders error state', () => {
    setHookDefaults({
      topSearches: { error: new Error('Network failure') },
    });

    render(<SearchesTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('error-state')).toBeInTheDocument();
    expect(screen.getByText('Error: Unable to load analytics data. Try again.')).toBeInTheDocument();
  });

  it('renders empty state when no search data exists', () => {
    setHookDefaults({
      topSearches: { data: { searches: [] } },
    });

    render(<SearchesTab index="movies" range={defaultRange} />);

    expect(screen.getByTestId('empty-state')).toBeInTheDocument();
    expect(screen.getByText(/No searches recorded yet/)).toBeInTheDocument();
  });

  it('sorts query rows alphabetically when the query header is toggled', async () => {
    const user = userEvent.setup();

    setHookDefaults({
      topSearches: {
        data: {
          searches: [
            { search: 'mango', count: 200, nbHits: 20 },
            { search: 'apple', count: 150, nbHits: 15 },
            { search: 'zebra', count: 100, nbHits: 10 },
          ],
        },
      },
    });

    render(<SearchesTab index="movies" range={defaultRange} />);

    await user.click(screen.getByText('Query'));

    expect(screen.getAllByTestId('search-query').map((cell) => cell.textContent?.trim())).toEqual([
      'zebra',
      'mango',
      'apple',
    ]);

    await user.click(screen.getByText('Query'));

    expect(screen.getAllByTestId('search-query').map((cell) => cell.textContent?.trim())).toEqual([
      'apple',
      'mango',
      'zebra',
    ]);
  });
});
