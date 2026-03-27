import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

vi.mock('@/hooks/useAnalytics', () => ({
  useGeoBreakdown: vi.fn(),
  useGeoTopSearches: vi.fn(),
  useGeoRegions: vi.fn(),
}));

import {
  useGeoBreakdown,
  useGeoRegions,
  useGeoTopSearches,
} from '@/hooks/useAnalytics';
import { GeographyTab } from '@/components/analytics/tabs/GeographyTab';

const mockUseGeoBreakdown = vi.mocked(useGeoBreakdown);
const mockUseGeoRegions = vi.mocked(useGeoRegions);
const mockUseGeoTopSearches = vi.mocked(useGeoTopSearches);

const range = { startDate: '2026-02-01', endDate: '2026-02-07' };

describe('GeographyTab', () => {
  it('renders summary cards and country table when geographic data exists', () => {
    mockUseGeoBreakdown.mockReturnValue({
      data: {
        countries: [
          { country: 'US', count: 150 },
          { country: 'DE', count: 50 },
        ],
        total: 200,
      },
      isLoading: false,
    } as any);
    mockUseGeoTopSearches.mockReturnValue({ data: { searches: [] }, isLoading: false } as any);
    mockUseGeoRegions.mockReturnValue({ data: { regions: [] }, isLoading: false } as any);

    render(<GeographyTab index="movies" range={range} />);

    expect(screen.getByTestId('geo-countries-count')).toBeInTheDocument();
    expect(screen.getByText('Searches by Country')).toBeInTheDocument();
    expect(screen.getByText('United States')).toBeInTheDocument();
    expect(screen.getByText('Germany')).toBeInTheDocument();
    expect(screen.getAllByTestId('country-count')).toHaveLength(2);
  });

  it('renders loading state while country breakdown is loading', () => {
    mockUseGeoBreakdown.mockReturnValue({ data: undefined, isLoading: true } as any);
    mockUseGeoTopSearches.mockReturnValue({ data: undefined, isLoading: false } as any);
    mockUseGeoRegions.mockReturnValue({ data: undefined, isLoading: false } as any);

    render(<GeographyTab index="movies" range={range} />);

    expect(screen.getByTestId('table-skeleton')).toBeInTheDocument();
  });

  it('renders empty state when no countries are returned', () => {
    mockUseGeoBreakdown.mockReturnValue({
      data: { countries: [], total: 0 },
      isLoading: false,
    } as any);
    mockUseGeoTopSearches.mockReturnValue({ data: undefined, isLoading: false } as any);
    mockUseGeoRegions.mockReturnValue({ data: undefined, isLoading: false } as any);

    render(<GeographyTab index="movies" range={range} />);

    expect(screen.getByTestId('empty-state')).toBeInTheDocument();
    expect(screen.getByText('No geographic data')).toBeInTheDocument();
  });

  it('renders an error state when the country breakdown request fails', () => {
    mockUseGeoBreakdown.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('Geography failed'),
    } as any);
    mockUseGeoTopSearches.mockReturnValue({ data: undefined, isLoading: false } as any);
    mockUseGeoRegions.mockReturnValue({ data: undefined, isLoading: false } as any);

    render(<GeographyTab index="movies" range={range} />);

    expect(screen.getByTestId('error-state')).toHaveTextContent('Unable to load analytics data. Try again.');
  });

  it('enters country drill-down on row click and returns with back button', async () => {
    const user = userEvent.setup();

    mockUseGeoBreakdown.mockReturnValue({
      data: {
        countries: [{ country: 'US', count: 150 }],
        total: 150,
      },
      isLoading: false,
    } as any);
    mockUseGeoTopSearches.mockReturnValue({
      data: {
        searches: [
          { search: 'movie', count: 22 },
          { search: 'series', count: 17 },
        ],
      },
      isLoading: false,
    } as any);
    mockUseGeoRegions.mockReturnValue({
      data: {
        regions: [
          { region: 'California', count: 70 },
          { region: 'New York', count: 30 },
        ],
      },
      isLoading: false,
    } as any);

    render(<GeographyTab index="movies" range={range} />);

    await user.click(screen.getByText('United States'));

    expect(screen.getByRole('button', { name: 'All Countries' })).toBeInTheDocument();
    expect(screen.getByText('Top Searches from United States')).toBeInTheDocument();
    expect(screen.getByText('States')).toBeInTheDocument();
    expect(mockUseGeoTopSearches).toHaveBeenLastCalledWith('movies', 'US', range);
    expect(mockUseGeoRegions).toHaveBeenLastCalledWith('movies', 'US', range);

    await user.click(screen.getByRole('button', { name: 'All Countries' }));

    expect(screen.getByText('Searches by Country')).toBeInTheDocument();
  });

  it('renders a drill-down error state when country search details fail', async () => {
    const user = userEvent.setup();

    mockUseGeoBreakdown.mockReturnValue({
      data: {
        countries: [{ country: 'US', count: 150 }],
        total: 150,
      },
      isLoading: false,
    } as any);
    mockUseGeoTopSearches.mockReturnValue({
      data: undefined,
      isLoading: false,
      error: new Error('Country searches failed'),
    } as any);
    mockUseGeoRegions.mockReturnValue({
      data: { regions: [] },
      isLoading: false,
    } as any);

    render(<GeographyTab index="movies" range={range} />);

    await user.click(screen.getByText('United States'));

    expect(screen.getByTestId('error-state')).toHaveTextContent('Unable to load analytics data. Try again.');
  });
});
