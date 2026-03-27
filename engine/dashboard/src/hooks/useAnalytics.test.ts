import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

// Mock the api module
vi.mock('@/lib/api', () => ({
  default: {
    get: vi.fn(),
  },
}));

import api from '@/lib/api';
import {
  useAddToCartRate,
  usePurchaseRate,
  useRevenue,
  useCountries,
  type DateRange,
} from './useAnalytics';

function createWrapper() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client: qc }, children);
}

const range: DateRange = { startDate: '2026-02-01', endDate: '2026-02-07' };

describe('useAddToCartRate', () => {
  beforeEach(() => vi.clearAllMocks());

  it('calls the correct endpoint with index and date range', async () => {
    const mockData = {
      rate: 0.25,
      addToCartCount: 5,
      trackedSearchCount: 20,
      dates: [
        { date: '2026-02-01', rate: 0.25, addToCartCount: 5, trackedSearchCount: 20 },
      ],
    };
    vi.mocked(api.get).mockResolvedValue({ data: mockData });

    const { result } = renderHook(() => useAddToCartRate('products', range), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('/2/conversions/addToCartRate')
    );
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('index=products')
    );
    expect(result.current.data).toEqual(mockData);
  });

  it('is disabled when index is empty', () => {
    const { result } = renderHook(() => useAddToCartRate('', range), {
      wrapper: createWrapper(),
    });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('passes country param when provided', async () => {
    vi.mocked(api.get).mockResolvedValue({ data: { rate: null, addToCartCount: 0, trackedSearchCount: 0, dates: [] } });

    const { result } = renderHook(
      () => useAddToCartRate('products', range, 'US'),
      { wrapper: createWrapper() }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('country=US')
    );
  });
});

describe('usePurchaseRate', () => {
  beforeEach(() => vi.clearAllMocks());

  it('calls the correct endpoint with index and date range', async () => {
    const mockData = {
      rate: 0.1,
      purchaseCount: 2,
      trackedSearchCount: 20,
      dates: [
        { date: '2026-02-01', rate: 0.1, purchaseCount: 2, trackedSearchCount: 20 },
      ],
    };
    vi.mocked(api.get).mockResolvedValue({ data: mockData });

    const { result } = renderHook(() => usePurchaseRate('products', range), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('/2/conversions/purchaseRate')
    );
    expect(result.current.data).toEqual(mockData);
  });

  it('is disabled when index is empty', () => {
    const { result } = renderHook(() => usePurchaseRate('', range), {
      wrapper: createWrapper(),
    });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('passes country param when provided', async () => {
    vi.mocked(api.get).mockResolvedValue({ data: { rate: null, purchaseCount: 0, trackedSearchCount: 0, dates: [] } });

    const { result } = renderHook(
      () => usePurchaseRate('products', range, 'DE'),
      { wrapper: createWrapper() }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('country=DE')
    );
  });
});

describe('useRevenue', () => {
  beforeEach(() => vi.clearAllMocks());

  it('calls the correct endpoint and returns currency map', async () => {
    const mockData = {
      currencies: {
        USD: { currency: 'USD', revenue: 199.99 },
      },
      dates: [
        {
          date: '2026-02-01',
          currencies: { USD: { currency: 'USD', revenue: 199.99 } },
        },
      ],
    };
    vi.mocked(api.get).mockResolvedValue({ data: mockData });

    const { result } = renderHook(() => useRevenue('products', range), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('/2/conversions/revenue')
    );
    expect(result.current.data).toEqual(mockData);
  });

  it('handles empty revenue data', async () => {
    const mockData = { currencies: {}, dates: [] };
    vi.mocked(api.get).mockResolvedValue({ data: mockData });

    const { result } = renderHook(() => useRevenue('products', range), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data?.currencies).toEqual({});
    expect(result.current.data?.dates).toEqual([]);
  });

  it('is disabled when index is empty', () => {
    const { result } = renderHook(() => useRevenue('', range), {
      wrapper: createWrapper(),
    });
    expect(result.current.fetchStatus).toBe('idle');
  });

  it('passes country param when provided', async () => {
    vi.mocked(api.get).mockResolvedValue({ data: { currencies: {}, dates: [] } });

    const { result } = renderHook(
      () => useRevenue('products', range, 'US'),
      { wrapper: createWrapper() }
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('country=US')
    );
  });
});

describe('useCountries', () => {
  beforeEach(() => vi.clearAllMocks());

  it('calls the correct endpoint and returns country list', async () => {
    const mockData = {
      countries: [
        { country: 'US', count: 150 },
        { country: 'DE', count: 42 },
      ],
    };
    vi.mocked(api.get).mockResolvedValue({ data: mockData });

    const { result } = renderHook(() => useCountries('products', range), {
      wrapper: createWrapper(),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('/2/countries')
    );
    expect(result.current.data).toEqual(mockData);
  });

  it('is disabled when index is empty', () => {
    const { result } = renderHook(() => useCountries('', range), {
      wrapper: createWrapper(),
    });
    expect(result.current.fetchStatus).toBe('idle');
  });
});
