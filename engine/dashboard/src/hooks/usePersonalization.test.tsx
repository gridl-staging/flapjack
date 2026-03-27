import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

vi.mock('@/lib/api', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
  },
}));

const mockToast = vi.fn();

vi.mock('@/hooks/use-toast', () => ({
  useToast: () => ({ toast: mockToast }),
}));

import api from '@/lib/api';
import type { PersonalizationStrategy } from '@/lib/types';
import { personalizationKeys } from '@/lib/queryKeys';
import {
  usePersonalizationProfile,
  usePersonalizationStrategy,
  useSaveStrategy,
} from './usePersonalization';

function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
}

function createWrapper(client: QueryClient) {
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client }, children);
}

const TEST_STRATEGY: PersonalizationStrategy = {
  eventsScoring: [
    { eventName: 'Product Viewed', eventType: 'view', score: 20 },
  ],
  facetsScoring: [
    { facetName: 'brand', score: 80 },
  ],
  personalizationImpact: 70,
};

describe('usePersonalization hooks', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns null when strategy GET returns 404', async () => {
    vi.mocked(api.get).mockRejectedValueOnce({ response: { status: 404 } } as never);

    const client = createQueryClient();
    const { result } = renderHook(() => usePersonalizationStrategy(), {
      wrapper: createWrapper(client),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data).toBeNull();
    expect(api.get).toHaveBeenCalledWith('/1/strategies/personalization');
  });

  it('returns null when profile GET returns 404', async () => {
    vi.mocked(api.get).mockRejectedValueOnce({ response: { status: 404 } } as never);

    const client = createQueryClient();
    const { result } = renderHook(() => usePersonalizationProfile('unknown-user'), {
      wrapper: createWrapper(client),
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data).toBeNull();
    expect(api.get).toHaveBeenCalledWith('/1/profiles/personalization/unknown-user');
  });

  it('invalidates personalization strategy query after successful save', async () => {
    vi.mocked(api.post).mockResolvedValueOnce({ data: { updatedAt: '2026-03-15T12:00:00Z' } } as never);

    const client = createQueryClient();
    const setQueryDataSpy = vi.spyOn(client, 'setQueryData');
    const invalidateQueriesSpy = vi.spyOn(client, 'invalidateQueries');

    const { result } = renderHook(() => useSaveStrategy(), {
      wrapper: createWrapper(client),
    });

    await act(async () => {
      await result.current.mutateAsync(TEST_STRATEGY);
    });

    expect(api.post).toHaveBeenCalledWith('/1/strategies/personalization', TEST_STRATEGY);
    expect(setQueryDataSpy).toHaveBeenCalledWith(personalizationKeys.strategy(), TEST_STRATEGY);
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: personalizationKeys.strategy() });
    expect(mockToast).toHaveBeenCalledWith({ title: 'Personalization strategy saved' });
  });
});
