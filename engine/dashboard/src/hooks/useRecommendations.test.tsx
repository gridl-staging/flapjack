import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

vi.mock('@/lib/api', () => ({
  default: {
    post: vi.fn(),
  },
}));

import api from '@/lib/api';
import {
  DEFAULT_RECOMMENDATION_MAX_RECOMMENDATIONS,
  DEFAULT_RECOMMENDATION_THRESHOLD,
} from '@/lib/recommendation-contract';
import { useRecommendations } from './useRecommendations';

function createWrapper() {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client }, children);
}

describe('useRecommendations', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('posts batched preview payload with shared defaults for omitted threshold and maxRecommendations', async () => {
    const responseResults = [
      {
        hits: [{ objectID: 'sku-2', _score: 98 }],
        processingTimeMS: 4,
      },
    ];
    vi.mocked(api.post).mockResolvedValueOnce({
      data: { results: responseResults },
    } as never);

    const { result } = renderHook(() => useRecommendations(), { wrapper: createWrapper() });

    let previewResults: unknown;
    await act(async () => {
      previewResults = await result.current.mutateAsync({
        indexName: 'products',
        model: 'related-products',
        objectID: 'sku-1',
      });
    });

    expect(api.post).toHaveBeenCalledWith('/1/indexes/*/recommendations', {
      requests: [
        {
          indexName: 'products',
          model: 'related-products',
          objectID: 'sku-1',
          threshold: DEFAULT_RECOMMENDATION_THRESHOLD,
          maxRecommendations: DEFAULT_RECOMMENDATION_MAX_RECOMMENDATIONS,
        },
      ],
    });
    expect(previewResults).toEqual(responseResults);
  });

  it('supports batched preview inputs and preserves explicit threshold and maxRecommendations values', async () => {
    vi.mocked(api.post).mockResolvedValueOnce({
      data: { results: [] },
    } as never);

    const { result } = renderHook(() => useRecommendations(), { wrapper: createWrapper() });

    await act(async () => {
      await result.current.mutateAsync([
        {
          indexName: 'products',
          model: 'trending-items',
        },
        {
          indexName: 'products',
          model: 'trending-facets',
          facetName: 'brand',
          facetValue: 'Apple',
          threshold: 55,
          maxRecommendations: 5,
        },
      ]);
    });

    expect(api.post).toHaveBeenCalledWith('/1/indexes/*/recommendations', {
      requests: [
        {
          indexName: 'products',
          model: 'trending-items',
          threshold: DEFAULT_RECOMMENDATION_THRESHOLD,
          maxRecommendations: DEFAULT_RECOMMENDATION_MAX_RECOMMENDATIONS,
        },
        {
          indexName: 'products',
          model: 'trending-facets',
          facetName: 'brand',
          facetValue: 'Apple',
          threshold: 55,
          maxRecommendations: 5,
        },
      ],
    });
  });

  it('passes through empty-hit responses', async () => {
    const emptyResults = [{ hits: [], processingTimeMS: 2 }];
    vi.mocked(api.post).mockResolvedValueOnce({
      data: { results: emptyResults },
    } as never);

    const { result } = renderHook(() => useRecommendations(), { wrapper: createWrapper() });

    await expect(
      result.current.mutateAsync({
        indexName: 'products',
        model: 'trending-items',
      }),
    ).resolves.toEqual(emptyResults);
  });

  it('propagates API errors', async () => {
    const error = new Error('recommendations failed');
    vi.mocked(api.post).mockRejectedValueOnce(error as never);

    const { result } = renderHook(() => useRecommendations(), { wrapper: createWrapper() });

    await expect(
      result.current.mutateAsync({
        indexName: 'products',
        model: 'trending-items',
      }),
    ).rejects.toThrow('recommendations failed');
  });
});
