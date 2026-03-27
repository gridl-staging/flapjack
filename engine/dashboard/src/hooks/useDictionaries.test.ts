import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

vi.mock('@/lib/api', () => ({
  default: {
    post: vi.fn(),
  },
}));

const mockToast = vi.fn();

vi.mock('@/hooks/use-toast', () => ({
  useToast: () => ({ toast: mockToast }),
}));

import api from '@/lib/api';
import {
  useDictionarySearch,
  useAddDictionaryEntry,
  useDeleteDictionaryEntry,
} from './useDictionaries';

function createWrapper(client: QueryClient) {
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client }, children);
}

describe('useDictionaries', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('useDictionarySearch returns data from /1/dictionaries/:name/search', async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    vi.mocked(api.post).mockResolvedValue({
      data: {
        hits: [{ objectID: 'stop-1', word: 'the', language: 'en', state: 'enabled' }],
        nbHits: 1,
        page: 0,
        nbPages: 1,
      },
    } as never);

    const { result } = renderHook(
      () => useDictionarySearch('stopwords', 'the'),
      { wrapper: createWrapper(client) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(api.post).toHaveBeenCalledWith('/1/dictionaries/stopwords/search', {
      query: 'the',
    });
    expect(result.current.data).toEqual({
      hits: [{ objectID: 'stop-1', word: 'the', language: 'en', state: 'enabled' }],
      nbHits: 1,
      page: 0,
      nbPages: 1,
    });
  });

  it('useAddDictionaryEntry posts addEntry batch and invalidates query cache', async () => {
    const client = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    const invalidateQueriesSpy = vi.spyOn(client, 'invalidateQueries');

    vi.mocked(api.post).mockResolvedValue({ data: { taskID: 1 } } as never);

    const { result } = renderHook(
      () => useAddDictionaryEntry('stopwords'),
      { wrapper: createWrapper(client) },
    );

    await act(async () => {
      await result.current.mutateAsync({
        objectID: 'stop-2',
        word: 'a',
        language: 'en',
        state: 'enabled',
      });
    });

    expect(api.post).toHaveBeenCalledWith('/1/dictionaries/stopwords/batch', {
      requests: [
        {
          action: 'addEntry',
          body: {
            objectID: 'stop-2',
            word: 'a',
            language: 'en',
            state: 'enabled',
          },
        },
      ],
    });
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ['dictionaries', 'stopwords'] });
    expect(mockToast).toHaveBeenCalledWith({ title: 'Dictionary entry added' });
  });

  it('useAddDictionaryEntry shows destructive toast on mutation failure', async () => {
    const client = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    const invalidateQueriesSpy = vi.spyOn(client, 'invalidateQueries');
    const error = new Error('request failed');

    vi.mocked(api.post).mockRejectedValue(error as never);

    const { result } = renderHook(
      () => useAddDictionaryEntry('stopwords'),
      { wrapper: createWrapper(client) },
    );

    await act(async () => {
      await expect(result.current.mutateAsync({
        objectID: 'stop-err',
        word: 'err',
        language: 'en',
        state: 'enabled',
      })).rejects.toThrow('request failed');
    });

    expect(invalidateQueriesSpy).not.toHaveBeenCalled();
    expect(mockToast).toHaveBeenCalledWith({
      title: 'Failed to add dictionary entry',
      description: 'request failed',
      variant: 'destructive',
    });
  });

  it('useAddDictionaryEntry reads backend message fields from object error payloads', async () => {
    const client = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    const error = {
      response: { data: { message: 'add failed from API' } },
      message: 'ignored fallback',
    };

    vi.mocked(api.post).mockRejectedValue(error as never);

    const { result } = renderHook(
      () => useAddDictionaryEntry('stopwords'),
      { wrapper: createWrapper(client) },
    );

    await act(async () => {
      await expect(result.current.mutateAsync({
        objectID: 'stop-obj-err',
        word: 'err',
        language: 'en',
        state: 'enabled',
      })).rejects.toEqual(error);
    });

    expect(mockToast).toHaveBeenCalledWith({
      title: 'Failed to add dictionary entry',
      description: 'add failed from API',
      variant: 'destructive',
    });
  });

  it('useDeleteDictionaryEntry posts deleteEntry batch and invalidates query cache', async () => {
    const client = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    const invalidateQueriesSpy = vi.spyOn(client, 'invalidateQueries');

    vi.mocked(api.post).mockResolvedValue({ data: { taskID: 2 } } as never);

    const { result } = renderHook(
      () => useDeleteDictionaryEntry('stopwords'),
      { wrapper: createWrapper(client) },
    );

    await act(async () => {
      await result.current.mutateAsync('stop-2');
    });

    expect(api.post).toHaveBeenCalledWith('/1/dictionaries/stopwords/batch', {
      requests: [
        {
          action: 'deleteEntry',
          body: { objectID: 'stop-2' },
        },
      ],
    });
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ['dictionaries', 'stopwords'] });
    expect(mockToast).toHaveBeenCalledWith({ title: 'Dictionary entry deleted' });
  });

  it('useDeleteDictionaryEntry shows destructive toast on mutation failure', async () => {
    const client = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    const invalidateQueriesSpy = vi.spyOn(client, 'invalidateQueries');
    const error = {
      response: { data: 'delete failed from API' },
      message: 'ignored',
    };

    vi.mocked(api.post).mockRejectedValue(error as never);

    const { result } = renderHook(
      () => useDeleteDictionaryEntry('stopwords'),
      { wrapper: createWrapper(client) },
    );

    await act(async () => {
      await expect(result.current.mutateAsync('stop-err')).rejects.toEqual(error);
    });

    expect(invalidateQueriesSpy).not.toHaveBeenCalled();
    expect(mockToast).toHaveBeenCalledWith({
      title: 'Failed to delete dictionary entry',
      description: 'delete failed from API',
      variant: 'destructive',
    });
  });
});
