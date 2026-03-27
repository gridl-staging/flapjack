import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

vi.mock('@/lib/api', () => ({
  default: {
    get: vi.fn(),
    post: vi.fn(),
    delete: vi.fn(),
  },
}));

const mockToast = vi.fn();

vi.mock('@/hooks/use-toast', () => ({
  useToast: () => ({ toast: mockToast }),
}));

import api from '@/lib/api';
import {
  useSecuritySources,
  useAppendSecuritySource,
  useDeleteSecuritySource,
} from './useSecuritySources';

function createWrapper(client: QueryClient) {
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client }, children);
}

describe('useSecuritySources', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('useSecuritySources fetches entries from /1/security/sources', async () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false } },
    });

    vi.mocked(api.get).mockResolvedValue({
      data: [{ source: '192.168.1.0/24', description: 'office network' }],
    } as never);

    const { result } = renderHook(
      () => useSecuritySources(),
      { wrapper: createWrapper(client) },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(api.get).toHaveBeenCalledWith('/1/security/sources');
    expect(result.current.data).toEqual([
      { source: '192.168.1.0/24', description: 'office network' },
    ]);
  });

  it('useAppendSecuritySource appends an entry, invalidates cache, and toasts on success', async () => {
    const client = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    const invalidateQueriesSpy = vi.spyOn(client, 'invalidateQueries');

    vi.mocked(api.post).mockResolvedValue({ data: { createdAt: '2026-03-16T00:00:00Z' } } as never);

    const { result } = renderHook(
      () => useAppendSecuritySource(),
      { wrapper: createWrapper(client) },
    );

    await act(async () => {
      await result.current.mutateAsync({
        source: '10.0.0.0/8',
        description: 'corp network',
      });
    });

    expect(api.post).toHaveBeenCalledWith('/1/security/sources/append', {
      source: '10.0.0.0/8',
      description: 'corp network',
    });
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ['securitySources'] });
    expect(mockToast).toHaveBeenCalledWith({ title: 'Security source added' });
  });

  it('useDeleteSecuritySource URL-encodes source in delete path, invalidates cache, and toasts on success', async () => {
    const client = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    const invalidateQueriesSpy = vi.spyOn(client, 'invalidateQueries');

    vi.mocked(api.delete).mockResolvedValue({ data: { deletedAt: '2026-03-16T00:00:00Z' } } as never);

    const { result } = renderHook(
      () => useDeleteSecuritySource(),
      { wrapper: createWrapper(client) },
    );

    await act(async () => {
      await result.current.mutateAsync('203.0.113.0/24');
    });

    expect(api.delete).toHaveBeenCalledWith('/1/security/sources/203.0.113.0%2F24');
    expect(invalidateQueriesSpy).toHaveBeenCalledWith({ queryKey: ['securitySources'] });
    expect(mockToast).toHaveBeenCalledWith({ title: 'Security source deleted' });
  });

  it('shows a destructive toast with backend error message when append fails', async () => {
    const client = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    const invalidateQueriesSpy = vi.spyOn(client, 'invalidateQueries');
    const error = {
      response: { data: 'Invalid source CIDR: not-a-cidr' },
      message: 'ignored fallback',
    };

    vi.mocked(api.post).mockRejectedValue(error as never);

    const { result } = renderHook(
      () => useAppendSecuritySource(),
      { wrapper: createWrapper(client) },
    );

    await act(async () => {
      await expect(result.current.mutateAsync({
        source: 'not-a-cidr',
        description: 'invalid input',
      })).rejects.toEqual(error);
    });

    expect(invalidateQueriesSpy).not.toHaveBeenCalled();
    expect(mockToast).toHaveBeenCalledWith({
      title: 'Failed to add security source',
      description: 'Invalid source CIDR: not-a-cidr',
      variant: 'destructive',
    });
  });

  it('shows a destructive toast with backend error message when delete fails', async () => {
    const client = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    });
    const invalidateQueriesSpy = vi.spyOn(client, 'invalidateQueries');
    const error = {
      response: { data: { message: 'delete failed from API' } },
      message: 'ignored fallback',
    };

    vi.mocked(api.delete).mockRejectedValue(error as never);

    const { result } = renderHook(
      () => useDeleteSecuritySource(),
      { wrapper: createWrapper(client) },
    );

    await act(async () => {
      await expect(result.current.mutateAsync('203.0.113.0/24')).rejects.toEqual(error);
    });

    expect(invalidateQueriesSpy).not.toHaveBeenCalled();
    expect(mockToast).toHaveBeenCalledWith({
      title: 'Failed to delete security source',
      description: 'delete failed from API',
      variant: 'destructive',
    });
  });
});
