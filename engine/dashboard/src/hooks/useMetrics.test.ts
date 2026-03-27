import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

const { parsePrometheusTextMock } = vi.hoisted(() => ({
  parsePrometheusTextMock: vi.fn(),
}));

vi.mock('@/lib/prometheusParser', () => ({
  parsePrometheusText: parsePrometheusTextMock,
}));

const { authStateRef, useAuthMock } = vi.hoisted(() => {
  const authStateRef = {
    current: {
      apiKey: null as string | null,
      appId: 'flapjack',
    },
  };
  const useAuthMock = Object.assign(
    vi.fn((selector?: (state: typeof authStateRef.current) => unknown) =>
      selector ? selector(authStateRef.current) : authStateRef.current
    ),
    {
      getState: vi.fn(() => authStateRef.current),
    }
  );

  return { authStateRef, useAuthMock };
});

vi.mock('@/hooks/useAuth', () => ({
  useAuth: useAuthMock,
}));

import {
  getPerIndexMetrics,
  getSystemMetric,
  usePrometheusMetrics,
} from './useMetrics';

function createWrapper() {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

  const wrapper = ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client }, children);

  return { client, wrapper };
}

describe('usePrometheusMetrics', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal('__BACKEND_URL__', 'http://backend.test');
    authStateRef.current = { apiKey: null, appId: 'flapjack' };
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('sends auth headers from the auth store when fetching metrics', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      text: vi.fn().mockResolvedValue('metrics-body'),
    });
    vi.stubGlobal('fetch', fetchMock);
    parsePrometheusTextMock.mockReturnValue([
      {
        name: 'flapjack_documents_count',
        labels: { index: 'products' },
        value: 12,
      },
    ]);
    authStateRef.current = {
      apiKey: 'admin-key',
      appId: 'tenant-app',
    };

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => usePrometheusMetrics(), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(fetchMock).toHaveBeenCalledWith('http://backend.test/metrics', {
      headers: {
        'x-algolia-application-id': 'tenant-app',
        'x-algolia-api-key': 'admin-key',
      },
    });
    expect(parsePrometheusTextMock).toHaveBeenCalledWith('metrics-body');
  });

  it('falls back to the default app id and omits the api key when auth is empty', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      text: vi.fn().mockResolvedValue('metrics-body'),
    });
    vi.stubGlobal('fetch', fetchMock);
    parsePrometheusTextMock.mockReturnValue([]);
    authStateRef.current = { apiKey: null, appId: '' };

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => usePrometheusMetrics(), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(fetchMock).toHaveBeenCalledWith('http://backend.test/metrics', {
      headers: {
        'x-algolia-application-id': 'flapjack',
      },
    });
  });

  it('refetches metrics when the auth scope changes', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        text: vi.fn().mockResolvedValue('metrics-body-1'),
      })
      .mockResolvedValueOnce({
        ok: true,
        text: vi.fn().mockResolvedValue('metrics-body-2'),
      });
    vi.stubGlobal('fetch', fetchMock);
    parsePrometheusTextMock.mockReturnValue([]);

    const { wrapper } = createWrapper();
    const { result, rerender } = renderHook(() => usePrometheusMetrics(), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);

    authStateRef.current = {
      apiKey: 'rotated-admin-key',
      appId: 'tenant-app',
    };
    rerender();

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(fetchMock).toHaveBeenNthCalledWith(2, 'http://backend.test/metrics', {
      headers: {
        'x-algolia-application-id': 'tenant-app',
        'x-algolia-api-key': 'rotated-admin-key',
      },
    });
  });

  it('refetches metrics when the api key rotates within the same app', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        text: vi.fn().mockResolvedValue('metrics-body-1'),
      })
      .mockResolvedValueOnce({
        ok: true,
        text: vi.fn().mockResolvedValue('metrics-body-2'),
      });
    vi.stubGlobal('fetch', fetchMock);
    parsePrometheusTextMock.mockReturnValue([]);
    authStateRef.current = {
      apiKey: 'admin-key-1',
      appId: 'tenant-app',
    };

    const { wrapper } = createWrapper();
    const { result, rerender } = renderHook(() => usePrometheusMetrics(), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);

    authStateRef.current = {
      apiKey: 'admin-key-2',
      appId: 'tenant-app',
    };
    rerender();

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(fetchMock).toHaveBeenNthCalledWith(2, 'http://backend.test/metrics', {
      headers: {
        'x-algolia-application-id': 'tenant-app',
        'x-algolia-api-key': 'admin-key-2',
      },
    });
  });

  it('does not refetch metrics when auth identity values are unchanged', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      text: vi.fn().mockResolvedValue('metrics-body'),
    });
    vi.stubGlobal('fetch', fetchMock);
    parsePrometheusTextMock.mockReturnValue([]);
    authStateRef.current = {
      apiKey: 'stable-admin-key',
      appId: 'tenant-app',
    };

    const { wrapper } = createWrapper();
    const { result, rerender } = renderHook(() => usePrometheusMetrics(), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(fetchMock).toHaveBeenCalledTimes(1);

    authStateRef.current = {
      apiKey: 'stable-admin-key',
      appId: 'tenant-app',
    };
    rerender();

    await waitFor(() => expect(result.current.fetchStatus).toBe('idle'));
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it('stores a non-secret credential fingerprint in the query cache key', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      text: vi.fn().mockResolvedValue('metrics-body'),
    });
    vi.stubGlobal('fetch', fetchMock);
    parsePrometheusTextMock.mockReturnValue([]);
    authStateRef.current = {
      apiKey: 'super-secret-admin-key',
      appId: 'tenant-app',
    };

    const { client, wrapper } = createWrapper();
    const { result } = renderHook(() => usePrometheusMetrics(), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    const metricsQuery = client
      .getQueryCache()
      .getAll()
      .find((query) => query.queryKey[0] === 'prometheus-metrics');

    expect(metricsQuery).toBeDefined();
    expect(String(metricsQuery?.queryKey[2])).toMatch(/^authenticated:/);
    expect(String(metricsQuery?.queryKey[2])).not.toContain('super-secret-admin-key');
  });

  it('uses the selector auth snapshot for headers instead of imperative store reads', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      text: vi.fn().mockResolvedValue('metrics-body'),
    });
    vi.stubGlobal('fetch', fetchMock);
    parsePrometheusTextMock.mockReturnValue([]);
    authStateRef.current = {
      apiKey: 'selector-admin-key',
      appId: 'selector-app',
    };
    useAuthMock.getState.mockReturnValue({
      apiKey: 'imperative-admin-key',
      appId: 'imperative-app',
    });

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => usePrometheusMetrics(), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(fetchMock).toHaveBeenCalledWith('http://backend.test/metrics', {
      headers: {
        'x-algolia-application-id': 'selector-app',
        'x-algolia-api-key': 'selector-admin-key',
      },
    });
    expect(useAuthMock.getState).not.toHaveBeenCalled();
  });
});

describe('metrics helpers', () => {
  it('groups per-index metrics by short name', () => {
    const grouped = getPerIndexMetrics([
      {
        name: 'flapjack_documents_count',
        labels: { index: 'products' },
        value: 12,
      },
      {
        name: 'flapjack_search_requests_total',
        labels: { index: 'products' },
        value: 8,
      },
      {
        name: 'flapjack_documents_count',
        labels: { index: 'books' },
        value: 4,
      },
    ]);

    expect(grouped.get('products')).toEqual({
      documents_count: 12,
      search_requests_total: 8,
    });
    expect(grouped.get('books')).toEqual({
      documents_count: 4,
    });
  });

  it('returns only unlabeled system metrics', () => {
    const metrics = [
      {
        name: 'flapjack_tenants_loaded',
        labels: { index: 'products' },
        value: 1,
      },
      {
        name: 'flapjack_tenants_loaded',
        labels: {},
        value: 3,
      },
    ];

    expect(getSystemMetric(metrics, 'flapjack_tenants_loaded')).toBe(3);
    expect(getSystemMetric(metrics, 'flapjack_bytes_in_total')).toBeUndefined();
  });
});
