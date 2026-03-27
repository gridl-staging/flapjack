import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('@/lib/api', () => ({
  default: {
    get: vi.fn(),
  },
}));

import api from '@/lib/api';
import { useHealthDetail } from '@/hooks/useSystemStatus';

const HEALTH_DETAIL_FIXTURE = {
  status: 'ok',
  active_writers: 1,
  max_concurrent_writers: 4,
  facet_cache_entries: 2,
  facet_cache_cap: 1000,
  tenants_loaded: 1,
  uptime_secs: 10,
  version: '0.1.0',
  heap_allocated_mb: 256,
  system_limit_mb: 1024,
  pressure_level: 'normal',
  allocator: 'system',
  build_profile: 'release',
  capabilities: {
    vectorSearch: true,
    vectorSearchLocal: true,
  },
};

function createWrapper() {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });

  return {
    client,
    wrapper: ({ children }: { children: React.ReactNode }) =>
      React.createElement(QueryClientProvider, { client }, children),
  };
}

describe('useHealthDetail', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('fetches /health on the health-detail query path and exposes capabilities shape', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: HEALTH_DETAIL_FIXTURE,
    } as never);

    const { client, wrapper } = createWrapper();
    const { result } = renderHook(() => useHealthDetail(), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(api.get).toHaveBeenCalledWith('/health');
    expect(client.getQueryData(['health-detail'])).toBeDefined();
    expect(result.current.data?.capabilities).toEqual({
      vectorSearch: true,
      vectorSearchLocal: true,
    });
  });

  it('returns stable false defaults for missing capability booleans', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: {
        ...HEALTH_DETAIL_FIXTURE,
        capabilities: undefined,
      },
    } as never);

    const { wrapper } = createWrapper();
    const { result } = renderHook(() => useHealthDetail(), {
      wrapper,
    });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data?.capabilities).toEqual({
      vectorSearch: false,
      vectorSearchLocal: false,
    });
  });
});
