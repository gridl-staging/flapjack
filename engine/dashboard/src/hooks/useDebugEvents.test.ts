import { describe, it, expect, vi, beforeEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

vi.mock('@/lib/api', () => ({
  default: {
    get: vi.fn(),
  },
}));

import api from '@/lib/api';
import { useDebugEvents } from './useDebugEvents';

function createWrapper() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client: qc }, children);
}

describe('useDebugEvents', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('includes from/until query params when provided', async () => {
    vi.mocked(api.get).mockResolvedValue({ data: { events: [], count: 0 } });

    const { result } = renderHook(
      () =>
        useDebugEvents({
          index: 'products',
          eventType: 'view',
          status: 'ok',
          limit: 50,
          from: 1700000000000,
          until: 1700003600000,
        }),
      { wrapper: createWrapper() },
    );

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('from=1700000000000'),
    );
    expect(api.get).toHaveBeenCalledWith(
      expect.stringContaining('until=1700003600000'),
    );
  });
});
