import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import React from 'react';

vi.mock('@/lib/api', () => ({
  default: {
    put: vi.fn(),
  },
}));

const mockToast = vi.fn();

vi.mock('@/hooks/use-toast', () => ({
  useToast: () => ({ toast: mockToast }),
}));

import api from '@/lib/api';
import { useUpdateSettings } from './useSettings';

function createWrapper() {
  const client = new QueryClient({
    defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
  });

  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client }, children);
}

describe('useUpdateSettings', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('surfaces unsupportedParams from HTTP 207 responses', async () => {
    vi.mocked(api.put).mockResolvedValue({
      status: 207,
      data: {
        unsupportedParams: ['renderingContent', 'replicas'],
      },
    } as never);

    const { result } = renderHook(() => useUpdateSettings('products'), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      await result.current.mutateAsync({ ranking: ['typo'] });
    });

    expect(mockToast).toHaveBeenCalledWith({
      title: 'Settings partially saved',
      description: 'Unsupported settings were ignored: renderingContent, replicas.',
    });
  });
});
