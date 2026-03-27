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
  useConcludeExperiment,
  useCreateExperiment,
  useDeleteExperiment,
  useExperiment,
  useExperimentResults,
  useExperiments,
  useStopExperiment,
} from './useExperiments';

function createWrapper() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return ({ children }: { children: React.ReactNode }) =>
    React.createElement(QueryClientProvider, { client }, children);
}

const ALGOLIA_EXPERIMENT = {
  abTestID: 12,
  name: 'algolia-exp',
  status: 'active',
  createdAt: '2026-03-14T10:00:00Z',
  endAt: '2026-03-28T10:00:00Z',
  variants: [
    {
      index: 'products',
      trafficPercentage: 40,
      description: 'control',
    },
    {
      index: 'products_v2',
      trafficPercentage: 60,
      description: 'variant',
      customSearchParameters: { filters: 'brand:Apple' },
    },
  ],
};

const STARTED_ALGOLIA_EXPERIMENT = {
  ...ALGOLIA_EXPERIMENT,
  startedAt: '2026-03-15T10:00:00Z',
};

const DRAFT_ALGOLIA_EXPERIMENT = {
  ...ALGOLIA_EXPERIMENT,
  id: '99',
  abTestID: undefined,
  status: 'draft',
};

const STOPPED_ALGOLIA_EXPERIMENT = {
  ...ALGOLIA_EXPERIMENT,
  id: '100',
  abTestID: undefined,
  status: 'stopped',
};

const CONCLUDED_ALGOLIA_EXPERIMENT = {
  ...ALGOLIA_EXPERIMENT,
  id: '101',
  abTestID: undefined,
  status: 'concluded',
};

const UNKNOWN_STATUS_ALGOLIA_EXPERIMENT = {
  ...ALGOLIA_EXPERIMENT,
  id: '102',
  abTestID: undefined,
  status: 'paused',
};

const EXPIRED_STATUS_ALGOLIA_EXPERIMENT = {
  ...ALGOLIA_EXPERIMENT,
  id: '103',
  abTestID: undefined,
  status: 'expired',
};

const CREATE_PAYLOAD = {
  name: 'created-from-hook',
  indexName: 'products',
  trafficSplit: 0.6,
  control: { name: 'control' },
  variant: {
    name: 'variant',
    indexName: 'products_v2',
    queryOverrides: { filters: 'brand:Apple' },
  },
  primaryMetric: 'ctr',
  minimumDays: 14,
};

const UNSAFE_EXPERIMENT_ID = 'exp/unsafe?draft#frag';
const ENCODED_UNSAFE_EXPERIMENT_ID = encodeURIComponent(UNSAFE_EXPERIMENT_ID);

describe('useExperiments normalization', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('maps Algolia list payload to dashboard Experiment model fields', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: { abtests: [STARTED_ALGOLIA_EXPERIMENT] },
    });

    const { result } = renderHook(() => useExperiments(), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data).toEqual([
      expect.objectContaining({
        id: '12',
        name: 'algolia-exp',
        status: 'running',
        indexName: 'products',
        trafficSplit: 0.6,
        primaryMetric: 'ctr',
        control: { name: 'control' },
        variant: {
          name: 'variant',
          indexName: 'products_v2',
          queryOverrides: { filters: 'brand:Apple' },
        },
      }),
    ]);
    expect(result.current.data?.[0].createdAt).toBe(Date.parse('2026-03-14T10:00:00Z'));
    expect(result.current.data?.[0].endedAt).toBe(Date.parse('2026-03-28T10:00:00Z'));
  });

  it('maps Algolia single-experiment payload for detail hook', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: STARTED_ALGOLIA_EXPERIMENT,
    });

    const { result } = renderHook(() => useExperiment('12'), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(
      expect.objectContaining({
        id: '12',
        status: 'running',
        indexName: 'products',
        trafficSplit: 0.6,
      }),
    );
  });

  it('preserves explicit draft status from list payloads', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: { abtests: [DRAFT_ALGOLIA_EXPERIMENT] },
    });

    const { result } = renderHook(() => useExperiments(), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([
      expect.objectContaining({
        id: '99',
        status: 'draft',
      }),
    ]);
  });

  it('treats active experiments without startedAt as drafts', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: { abtests: [{ ...ALGOLIA_EXPERIMENT, startedAt: undefined }] },
    });

    const { result } = renderHook(() => useExperiments(), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([
      expect.objectContaining({
        id: '12',
        status: 'draft',
      }),
    ]);
  });

  it('passes through explicit stopped and concluded statuses from list payloads', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: {
        abtests: [
          STOPPED_ALGOLIA_EXPERIMENT,
          CONCLUDED_ALGOLIA_EXPERIMENT,
        ],
      },
    });

    const { result } = renderHook(() => useExperiments(), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([
      expect.objectContaining({
        id: '100',
        status: 'stopped',
      }),
      expect.objectContaining({
        id: '101',
        status: 'concluded',
      }),
    ]);
  });

  it('falls back unknown list statuses to draft', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: { abtests: [UNKNOWN_STATUS_ALGOLIA_EXPERIMENT] },
    });

    const { result } = renderHook(() => useExperiments(), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([
      expect.objectContaining({
        id: '102',
        status: 'draft',
      }),
    ]);
  });

  it('preserves explicit expired status from list payloads', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: { abtests: [EXPIRED_STATUS_ALGOLIA_EXPERIMENT] },
    });

    const { result } = renderHook(() => useExperiments(), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([
      expect.objectContaining({
        id: '103',
        status: 'expired',
      }),
    ]);
  });

  it('returns an empty list when Algolia abtests is null', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: { abtests: null },
    });

    const { result } = renderHook(() => useExperiments(), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual([]);
  });

  it('normalizes create payload to Algolia variants contract', async () => {
    vi.mocked(api.post)
      .mockResolvedValueOnce({
        data: { abTestID: 42 },
      })
      .mockResolvedValueOnce({
        data: { abTestID: 42, taskID: 42, index: 'products' },
      });

    const { result } = renderHook(() => useCreateExperiment(), { wrapper: createWrapper() });

    await act(async () => {
      await result.current.mutateAsync(CREATE_PAYLOAD);
    });

    expect(api.post).toHaveBeenNthCalledWith(1, '/2/abtests', {
      name: 'created-from-hook',
      variants: [
        {
          index: 'products',
          trafficPercentage: 40,
          description: 'control',
        },
        {
          index: 'products_v2',
          trafficPercentage: 60,
          description: 'variant',
          customSearchParameters: { filters: 'brand:Apple' },
        },
      ],
      endAt: expect.any(String),
      metrics: [{ name: 'clickThroughRate' }],
    });
    expect(api.post).toHaveBeenNthCalledWith(2, '/2/abtests/42/start');
    expect(mockToast).toHaveBeenCalledWith({ title: 'Experiment launched' });
  });

  it('starts the created experiment when create response uses id field', async () => {
    vi.mocked(api.post)
      .mockResolvedValueOnce({
        data: { id: 'exp-77' },
      })
      .mockResolvedValueOnce({
        data: { id: 'exp-77', taskID: 77, index: 'products' },
      });

    const { result } = renderHook(() => useCreateExperiment(), { wrapper: createWrapper() });

    await act(async () => {
      await result.current.mutateAsync(CREATE_PAYLOAD);
    });

    expect(api.post).toHaveBeenNthCalledWith(2, '/2/abtests/exp-77/start');
    expect(mockToast).toHaveBeenCalledWith({ title: 'Experiment launched' });
  });

  it('fails when create response has no experiment id and does not call start', async () => {
    vi.mocked(api.post).mockResolvedValueOnce({
      data: { taskID: 42, index: 'products' },
    });

    const { result } = renderHook(() => useCreateExperiment(), { wrapper: createWrapper() });

    let thrownError: unknown;

    await act(async () => {
      try {
        await result.current.mutateAsync(CREATE_PAYLOAD);
      } catch (error) {
        thrownError = error;
      }
    });

    expect(api.post).toHaveBeenCalledTimes(1);
    expect(api.post).toHaveBeenCalledWith('/2/abtests', expect.any(Object));
    expect(thrownError).toBeInstanceOf(Error);
    expect((thrownError as Error).message).toContain('missing experiment id');
    expect(mockToast).toHaveBeenCalledWith(
      expect.objectContaining({
        variant: 'destructive',
        title: 'Failed to launch experiment',
      }),
    );
  });

  it('keeps the created draft when launch start fails', async () => {
    vi.mocked(api.post)
      .mockResolvedValueOnce({
        data: { abTestID: 42 },
      })
      .mockRejectedValueOnce(new Error('index already has a running experiment'));

    const { result } = renderHook(() => useCreateExperiment(), { wrapper: createWrapper() });

    let thrownError: unknown;

    await act(async () => {
      try {
        await result.current.mutateAsync(CREATE_PAYLOAD);
      } catch (error) {
        thrownError = error;
      }
    });

    expect(api.delete).not.toHaveBeenCalled();
    expect(thrownError).toBeInstanceOf(Error);
    expect((thrownError as Error).message).toContain('created as a draft');
    expect((thrownError as Error).message).toContain('index already has a running experiment');
    expect(mockToast).toHaveBeenCalledWith(
      expect.objectContaining({
        variant: 'destructive',
        title: 'Failed to launch experiment',
      }),
    );
  });

  it('encodes experiment ids when requesting experiment details', async () => {
    vi.mocked(api.get).mockResolvedValue({
      data: STARTED_ALGOLIA_EXPERIMENT,
    });

    const { result } = renderHook(() => useExperiment(UNSAFE_EXPERIMENT_ID), { wrapper: createWrapper() });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(`/2/abtests/${ENCODED_UNSAFE_EXPERIMENT_ID}`);
  });

  it('encodes experiment ids returned from create before starting the experiment', async () => {
    vi.mocked(api.post)
      .mockResolvedValueOnce({
        data: { id: UNSAFE_EXPERIMENT_ID },
      })
      .mockResolvedValueOnce({
        data: { id: UNSAFE_EXPERIMENT_ID, taskID: 77, index: 'products' },
      });

    const { result } = renderHook(() => useCreateExperiment(), { wrapper: createWrapper() });

    await act(async () => {
      await result.current.mutateAsync(CREATE_PAYLOAD);
    });

    expect(api.post).toHaveBeenNthCalledWith(
      2,
      `/2/abtests/${ENCODED_UNSAFE_EXPERIMENT_ID}/start`,
    );
  });

  it('encodes experiment ids for stop, delete, conclude, and results requests', async () => {
    vi.mocked(api.post).mockResolvedValue({ data: { ok: true } });
    vi.mocked(api.delete).mockResolvedValue({ data: { ok: true } } as never);
    vi.mocked(api.get).mockResolvedValue({ data: { ok: true } });

    const stopHook = renderHook(() => useStopExperiment(), { wrapper: createWrapper() });
    await act(async () => {
      await stopHook.result.current.mutateAsync(UNSAFE_EXPERIMENT_ID);
    });
    expect(api.post).toHaveBeenCalledWith(`/2/abtests/${ENCODED_UNSAFE_EXPERIMENT_ID}/stop`);

    const deleteHook = renderHook(() => useDeleteExperiment(), { wrapper: createWrapper() });
    await act(async () => {
      await deleteHook.result.current.mutateAsync(UNSAFE_EXPERIMENT_ID);
    });
    expect(api.delete).toHaveBeenCalledWith(`/2/abtests/${ENCODED_UNSAFE_EXPERIMENT_ID}`);

    const concludeHook = renderHook(() => useConcludeExperiment(), { wrapper: createWrapper() });
    await act(async () => {
      await concludeHook.result.current.mutateAsync({
        id: UNSAFE_EXPERIMENT_ID,
        payload: {
          winner: 'variant',
          reason: 'significant uplift',
          controlMetric: 0.1,
          variantMetric: 0.2,
          confidence: 0.95,
          significant: true,
          promoted: true,
        },
      });
    });
    expect(api.post).toHaveBeenCalledWith(
      `/2/abtests/${ENCODED_UNSAFE_EXPERIMENT_ID}/conclude`,
      expect.objectContaining({
        winner: 'variant',
      }),
    );

    const resultsHook = renderHook(() => useExperimentResults(UNSAFE_EXPERIMENT_ID), {
      wrapper: createWrapper(),
    });
    await waitFor(() => expect(resultsHook.result.current.isSuccess).toBe(true));
    expect(api.get).toHaveBeenCalledWith(`/2/abtests/${ENCODED_UNSAFE_EXPERIMENT_ID}/results`);
  });
});
