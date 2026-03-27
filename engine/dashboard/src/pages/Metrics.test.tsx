import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Metrics } from './Metrics';

vi.mock('@/hooks/useMetrics', async () => {
  const actual = await vi.importActual<typeof import('@/hooks/useMetrics')>('@/hooks/useMetrics');
  return {
    ...actual,
    usePrometheusMetrics: vi.fn(),
  };
});

vi.mock('@/hooks/useSystemStatus', () => ({
  useHealthDetail: vi.fn(),
}));

import { usePrometheusMetrics } from '@/hooks/useMetrics';
import { useHealthDetail } from '@/hooks/useSystemStatus';

const MOCK_METRICS = [
  { name: 'flapjack_search_requests_total', labels: { index: 'books' }, value: 120 },
  { name: 'flapjack_write_operations_total', labels: { index: 'books' }, value: 40 },
  { name: 'flapjack_read_requests_total', labels: { index: 'books' }, value: 80 },
  { name: 'flapjack_bytes_in_total', labels: { index: 'books' }, value: 1024 },
  { name: 'flapjack_documents_count', labels: { index: 'books' }, value: 30 },
  { name: 'flapjack_storage_bytes', labels: { index: 'books' }, value: 4096 },
  { name: 'flapjack_oplog_current_seq', labels: { index: 'books' }, value: 11 },
  { name: 'flapjack_search_requests_total', labels: { index: 'movies' }, value: 80 },
  { name: 'flapjack_write_operations_total', labels: { index: 'movies' }, value: 20 },
  { name: 'flapjack_read_requests_total', labels: { index: 'movies' }, value: 60 },
  { name: 'flapjack_bytes_in_total', labels: { index: 'movies' }, value: 2048 },
  { name: 'flapjack_documents_count', labels: { index: 'movies' }, value: 40 },
  { name: 'flapjack_storage_bytes', labels: { index: 'movies' }, value: 8192 },
  { name: 'flapjack_oplog_current_seq', labels: { index: 'movies' }, value: 22 },
  { name: 'flapjack_tenants_loaded', labels: {}, value: 2 },
];

describe('Metrics page', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(usePrometheusMetrics).mockReturnValue({
      data: MOCK_METRICS,
      isLoading: false,
      isError: false,
    } as unknown as ReturnType<typeof usePrometheusMetrics>);
    vi.mocked(useHealthDetail).mockReturnValue({
      data: {
        version: '0.1.0',
        uptime_secs: 120,
        capabilities: {
          vectorSearch: true,
          vectorSearchLocal: true,
        },
      },
    } as unknown as ReturnType<typeof useHealthDetail>);
  });

  it('renders overview cards and per-index tab shell with index rows', async () => {
    const user = userEvent.setup();
    render(<Metrics />);

    expect(screen.getByRole('tab', { name: 'Overview' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Per-Index' })).toBeInTheDocument();
    expect(screen.getByTestId('metrics-total-searches')).toHaveTextContent('200');
    expect(screen.getByTestId('metrics-total-docs')).toHaveTextContent('70');

    await user.click(screen.getByRole('tab', { name: 'Per-Index' }));

    expect(screen.getByTestId('metrics-per-index-table')).toBeInTheDocument();
    expect(screen.getByTestId('metrics-index-row-books')).toBeInTheDocument();
    expect(screen.getByTestId('metrics-index-row-movies')).toBeInTheDocument();
  });
});
