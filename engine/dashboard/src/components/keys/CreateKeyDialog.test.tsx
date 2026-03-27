import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CreateKeyDialog } from './CreateKeyDialog';

const mockUseCreateApiKey = vi.hoisted(() => vi.fn());
const mockUseIndexes = vi.hoisted(() => vi.fn());

vi.mock('@/hooks/useApiKeys', () => ({
  useCreateApiKey: mockUseCreateApiKey,
}));

vi.mock('@/hooks/useIndexes', () => ({
  useIndexes: mockUseIndexes,
}));

describe('CreateKeyDialog', () => {
  const mutateAsync = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();

    mutateAsync.mockResolvedValue({ value: 'created-key' });

    mockUseCreateApiKey.mockReturnValue({
      mutateAsync,
      isPending: false,
    });

    mockUseIndexes.mockReturnValue({
      data: [],
      isLoading: false,
      error: null,
    });
  });

  it('normalizes comma-separated and newline-separated restrict sources before submit', async () => {
    const user = userEvent.setup();

    render(<CreateKeyDialog open={true} onOpenChange={vi.fn()} />);

    await user.type(screen.getByPlaceholderText('e.g., Frontend search key'), 'E2E Restrict Sources Key');
    await user.type(
      screen.getByLabelText(/Restrict Sources/i),
      '10.0.0.0/8, 192.168.1.0/24\n172.16.0.0/12,\n 203.0.113.0/24 ',
    );
    await user.click(screen.getByRole('button', { name: 'Create Key' }));

    await waitFor(() => {
      expect(mutateAsync).toHaveBeenCalledTimes(1);
    });

    expect(mutateAsync).toHaveBeenCalledWith(
      expect.objectContaining({
        description: 'E2E Restrict Sources Key',
        acl: ['search'],
        restrictSources: ['10.0.0.0/8', '192.168.1.0/24', '172.16.0.0/12', '203.0.113.0/24'],
      }),
    );
  });

  it('omits restrictSources from payload when field is blank or whitespace-only', async () => {
    const user = userEvent.setup();

    render(<CreateKeyDialog open={true} onOpenChange={vi.fn()} />);

    await user.type(screen.getByPlaceholderText('e.g., Frontend search key'), 'E2E No Restrict Sources');
    await user.type(screen.getByLabelText(/Restrict Sources/i), '  ,\n   ');
    await user.click(screen.getByRole('button', { name: 'Create Key' }));

    await waitFor(() => {
      expect(mutateAsync).toHaveBeenCalledTimes(1);
    });

    const payload = mutateAsync.mock.calls[0]?.[0] as Record<string, unknown>;
    expect(payload).not.toHaveProperty('restrictSources');
    expect(payload.description).toBe('E2E No Restrict Sources');
    expect(payload.acl).toEqual(['search']);
  });

  it('resets form fields after closing and reopening the dialog', async () => {
    const user = userEvent.setup();
    const { rerender } = render(<CreateKeyDialog open={true} onOpenChange={vi.fn()} />);

    await user.type(screen.getByPlaceholderText('e.g., Frontend search key'), 'Sticky API Key');
    await user.type(screen.getByLabelText(/Restrict Sources/i), '10.0.0.0/8');

    expect(screen.getByPlaceholderText('e.g., Frontend search key')).toHaveValue('Sticky API Key');
    expect(screen.getByLabelText(/Restrict Sources/i)).toHaveValue('10.0.0.0/8');

    rerender(<CreateKeyDialog open={false} onOpenChange={vi.fn()} />);
    rerender(<CreateKeyDialog open={true} onOpenChange={vi.fn()} />);

    expect(screen.getByPlaceholderText('e.g., Frontend search key')).toHaveValue('');
    expect(screen.getByLabelText(/Restrict Sources/i)).toHaveValue('');
    expect(screen.getByTestId('selected-permissions')).toHaveTextContent('search');
  });
});
