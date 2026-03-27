import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { SecuritySources } from './SecuritySources';

const mockUseSecuritySources = vi.hoisted(() => vi.fn());
const mockUseAppendSecuritySource = vi.hoisted(() => vi.fn());
const mockUseDeleteSecuritySource = vi.hoisted(() => vi.fn());

vi.mock('@/hooks/useSecuritySources', () => ({
  useSecuritySources: mockUseSecuritySources,
  useAppendSecuritySource: mockUseAppendSecuritySource,
  useDeleteSecuritySource: mockUseDeleteSecuritySource,
}));

describe('SecuritySources', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUseAppendSecuritySource.mockReturnValue({
      isPending: false,
      mutateAsync: vi.fn(),
    });
    mockUseDeleteSecuritySource.mockReturnValue({
      isPending: false,
      mutateAsync: vi.fn(),
    });
  });

  it('shows an error state when the security sources query fails', () => {
    mockUseSecuritySources.mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: true,
      error: new Error('Request failed with status code 500'),
    });

    render(<SecuritySources />);

    expect(screen.getByText('Unable to load security sources.')).toBeInTheDocument();
    expect(screen.queryByTestId('security-sources-empty-state')).not.toBeInTheDocument();
  });
});
