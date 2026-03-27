import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, vi } from 'vitest';
import { SearchModeSection } from '../SearchModeSection';
import type { EmbedderConfig } from '@/lib/types';

describe('SearchModeSection', () => {
  const defaultProps = {
    mode: undefined as 'neuralSearch' | 'keywordSearch' | undefined,
    vectorSearchEnabled: true,
    embedders: undefined as Record<string, EmbedderConfig> | undefined,
    onChange: vi.fn(),
  };

  it('renders select with Keyword Search and Neural Search options', () => {
    render(<SearchModeSection {...defaultProps} />);

    const select = screen.getByTestId('search-mode-select');
    expect(select).toBeInTheDocument();

    const options = select.querySelectorAll('option');
    expect(options).toHaveLength(2);
    expect(options[0]).toHaveTextContent('Keyword Search');
    expect(options[1]).toHaveTextContent('Neural Search');
  });

  it('shows Keyword Search as selected when mode is undefined (default behavior)', () => {
    render(<SearchModeSection {...defaultProps} />);

    const select = screen.getByTestId('search-mode-select') as HTMLSelectElement;
    expect(select.value).toBe('keywordSearch');
  });

  it('shows Neural Search as selected when mode is neuralSearch', () => {
    render(<SearchModeSection {...defaultProps} mode="neuralSearch" />);

    const select = screen.getByTestId('search-mode-select') as HTMLSelectElement;
    expect(select.value).toBe('neuralSearch');
  });

  it('calls onChange with mode when selection changes to neuralSearch', async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();

    render(<SearchModeSection {...defaultProps} onChange={onChange} />);

    const select = screen.getByTestId('search-mode-select');
    await user.selectOptions(select, 'neuralSearch');

    expect(onChange).toHaveBeenCalledWith({ mode: 'neuralSearch' });
  });

  it('shows warning when mode is neuralSearch but no embedders configured', () => {
    render(
      <SearchModeSection {...defaultProps} mode="neuralSearch" embedders={undefined} />
    );

    const warning = screen.getByTestId('search-mode-warning');
    expect(warning).toBeInTheDocument();
    expect(warning).toHaveTextContent(/no embedders configured/i);
  });

  it('does not show warning when mode is neuralSearch and embedders exist', () => {
    const embedders: Record<string, EmbedderConfig> = {
      default: { source: 'userProvided', dimensions: 384 },
    };

    render(
      <SearchModeSection {...defaultProps} mode="neuralSearch" embedders={embedders} />
    );

    expect(screen.queryByTestId('search-mode-warning')).not.toBeInTheDocument();
  });

  it('shows compiled-out warning instead of no-embedders warning when vector search is disabled', () => {
    render(
      <SearchModeSection
        {...defaultProps}
        mode="neuralSearch"
        embedders={undefined}
        vectorSearchEnabled={false}
      />
    );

    const compiledOutWarning = screen.getByTestId('search-mode-compiled-out-warning');
    expect(compiledOutWarning).toBeInTheDocument();
    expect(compiledOutWarning).toHaveTextContent(/not compiled in/i);
    expect(screen.queryByTestId('search-mode-warning')).not.toBeInTheDocument();
  });

  it('waits for capability data before enabling neural search controls', () => {
    render(
      <SearchModeSection
        {...defaultProps}
        mode="keywordSearch"
        embedders={undefined}
        vectorSearchEnabled={undefined}
      />
    );

    expect(screen.getByTestId('search-mode-capability-pending')).toBeInTheDocument();
    expect(screen.queryByTestId('search-mode-compiled-out-warning')).not.toBeInTheDocument();
    expect(screen.queryByTestId('search-mode-warning')).not.toBeInTheDocument();
    expect(
      screen.getByRole('option', { name: 'Neural Search' })
    ).toBeDisabled();
  });

  it('calls onChange with keywordSearch when switching back from neuralSearch', async () => {
    const onChange = vi.fn();
    const user = userEvent.setup();

    render(<SearchModeSection {...defaultProps} mode="neuralSearch" onChange={onChange} />);

    const select = screen.getByTestId('search-mode-select');
    await user.selectOptions(select, 'keywordSearch');

    expect(onChange).toHaveBeenCalledWith({ mode: 'keywordSearch' });
  });

  it('shows warning when mode is neuralSearch and embedders is empty object', () => {
    render(
      <SearchModeSection {...defaultProps} mode="neuralSearch" embedders={{}} />
    );

    expect(screen.getByTestId('search-mode-warning')).toBeInTheDocument();
  });
});
