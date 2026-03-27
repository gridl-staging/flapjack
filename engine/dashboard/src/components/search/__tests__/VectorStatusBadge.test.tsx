import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { VectorStatusBadge } from '../VectorStatusBadge';
import type { EmbedderConfig } from '@/lib/types';

describe('VectorStatusBadge', () => {
  it('renders nothing when no embedders configured', () => {
    const { container } = render(
      <VectorStatusBadge
        embedders={undefined}
        mode={undefined}
        vectorSearchEnabled={true}
      />
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders "Vector Search" badge with embedder count when embedders configured', () => {
    const embedders: Record<string, EmbedderConfig> = {
      default: { source: 'userProvided', dimensions: 384 },
    };

    render(
      <VectorStatusBadge
        embedders={embedders}
        mode={undefined}
        vectorSearchEnabled={true}
      />
    );

    const badge = screen.getByTestId('vector-status-badge');
    expect(badge).toBeInTheDocument();
    expect(badge).toHaveTextContent(/vector search/i);
    expect(badge).toHaveTextContent(/1 embedder/i);
  });

  it('shows mode label: "Neural" when mode is neuralSearch', () => {
    const embedders: Record<string, EmbedderConfig> = {
      default: { source: 'userProvided', dimensions: 384 },
    };

    render(
      <VectorStatusBadge
        embedders={embedders}
        mode="neuralSearch"
        vectorSearchEnabled={true}
      />
    );

    const badge = screen.getByTestId('vector-status-badge');
    expect(badge).toHaveTextContent(/neural/i);
  });

  it('shows mode label: "Keyword" when mode is keywordSearch or undefined', () => {
    const embedders: Record<string, EmbedderConfig> = {
      default: { source: 'userProvided', dimensions: 384 },
    };

    const { rerender } = render(
      <VectorStatusBadge
        embedders={embedders}
        mode="keywordSearch"
        vectorSearchEnabled={true}
      />
    );
    expect(screen.getByTestId('vector-status-badge')).toHaveTextContent(
      /keyword/i
    );

    rerender(
      <VectorStatusBadge
        embedders={embedders}
        mode={undefined}
        vectorSearchEnabled={true}
      />
    );
    expect(screen.getByTestId('vector-status-badge')).toHaveTextContent(
      /keyword/i
    );
  });

  it('renders nothing when embedders is empty object', () => {
    const { container } = render(
      <VectorStatusBadge
        embedders={{}}
        mode={undefined}
        vectorSearchEnabled={true}
      />
    );
    expect(container.firstChild).toBeNull();
  });

  it('shows plural "embedders" when multiple embedders configured', () => {
    const embedders: Record<string, EmbedderConfig> = {
      default: { source: 'userProvided', dimensions: 384 },
      backup: { source: 'openAi', model: 'text-embedding-3-small' },
    };

    render(
      <VectorStatusBadge
        embedders={embedders}
        mode={undefined}
        vectorSearchEnabled={true}
      />
    );

    const badge = screen.getByTestId('vector-status-badge');
    expect(badge).toHaveTextContent(/2 embedders/i);
  });

  it('renders compiled-out badge when vector search is disabled', () => {
    const embedders: Record<string, EmbedderConfig> = {
      default: { source: 'userProvided', dimensions: 384 },
    };

    render(
      <VectorStatusBadge
        embedders={embedders}
        mode="neuralSearch"
        vectorSearchEnabled={false}
      />
    );

    const badge = screen.getByTestId('vector-status-badge-disabled');
    expect(badge).toBeInTheDocument();
    expect(badge).toHaveTextContent(/not compiled in/i);
    expect(screen.queryByTestId('vector-status-badge')).not.toBeInTheDocument();
  });

  it('renders nothing while capability data is unresolved', () => {
    const embedders: Record<string, EmbedderConfig> = {
      default: { source: 'userProvided', dimensions: 384 },
    };

    const { container } = render(
      <VectorStatusBadge
        embedders={embedders}
        mode="neuralSearch"
        vectorSearchEnabled={undefined}
      />
    );

    expect(container.firstChild).toBeNull();
  });
});
