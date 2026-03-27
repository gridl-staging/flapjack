import type { ComponentProps } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { SettingsForm } from '../SettingsForm';
import type { IndexSettings } from '@/lib/types';

vi.mock('@/hooks/useIndexFields', () => ({
  useIndexFields: vi.fn(),
}));

vi.mock('@/hooks/useReindex', () => ({
  useReindex: vi.fn(),
}));

vi.mock('@/hooks/useSystemStatus', () => ({
  useHealthDetail: vi.fn(),
}));

import { useIndexFields } from '@/hooks/useIndexFields';
import { useReindex } from '@/hooks/useReindex';
import { useHealthDetail } from '@/hooks/useSystemStatus';

const BASE_SETTINGS: Partial<IndexSettings> = {
  searchableAttributes: ['title'],
  hitsPerPage: 20,
  ranking: ['typo'],
  customRanking: ['desc(popularity)'],
  queryType: 'prefixLast',
  queryLanguages: ['en'],
  removeStopWords: true,
  ignorePlurals: true,
  attributesForFaceting: ['brand'],
  attributesToRetrieve: ['title'],
  attributesToHighlight: ['title'],
  unretrievableAttributes: ['internal_notes'],
  semanticSearch: {
    eventSources: ['click'],
  },
  userData: {
    aiProvider: {
      baseUrl: 'https://api.openai.com/v1',
      model: 'gpt-4',
      apiKey: 'sk-123',
    },
  },
  distinct: 1,
  attributeForDistinct: 'brand',
};

function buildProps(overrides: Partial<ComponentProps<typeof SettingsForm>> = {}) {
  const settings = { ...BASE_SETTINGS, ...(overrides.settings || {}) };

  return {
    settings,
    savedSettings: settings,
    onChange: vi.fn(),
    indexName: 'products',
    ...overrides,
  };
}

describe('SettingsForm', () => {
  beforeEach(() => {
    vi.clearAllMocks();

    vi.mocked(useIndexFields).mockReturnValue({
      data: [
        { name: 'title' },
        { name: 'brand' },
        { name: 'price' },
        { name: 'internal_notes' },
      ],
      isLoading: false,
    } as any);

    vi.mocked(useReindex).mockReturnValue({
      mutate: vi.fn(),
      isPending: false,
    } as any);

    vi.mocked(useHealthDetail).mockReturnValue({
      data: {
        status: 'ok',
        capabilities: { vectorSearch: true, vectorSearchLocal: false },
      },
      isLoading: false,
    } as any);
  });

  it('renders all six tabs and defaults to the Search section', () => {
    render(<SettingsForm {...buildProps()} />);

    expect(screen.getByRole('tab', { name: 'Search' })).toHaveAttribute('data-state', 'active');
    expect(screen.getByRole('tab', { name: 'Ranking' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Language & Text' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Facets & Filters' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Display' })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Vector / AI' })).toBeInTheDocument();

    expect(screen.getByText('Configure how search queries are processed')).toBeInTheDocument();
  });

  it('keeps stable tab test ids for smoke and e2e selectors', () => {
    render(<SettingsForm {...buildProps()} />);

    expect(screen.getByTestId('settings-tab-search')).toBeInTheDocument();
    expect(screen.getByTestId('settings-tab-ranking')).toBeInTheDocument();
    expect(screen.getByTestId('settings-tab-language-text')).toBeInTheDocument();
    expect(screen.getByTestId('settings-tab-facets-filters')).toBeInTheDocument();
    expect(screen.getByTestId('settings-tab-display')).toBeInTheDocument();
    expect(screen.getByTestId('settings-tab-vector-ai')).toBeInTheDocument();
  });

  it('switches tabs and renders expected section content', async () => {
    const user = userEvent.setup();
    render(<SettingsForm {...buildProps()} />);

    await user.click(screen.getByRole('tab', { name: 'Ranking' }));
    expect(screen.getByText('Ranking Criteria')).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Language & Text' }));
    expect(screen.getByTestId('query-languages-select')).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Facets & Filters' }));
    expect(screen.getByText('Attributes For Faceting')).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Display' }));
    expect(screen.getByText('Unretrievable Attributes')).toBeInTheDocument();

    await user.click(screen.getByRole('tab', { name: 'Vector / AI' }));
    expect(screen.getByText('AI Provider')).toBeInTheDocument();
    expect(screen.getByPlaceholderText('click, conversion')).toBeInTheDocument();
  });

  it('defaults queryType to prefixLast when unset', () => {
    render(
      <SettingsForm
        {...buildProps({ settings: { ...BASE_SETTINGS, queryType: undefined } })}
      />
    );

    expect(screen.getByRole('combobox')).toHaveValue('prefixLast');
  });

  it('fires onChange when queryType changes', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(<SettingsForm {...buildProps({ onChange })} />);

    const select = screen.getByRole('combobox');
    await user.selectOptions(select, 'prefixNone');

    expect(onChange).toHaveBeenCalledWith({ queryType: 'prefixNone' });
  });

  it('toggles searchable attribute chips on the Search tab', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <SettingsForm
        {...buildProps({
          onChange,
          settings: { ...BASE_SETTINGS, searchableAttributes: ['title'] },
        })}
      />
    );

    await user.click(screen.getByTestId('attr-chip-brand'));

    expect(onChange).toHaveBeenCalledWith({ searchableAttributes: ['title', 'brand'] });
  });

  it('enables distinct via the ranking switch', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <SettingsForm
        {...buildProps({
          onChange,
          settings: { ...BASE_SETTINGS, distinct: false, attributeForDistinct: undefined },
        })}
      />
    );

    await user.click(screen.getByRole('tab', { name: 'Ranking' }));
    await user.click(screen.getByTestId('distinct-enabled-switch'));
    expect(onChange).toHaveBeenCalledWith({ distinct: 1 });
  });

  it('disables distinct and clears attributeForDistinct', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(
      <SettingsForm
        {...buildProps({
          onChange,
          settings: { ...BASE_SETTINGS, distinct: 2, attributeForDistinct: 'brand' },
        })}
      />
    );

    await user.click(screen.getByRole('tab', { name: 'Ranking' }));
    await user.click(screen.getByTestId('distinct-enabled-switch'));
    expect(onChange).toHaveBeenCalledWith({ distinct: false, attributeForDistinct: undefined });
  });

  it('updates queryLanguages from the multi-select control', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(<SettingsForm {...buildProps({ onChange })} />);

    await user.click(screen.getByRole('tab', { name: 'Language & Text' }));

    const select = screen.getByTestId('query-languages-select');
    await user.selectOptions(select, ['en', 'fr']);

    expect(onChange).toHaveBeenCalledWith({ queryLanguages: ['en', 'fr'] });
  });

  it('updates semantic event sources from the Vector / AI tab', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(<SettingsForm {...buildProps({ onChange })} />);

    await user.click(screen.getByRole('tab', { name: 'Vector / AI' }));

    fireEvent.change(screen.getByPlaceholderText('click, conversion'), {
      target: { value: 'click, conversion' },
    });

    expect(onChange).toHaveBeenCalledWith({
      semanticSearch: {
        eventSources: ['click', 'conversion'],
      },
    });

    fireEvent.change(screen.getByPlaceholderText('click, conversion'), {
      target: { value: '' },
    });

    expect(onChange).toHaveBeenCalledWith({ semanticSearch: undefined });
  });

  it('preserves sibling aiProvider fields when editing AI Base URL', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(<SettingsForm {...buildProps({ onChange })} />);

    await user.click(screen.getByRole('tab', { name: 'Vector / AI' }));

    fireEvent.change(screen.getByLabelText('AI Base URL'), {
      target: { value: 'https://api.anthropic.com/v1' },
    });

    expect(onChange).toHaveBeenCalledWith({
      userData: {
        aiProvider: {
          baseUrl: 'https://api.anthropic.com/v1',
          model: 'gpt-4',
          apiKey: 'sk-123',
        },
      },
    });
  });

  it('updates unretrievable attributes from the Display tab', async () => {
    const user = userEvent.setup();
    const onChange = vi.fn();

    render(<SettingsForm {...buildProps({ onChange })} />);

    await user.click(screen.getByRole('tab', { name: 'Display' }));

    fireEvent.change(screen.getByPlaceholderText('internal_notes, supplier_cost'), {
      target: { value: 'internal_notes, supplier_cost' },
    });

    expect(onChange).toHaveBeenCalledWith({
      unretrievableAttributes: ['internal_notes', 'supplier_cost'],
    });
  });

  it('shows reindex warning and calls reindex mutate after confirm', async () => {
    const user = userEvent.setup();
    const reindexMutate = vi.fn();

    vi.mocked(useReindex).mockReturnValue({
      mutate: reindexMutate,
      isPending: false,
    } as any);

    render(
      <SettingsForm
        {...buildProps({
          settings: { ...BASE_SETTINGS, attributesForFaceting: ['brand'] },
          savedSettings: { ...BASE_SETTINGS, attributesForFaceting: ['category'] },
        })}
      />
    );

    await user.click(screen.getByRole('tab', { name: 'Facets & Filters' }));

    expect(screen.getByText('Reindex needed')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: /re-index now/i }));
    expect(screen.getByText('Re-index All Documents')).toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Re-index' }));

    expect(reindexMutate).toHaveBeenCalledWith(undefined, expect.objectContaining({
      onSettled: expect.any(Function),
    }));
  });
});
