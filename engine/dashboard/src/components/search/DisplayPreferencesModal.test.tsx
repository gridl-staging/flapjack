import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import type { DisplayPreferences, FieldInfo } from '@/lib/types';
import { DisplayPreferencesModal } from './DisplayPreferencesModal';

const mockUseIndexFields = vi.hoisted(() => vi.fn());
const mockUseDisplayPreferences = vi.hoisted(() => vi.fn());
const mockSetPreferences = vi.hoisted(() => vi.fn());
const mockClearPreferences = vi.hoisted(() => vi.fn());
const mockAutoDetectPreferences = vi.hoisted(() => vi.fn());

vi.mock('@/hooks/useIndexFields', () => ({
  useIndexFields: mockUseIndexFields,
}));

vi.mock('@/hooks/useDisplayPreferences', () => ({
  useDisplayPreferences: mockUseDisplayPreferences,
  autoDetectPreferences: mockAutoDetectPreferences,
}));

const productsFields: FieldInfo[] = [
  { name: 'name', type: 'text' },
  { name: 'description', type: 'text' },
  { name: 'image_url', type: 'text' },
  { name: 'tags', type: 'text' },
];

const ordersFields: FieldInfo[] = [
  { name: 'order_id', type: 'text' },
  { name: 'status', type: 'text' },
  { name: 'created_at', type: 'text' },
];

const savedProductsPreferences: DisplayPreferences = {
  titleAttribute: 'name',
  subtitleAttribute: 'description',
  imageAttribute: 'image_url',
  tagAttributes: ['tags'],
};

const savedOrdersPreferences: DisplayPreferences = {
  titleAttribute: 'order_id',
  subtitleAttribute: null,
  imageAttribute: null,
  tagAttributes: ['status'],
};

type PreferenceMap = Record<string, DisplayPreferences | null>;

const preferencesByIndex: PreferenceMap = {};

function setMockStore(map: PreferenceMap) {
  Object.keys(preferencesByIndex).forEach((key) => delete preferencesByIndex[key]);
  Object.assign(preferencesByIndex, map);
}

function setupHookMocks() {
  mockUseIndexFields.mockImplementation((indexName: string) => ({
    data: indexName === 'orders' ? ordersFields : productsFields,
    isLoading: false,
  }));

  mockUseDisplayPreferences.mockImplementation((indexName: string) => ({
    preferences: preferencesByIndex[indexName] ?? null,
    setPreferences: mockSetPreferences,
    clearPreferences: mockClearPreferences,
  }));

  mockSetPreferences.mockImplementation((indexName: string, draft: DisplayPreferences) => {
    preferencesByIndex[indexName] = draft;
  });

  mockClearPreferences.mockImplementation((indexName: string) => {
    delete preferencesByIndex[indexName];
  });

  mockAutoDetectPreferences.mockReturnValue({
    titleAttribute: 'name',
    subtitleAttribute: null,
    imageAttribute: 'image_url',
    tagAttributes: ['tags'],
  });
}

function expectEmptyDraft() {
  expect(screen.getByLabelText('Title field')).toHaveValue('');
  expect(screen.getByLabelText('Subtitle field')).toHaveValue('');
  expect(screen.getByLabelText('Image field')).toHaveValue('');
  expect(screen.getByTestId('attr-chip-tags')).not.toHaveClass('bg-primary');
}

describe('DisplayPreferencesModal shell contract', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setMockStore({});
    setupHookMocks();
  });

  it('renders title/helper text and field controls when open', () => {
    render(<DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="products" />);

    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Display Preferences' })).toBeInTheDocument();
    expect(
      screen.getByText('Configure browse card fields for this index. Changes are saved per index.')
    ).toBeInTheDocument();
    expect(screen.getByLabelText('Title field')).toBeInTheDocument();
    expect(screen.getByLabelText('Subtitle field')).toBeInTheDocument();
    expect(screen.getByLabelText('Image field')).toBeInTheDocument();
    expect(screen.getByText('Tag fields')).toBeInTheDocument();
  });

  it('queries index fields with useIndexFields(indexName, open)', () => {
    const { rerender } = render(
      <DisplayPreferencesModal open={false} onOpenChange={vi.fn()} indexName="products" />
    );

    expect(mockUseIndexFields).toHaveBeenLastCalledWith('products', false);

    rerender(<DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="orders" />);

    expect(mockUseIndexFields).toHaveBeenLastCalledWith('orders', true);
  });

  it('hydrates draft from saved preferences when available', () => {
    setMockStore({ products: savedProductsPreferences });

    render(<DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="products" />);

    expect(screen.getByLabelText('Title field')).toHaveValue('name');
    expect(screen.getByLabelText('Subtitle field')).toHaveValue('description');
    expect(screen.getByLabelText('Image field')).toHaveValue('image_url');
    expect(screen.getByTestId('attr-chip-tags')).toHaveClass('bg-primary');
  });

  it('hydrates draft to empty baseline when no preferences exist', () => {
    render(<DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="products" />);

    expectEmptyDraft();
  });

  it('rehydrates draft when modal reopens or indexName changes', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    setMockStore({
      products: savedProductsPreferences,
      orders: savedOrdersPreferences,
    });

    const { rerender } = render(
      <DisplayPreferencesModal open={true} onOpenChange={onOpenChange} indexName="products" />
    );

    await user.selectOptions(screen.getByLabelText('Title field'), 'description');
    expect(screen.getByLabelText('Title field')).toHaveValue('description');

    rerender(<DisplayPreferencesModal open={false} onOpenChange={onOpenChange} indexName="products" />);
    rerender(<DisplayPreferencesModal open={true} onOpenChange={onOpenChange} indexName="products" />);

    expect(screen.getByLabelText('Title field')).toHaveValue('name');

    rerender(<DisplayPreferencesModal open={true} onOpenChange={onOpenChange} indexName="orders" />);

    expect(screen.getByLabelText('Title field')).toHaveValue('order_id');
    expect(screen.getByTestId('attr-chip-status')).toHaveClass('bg-primary');
  });

  it('surfaces index-field loading failures instead of presenting an empty editable form', () => {
    mockUseIndexFields.mockReturnValue({
      data: [],
      isLoading: false,
      error: new Error('schema lookup failed'),
    });

    render(<DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="products" />);

    expect(screen.getByRole('alert')).toHaveTextContent(
      'Unable to load index fields. Try reopening the dialog.'
    );
    expect(screen.getByLabelText('Title field')).toBeDisabled();
    expect(screen.getByLabelText('Subtitle field')).toBeDisabled();
    expect(screen.getByLabelText('Image field')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Auto-detect' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
  });
});

describe('DisplayPreferencesModal interaction contract', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setMockStore({ products: savedProductsPreferences, orders: savedOrdersPreferences });
    setupHookMocks();
  });

  it('applies auto-detect to local draft only', async () => {
    const user = userEvent.setup();
    mockAutoDetectPreferences.mockReturnValue({
      titleAttribute: 'description',
      subtitleAttribute: null,
      imageAttribute: null,
      tagAttributes: ['tags'],
    });

    render(<DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="products" />);

    await user.click(screen.getByRole('button', { name: 'Auto-detect' }));

    expect(mockAutoDetectPreferences).toHaveBeenCalledWith(productsFields);
    expect(screen.getByLabelText('Title field')).toHaveValue('description');
    expect(mockSetPreferences).not.toHaveBeenCalled();
  });

  it('discards unsaved edits after close and reopen', async () => {
    const user = userEvent.setup();
    const { rerender } = render(
      <DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="products" />
    );

    await user.selectOptions(screen.getByLabelText('Title field'), 'description');
    expect(screen.getByLabelText('Title field')).toHaveValue('description');

    rerender(<DisplayPreferencesModal open={false} onOpenChange={vi.fn()} indexName="products" />);
    rerender(<DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="products" />);

    expect(screen.getByLabelText('Title field')).toHaveValue('name');
  });

  it('saves draft preferences and closes the dialog', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();

    render(<DisplayPreferencesModal open={true} onOpenChange={onOpenChange} indexName="products" />);

    await user.selectOptions(screen.getByLabelText('Subtitle field'), 'name');
    await user.selectOptions(screen.getByLabelText('Image field'), 'description');
    await user.click(screen.getByRole('button', { name: 'Save' }));

    expect(mockSetPreferences).toHaveBeenCalledWith('products', {
      titleAttribute: 'name',
      subtitleAttribute: 'name',
      imageAttribute: 'description',
      tagAttributes: ['tags'],
    });
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('clears only current index and resets draft to empty baseline', async () => {
    const user = userEvent.setup();

    render(<DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="products" />);

    await user.click(screen.getByRole('button', { name: 'Clear' }));

    expect(mockClearPreferences).toHaveBeenCalledWith('products');
    expect(preferencesByIndex.orders).toEqual(savedOrdersPreferences);
    expectEmptyDraft();
  });

  it('offers explicit None choices for title/subtitle/image', async () => {
    const user = userEvent.setup();

    render(<DisplayPreferencesModal open={true} onOpenChange={vi.fn()} indexName="products" />);

    await user.selectOptions(screen.getByLabelText('Title field'), '');
    await user.selectOptions(screen.getByLabelText('Subtitle field'), '');
    await user.selectOptions(screen.getByLabelText('Image field'), '');

    expect(screen.getByLabelText('Title field')).toHaveValue('');
    expect(screen.getByLabelText('Subtitle field')).toHaveValue('');
    expect(screen.getByLabelText('Image field')).toHaveValue('');
    expect(screen.getAllByRole('option', { name: 'None' })).toHaveLength(3);
  });
});
